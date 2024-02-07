# -*- coding: utf-8 -*-
"""Provides functionality for updating the GTFS schedule, based on AVL data."""

##### IMPORTS #####

# Built-Ins
import logging
import pathlib
import re
import zipfile

# Third Party
import numpy as np
import pandas as pd
from caf.toolkit import config_base, log_helpers
from pydantic import dataclasses, types

# Local Imports
import bodse
from bodse import utils
from bodse.avl import database, gtfs

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
_GTFS_FILE_NAMES = {"stops": "stops.txt", "stop_times": "stop_times.txt"}
_GTFS_FILE_COLUMNS: dict[str, dict[str, type]] = {
    "stops": {
        "stop_id": str,
        "stop_code": str,
        "stop_name": str,
        "stop_lat": float,
        "stop_lon": float,
        "wheelchair_boarding": int,
        "location_type": float,
        "parent_station": str,
        "platform_code": str,
    },
    "stop_times": {
        "trip_id": str,
        "arrival_time": str,
        "departure_time": str,
        "stop_id": str,
        "stop_sequence": int,
        "stop_headsign": str,
        "pickup_type": int,
        "drop_off_type": int,
        "shape_dist_traveled": float,
        "timepoint": int,
        "stop_direction_name": str,
    },
}

##### CLASSES & FUNCTIONS #####


@dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class _GTFSStops:
    """GTFS schedule stops and stop time data."""

    stops: pd.DataFrame
    stop_times: pd.DataFrame


def _load_stop_times(path: pathlib.Path) -> _GTFSStops:
    """Load GTFS stop locations and stop times tables.

    Raises
    ------
    FileNotFoundError
        If tables are missing from the GTFS file.
    """
    LOG.info("Loading stop times and locations from GTFS file: %s", path.name)
    missing = []
    data = {}

    with zipfile.ZipFile(path) as gtfs_zip:
        for name, filename in _GTFS_FILE_NAMES.items():
            try:
                with gtfs_zip.open(filename) as file:
                    data[name] = pd.read_csv(file, dtype=_GTFS_FILE_COLUMNS[name])
            except KeyError:
                missing.append(name)

    if len(missing) > 0:
        raise FileNotFoundError(
            f"{len(missing)} files missing from GTFS file ({path.name}): {missing}"
        )

    return _GTFSStops(data["stops"], data["stop_times"])


def _translate_stop_coordinates(stops: pd.DataFrame) -> pd.DataFrame:
    """Translate stops longitude and latitude coordinates to easting / northing.

    Parameters
    ----------
    stops : pd.DataFrame
        DataFrame containing stop locations, requires columns
        'stop_lat' and 'stop_lon' which contain the latitude
        and longitude values.

    Returns
    -------
    pd.DataFrame
        `stops` with additional 'stop_east' and 'stop_north' columns
        appended, containing the easting and northing values. Any
        translation errors will cause the new columns to contain NaNs.
    """
    eastnorth = pd.DataFrame(
        stops[["stop_lat", "stop_lon"]]
        .apply(lambda x: gtfs.lat_lon_to_bng(x["stop_lat"], x["stop_lon"]), axis=1)
        .tolist(),
        columns=["stop_east", "stop_north"],
    )

    return pd.concat([stops, eastnorth], axis=1)


def _time_seconds(time_str: str) -> int:
    """Parse time string and convert to seconds.

    Parameters
    ----------
    time_str : str
        Expected format 'HH:MM:SS'.

    Raises
    ------
    ValueError
        If `time_str` isn't in the expected format.
    """
    match = re.match(r"^(\d{,2}):(\d{,2}):(\d{,2})$", time_str.strip())
    if match is None:
        raise ValueError(f"time format should be 'HH:MM:SS' not '{time_str}'")

    groups = [int(i) for i in match.groups()]

    return groups[0] * 3600 + groups[1] * 60 + groups[2]


def distance(x1: pd.Series, x2: pd.Series, y1: pd.Series, y2: pd.Series) -> pd.Series:
    """Calculate crow-fly distance between 2 sets of coordinates."""
    return np.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def extract_stop_times_locations(gtfs_path: pathlib.Path) -> pd.DataFrame:
    """Extract stop times and locations (easting / northing) from GTFS file.

    Translates the stop longitude and latitudes to British National Grid
    (BNG) easting and northing and appends them to the stop times table.
    Also, calculates the arrival and departure time in seconds and appends
    two columns containing those values.

    Returns
    -------
    pd.DataFrame
        Stop times data as defined in
        [GTFS spec](https://gtfs.org/schedule/reference/#stop_timestxt)
        with 'stop_east', 'stop_north', 'arrival_secs', 'departure_secs'
        and 'stop_distance_metres' columns appended.
    """
    data = _load_stop_times(gtfs_path)
    stops = _translate_stop_coordinates(data.stops)

    stop_times = data.stop_times.merge(
        stops[["stop_id", "stop_east", "stop_north"]],
        how="left",
        on="stop_id",
        validate="m:1",
        indicator=True,
    )
    utils.merge_indicator_check(stop_times, "stop times", "stop locations")
    stop_times.drop(columns="_merge", inplace=True)

    # Calculate arrival and departure time in seconds, GTFS provides arrival
    # and departure times > 24:00:00 for routes which run across midnight
    stop_times.loc[:, "arrival_secs"] = stop_times["arrival_time"].apply(_time_seconds)
    stop_times.loc[:, "departure_secs"] = stop_times["departure_time"].apply(_time_seconds)

    # Calculate the distance to the previous stop
    stop_times = stop_times.set_index(["trip_id", "stop_sequence"], verify_integrity=True)

    previous = stop_times[["stop_east", "stop_north"]].rename(
        columns={f"stop_{i}": f"prev_{i}" for i in ("east", "north")}
    )
    previous.index = pd.MultiIndex.from_arrays(
        [previous.index.get_level_values(0), previous.index.get_level_values(1) + 1]
    )

    stop_times = stop_times.merge(
        previous, how="left", left_index=True, right_index=True, validate="1:1"
    )

    stop_times["stop_distance_metres"] = distance(
        stop_times["stop_east"],
        stop_times["prev_east"],
        stop_times["stop_north"],
        stop_times["prev_north"],
    )
    LOG.debug("Done loading and pre-processing GTFS stop times")

    return stop_times.reset_index().drop(columns=["prev_east", "prev_north"])


def calculate_stop_times_delays(
    db_path: pathlib.Path, stop_times: pd.DataFrame, output_folder: pathlib.Path
) -> pd.DataFrame:
    gtfs_db = database.GTFSRTDatabase(db_path)

    with gtfs_db.connect() as conn:
        gtfs_db.insert_stop_times(conn, stop_times)
        conn.commit()

        # Remove incorrect GPS data based on speeds, distances and delays
        # then recalculate speeds with remaining vehicle positions
        gtfs_db.filter_vehicle_positions(conn)
        gtfs_db.fill_vehicle_speeds_table(conn)
        conn.commit()

        gtfs_db.calculate_stop_delays(conn)
        conn.commit()

        summary = gtfs_db.get_stop_delays_summary(conn)
        delays = gtfs_db.get_average_stop_delays(conn)

    out_file = output_folder / "stop_delays_summary.csv"
    summary.to_csv(out_file, index=False)
    LOG.info("Written: %s", out_file)

    out_file = output_folder / "stop_delays.csv"
    delays.to_csv(out_file, index=False)
    LOG.info("Written: %s", out_file)

    return delays


def calculate_observed_stop_times(
    stop_times: pd.DataFrame, delays: pd.DataFrame, output_folder: pathlib.Path
) -> pd.DataFrame:
    index = ["trip_id", "stop_sequence"]
    stop_times = stop_times.set_index(index)
    delays = delays.rename(columns={"current_stop_sequence": "stop_sequence"})

    stop_times = stop_times.merge(
        delays.set_index(index),
        left_index=True,
        right_index=True,
        how="outer",
        validate="1:1",
        indicator=True,
    )

    try:
        utils.merge_indicator_check(stop_times, "stop_times", "avl_delays")
    except ValueError as err:
        LOG.warning("Merging AVL delays with stop times data:\n%s", str(err))

    stop_times.to_csv(output_folder / "avl_updated_stop_times.csv")

    raise NotImplementedError(
        "WIP infill delays where missing and make sure final stop times are"
        " sensible i.e. delay reduces gradually over  a number of stops if"
        " possible and time increases with new stops"
    )
    return stop_times


class AdjustConfig(config_base.BaseConfig):
    """Parameters for the AVL adjustment process, requires AVL downloader database."""

    avl_database: types.FilePath
    gtfs_file: types.FilePath
    output_folder: types.DirectoryPath


def main(parameters: AdjustConfig, log_console: bool = True) -> None:
    tool_details = log_helpers.ToolDetails(bodse.__package__, bodse.__version__)

    output_folder = parameters.output_folder
    log_file = output_folder / "AVL_adjust.log"

    with log_helpers.LogHelper(
        bodse.__package__, tool_details, log_file=log_file, console=log_console
    ):
        LOG.debug("AVL downloader parameters:\n%s", parameters.to_yaml())
        LOG.info("Outputs saved to: %s", output_folder)

        stop_times = extract_stop_times_locations(parameters.gtfs_file)
        delays = calculate_stop_times_delays(
            parameters.avl_database, stop_times, output_folder
        )
        calculate_observed_stop_times(stop_times, delays, output_folder)
