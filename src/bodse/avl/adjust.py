# -*- coding: utf-8 -*-
"""Provides functionality for updating the GTFS schedule, based on AVL data."""

##### IMPORTS #####

# Built-Ins
import io
import itertools
import logging
import pathlib
import re
import zipfile
from typing import Optional

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
_STOP_TIMES_INDICATOR_COLUMN = "dataset"

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
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Connect to AVL SQLite database and calculate and extract stop delays.

    Parameters
    ----------
    db_path : pathlib.Path
        Path to SQlite database containing AVL data.
    stop_times : pd.DataFrame
        Scheduled stop times from GTFS file.
    output_folder : pathlib.Path
        Folder to save summary outputs to.

    Returns
    -------
    pd.DataFrame
        Average stop delays with columns: 'trip_id', 'current_stop_sequence',
        'sample_size' and delay columns.
    dict[str, str]
        Lookup from average type (min, max, mean) to
        the delay column name.
    """
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
        delays, delay_columns = gtfs_db.get_average_stop_delays(conn)

    out_file = output_folder / "stop_delays_summary.csv"
    summary.to_csv(out_file, index=False)
    LOG.info("Written: %s", out_file)

    out_file = output_folder / "stop_delays.csv"
    delays.to_csv(out_file, index=False)
    LOG.info("Written: %s", out_file)

    return delays, delay_columns


def _seconds_to_time(value: pd.Series) -> pd.Series:
    if not isinstance(value, pd.Series):
        raise TypeError(f"value should be a Series not {type(value)}")

    hours, secs = divmod(value, 3600)
    mins = secs / 60

    hour_str = hours.astype(str).str.zfill(2)
    min_str = mins.round(0).astype(int).astype(str).str.zfill(2)

    return hour_str + ":" + min_str


def _calculate_delayed_times(
    stop_times: pd.DataFrame, delay_columns: dict[str, str]
) -> dict[str, tuple[str, str]]:
    LOG.info("Calculating arrival / departure times, this may take some time")
    time_columns: dict[str, tuple[str, str]] = {}
    for name, column in delay_columns.items():
        col_names = []
        for ad in ("arrival", "departure"):
            nans = stop_times[column].isna().sum()
            LOG.info(
                "Found %s (%s) stop times without calculated %s %s delays,"
                " these will not be updated from the scheduled stop times",
                f"{nans:,}",
                f"{nans / len(stop_times):.1%}",
                name,
                ad,
            )
            delayed_secs = stop_times[f"{ad}_secs"] + stop_times[column].fillna(0)

            col_names.append(f"{name}_{ad}_time")
            stop_times[col_names[-1]] = _seconds_to_time(delayed_secs)

        time_columns[name] = tuple(col_names)  # type: ignore

    return time_columns


def calculate_observed_stop_times(
    stop_times: pd.DataFrame,
    delays: pd.DataFrame,
    delay_columns: dict[str, str],
    output_path: Optional[pathlib.Path] = None,
) -> tuple[pd.DataFrame, dict[str, tuple[str, str]]]:
    """Calculate observed stop times using average `delays`.

    Estimared stop times area calculated for each of the
    `delay_columns`.

    Parameters
    ----------
    stop_times : pd.DataFrame
        Scheduled stop times from GTFS.
    delays : pd.DataFrame
        Average delays from `calculate_stop_times_delays`.
    delay_columns : dict[str, str]
        Names of delay types and column name.
    output_path : Optional[pathlib.Path], optional
        CSV (or BZ2) path to save output `stop_times` with delays
        to, if not given stop times aren't output to file.

    Returns
    -------
    pd.DataFrame
        Stop times data containing the usual columns plus estimated stop
        times columns for each delay type and column defining what dataset
        the rows were found in.
    dict[str, tuple[str, str]]
        Delay types (key) and the estimated time columns
        (arrival_time, departure_time).
    """
    index = ["trip_id", "stop_sequence"]
    stop_times = stop_times.set_index(index)
    delays = delays.rename(columns={"current_stop_sequence": "stop_sequence"})

    stop_times = stop_times.merge(
        delays.set_index(index),
        left_index=True,
        right_index=True,
        how="outer",
        validate="1:1",
        indicator=_STOP_TIMES_INDICATOR_COLUMN,
    )

    try:
        utils.merge_indicator_check(
            stop_times, "stop_times", "avl_delays", indicator_column="dataset"
        )
    except ValueError as err:
        LOG.warning("Merging AVL delays with stop times data:\n%s", str(err))

    stop_times[_STOP_TIMES_INDICATOR_COLUMN] = stop_times[
        _STOP_TIMES_INDICATOR_COLUMN
    ].replace({"left_only": "stop_times_only", "right_only": "avl_delays_only"})

    columns = list(delay_columns.values())
    LOG.info(
        "Infilling stop time columns with linear interpolation (or extrapolation): %s",
        ", ".join(columns),
    )
    stop_times[columns] = stop_times.groupby(level=0)[columns].transform(
        lambda x: x.interpolate()
    )

    time_columns = _calculate_delayed_times(stop_times, delay_columns)

    if output_path is not None:
        LOG.info("Writing stop times, with delays, to CSV")
        stop_times.to_csv(output_path)
        LOG.info("Saved stop times to %s", output_path)

    original_time_columns = ["arrival_time", "departure_time"]
    not_needed = list(stop_times.index.names) + original_time_columns
    return (
        stop_times.loc[
            :,
            [i for i in _GTFS_FILE_COLUMNS["stop_times"] if i not in not_needed]
            + list(itertools.chain.from_iterable(time_columns.values()))
            + [_STOP_TIMES_INDICATOR_COLUMN],
        ],
        time_columns,
    )


class AdjustConfig(config_base.BaseConfig):
    """Parameters for the AVL adjustment process, requires AVL downloader database."""

    avl_database: types.FilePath
    gtfs_file: types.FilePath
    output_folder: types.DirectoryPath


def _write_gtfs_zip(
    old_gtfs_path: pathlib.Path, new_gtfs_path: pathlib.Path, stop_times: pd.DataFrame
) -> pathlib.Path:
    out_columns = list(_GTFS_FILE_COLUMNS["stop_times"].keys())
    zipfile_name = "stop_times.txt"

    csv_str = io.StringIO()
    stop_times.reset_index()[out_columns].to_csv(csv_str, index=False)

    with zipfile.ZipFile(
        new_gtfs_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as new_gtfs:
        new_gtfs.writestr(zipfile_name, csv_str.getvalue())
        csv_str.close()

        with zipfile.ZipFile(old_gtfs_path, "r") as old_gtfs:
            for filename in old_gtfs.namelist():
                if filename == zipfile_name:
                    continue

                new_gtfs.writestr(filename, old_gtfs.read(filename))

    return new_gtfs_path


def output_delayed_gtfs(
    gtfs_path: pathlib.Path,
    stop_times: pd.DataFrame,
    time_columns: dict[str, tuple[str, str]],
    output_base: pathlib.Path,
) -> dict[str, pathlib.Path]:
    """Write average delay estimates to different GTFS files.

    Creates one new GTFS timetable including the estimated delay
    for each of the `time_columns`.

    Parameters
    ----------
    gtfs_path : pathlib.Path
        Path to the original scheduled GTFS timetable.
    stop_times : pd.DataFrame
        Stop time data with delays.
    time_columns : dict[str, tuple[str, str]]
        Delay type (key) with the names of the arrival and departure
        time columns (arrival_time, departure_time).
    output_base : pathlib.Path
        Base path for the output GTFS file.

    Returns
    -------
    dict[str, pathlib.Path]
        Paths for the GTFS files for each of the delay types.

    Raises
    ------
    FileNotFoundError
        If the original `gtfs_path` file doesn't exist.
    """
    if not gtfs_path.is_file():
        raise FileNotFoundError(f"GTFS file doesn't exist: {gtfs_path}")

    output_base.parent.mkdir(exist_ok=True)
    gtfs_paths = {}
    LOG.info("Writing AVL adjusted GTFS files to: %s", output_base.parent)

    for name, columns in time_columns.items():
        LOG.info("Writing %s delays GTFS", name)
        path = output_base.with_name(output_base.stem + f"-{name}.zip")

        try:
            path = _write_gtfs_zip(
                gtfs_path,
                path,
                stop_times=stop_times.rename(
                    columns={columns[0]: "arrival_time", columns[1]: "departure_time"}
                ),
            )

        except Exception as exc:
            LOG.error(
                "Failed updating stop times, so removing partial GTFS (%s). %s: %s",
                path.name,
                exc.__class__.__name__,
                exc,
            )
            # Delete the GTFS copy if there was an error in updating stop times
            path.unlink(missing_ok=True)
            raise

        gtfs_paths[name] = path
        LOG.info("Written: %s", path.name)

    return gtfs_paths


def main(parameters: AdjustConfig, log_console: bool = True) -> None:
    """Main function for running AVL adjustment process."""
    tool_details = log_helpers.ToolDetails(bodse.__package__, bodse.__version__)

    output_folder = parameters.output_folder
    log_file = output_folder / "AVL_adjust.log"

    with log_helpers.LogHelper(
        bodse.__package__, tool_details, log_file=log_file, console=log_console
    ):
        LOG.debug("AVL downloader parameters:\n%s", parameters.to_yaml())
        LOG.info("Outputs saved to: %s", output_folder)

        stop_times = extract_stop_times_locations(parameters.gtfs_file)
        delays, delay_columns = calculate_stop_times_delays(
            parameters.avl_database, stop_times, output_folder
        )
        delayed_stop_times, time_columns = calculate_observed_stop_times(
            stop_times,
            delays,
            delay_columns,
            output_path=output_folder / "avl_updated_stop_times.csv.bz2",
        )
        del stop_times, delays

        output_delayed_gtfs(
            parameters.gtfs_file,
            delayed_stop_times,
            time_columns,
            output_folder / f"Delayed GTFS/{parameters.gtfs_file.stem}",
        )
