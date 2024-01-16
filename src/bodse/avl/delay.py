# -*- coding: utf-8 -*-
"""Provides functionality for updating the GTFS schedule, based on AVL data."""

##### IMPORTS #####

# Built-Ins
import dataclasses
import logging
import pathlib
import zipfile

# Third Party
import pandas as pd

# Local Imports
from bodse import utils
from bodse.avl import gtfs

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


@dataclasses.dataclass
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


def extract_stop_times_locations(gtfs_path: pathlib.Path) -> pd.DataFrame:
    """Extract stop times and locations (easting / northing) from GTFS file.

    Translates the stop longitude and latitudes to British National Grid
    (BNG) easting and northing and appends them to the stop times table.

    Returns
    -------
    pd.DataFrame
        Stop times data as defined in
        [GTFS spec](https://gtfs.org/schedule/reference/#stop_timestxt)
        with 'stop_east' and 'stop_north' columns appended containing
        the BNG easting and northing values respectively.
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

    return stop_times
