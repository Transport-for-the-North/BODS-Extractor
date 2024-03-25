# -*- coding: utf-8 -*-
"""Functionality for downloading and processing BODS AVL data."""

##### IMPORTS #####

# Built-Ins
import datetime as dt
import logging
import pathlib
import time
from typing import Iterator
from urllib import parse
import warnings

# Third Party
import pydantic
import requests
from caf.toolkit import config_base, log_helpers
from pydantic import dataclasses, types

# Local Imports
import bodse
from bodse import request, utils
from bodse.avl import database, gtfs, raw

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
CONFIG_PATH = pathlib.Path("avl_downloader.yml")

##### CLASSES & FUNCTIONS #####

# Ignore private attr warnings in the DownloadTime class
warnings.filterwarnings(
    "ignore",
    "fields may not start with an underscore, ignoring",
    RuntimeWarning,
    module=__name__,
)


@dataclasses.dataclass
class DownloadTime:
    """AVL download duration."""

    minutes: int = pydantic.Field(0, ge=0)
    hours: int = pydantic.Field(0, ge=0)
    days: int = pydantic.Field(0, ge=0)
    wait_minutes: int = pydantic.Field(1, ge=1)

    _MINIMUM_DOWNLOAD_MINUTES = 10
    _attribute_names: tuple[str, ...] = ("days", "hours", "minutes", "wait_minutes")

    @pydantic.root_validator
    def _check_time(cls, values: dict) -> dict:
        # pylint: disable=no-self-argument
        if values["minutes"] + values["hours"] + values["days"] <= 0:
            raise ValueError(
                "at least one of minutes, hours or days must be a positive integer"
            )

        if (
            values["hours"] + values["days"] <= 0
            and values["minutes"] < cls._MINIMUM_DOWNLOAD_MINUTES
        ):
            raise ValueError(
                f"total time should be greater than {cls._MINIMUM_DOWNLOAD_MINUTES} "
                f"minutes, not {values['minutes']}"
            )

        # Convert to minimum values for each attribute i.e. 60 mins should be 1 hour
        if values["minutes"] >= 60:
            extra_hours, values["minutes"] = divmod(values["minutes"], 60)
            values["hours"] += extra_hours

        if values["hours"] >= 24:
            extra_days, values["hours"] = divmod(values["hours"], 24)
            values["days"] += extra_days

        return values

    def __str__(self) -> str:
        params = "".join(
            f"\n  {i:<12.12} = {getattr(self, i)!s:>5}" for i in self._attribute_names
        )

        return f"DownloadTime({params}\n)"

    def asdict(self) -> dict[str, int]:
        return {i: getattr(self, i) for i in self._attribute_names}


class DownloaderConfig(config_base.BaseConfig):
    """Config for downloading AVL data."""

    output_folder: types.DirectoryPath
    # TODO(MB) Allow this to be file path or API auth class
    api_auth_config: types.FilePath
    download_time: DownloadTime


def store_raw(avl_database: database.RawAVLDatabase, auth: request.APIAuth):
    """Download Siri XML data from BODS and store in `avl_database`.

    Parameters
    ----------
    avl_database : database.RawAVLDatabase
        Database to store the raw AVL data in.
    auth : request.APIAuth
        BODS account username and password.
    """
    url = parse.urljoin(request.BODS_API_BASE_URL, raw.API_ENDPOINT)
    xml_response = request.get_str(
        url=url,
        auth=auth,
        # params={"boundingBox": "51.401,51.509,0.01,0.201"},
    )
    LOG.info("Downloaded raw XML data from %s", url)

    siri, metadata = raw.parse_metadata(xml_response)

    with avl_database.connect() as connection:
        meta_id = avl_database.insert_avl_metadata(connection, metadata)

        for activity in raw.iterate_activities(siri):
            avl_database.insert_vehicle_activity(connection, activity, meta_id)

        connection.commit()


def _download_iterator(timings: DownloadTime) -> Iterator[int]:
    """Yield every `wait_time` seconds until end time.

    Parameters
    ----------
    timings : DownloadTime
        Parameters for determining download schedule.

    Yields
    ------
    int
        Iterator count.
    """
    start = dt.datetime.now()
    end = start + dt.timedelta(days=timings.days, hours=timings.hours, minutes=timings.minutes)
    LOG.info(
        "Starting continuous downloads approximately "
        "every %s minute(s), which will finish at %s",
        timings.wait_minutes,
        f"{end:%c}",
    )

    count = 0
    while True:
        count += 1
        prev_time = dt.datetime.now()
        yield count

        if dt.datetime.now() >= end:
            LOG.info(
                "%s complete, finished continuous downloads after %s",
                count,
                utils.readable_timedelta(dt.datetime.now() - start),
            )
            break

        time_taken = dt.datetime.now() - prev_time
        remaining_wait = (timings.wait_minutes * 60) - time_taken.total_seconds()
        message_args = [
            count,
            utils.readable_timedelta(time_taken),
            utils.readable_timedelta(dt.timedelta(seconds=abs(remaining_wait))),
            utils.readable_timedelta(end - dt.datetime.now()),
        ]

        if remaining_wait < 0:
            LOG.info(
                "%s complete in %s, overran by %s so not waiting. Total time remaining %s",
                *message_args,
            )
        else:
            LOG.info("%s complete in %s, waiting %s. Total time remaining %s", *message_args)
            time.sleep(abs(remaining_wait))


def download(
    gtfs_db: database.GTFSRTDatabase, download_time: DownloadTime, bods_auth: request.APIAuth
) -> None:
    """Download AVL GTFS-rt feeds and save in database.

    Parameters
    ----------
    gtfs_db : database.GTFSRTDatabase
        Database configuration to save data to.
    download_time : DownloadTime
        Parameters defining how long to download data for.
    bods_auth : request.APIAuth
        User and password for connecting to BODS API.
    """
    LOG.info("Started downloading AVL data to %s", gtfs_db.path)
    LOG.debug("Accessing BODS using user account: %s", bods_auth.name)

    for i in _download_iterator(download_time):
        # TODO Look into using concurrent.futures.ThreadPoolExecutor to perform
        # download and insert while the iterator is waiting
        try:
            feed = gtfs.download(bods_auth)
        except requests.RequestException as exc:
            LOG.error("Error downloading AVL feed %s, %s: %s", i, exc.__class__.__name__, exc)
            continue

        try:
            with gtfs_db.connect() as conn:
                gtfs_db.insert_feed(conn, feed)
                conn.commit()

        except Exception as exc:
            LOG.error(
                "Error inserting the AVL feed data (%s) into the database, %s: %s",
                i,
                exc.__class__.__name__,
                exc,
            )
            continue

    LOG.info("Finished downloading AVL data")


def main(output_folder: pathlib.Path, download_time: DownloadTime, bods_auth: request.APIAuth):
    """Main function for running AVL downloader."""
    tool_details = log_helpers.ToolDetails(bodse.__package__, bodse.__version__)

    output_folder = output_folder / f"BODSE AVL Outputs - {dt.date.today():%Y-%m-%d}"
    output_folder.mkdir(exist_ok=True, parents=True)
    log_file = output_folder / "AVL_downloader.log"

    with log_helpers.LogHelper(bodse.__package__, tool_details, log_file=log_file):
        LOG.debug("AVL downloader parameters:\n%s", download_time)
        LOG.info("Outputs saved to: %s", output_folder)

        gtfs_db = database.GTFSRTDatabase(output_folder / "gtfs-rt.sqlite")
        download(gtfs_db, download_time, bods_auth)

        LOG.info("Tidying up AVL database tables")
        with gtfs_db.connect() as conn:
            gtfs_db.delete_duplicate_positions(conn)
            conn.commit()

            gtfs_db.fill_vehicle_speeds_table(conn)
            conn.commit()
