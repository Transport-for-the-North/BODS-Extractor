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


##### CLASSES #####
@dataclasses.dataclass
class DownloadTime:
    minutes: types.conint(ge=0) = 0
    hours: types.conint(ge=0) = 0
    days: types.conint(ge=0) = 0
    wait_minutes: types.conint(ge=1) = 1

    _MINIMUM_DOWNLOAD_MINUTES = 10

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

        return values


class DownloaderConfig(config_base.BaseConfig):
    output_folder: types.DirectoryPath
    # TODO(MB) Allow this to be file path or API auth class
    api_auth_config: types.FilePath
    download_time: DownloadTime


##### FUNCTIONS #####
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
        auth=auth
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


def main(params: DownloaderConfig):
    tool_details = log_helpers.ToolDetails(bodse.__package__, bodse.__version__)

    output_folder = params.output_folder / f"BODSE AVL Outputs - {dt.date.today():%Y-%m-%d}"
    output_folder.mkdir(exist_ok=True, parents=True)
    log_file = output_folder / "AVL_downloader.log"

    with log_helpers.LogHelper(bodse.__package__, tool_details, log_file=log_file):
        LOG.debug("AVL downloader parameters:\n%s", params.to_yaml())
        LOG.info("Outputs saved to: %s", output_folder)

        bods_auth = request.APIAuth.load_yaml(params.api_auth_config)
        LOG.info("Accessing BODS using user account: %s", bods_auth.name)

        gtfs_db = database.GTFSRTDatabase(output_folder / "gtfs-rt.sqlite")

        for _ in _download_iterator(params.download_time):
            # TODO Look into using concurrent.futures.ThreadPoolExecutor to perform
            # download and insert while the iterator is waiting
            try:
                feed = gtfs.download(bods_auth)
            except requests.HTTPError as exc:
                LOG.error("HTTP error when downloading AVL feed: %s", exc)
                continue

            with gtfs_db.connect() as conn:
                gtfs_db.insert_feed(conn, feed)
                conn.commit()

        LOG.info("Finished downloading AVL data")

        LOG.info("Tidying up AVL database tables")
        with gtfs_db.connect() as conn:
            gtfs_db.delete_duplicate_positions(conn)
            conn.commit()

            gtfs_db.fill_vehicle_speeds_table(conn)
            conn.commit()
