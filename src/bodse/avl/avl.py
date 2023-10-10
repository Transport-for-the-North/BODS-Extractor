# -*- coding: utf-8 -*-
"""Functionality for downloading and processing BODS AVL data."""

##### IMPORTS #####
# Standard imports
import datetime as dt
import logging
import pathlib
import time
from urllib import parse

# Third party imports
from caf.toolkit import log_helpers

# Local imports
from bodse import request
import bodse
from bodse.avl import gtfs, raw, database


##### CONSTANTS #####
LOG = logging.getLogger(__name__)


##### CLASSES #####


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


def main():
    output_folder = pathlib.Path("Outputs")
    output_folder = output_folder / f"BODSE AVL Outputs - {dt.date.today():%Y-%m-%d}"
    output_folder.mkdir(exist_ok=True, parents=True)

    with log_helpers.LogHelper(
        bodse.__package__,
        log_helpers.ToolDetails(bodse.__package__, bodse.__version__),
        log_file=output_folder / "avl.log",
    ):
        bods_auth = request.APIAuth.load_yaml(pathlib.Path("bods_auth.yml"))

        gtfs_feed_path = output_folder / "gtfs-rt_request.txt"
        gtfs.write_feed(gtfs_feed_path, bods_auth)

        database_path = output_folder / f"avl_data-{dt.date.today():%Y%m%d}.sqlite"
        raw_database = database.RawAVLDatabase(database_path)
        LOG.info("Created: %s", database_path)

        wait_seconds = 60
        count = 0
        max_count = 50
        while True:
            LOG.info(
                "Downloading %s/%s (%s)", count, max_count, f"{count / max_count:.0%}"
            )
            store_raw(raw_database, bods_auth)
            count += 1

            if count >= max_count:
                break

            LOG.info("Waiting %s seconds...", wait_seconds)
            time.sleep(wait_seconds)
