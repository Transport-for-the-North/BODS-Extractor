# -*- coding: utf-8 -*-
"""
Functionality for automating the downloading and processing of BODS data.

The scheduler will perform the following tasks on a regular schedule:
- Download the GTFS schedule from BODS
- Download AVL data from BODS for a set period
- Calculate an adjusted GTFS schedule using AVL data
- Upload all datasets to a PostgreSQL database
"""

##### IMPORTS #####

# Built-Ins
import datetime
import json
import logging
import pathlib
import time
import traceback

# Third Party
import pydantic
from caf.toolkit import config_base, log_helpers
from pydantic import dataclasses, types

# Local Imports
import bodse
from bodse import database, timetable

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
WAIT_TIME = 3600


##### CLASSES & FUNCTIONS #####


@dataclasses.dataclass
class TaskParameters:
    gtfs_folder: pydantic.DirectoryPath
    max_timetable_age_days: int = pydantic.Field(7, ge=0)


class SchedulerConfig(config_base.BaseConfig):
    output_folder: types.DirectoryPath
    task_parameters: TaskParameters
    database_parameters: database.DatabaseConfig

    run_months: int = pydantic.Field(2, ge=1, le=12)
    run_day: int = pydantic.Field(5, ge=1, le=28)


def get_scheduled_timetable(
    bsip_database: database.Database, timetable_folder: pathlib.Path, timetable_max_days: int
):
    start_time = datetime.datetime.now()
    params = json.dumps(
        {
            "timetable_folder": str(timetable_folder),
            "timetable_max_days": timetable_max_days,
        }
    )

    scheduled_timetable = bsip_database.find_recent_timetable()

    age = datetime.date.today() - scheduled_timetable.upload_date
    if age.days <= timetable_max_days:
        LOG.info(
            "Found GTFS file <= %s days old (%s), so not downloading a new one",
            timetable_max_days,
            pathlib.Path(scheduled_timetable.timetable_path).name,
        )
        return scheduled_timetable

    path = timetable_folder / timetable.gtfs_filename()
    if path.is_file():
        raise FileExistsError(
            f"GTFS timetable already exists for '{path}' but isn't in the database"
        )

    try:
        path = timetable.download_to_file(path)
    except Exception:
        bsip_database.insert_run_metadata(
            model_name=database.ModelName.BODSE_SCHEDULED,
            start_datetime=start_time,
            parameters=params,
            sucessful=False,
            error=traceback.format_exc(),
        )
        raise

    run_id = bsip_database.insert_run_metadata(
        model_name=database.ModelName.BODSE_SCHEDULED,
        start_datetime=start_time,
        parameters=params,
        sucessful=True,
        output=f"Downloaded timetable to: {path.absolute()}",
    )
    return bsip_database.insert_timetable(
        run_id=run_id,
        feed_update_time=timetable.get_feed_version(path),
        timetable_path=str(path),
    )


def run_tasks(
    params: TaskParameters,
    database_config: database.DatabaseConfig,
    output_folder: pathlib.Path,
):
    db = database.Database(database_config)

    scheduled_timetable = get_scheduled_timetable(
        db, params.gtfs_folder, params.max_timetable_age_days
    )

    # Start AVL downloader for given period
    # Calculate AVL adjusted GTFS schedule and upload to database
    # Post sucesses to MS Teams


def main(parameters: SchedulerConfig) -> None:
    log_file = parameters.output_folder / "BODSE_scheduler.log"
    details = log_helpers.ToolDetails(__package__, bodse.__version__)

    with log_helpers.LogHelper("", details, log_file=log_file):
        logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

        while True:
            try:
                run_tasks(
                    parameters.task_parameters,
                    parameters.database_parameters,
                    parameters.output_folder,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                LOG.critical("error during schedule tasks", exc_info=True)
                # TODO(MB) Log errors to MS Teams

            time.sleep(WAIT_TIME)
