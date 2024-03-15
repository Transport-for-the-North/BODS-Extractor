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
import re
import time
import traceback

# Third Party
import pydantic
from caf.toolkit import config_base, log_helpers
from pydantic import dataclasses, types

# Local Imports
import bodse
from bodse import database, timetable
from bodse import request
from bodse.avl import adjust, avl, database as avl_db

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
WAIT_TIME = 3600


##### CLASSES & FUNCTIONS #####


@dataclasses.dataclass
class TaskParameters:
    gtfs_folder: pydantic.DirectoryPath
    avl_download_time: avl.DownloadTime
    max_timetable_age_days: int = pydantic.Field(7, ge=0)
    max_avl_age_days: int = pydantic.Field(7, ge=0)
    avl_database_delete_age_days: int = pydantic.Field(30, ge=0)


class SchedulerConfig(config_base.BaseConfig):
    output_folder: types.DirectoryPath
    task_parameters: TaskParameters
    database_parameters: database.DatabaseConfig
    api_auth_config: request.APIAuth

    run_months: int = pydantic.Field(2, ge=1, le=12)
    run_day: int = pydantic.Field(5, ge=1, le=28)


def get_scheduled_timetable(
    bsip_database: database.Database, timetable_folder: pathlib.Path, timetable_max_days: int
) -> database.Timetable:
    start_time = datetime.datetime.now()
    params = json.dumps(
        {
            "timetable_folder": str(timetable_folder),
            "timetable_max_days": timetable_max_days,
        }
    )

    scheduled_timetable = bsip_database.find_recent_timetable()

    if scheduled_timetable is not None:
        if not scheduled_timetable.actual_timetable_path.is_file():
            raise FileNotFoundError(
                f"cannot find file linked in database: '{scheduled_timetable.timetable_path}'"
            )

        age = datetime.date.today() - scheduled_timetable.upload_date
        if age.days <= timetable_max_days:
            LOG.info(
                "Found GTFS file <= %s days old (%s) in database"
                " with ID = %s, so not downloading a new one",
                timetable_max_days,
                scheduled_timetable.actual_timetable_path.name,
                scheduled_timetable.id,
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
            successful=False,
            error=traceback.format_exc(),
        )
        raise

    run_id = bsip_database.insert_run_metadata(
        model_name=database.ModelName.BODSE_SCHEDULED,
        start_datetime=start_time,
        parameters=params,
        successful=True,
        output=f"Downloaded timetable to: {path.absolute()}",
    )
    return bsip_database.insert_timetable(
        run_id=run_id,
        feed_update_time=timetable.get_feed_version(path),
        timetable_path=str(path),
    )


def avl_download(
    folder: pathlib.Path,
    download_time: avl.DownloadTime,
    bods_auth: request.APIAuth,
    max_age_days: int,
    delete_age_days: int,
) -> pathlib.Path:
    LOG.debug("AVL downloader parameters:\n%s", download_time)

    # Check for recent AVL databases
    name_format = "{}-AVL_download_db.sqlite"

    database_paths: dict[datetime.date, pathlib.Path] = {}
    for path in folder.glob(name_format.format("*")):
        match = re.match(name_format.format(r"(\d{4})(\d{2})(\d{2})"), path.name, re.I)
        if match is None:
            LOG.warning("found AVL database with unexpected name: %s", path.name)
            continue

        try:
            date = datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError as exc:
            LOG.warning(
                "found AVL database with incorrect date in name ('%s'): %s", path.name, exc
            )
            continue

        age = datetime.date.today() - date
        if age.days > delete_age_days:
            LOG.info(
                "Removing database older than %s days: %s (age %s days)",
                delete_age_days,
                path.name,
                age.days,
            )
            path.unlink()

        else:
            database_paths[date] = path

    if len(database_paths) > 0:
        date = max(database_paths)
        age = datetime.date.today() - date
        if age.days <= max_age_days:
            return database_paths[date]

    db_path = folder / name_format.format(f"{datetime.date.today():%Y%m%d}")
    gtfs_db = avl_db.GTFSRTDatabase(db_path)
    avl.download(gtfs_db, download_time, bods_auth)

    LOG.info("Tidying up AVL database tables")
    with gtfs_db.connect() as conn:
        gtfs_db.delete_duplicate_positions(conn)
        conn.commit()

        gtfs_db.fill_vehicle_speeds_table(conn)
        conn.commit()

    return db_path


def avl_adjustment(
    avl_db_path: pathlib.Path,
    scheduled_timetable: database.Timetable,
    bsip_database: database.Database,
    avl_download_parameters: dict[str, int],
):
    start_time = datetime.datetime.now()
    params = {
        "avl_db_path": str(avl_db_path),
        "base_timetable_path": scheduled_timetable.timetable_path,
        "avl_download_parameters": avl_download_parameters,
    }

    stop_times = adjust.extract_stop_times_locations(scheduled_timetable.actual_timetable_path)
    delays, delay_columns = adjust.calculate_stop_times_delays(avl_db_path, stop_times)
    delayed_stop_times, time_columns = adjust.calculate_observed_stop_times(
        stop_times, delays, delay_columns
    )
    del stop_times, delays

    adjusted_gtfs_files = adjust.output_delayed_gtfs(
        scheduled_timetable.actual_timetable_path,
        delayed_stop_times,
        time_columns,
        scheduled_timetable.actual_timetable_path,
    )

    LOG.info("Inserting adjusted GTFS files into database")
    run_id = bsip_database.insert_run_metadata(
        model_name=database.ModelName.BODSE_ADJUSTED,
        start_datetime=start_time,
        parameters=json.dumps(params),
        successful=True,
        output="Downloaded timetables to:\n - "
        + "\n - ".join(f"{i}: {j.absolute()}" for i, j in adjusted_gtfs_files.items()),
    )

    for path in adjusted_gtfs_files.values():
        bsip_database.insert_timetable(
            run_id=run_id,
            feed_update_time=timetable.get_feed_version(path),
            timetable_path=str(path),
            adjusted=True,
            base_timetable_id=scheduled_timetable.id,
        )


def run_tasks(
    params: TaskParameters,
    database_config: database.DatabaseConfig,
    output_folder: pathlib.Path,
    bods_auth: request.APIAuth,
):
    db = database.Database(database_config)

    scheduled_timetable = get_scheduled_timetable(
        bsip_database=db,
        timetable_folder=params.gtfs_folder,
        timetable_max_days=params.max_timetable_age_days,
    )

    avl_database_path = avl_download(
        folder=output_folder,
        download_time=params.avl_download_time,
        bods_auth=bods_auth,
        max_age_days=params.max_avl_age_days,
        delete_age_days=params.avl_database_delete_age_days,
    )

    avl_adjustment(
        avl_database_path,
        scheduled_timetable,
        bsip_database=db,
        avl_download_parameters=params.avl_download_time.asdict(),
    )

    # TODO Post sucesses to MS Teams


def main(parameters: SchedulerConfig) -> None:
    log_file = (
        parameters.output_folder / f"logs/BODSE_scheduler-{datetime.date.today():%Y%m%d}.log"
    )
    log_file.parent.mkdir(exist_ok=True)
    details = log_helpers.ToolDetails(__package__, bodse.__version__)

    with log_helpers.LogHelper("", details, log_file=log_file) as helper:
        database.init_sqlalchemy_logging(helper.logger)

        # TODO Check date to see if tasks need running
        while True:
            try:
                run_tasks(
                    parameters.task_parameters,
                    parameters.database_parameters,
                    parameters.output_folder,
                    parameters.api_auth_config,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                LOG.critical("error during schedule tasks", exc_info=True)
                # TODO(MB) Log errors to MS Teams

            LOG.info("Completed task, waiting %.0f mins...", WAIT_TIME / 60)
            time.sleep(WAIT_TIME)
