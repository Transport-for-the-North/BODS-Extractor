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
import calendar
import datetime
import enum
import json
import logging
import pathlib
import re
import time
import traceback
from typing import Optional

# Third Party
import pydantic
from caf.toolkit import config_base, log_helpers
from pydantic import dataclasses, types

# Local Imports
import bodse
from bodse import database, request, teams, timetable
from bodse.avl import adjust, avl
from bodse.avl import database as avl_db

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
WAIT_TIME = 3600 * 12  # 0.5 days


##### CLASSES & FUNCTIONS #####


class Day(enum.IntEnum):
    """Days of the week."""

    MONDAY = calendar.MONDAY
    TUESDAY = calendar.TUESDAY
    WEDNESDAY = calendar.WEDNESDAY
    THURSDAY = calendar.THURSDAY
    FRIDAY = calendar.FRIDAY
    SATURDAY = calendar.SATURDAY
    SUNDAY = calendar.SUNDAY


@dataclasses.dataclass
class TaskParameters:
    """Parameters for scheduled BODSE tasks."""

    gtfs_folder: pydantic.DirectoryPath
    avl_download_time: avl.DownloadTime
    max_timetable_age_days: int = pydantic.Field(7, ge=0)
    max_avl_age_days: int = pydantic.Field(7, ge=0)
    avl_database_delete_age_days: int = pydantic.Field(30, ge=0)


class SchedulerConfig(config_base.BaseConfig):
    """Parameters for running BODSE scheduler."""

    output_folder: types.DirectoryPath
    task_parameters: TaskParameters
    database_parameters: database.DatabaseConfig
    api_auth_config: request.APIAuth
    teams_webhook_url: Optional[pydantic.HttpUrl] = None

    run_months: int = pydantic.Field(2, ge=1, le=12)
    run_month_day: int = pydantic.Field(5, ge=1, le=28)
    run_weekday: Day = Day.MONDAY


def _log_success(message: str, teams_post: Optional[teams.TeamsPost] = None) -> None:
    """Log message and post to Teams channel, if available."""
    LOG.info(message)

    if teams_post is not None:
        teams_post.post_success(message)


def get_scheduled_timetable(
    bsip_database: database.Database,
    timetable_folder: pathlib.Path,
    timetable_max_days: int,
    teams_post: Optional[teams.TeamsPost] = None,
) -> database.Timetable:
    """Download scheduled GTFS timetable from BODS.

    Before downloading, checks if a recent timetable is
    already available in the database.

    Parameters
    ----------
    bsip_database : database.Database
        Database for storing the timetable data.
    timetable_folder : pathlib.Path
        Folder to store the GTFS timetable files in.
    timetable_max_days : int
        If the most recent scheduled timetable in the database is
        <= this value, then it is returned instead of downloading
        a new timetable file.
    teams_post : teams.TeamsPost, optional
        Instance of TeamsPost to use for posting success messages
        to a Teams channel.

    Returns
    -------
    database.Timetable
        Read-only row of data from the database for the timetable.

    Raises
    ------
    FileNotFoundError
        If timetable data is found in the database but the GTFS file
        can't be found.
    FileExistsError
        If a file already exists at the location the new GTFS file
        will be downloaded to, downloaded GTFS files contain today's
        date in the filename.
    """
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

    run_metadata_id = bsip_database.insert_run_metadata(
        model_name=database.ModelName.BODSE_SCHEDULED,
        start_datetime=start_time,
        parameters=params,
        successful=True,
        output=f"Downloaded timetable to: {path.absolute()}",
    )
    timetable_data = bsip_database.insert_timetable(
        run_metadata_id=run_metadata_id,
        feed_update_time=timetable.get_feed_version(path),
        timetable_path=str(path),
    )

    _log_success(
        f"Downloaded timetable from BODS to: {path.absolute()}\nSaved to database"
        f" with run_metadata_id {run_metadata_id:,} and timetable id {timetable_data.id}",
        teams_post,
    )

    return timetable_data


def avl_download(
    folder: pathlib.Path,
    download_time: avl.DownloadTime,
    bods_auth: request.APIAuth,
    max_age_days: int,
    delete_age_days: int,
    teams_post: Optional[teams.TeamsPost] = None,
) -> pathlib.Path:
    """Download AVL data (GTFS-rt) from BODS for given period of time.

    Parameters
    ----------
    folder : pathlib.Path
        Folder to save SQLite database to containing downloaded AVL data.
    download_time : avl.DownloadTime
        Time to download AVL data for.
    bods_auth : request.APIAuth
        Username and password for BODS.
    max_age_days : int
        If the most recent AVL database is <= this value,
        then new AVL data isn't download and instead the path
        to this SQLite database is returned.
    delete_age_days : int
        Any AVL SQLite databases found in `folder` which are older
        than this are deleted.
    teams_post : teams.TeamsPost, optional
        Instance of TeamsPost to use for posting success messages
        to a Teams channel.

    Returns
    -------
    pathlib.Path
        Path to SQLite database containing downloaded AVL data.
    """
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

    _log_success(
        f"Finished AVL download with all AVL data stored in '{gtfs_db.path}'", teams_post
    )

    return db_path


def avl_adjustment(
    avl_db_path: pathlib.Path,
    scheduled_timetable: database.Timetable,
    bsip_database: database.Database,
    avl_download_parameters: dict[str, int],
    teams_post: Optional[teams.TeamsPost] = None,
):
    """Adjust the GTFS schedule using delays calculated from AVL data.

    Parameters
    ----------
    avl_db_path : pathlib.Path
        Path to SQLite database containing downloaded AVL data.
    scheduled_timetable : database.Timetable
        Scheduled timetable data downloaded from BODS.
    bsip_database : database.Database
        PostgreSQL database for storing the adjusted timetables data.
    avl_download_parameters : dict[str, int]
        Parameters used for AVL download, will be stored in the run
        metadata table in the database.
    teams_post : teams.TeamsPost, optional
        Instance of TeamsPost to use for posting success messages
        to a Teams channel.
    """
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
    run_metadata_id = bsip_database.insert_run_metadata(
        model_name=database.ModelName.BODSE_ADJUSTED,
        start_datetime=start_time,
        parameters=json.dumps(params),
        successful=True,
        output="Downloaded timetables to:\n - "
        + "\n - ".join(f"{i}: {j.absolute()}" for i, j in adjusted_gtfs_files.items()),
    )

    db_ids = []
    for path in adjusted_gtfs_files.values():
        timetable_data = bsip_database.insert_timetable(
            run_metadata_id=run_metadata_id,
            feed_update_time=timetable.get_feed_version(path),
            timetable_path=str(path),
            adjusted=True,
            base_timetable_id=scheduled_timetable.id,
        )
        db_ids.append(timetable_data.id)

    _log_success(
        "Finished AVL timetable adjustment, metadata saved to database with id"
        f" {run_metadata_id} and adjusted timetables saved to database with ids:"
        + ", ".join(str(i) for i in db_ids),
        teams_post,
    )


def run_tasks(
    params: TaskParameters,
    database_config: database.DatabaseConfig,
    output_folder: pathlib.Path,
    bods_auth: request.APIAuth,
    teams_post: Optional[teams.TeamsPost] = None,
):
    """Run scheduler tasks to download and adjust timetable and save to database.

    Parameters
    ----------
    params : TaskParameters
        Various required parameters / settings.
    database_config : database.DatabaseConfig
        Connection parameters for the PostgreSQL database.
    output_folder : pathlib.Path
        Folder to save any outputs to.
    bods_auth : request.APIAuth
        Username and password for connecting to the BODS API.
    teams_post : teams.TeamsPost, optional
        Instance of TeamsPost for posting success messages to MS Teams channel.
    """
    db = database.Database(database_config)

    scheduled_timetable = get_scheduled_timetable(
        bsip_database=db,
        timetable_folder=params.gtfs_folder,
        timetable_max_days=params.max_timetable_age_days,
        teams_post=teams_post,
    )

    avl_database_path = avl_download(
        folder=output_folder,
        download_time=params.avl_download_time,
        bods_auth=bods_auth,
        max_age_days=params.max_avl_age_days,
        delete_age_days=params.avl_database_delete_age_days,
        teams_post=teams_post,
    )

    avl_adjustment(
        avl_database_path,
        scheduled_timetable,
        bsip_database=db,
        avl_download_parameters=params.avl_download_time.asdict(),
        teams_post=teams_post,
    )


class RunDate:
    """Determine if scheduled run is required today and log run date to file.

    Parameters
    ----------
    folder : pathlib.Path
        Folder to save run date file to.
    month_day : int
        Earliest day of the month to run scheduled tasks on.
    month_frequency : int
        Frequency to run the scheduled tasks in months e.g.
        1 would be run every month.
    day_of_week : Day
        Weekday to run scheduled tasks on.
    """

    _file_encoding = "utf-8"
    _filename = "bodse_last_run"
    _filename_hash = "868ea1ebd8286461461ada57d5f9487f"

    def __init__(
        self, folder: pathlib.Path, month_day: int, month_frequency: int, day_of_week: Day
    ) -> None:
        if not folder.is_dir():
            raise NotADirectoryError(f"not a directory: '{folder}'")
        folder = folder.resolve()

        self._path = folder / self._filename
        self._month_day = int(month_day)
        self._month_frequency = int(month_frequency)
        self._day_of_week = Day(day_of_week)

        if self._month_day > 28 or self._month_day < 1:
            raise ValueError(f"month day should be 1 - 28 (inclusive) not {month_day}")

        if self._month_frequency < 1 or self._month_frequency > 12:
            raise ValueError(
                f"month frequency should be 1 - 12 (inclusive) not {month_frequency}"
            )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"folder={self._path.parent.name!r}, month_day={self._month_day},"
            f" month_frequency={self._month_frequency},"
            f" day_of_week={self._day_of_week.name})"
        )

    def check_file(self) -> None:
        """Check that the previous run date file isn't an unexpected format.

        Raises
        ------
        FileExistsError
            If the previous run file exists but doesn't
            start with the correct hash.
        """
        if not self._path.is_file():
            return

        with open(self._path, "rt", encoding=self._file_encoding) as file:
            line = file.readline().strip()

        if line != self._filename_hash:
            raise FileExistsError(
                f"BODSE date file already exists ({self._path.name})"
                " but with unexpected hash, what is this file?"
            )

    def get_date(self) -> datetime.date | None:
        """Get previous run date from file, if available."""
        if not self._path.is_file():
            return None

        with open(self._path, "rt", encoding=self._file_encoding) as file:
            _ = file.readline()  # hash line
            text = file.readline().strip()

        run_datetime = datetime.date.fromisoformat(text)

        return run_datetime

    def update(self) -> None:
        """Update previous run file with today's date."""
        with open(self._path, "wt", encoding=self._file_encoding) as file:
            file.writelines(
                [f"{self._filename_hash}\n", f"{datetime.date.today().isoformat()}\n"]
            )

    def needs_running(self, verbose: bool = True) -> bool:
        """Check if scheduled tasks need running today."""
        today = datetime.date.today()

        if today.weekday() != self._day_of_week:
            if verbose:
                LOG.info(
                    "No runs required as today is %s not %s",
                    Day(today.weekday()).name.title(),
                    self._day_of_week.name.title(),
                )
            return False

        if today.day < self._month_day:
            if verbose:
                LOG.info(
                    "No runs required, today's date (%s) is earlier"
                    " in the month than scheduled date (%s)",
                    today.day,
                    self._month_day,
                )
            return False

        previous_date = self.get_date()
        if previous_date is None:
            return True

        year_diff = today.year - previous_date.year
        month_diff = year_diff * 12 + today.month - previous_date.month

        if month_diff < self._month_frequency:
            if verbose:
                LOG.info(
                    "No runs required, run was done %s month(s)"
                    " ago and will be done every %s month(s)",
                    month_diff,
                    self._month_frequency,
                )
            return False

        return True

    def next_run_date(self) -> datetime.date:
        """Calculate the date for the next scheduled run."""
        if self.needs_running(verbose=False):
            return datetime.date.today()

        previous_run = self.get_date()
        if previous_run is None:
            return datetime.date.today()

        year, month = divmod(previous_run.month + self._month_frequency, 12)
        if month == 0:
            month = 12
            year -= 1

        return datetime.date(year=previous_run.year + year, month=month, day=self._month_day)

    def time_until_run(self) -> datetime.timedelta:
        """Calculate the time left until the next scheduled run."""
        new_date = self.next_run_date()
        return new_date - datetime.date.today()


def main(parameters: SchedulerConfig) -> None:
    """Run BODSE scheduler, to regularly download and adjust timetables."""
    log_file = (
        parameters.output_folder / f"logs/BODSE_scheduler-{datetime.date.today():%Y%m%d}.log"
    )
    log_file.parent.mkdir(exist_ok=True)
    details = log_helpers.ToolDetails(__package__, bodse.__version__)

    with log_helpers.LogHelper(__package__, details, log_file=log_file):
        if parameters.teams_webhook_url is not None:
            teams_post = teams.TeamsPost(
                parameters.teams_webhook_url,
                teams.TOOL_NAME,
                bodse.__version__,
                teams.SOURCE_CODE_URL,  # type: ignore
                allow_missing_module=True,
            )
        else:
            teams_post = None

        run_date = RunDate(
            parameters.output_folder,
            parameters.run_month_day,
            parameters.run_months,
            parameters.run_weekday,
        )

        while True:
            wait_time = WAIT_TIME
            try:
                if run_date.needs_running():
                    run_tasks(
                        parameters.task_parameters,
                        parameters.database_parameters,
                        parameters.output_folder,
                        parameters.api_auth_config,
                        teams_post=teams_post,
                    )
                    run_date.update()

            except Exception as exc:  # pylint: disable=broad-exception-caught
                LOG.critical(
                    "error during scheduled BODSE tasks, detailed"
                    " log file can be found at '%s'",
                    log_file.absolute(),
                    exc_info=True,
                )

                if teams_post is not None:
                    teams_post.post_error("error during scheduled tasks", exc)

            LOG.info("Completed task, waiting %.1f hours...", wait_time / 3600)
            time.sleep(wait_time)
