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
import logging
import time

# Third Party
from caf.toolkit import config_base, log_helpers
from pydantic import types, dataclasses

# Local Imports
import bodse

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
WAIT_TIME = 3600


##### CLASSES & FUNCTIONS #####


class SchedulerConfig(config_base.BaseConfig):
    output_folder: types.DirectoryPath


@dataclasses.dataclass
class TaskParameters: ...


def run_tasks(params: TaskParameters):
    # Download new GTFS file if required, and upload to database
    # Start AVL downloader for given period
    # Calculate AVL adjusted GTFS schedule and upload to database
    # Post sucesses to MS Teams
    ...


def main(parameters: SchedulerConfig) -> None:
    log_file = parameters.output_folder / "BODSE_scheduler.log"
    details = log_helpers.ToolDetails(__package__, bodse.__version__)

    task_params = TaskParameters()

    with log_helpers.LogHelper("", details, log_file=log_file):
        while True:
            try:
                run_tasks(task_params)
            except Exception:  # pylint: disable=broad-exception-caught
                LOG.critical("error during schedule tasks", exc_info=True)
                # TODO(MB) Log errors to MS Teams

            time.sleep(WAIT_TIME)
