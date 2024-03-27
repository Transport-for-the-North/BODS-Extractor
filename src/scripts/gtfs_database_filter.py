# -*- coding: utf-8 -*-
"""Script to extract data from AVL database for given area."""

##### IMPORTS #####

# Built-Ins
import logging
import pathlib
import textwrap

# Third Party
import caf.toolkit as ctk
from caf.toolkit import log_helpers
from pydantic import dataclasses, types
import sqlalchemy
from sqlalchemy import sql
import pandas as pd

# Local Imports
import bodse
from bodse.avl import database

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
_CONFIG_FILE = pathlib.Path(__file__).with_suffix(".yml")
_TOOL_NAME = f"bodse.{pathlib.Path(__file__).stem}"
DATA_TABLES = [
    "gtfs_rt_vehicle_positions",
    "gtfs_rt_vehicle_speed_estimates",
    "gtfs_stop_times",
    "gtfs_stop_delays",
]


##### CLASSES & FUNCTIONS #####
@dataclasses.dataclass
class Bounds:
    min_easting: float
    max_easting: float
    min_northing: float
    max_northing: float

    def __str__(self) -> str:
        text = "Bounds("

        for j in ("easting", "northing"):
            for i in ("min", "max"):
                value: float = getattr(self, f"{i}_{j}")
                text_val = f"{value:.5f}"
                text += f"\n {i}_{j:<8.8}: {text_val:>15}"

        text += "\n)"
        return text


class _Config(ctk.BaseConfig):
    output_folder: types.DirectoryPath
    avl_database: types.FilePath
    bounds: Bounds


def _generate_table_sql(table_name: str) -> str:
    positions_tables = ("gtfs_rt_vehicle_positions", "gtfs_rt_vehicle_speed_estimates")
    stop_tables = ("gtfs_stop_times", "gtfs_stop_delays")
    if table_name in positions_tables:
        stmt = f"""
            SELECT *
            FROM "{table_name}"
            WHERE
                "easting" > :min_easting
                AND "easting" < :max_easting
                AND "northing" > :min_northing
                AND "northing" < :max_northing;
            """
    elif table_name in stop_tables:
        stmt = f"""
        SELECT *
        FROM {table_name}
        WHERE
            "stop_east" > :min_easting
            AND "stop_east" < :max_easting
            AND "stop_north" > :min_northing
            AND "stop_north" < :max_northing;
        """
    else:
        raise ValueError(f"unknown table '{table_name}'")

    return textwrap.dedent(stmt.strip())


def extract_from_table(
    conn: sqlalchemy.Connection, table_name: str, bounds: Bounds
) -> pd.DataFrame:
    LOG.info("Fetching fitlered data from %s", table_name)
    stmt = sql.text(_generate_table_sql(table_name))
    result = conn.execute(
        stmt,
        parameters={
            "min_easting": bounds.min_easting,
            "max_easting": bounds.max_easting,
            "min_northing": bounds.min_northing,
            "max_northing": bounds.max_northing,
        },
    )

    data = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
    LOG.info("Extracted %s rows from %s", f"{len(data):,}", table_name)
    return data


def extract_all_tables(
    conn: sqlalchemy.Connection, bounds: Bounds, output_folder: pathlib.Path
):
    LOG.info("Extracting data from tables within bounds:\n%s", bounds)
    for table in DATA_TABLES:
        data = extract_from_table(conn, table, bounds)
        out_file = output_folder / f"{table}.csv"
        data.to_csv(out_file, index=False)
        LOG.info("Written: %s", out_file)


def main() -> None:
    parameters = _Config.load_yaml(_CONFIG_FILE)

    log_file = parameters.output_folder / "AVL_filter.log"
    details = log_helpers.ToolDetails(_TOOL_NAME, bodse.__version__)

    with ctk.LogHelper("", details, log_file=log_file):

        gtfs_db = database.GTFSRTDatabase(parameters.avl_database)

        with gtfs_db.connect() as conn:
            extract_all_tables(conn, parameters.bounds, parameters.output_folder)


##### MAIN #####
if __name__ == "__main__":
    main()
