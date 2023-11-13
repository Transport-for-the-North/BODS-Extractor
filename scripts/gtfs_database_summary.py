# -*- coding: utf-8 -*-
"""
    Script to produce summary statistics for the GTFS-rt AVL
    SQLite database.
"""

##### IMPORTS #####
# Standard imports
import logging
import pathlib
import numpy as np

import pandas as pd
import sqlalchemy
from sqlalchemy import sql

# Third party imports

# Local imports
from bodse.avl import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
META_IDS = (8, 100)

##### CLASSES #####


##### FUNCTIONS #####
def _load_metadata(conn: sqlalchemy.Connection, start_id: int, end_id: int) -> pd.DataFrame:
    """Load some metadata rows from the GTFS-rt database."""
    LOG.info("Loading GTFS-rt metadata")
    stmt = sql.text(
        """SELECT id, timestamp, gtfs_realtime_version, incrementality
            FROM gtfs_rt_meta
            WHERE id >= :start_id AND id <= :end_id;
        """
    )
    data = pd.read_sql(
        stmt, conn, index_col="id", params=dict(start_id=start_id, end_id=end_id)
    )
    data.loc[:, "timestamp"] = pd.to_datetime(data["timestamp"])
    return data


def _load_data(conn: sqlalchemy.Connection, start_id: int, end_id: int) -> pd.DataFrame:
    """Load GTFS-rt rows from the database."""
    LOG.info("Loading GTFS-rt data")
    stmt = sql.text(
        """SELECT *
            FROM gtfs_rt_vehicle_positions
            WHERE metadata_id >= :start_id AND metadata_id <= :end_id;
        """
    )
    data = pd.read_sql(
        stmt, conn, index_col="id", params=dict(start_id=start_id, end_id=end_id)
    )

    for column in data.columns:
        try:
            data.loc[:, column] = data.str.strip()
        except AttributeError:
            pass

        data.loc[:, column] = data.replace(["", None], np.nan)

    data.loc[:, "start_time"] = pd.to_datetime(data["start_time"]).dt.time
    data.loc[:, "start_date"] = pd.to_datetime(data["start_date"]).dt.date
    data.loc[:, "timestamp"] = pd.to_datetime(data["timestamp"])

    return data


def _dataset_summary(data: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Summarise GTFS-rt dataset into various DataFrames for output."""
    summaries = {}

    summary_data = [
        ("Field Count", np.full(len(data), True)),
        ("Trips Count", ~data["trip_id"].isna()),
    ]
    for name, mask in summary_data:
        summary = (
            data.loc[mask, :]
            .describe(include="all", percentiles=[], datetime_is_numeric=True)
            .T
        )

        count_perc = summary["count"] / summary.loc["metadata_id", "count"]
        summary.insert(1, "count percentage", count_perc)

        for column in summary.columns:
            summary.loc[:, column] = pd.to_numeric(
                summary[column], downcast="integer", errors="ignore"
            )

        summaries[name] = summary

    return summaries


def main():
    folder = pathlib.Path(r"Outputs\BODSE AVL Outputs - 2023-11-07")
    db_path = folder / "gtfs-rt.sqlite"

    start_id = int(META_IDS[0])
    end_id = int(META_IDS[1])

    gtfs_db = database.GTFSRTDatabase(db_path)

    summaries: dict[str, pd.DataFrame] = {}
    with gtfs_db.connect() as conn:
        metadata = _load_metadata(conn, start_id, end_id)
        summaries["Metadata"] = metadata.describe(datetime_is_numeric=True, include="all").T
        del metadata

        full_dataset = _load_data(conn, start_id, end_id)
        summaries.update(_dataset_summary(full_dataset))

    out_file = folder / "GTFS-rt_summary.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as excel:
        for name, data in summaries.items():
            data.columns = data.columns.str.title()
            data.to_excel(excel, sheet_name=name)
    LOG.info("Written: %s", out_file)


if __name__ == "__main__":
    main()
