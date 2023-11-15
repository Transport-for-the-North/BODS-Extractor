# -*- coding: utf-8 -*-
"""
    Script to produce summary statistics for the GTFS-rt AVL
    SQLite database.
"""

##### IMPORTS #####
# Standard imports
import logging
import pathlib
import textwrap
from typing import Optional

# Third party imports
import numpy as np
import pandas as pd
import pydantic
import sqlalchemy
from caf import toolkit as ctk
from matplotlib.backends import backend_pdf
from matplotlib import pyplot as plt, figure, patches
from sqlalchemy import sql

# Local imports
from bodse.avl import database

##### CONSTANTS #####
LOG = logging.getLogger(__name__)
CONFIG_PATH = pathlib.Path("gtfs_database_summary.yml")


##### CLASSES #####
class SummaryConfig(ctk.BaseConfig):
    """Parameters for running GTFS database summaries."""

    avl_output_folder: pydantic.DirectoryPath
    metadata_ids: tuple[int, int]

    @pydantic.validator("metadata_ids", pre=True)
    def _str_to_list(cls, value: Optional[str]) -> Optional[list]:
        # pylint: disable=no-self-argument
        if value is None:
            return None
        return value.split(",")

    @pydantic.validator("metadata_ids")
    def _id_comparison(cls, value: tuple[int, int]) -> tuple[int, int]:
        # pylint: disable=no-self-argument
        if len(value) != 2:
            raise ValueError(f"2 integers should be given not {len(value)}")

        if value[0] > value[1]:
            raise ValueError(
                f"should have a start index which is lower than the end index not {value}"
            )

        return value


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


def _label_patches(
    ax: plt.Axes, heights: np.ndarray, rects: list[patches.Rectangle], total: int
) -> None:
    for h, r in zip(heights, rects):
        perc = h / total
        ax.annotate(
            f"{perc:.0%}" if perc > 0.005 else f"{perc:.1%}",
            xy=(r.get_center()[0], r.get_height()),
            xytext=(0, 5),
            textcoords="offset pixels",
            ha="center",
            va="bottom",
            fontsize="small",
            bbox=dict(boxstyle="round", alpha=0.5),
        )


def _histogram_table(
    counts: np.ndarray,
    below_range: int,
    above_range: int,
    bins: np.ndarray,
    total_count: int,
    title: str,
) -> figure.Figure:
    """Produce a table figure showing histogram counts for each bin."""
    row_labels = [f"{i:.0f} - {j:.0f}" for i, j in zip(bins[:-1], bins[1:])]
    row_labels.insert(0, f"< {np.min(bins)}")
    row_labels.append(f">= {np.max(bins)}")

    counts = [below_range, *counts, above_range]
    cells = [(l, f"{i:,.0f}", f"{i/total_count:.0%}") for l, i in zip(row_labels, counts)]

    fig, ax = plt.subplots(layout="tight", figsize=(5, 4))
    assert isinstance(ax, plt.Axes)

    ax.axis("off")
    ax.set_title(textwrap.fill(title, width=45), fontsize=12)
    col_labels = [
        "Time Difference\n(minutes)",
        "Count of\nRecords",
        "Percentage of\nTotal Records",
    ]
    table = ax.table(cells, colLabels=col_labels, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)

    for i in range(len(col_labels)):
        for j in range(len(cells) + 1):
            height_factor = 1.2 if j == 0 else 0.6
            cell = table[j, i]
            cell.set_height(height_factor / len(cells))

            if j == 0:
                cell.set_text_props(weight="bold")

    return fig


def _compare_timestamps(
    conn: sqlalchemy.Connection, start_id: int, end_id: int, out_path: pathlib.Path
) -> None:
    """Plot difference between metadata timestamp and the vehicle position timestamp."""
    stmt = sql.text(
        """SELECT m.id AS meta_id, m.timestamp AS meta_time,
            v.id, v.timestamp AS vehicle_time

        FROM gtfs_rt_meta m
            LEFT JOIN gtfs_rt_vehicle_positions v ON m.id = v.metadata_id

        WHERE m.id >= :start_id AND m.id <= :end_id
            AND length(v.trip_id) > 0
        """
    )
    data = pd.read_sql(
        stmt, conn, index_col="id", params=dict(start_id=start_id, end_id=end_id)
    )
    for column in ("meta_time", "vehicle_time"):
        data[column] = pd.to_datetime(data[column])

    delta_time = data["meta_time"] - data["vehicle_time"]
    delta_minutes = delta_time.dt.seconds / 60

    count_values = {}

    fig, ax = plt.subplots(figsize=(10, 6), layout="tight")
    assert isinstance(ax, plt.Axes)

    bins = np.array([0, 1, 5, 10, 20, 30, 60, 120, 180])
    counts, _, bars = ax.hist(delta_minutes, bins=bins, linewidth=1, edgecolor="white")

    title = textwrap.fill(
        "Distribution of Vehicle Position Timestamps "
        "Compared to the AVL Feed Response Timestamp",
        width=60,
    )

    ax.set_title(title)
    ax.set_ylabel("Count of Records")
    ax.set_xlabel("Vehicle Postion Time Before Response (minutes)")

    _label_patches(ax, counts, bars, len(delta_minutes.values))

    count_values["Total"] = len(delta_minutes.values)
    count_values[f"< {np.min(bins)}"] = np.sum(delta_minutes.values < np.min(bins))
    count_values[f"{np.min(bins)} - {np.max(bins)}"] = np.sum(counts)
    count_values[f">= {np.max(bins)}"] = np.sum(delta_minutes.values >= np.max(bins))

    table = _histogram_table(
        counts,
        count_values[f"< {np.min(bins)}"],
        count_values[f">= {np.max(bins)}"],
        bins,
        count_values["Total"],
        title,
    )

    msg = []
    for name, value in count_values.items():
        msg.append(
            "{note!s:10.10}:{values!s:>10.10}{perc!s:>7.7}".format(
                note=name,
                values=f"{value:,.0f}",
                perc=f"({value / count_values['Total']:.0%})",
            )
        )

    ax.annotate(
        "\n".join(msg),
        (0.95, 0.95),
        xycoords="axes fraction",
        ha="right",
        va="top",
        bbox=dict(boxstyle="round", alpha=0.5),
    )

    with backend_pdf.PdfPages(out_path.with_suffix(".pdf")) as pdf:
        pdf.savefig(fig)
        pdf.savefig(table)

    plt.close(fig)
    plt.close(table)
    LOG.info("Written: %s", out_path)


def main():
    parameters = SummaryConfig.load_yaml(CONFIG_PATH)
    db_path = parameters.avl_output_folder / "gtfs-rt.sqlite"

    # Setup matplotlib style parameters
    plt.style.use("bmh")
    plt.rcParams["font.family"] = "monospace"
    plt.rcParams["patch.facecolor"] = "white"

    gtfs_db = database.GTFSRTDatabase(db_path)

    summaries: dict[str, pd.DataFrame] = {}
    with gtfs_db.connect() as conn:
        metadata = _load_metadata(conn, *parameters.metadata_ids)
        summaries["Metadata"] = metadata.describe(datetime_is_numeric=True, include="all").T
        del metadata

        full_dataset = _load_data(conn, *parameters.metadata_ids)
        summaries.update(_dataset_summary(full_dataset))

        _compare_timestamps(
            conn,
            *parameters.metadata_ids,
            parameters.avl_output_folder / "GTFS-rt_timestamp_distribution.png",
        )

    out_file = parameters.avl_output_folder / "GTFS-rt_summary.xlsx"
    with pd.ExcelWriter(out_file, engine="openpyxl") as excel:
        for name, data in summaries.items():
            data.columns = data.columns.str.title()
            data.to_excel(excel, sheet_name=name)
    LOG.info("Written: %s", out_file)


if __name__ == "__main__":
    main()
