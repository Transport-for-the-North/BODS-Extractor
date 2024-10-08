# -*- coding: utf-8 -*-
"""
    Script to produce summary statistics for the GTFS-rt AVL
    SQLite database.
"""

##### IMPORTS #####
# Standard imports
import dataclasses
import datetime as dt
import logging
import pathlib
import re
import textwrap
import warnings
from typing import Any, Optional

# Third party imports
import numpy as np
import pandas as pd
import pydantic
import sqlalchemy
from caf import toolkit as ctk
from matplotlib import dates as mdates
from matplotlib import figure, patches
from matplotlib import pyplot as plt
from matplotlib.backends import backend_pdf
from sqlalchemy import sql

# Local imports
from bodse.avl import database

##### CONSTANTS #####

_ROOT_NAME = "bodse"
_SCRIPT_NAME = pathlib.Path(__file__).stem
_TOOL_NAME = f"{_ROOT_NAME}.{_SCRIPT_NAME}"
LOG = logging.getLogger(_TOOL_NAME)
CONFIG_PATH = pathlib.Path(__file__).with_suffix(".yml")


##### CLASSES #####


@dataclasses.dataclass
class _InsertTimeColumns:
    """Names of columns in the insert timings dataframe."""

    ID = "id"
    COUNT = "current_count"
    INSERT_TIME = "insert_time"
    COMPLETE_TIME = "complete_time"
    TIME_TAKEN = "time_taken_seconds"
    POSITIONS = "response_positions_count"
    RESPONSE_POSITIONS = "count_positions_in_response_data"
    CUMM_POSITIONS = "cummulative_positions_count"
    OUTLIER = "outlier_mask"


class SummaryConfig(ctk.BaseConfig):
    """Parameters for running GTFS database summaries."""

    avl_output_folder: pydantic.DirectoryPath
    metadata_ids: tuple[int, int]

    @pydantic.field_validator("metadata_ids", mode="before")
    @classmethod
    def _str_to_list(cls, value: Optional[str]) -> Optional[list]:
        if value is None:
            return None
        return value.split(",")

    @pydantic.field_validator("metadata_ids")
    @classmethod
    def _id_comparison(cls, value: tuple[int, int]) -> tuple[int, int]:
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
    cells: list[tuple[str, str, str]] = []
    for label, count in zip(row_labels, counts):
        percentage = count / total_count
        # Set label to 1 d.p. for percentages < 1%
        perc_label = f"{percentage:.0%}" if percentage >= 0.01 else f"{percentage:.1%}"

        cells.append((label, f"{count:,.0f}", perc_label))

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
            AND v.trip_id IS NOT NULL
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


def _extract_total_seconds(text: str) -> int:
    r"""Parse time taken `text` and calculate total seconds.

    Parameters
    ----------
    text : str
        Expected to contain any number of time groups
        in the format "\d+ (hrs|mins|secs)" e.g.
        "12 mins, 30 secs".
    """
    time_groups = re.findall(r"(\d+)\s?(hrs|mins|secs)", text, re.I)

    if len(time_groups) == 0:
        raise ValueError(f"text doesn't contain time in correct format: '{text}'")

    total_seconds = 0
    for value, units in time_groups:
        value = int(value)
        units = units.lower().strip()

        if units == "secs":
            total_seconds += value
        elif units == "mins":
            total_seconds += value * 60
        elif units == "hrs":
            total_seconds += value * 3600
        else:
            raise ValueError(f"unexpected units '{units}'")

    return total_seconds


def _extract_insert_timings(
    log_file: pathlib.Path, output_folder: pathlib.Path, sd_filter: float = 2
) -> tuple[pd.DataFrame, pathlib.Path]:
    """Parse AVL log file to determine time to insert rows into database.

    Parameters
    ----------
    log_file : pathlib.Path
        Path to log file.
    output_folder : pathlib.Path
        Folder to save output spreadsheet to.
    sd_filter : float, default 2.0
        Number of standard deviations away from mean to
        flag insert times as outliers.

    Returns
    -------
    pd.DataFrame
        DataFrame (index is arbitary) containing columns defined
        in `_InsertTimeColumns`.
    pathlib.Path
        Path to Excel file produced.
    """
    LOG.info('Extracting insert times from log file: "%s"', log_file)
    log_line = re.compile(
        r"^(?P<datetime>\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2})"  # date & time
        r" \[(?P<module>[\s\w\.\d]+)\]"  # Python module
        r" \[(?P<level>[\w\s]+)\]"  # Log level
        r"(?P<message>.*)$",  # log message
        re.I,
    )
    datetime_format = "%d-%m-%Y %H:%M:%S"

    id_pattern = re.compile(r"^\s*Inserting feed into database with ID: (\d+).*$")
    inserting_pattern = re.compile(r"^\s*inserting (\d+) positions.*$", re.I)
    inserted_pattern = re.compile(r"^\s*inserted (\d+) new rows into the database.*$", re.I)
    complete_pattern = re.compile(
        r"^\s*(\d+) complete(?: in ((?:\d+\s?(?:hrs|mins|secs)(?:,\s)?)+)).*$", re.I
    )

    data: list[dict] = []
    row: dict[str, Any] = {}

    with open(log_file, "rt", encoding="utf-8") as file:
        for line in file:
            line_match = log_line.match(line)
            if line_match is None:
                continue

            level = line_match.group("level").lower().strip()
            if level == "info":
                id_match = id_pattern.match(line_match.group("message"))
                if id_match is not None:
                    row[_InsertTimeColumns.ID] = int(id_match.group(1))
                    continue

                inserting_match = inserting_pattern.match(line_match.group("message"))
                if inserting_match is not None:
                    row[_InsertTimeColumns.INSERT_TIME] = dt.datetime.strptime(
                        line_match.group("datetime"), datetime_format
                    )
                    row[_InsertTimeColumns.RESPONSE_POSITIONS] = int(inserting_match.group(1))
                    continue

            elif level != "debug":
                continue

            inserted_match = inserted_pattern.match(line_match.group("message"))
            if inserted_match is not None:
                row[_InsertTimeColumns.POSITIONS] = int(inserted_match.group(1))
                continue

            complete_match = complete_pattern.match(line_match.group("message"))
            if complete_match is not None:
                row[_InsertTimeColumns.COUNT] = int(complete_match.group(1))
                row[_InsertTimeColumns.COMPLETE_TIME] = dt.datetime.strptime(
                    line_match.group("datetime"), datetime_format
                )
                row[_InsertTimeColumns.TIME_TAKEN] = _extract_total_seconds(
                    complete_match.group(2)
                )

                data.append(row)
                row = {}

    if len(row) != 0:
        warnings.warn(
            "log data found without final complete line, "
            "this incomplete row will be included in data"
        )
        data.append(row)
        row = {}

    dataset = pd.DataFrame(data)
    dataset.loc[:, _InsertTimeColumns.CUMM_POSITIONS] = dataset[
        _InsertTimeColumns.POSITIONS
    ].cumsum()

    LOG.info(
        "Log file ranges from initial insert on %s to final complete on %s",
        dataset[_InsertTimeColumns.INSERT_TIME].min(),
        dataset[_InsertTimeColumns.COMPLETE_TIME].max(),
    )

    # Create column for outliers > X s.d.
    mean_time = dataset[_InsertTimeColumns.TIME_TAKEN].mean()
    sd_time = dataset[_InsertTimeColumns.TIME_TAKEN].std()
    dataset.loc[:, _InsertTimeColumns.OUTLIER] = (
        dataset[_InsertTimeColumns.TIME_TAKEN] < (mean_time - sd_filter * sd_time)
    ) | (dataset[_InsertTimeColumns.TIME_TAKEN] > (mean_time + sd_filter * sd_time))
    LOG.info(
        "Flagged %s outliers which are more than %.1f "
        "standard deviations (%.1f) away from the mean (%.1f)",
        dataset[_InsertTimeColumns.OUTLIER].sum(),
        sd_filter,
        sd_filter * sd_time,
        mean_time,
    )

    time_summary = pd.DataFrame(
        {
            "time_taken_seconds": dataset[_InsertTimeColumns.TIME_TAKEN].describe(),
            "time_taken_without_outliers": dataset.loc[
                ~dataset[_InsertTimeColumns.OUTLIER], _InsertTimeColumns.TIME_TAKEN
            ].describe(),
        }
    )

    out_file = output_folder / "database_insert_timings.xlsx"
    # pylint: disable=abstract-class-instantiated
    with pd.ExcelWriter(out_file, engine="openpyxl") as excel:
        dataset.to_excel(excel, sheet_name="DB Insert Times", index=False)
        time_summary.to_excel(excel, sheet_name="Time Summary")
    LOG.info("Written: %s", out_file)

    return dataset, out_file


def _rolling_average(array: np.ndarray, n: int):
    return np.convolve(array, np.ones(n), "valid") / n


def _insert_timings_figure(
    x: np.ndarray,
    y: np.ndarray,
    mean_period: int,
    xlabel: str,
    ylabel: str,
    title: str,
    xdata_date: bool = False,
) -> figure.Figure:
    """Create scatter plot with rolling mean."""
    ax: plt.Axes
    fig, ax = plt.subplots(figsize=(10, 8), layout="tight")

    ax.scatter(x, y, c="C0", s=3)

    # period should be odd so the offset calculation is correct
    if mean_period % 2 == 0:
        raise ValueError(f"mean_period should be an odd number, not {mean_period}")

    offset = mean_period // 2
    ax.plot(
        x[offset:-offset],
        _rolling_average(y, mean_period),
        label=f"Rolling Mean ({mean_period})",
        c="C1",
    )

    if xdata_date:
        locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

    ax.legend()
    ax.set_ylim(0)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.set_title(title)

    return fig


def _plot_insert_timings(timings: pd.DataFrame, output_file: pathlib.Path) -> None:
    """Create scatterplot, with rolling mean, of database insert times.

    Parameters
    ----------
    timings : pd.DataFrame
        DataFrame (index is arbitary) containing columns defined
        in `_InsertTimeColumns`.
    output_file : pathlib.Path
        PDF file to save plot to.

    See Also
    --------
    _extract_insert_timings
    """
    LOG.info("Plotting insert timings")
    timings = timings.loc[~timings[_InsertTimeColumns.OUTLIER]]

    period = 21 if len(timings) > 100 else 3
    ylabel = "Insert Time Taken (seconds)"

    plot_parameters = (
        ("Response ID", _InsertTimeColumns.ID, False),
        ("No. Rows in Database", _InsertTimeColumns.CUMM_POSITIONS, False),
        ("Insert Time", _InsertTimeColumns.INSERT_TIME, True),
    )

    with backend_pdf.PdfPages(output_file.with_suffix(".pdf")) as pdf:
        for label, column, date_fmt in plot_parameters:
            fig = _insert_timings_figure(
                timings[column].values,
                timings[_InsertTimeColumns.TIME_TAKEN].values,
                period,
                label,
                ylabel,
                "AVL Database Row Insert Times Over Time,\n"
                "with Increasing Total Rows in the Database",
                xdata_date=date_fmt,
            )
            pdf.savefig(fig)
            plt.close(fig)

        fig = _insert_timings_figure(
            timings[_InsertTimeColumns.INSERT_TIME].values,
            timings[_InsertTimeColumns.POSITIONS].values,
            period,
            "Insert Time",
            "No. Positions Inserted into Database",
            "AVL Count of Positions in Response Over Time",
            xdata_date=True,
        )
        pdf.savefig(fig)
        plt.close(fig)

    LOG.info("Written: %s", output_file)


def _load_response_positions_counts(conn: sqlalchemy.Connection) -> pd.DataFrame:
    """Extract count of positions per response from database.

    Returns
    -------
    pd.DataFrame
        DataFrame with multilevel index:
        - "Delay (mins)": difference between response and AVL timestamps
        as a range e.g. '0-5'
        - "Response Time": response timestamp

        Columns all contain the number of positions for that response split
        into: "Total Positions", "With Trip ID", "Without Trip ID".
    """
    LOG.info("Extracting count of positions per response")

    seconds_divisor = 300
    stmt = sql.text(
        """SELECT
            meta.timestamp, pos.positions_delay_group,
            pos.n_rows, pos.trip_id, pos.null_trip_id

        FROM (
            SELECT
                metadata_id,
                ceil(position_delay_seconds / :seconds_div) AS positions_delay_group,
                count(*) AS n_rows,
                count(trip_id) AS trip_id,
                count(*) - count(trip_id) AS null_trip_id

            FROM gtfs_rt_vehicle_positions

            GROUP BY metadata_id, positions_delay_group
        ) pos
            LEFT JOIN gtfs_rt_meta meta ON pos.metadata_id = meta.id
        """
    ).bindparams(seconds_div=float(seconds_divisor))
    # Needs to be a float so SQLite doesn't do an int divide

    data = pd.read_sql(stmt, conn)
    data.loc[:, "positions_delay_group"] = data["positions_delay_group"] * (
        seconds_divisor / 60
    )
    data = data.rename(columns={"positions_delay_group": "positions_delay_minutes"})

    groups = [
        (0, 5),
        (5, 10),
        (10, 30),
        (30, np.ceil(np.max(data["positions_delay_minutes"]))),
    ]
    for i, j in groups:
        mask = (data["positions_delay_minutes"] > i) & (data["positions_delay_minutes"] <= j)
        data.loc[mask, "delay_group"] = f"{i:.0f}-{j:.0f}"

    if data["delay_group"].isna().any():
        raise ValueError("some rows have an undefined delay group")

    data.loc[:, "timestamp"] = pd.to_datetime(data["timestamp"])

    data = data.rename(
        columns={
            "delay_group": "Delay (mins)",
            "timestamp": "Response Time",
            "n_rows": "Total Positions",
            "trip_id": "With Trip ID",
            "null_trip_id": "Without Trip ID",
        }
    )

    return data.groupby(["Delay (mins)", "Response Time"])[
        "Total Positions", "With Trip ID", "Without Trip ID"
    ].sum()


def _plot_response_positions_counts(
    data: pd.DataFrame, title: str, mean_period: int, index_date: bool = False
) -> figure.Figure:
    """Plot rolling mean of count of positions per response.

    Parameters
    ----------
    data : pd.DataFrame
        Count of positions, index values used for x-axis and
        name used for labels and all columns are plotted as
        separate lines on the same axes.
    title : str
        Figure title.
    mean_period : int
        Rolling mean period, should be odd.
    index_date : bool, optional
        If True formats the x-axis labels as dates.

    Raises
    ------
    ValueError
        If `mean_period` is odd.
    """
    ax: plt.Axes
    fig, ax = plt.subplots(figsize=(10, 8), layout="tight")

    # Period should be odd so the offset calculation is correct
    if mean_period % 2 == 0:
        raise ValueError(f"mean_period should be an odd number, not {mean_period}")

    offset = mean_period // 2

    for i, column in enumerate(data.columns):
        ax.plot(
            data.index.values[offset:-offset],
            _rolling_average(data[column].values, mean_period),
            label=f"{column.title()} Rolling Mean ({mean_period})",
            c=f"C{i}",
        )

    if index_date:
        locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

    ax.legend()
    ax.set_ylim(0)
    ax.set_ylabel("No. Vehicle Positions per Response")
    ax.set_xlabel(data.index.name)
    ax.set_title(title)

    return fig


def response_position_counts(conn: sqlalchemy.Connection, output_folder: pathlib.Path) -> None:
    """Extract count of positions per response from database and plot.

    Parameters
    ----------
    conn : sqlalchemy.Connection
        Database connection to extract from.
    output_folder : pathlib.Path
        Folder to output files to, creates 2 new files:
        "response_positions_summary.csv", "response_positions_summary.pdf".
    """
    counts = _load_response_positions_counts(conn)
    out_file = output_folder / "response_positions_summary.csv"
    counts.to_csv(out_file)
    LOG.info("Written: %s", out_file)

    base_title = (
        "Comparing the Vehicle Positions Provided per Response\n"
        "Including Rows with Difference from Response Time {delay} minutes"
    )
    mean_period = 21 if len(counts) > 100 else 3

    out_file = out_file.with_suffix(".pdf")
    with backend_pdf.PdfPages(out_file) as pdf:
        fig = _plot_response_positions_counts(
            counts.groupby(level="Response Time").sum(),
            "Comparing the Vehicle Positions Provided per Response\nIncluding All Rows",
            mean_period=mean_period,
            index_date=True,
        )
        pdf.savefig(fig)
        plt.close(fig)

        for name in counts.index.get_level_values("Delay (mins)").unique():
            fig = _plot_response_positions_counts(
                counts.loc[name],
                base_title.format(delay=name),
                mean_period=mean_period,
                index_date=True,
            )
            pdf.savefig(fig)
            plt.close(fig)

    LOG.info("Written: %s", out_file)


def main(parameters: SummaryConfig, output_folder: pathlib.Path):
    db_path = parameters.avl_output_folder / "gtfs-rt.sqlite"
    log_file = parameters.avl_output_folder / "AVL_downloader.log"

    # Setup matplotlib style parameters
    plt.style.use("bmh")
    plt.rcParams["font.family"] = "monospace"
    plt.rcParams["patch.facecolor"] = "white"

    insert_timings, excel_file = _extract_insert_timings(log_file, output_folder)
    _plot_insert_timings(insert_timings, excel_file.with_suffix(".pdf"))

    gtfs_db = database.GTFSRTDatabase(db_path)

    summaries: dict[str, pd.DataFrame] = {}
    with gtfs_db.connect() as conn:
        response_position_counts(conn, output_folder)

        metadata = _load_metadata(conn, *parameters.metadata_ids)
        summaries["Metadata"] = metadata.describe(datetime_is_numeric=True, include="all").T
        del metadata

        full_dataset = _load_data(conn, *parameters.metadata_ids)
        summaries.update(_dataset_summary(full_dataset))

        _compare_timestamps(
            conn,
            *parameters.metadata_ids,
            output_folder / "GTFS-rt_timestamp_distribution.png",
        )

    out_file = output_folder / "GTFS-rt_summary.xlsx"
    # pylint: disable=abstract-class-instantiated
    with pd.ExcelWriter(out_file, engine="openpyxl") as excel:
        for name, data in summaries.items():
            data.columns = data.columns.str.title()
            data.to_excel(excel, sheet_name=name)
    LOG.info("Written: %s", out_file)


def _run() -> None:
    """Load config, setup logger and run `main`."""
    parameters = SummaryConfig.load_yaml(CONFIG_PATH)
    output_folder = parameters.avl_output_folder / _SCRIPT_NAME
    output_folder.mkdir(exist_ok=True)

    tool_details = ctk.ToolDetails(_TOOL_NAME, "0.0.0")
    with ctk.LogHelper(
        _ROOT_NAME,
        tool_details=tool_details,
        log_file=output_folder / (CONFIG_PATH.stem + ".log"),
    ):
        main(parameters, output_folder)


if __name__ == "__main__":
    _run()
