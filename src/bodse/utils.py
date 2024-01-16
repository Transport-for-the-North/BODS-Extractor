# -*- coding: utf-8 -*-
"""Misc utility functions."""

##### IMPORTS #####

# Built-Ins
import collections
import datetime as dt
import logging

# Third Party
import numpy as np
import pandas as pd

##### CONSTANTS #####

LOG = logging.getLogger(__name__)


##### CLASSES & FUNCTIONS #####


def readable_seconds(seconds: int, inc_minutes: bool = True) -> str:
    """Convert to readable string with varying resolution.

    String contains only the largest 2 units from hours, minutes
    and seconds e.g. "10 hrs, 16 mins" or "37 mins, 42 secs".
    """
    seconds = int(seconds)
    if seconds <= 0:
        return f"{seconds} secs"

    readable = []

    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        readable.append(f"{hours} hrs")

    if minutes > 0 and inc_minutes:
        readable.append(f"{minutes} mins")

    if seconds > 0 and hours == 0:
        readable.append(f"{seconds} secs")

    return ", ".join(readable)


def readable_timedelta(delta: dt.timedelta) -> str:
    """Convert to readable string with varying resolution.

    String contains only the largest 2 units from days, hours,
    minutes and seconds e.g. "3 days, 2 hrs", "10 hrs, 16 mins"
    or "37 mins, 42 secs".
    """
    readable = []
    if delta.days > 0:
        readable.append(f"{delta.days:,} days")

    if delta.seconds > 0:
        readable.append(readable_seconds(delta.seconds, delta.days == 0))

    return ", ".join(readable)


def merge_indicator_check(
    data: pd.DataFrame, left_name: str, right_name: str, indicator_column: str = "_merge"
) -> None:
    """Validate a pandas DataFrame merge is an inner join.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame after merge, requires an indicator column.
    left_name, right_name : str
        Name of the left and right datasets, used for error message.
    indicator_column : str, default "_merge"
        Name of the indicator column.

    Raises
    ------
    ValueError
        If `indicator_column` contains values other than 'both'.

    See Also
    --------
    pd.merge
    """
    merge: dict[str, int] = collections.defaultdict(
        lambda: 0, zip(*np.unique(data[indicator_column], return_counts=True))  # type: ignore
    )
    if set(merge.keys()) == {"both"}:
        return None

    msg = (
        f"{merge['both']:,} rows in both datasets\n"
        f"{merge['left_only']:,} rows in {left_name} only\n"
        f"{merge['right_only']:,} rows in {right_name} only\n"
        f"{len(data)} rows in full dataset"
    )

    raise ValueError(f"merging {left_name} and {right_name} is not complete:\n{msg}")
