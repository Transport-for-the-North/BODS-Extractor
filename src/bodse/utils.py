# -*- coding: utf-8 -*-
"""Misc utility functions."""

##### IMPORTS #####

# Built-Ins
import datetime as dt
import logging

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
