# -*- coding: utf-8 -*-
"""Functionality for posting messages to MS Teams."""

##### IMPORTS #####

# Built-Ins
import dataclasses
import functools
import logging
import textwrap
import traceback
import warnings
from typing import Optional

# Third Party
import caf.toolkit as ctk
import pydantic
from psutil import _common

try:
    # Third Party
    import pymsteams
except ModuleNotFoundError:
    warnings.warn(
        "optional module pymsteams not installed,"
        " so cannot post messages to MS Teams channel",
        RuntimeWarning,
    )
    pymsteams = None  # pylint: disable=invalid-name

##### CONSTANTS #####

LOG = logging.getLogger(__name__)
TOOL_NAME = "BODS-Extractor Scheduler"
SOURCE_CODE_URL = "https://github.com/Transport-for-the-North/BODS-Extractor"


##### CLASSES & FUNCTIONS #####


@pydantic.dataclasses.dataclass
class TeamsPost:
    """Post messages to Teams channel using webhook.

    Parameters
    ----------
    webhook_url : pydantic.HttpUrl
        Webhook URL for the MS Teams channel to post to.
    tool_name : str
        Name of the tool, used in channel posts.
    tool_version : str
        Version of the tool, used in channel posts.
    source_code_url : pydantic.HttpUrl
        URL for the source code of the tool, used in channel posts.
    allow_missing_module : bool, default False
        If False raises ModuleNotFoundError if 'pymsteams' isn't
        installed. If True will warn with RuntimeWarnings whenever
        trying to post to Teams channel, instead of erroring.
    """

    webhook_url: pydantic.HttpUrl
    tool_name: str
    tool_version: str
    source_code_url: pydantic.HttpUrl

    allow_missing_module: dataclasses.InitVar[bool] = False

    _warning_message_length = 100
    _success_teal = "00dec6"
    _error_red = "ff1100"

    def __post_init__(self, allow_missing_module: bool) -> None:
        if pymsteams is None and not allow_missing_module:
            raise ModuleNotFoundError("pymsteams")

        warnings.warn(
            "'pymsteams' isn't installed, so messages cannot be posted to MS Teams channel",
            RuntimeWarning,
        )

    @pydantic.validator("webhook_url")
    def validate_webhook_host(  # pylint: disable=no-self-argument
        cls, value: pydantic.HttpUrl
    ) -> pydantic.HttpUrl:
        """Validate URL points to office webhook."""
        end = "webhook.office.com"

        if value.host is None:
            raise ValueError("webhook URL invalid, no host given")

        if not value.host.lower().endswith(end):
            raise ValueError(
                "webhook URL has incorrect host, expected host"
                f" to end with '{end}' but got '{value.host}'"
            )

        return value

    @functools.cached_property
    def _tool_details_card(self) -> "pymsteams.cardsection":
        """Create cardsection containing tool information."""
        assert pymsteams is not None, "module 'pymsteams' not loaded"
        section = pymsteams.cardsection()

        section.title("Tool Details")
        section.addFact("Version", self.tool_version)
        section.addFact(
            "Source code",
            f"[{self.tool_name}]({self.source_code_url})",
        )
        return section

    @functools.cached_property
    def _system_info_card(self) -> "pymsteams.cardsection":
        """Create cardsection containing PC / VM system information."""
        assert pymsteams is not None, "module 'pymsteams' not loaded"
        info = ctk.SystemInformation.load()

        section = pymsteams.cardsection()
        section.title("System Information")
        section.text("Information about the PC running the scheduler.")

        for name in info.__dataclass_fields__:  # pylint: disable=no-member
            value = getattr(info, name)
            if value is None:
                value = "unknown"
            elif name == "total_ram":
                value = _common.bytes2human(value)
            name = name.replace("_", " ").title()

            for old in ("Pc", "Cpu", "Ram"):
                name = name.replace(old, old.upper())

            section.addFact(name, value)

        return section

    def post(
        self, title: str, message: str, color: str = "ffffff", summary: Optional[str] = None
    ) -> None:
        """Post a message to the Teams channel.

        Parameters
        ----------
        title : str
            Title of the post.
        message : str
            Main body of the post, should use markdown formatting.
        color : str,  default "ffffff" (white)
            Color of the post header bar.
        summary : str, optional
            Optional shorter summary message.
        """
        if pymsteams is None:
            title = textwrap.shorten(title, width=self._warning_message_length)
            message = textwrap.shorten(message, width=self._warning_message_length)
            if summary is not None:
                summary = textwrap.shorten(summary, width=self._warning_message_length)

            warnings.warn(
                f"cannot post teams message:\ntitle: {title!r}"
                f"\nsummary: {summary!r}\nmessage: {message!r}",
                RuntimeWarning,
            )
            return

        card = pymsteams.connectorcard(self.webhook_url)
        card.title(title)
        card.color(color)

        if summary is not None:
            card.summary(summary)

        card.text(message.strip() + "\n\n")

        card.addSection(self._tool_details_card)
        card.addSection(self._system_info_card)
        card.send()

    def post_success(self, message: str) -> None:
        """Post a message, using the default success title / color.

        Parameters
        ----------
        message : str
            Main body of the post, should use markdown formatting.
        """
        self.post(
            title=f"{TOOL_NAME} - Output",
            message=message,
            color=self._success_teal,
        )

    def post_error(self, message: str, exc: BaseException) -> None:
        """Post an error message, using the default error title / color.

        Parameters
        ----------
        message : str
            Main body of the post, should use markdown formatting.
        exc : BaseException
            Python exception, used for detailed error traceback in post.
        """
        summary = f"{message}\n\n{exc.__class__.__name__}: {exc}"

        trace = "".join(traceback.format_exception(exc))
        text = f"{summary}\n\n```python{trace}```"

        self.post(
            title=f"{TOOL_NAME} - Error",
            message=text,
            color=self._error_red,
            summary=summary,
        )
