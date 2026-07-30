from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TypeAlias

    # The logging level for logging e.g. CRITICAL or WARNING
    Severity: TypeAlias = int

from rich import print


def severity_to_color(severity: Severity) -> str:
    if severity > logging.WARNING:
        return "[red]"
    elif severity == logging.WARNING:
        return "[yellow]"
    elif severity <= logging.INFO:
        return ""  # No color
    # No color
    return ""


def print_and_log(logger: logging.Logger, msg: str, severity: Severity = logging.INFO):
    """
    Print to the user and log. Changes print color based on the severity.

    The default logger uses the logging module.
    """
    print(f"{severity_to_color(severity)}{msg}")
    logger.log(severity, msg)
