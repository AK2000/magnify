from __future__ import annotations

import datetime
from typing import Any
from typing import NamedTuple

import polars

from magnify.client import TaskEvent


class TimedMeasurement(NamedTuple):
    """Type for readings from sensors."""

    time: datetime.datetime
    measurement: float | polars.DataFrame


class TimedTask(NamedTuple):
    """Type for recording task information."""

    task_id: Any
    pid: int
    timestamp: datetime.datetime
    event: TaskEvent
