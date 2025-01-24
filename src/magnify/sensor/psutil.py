from __future__ import annotations

import datetime
import os
from typing import Any

import polars
import psutil

from magnify.sensor.base import BaseSensor
from magnify.types import TimedMeasurement

DEFAULT_READINGS = [
    'cpu_percent',
    'memory_percent',
    'nice',
    'name',
    'pid',
    'ppid',
    'status',
]


class PsutilSensor(BaseSensor):
    """Read measurements from /sys/proc using psutil library."""

    def __init__(self, *, metrics: list[str] = DEFAULT_READINGS):
        """Initialize sensor to read metrics from psutil.

        Args:
            metrics: metrics to read. Metrics must be able to be read from
                     proc.as_dict
        """
        self.metrics = metrics
        self.username = os.getlogin()

    @property
    def name(self) -> str:
        """Return the logical name of this sensor."""
        return 'psutil'

    def measure_resource_utilization(
        self,
        proc: psutil.Process,
    ) -> dict[Any]:
        """Record metrics of a single process into a dict."""
        d = {}
        d.update(
            {
                'psutil_process_' + str(k): v
                for k, v in proc.as_dict().items()
                if k in self.metrics
            },
        )
        d['psutil_process_memory_virtual'] = proc.memory_info().vms
        d['psutil_process_memory_resident'] = proc.memory_info().rss
        d['psutil_process_time_user'] = proc.cpu_times().user
        d['psutil_process_time_system'] = proc.cpu_times().system
        try:
            d['psutil_process_disk_write'] = proc.io_counters().write_chars
            d['psutil_process_disk_read'] = proc.io_counters().read_chars
        except Exception:
            # occasionally pid temp files that hold this information are
            # unavailable to be read so set to zero
            d['psutil_process_disk_write'] = 0
            d['psutil_process_disk_read'] = 0

        return d

    def invoke(self):
        """Record the information of all user processes."""
        process_info = []
        for proc in psutil.process_iter(['pid', 'username', 'name', 'ppid']):
            if proc.info['username'] != self.username:
                continue

            d = self.measure_resource_utilization(proc)
            process_info.append(d)

        return TimedMeasurement(
            datetime.datetime.now(datetime.UTC),
            polars.from_dicts(process_info),
        )
