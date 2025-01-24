from __future__ import annotations

import datetime
import os

import performance_features
import polars
import psutil

from magnify.sensor.base import BaseSensor
from magnify.types import TimedMeasurement

# Default events to use to predict energy on an intel x86 machine
_DEFAULT_EVENTS = ['UNHALTED_CORE_CYCLES', 'LLC_MISSES', 'INSTRUCTION_RETIRED']


class PerfSensor(BaseSensor):
    """A sensor that uses performance features to read hardware counters."""

    def __init__(self, *, events: list[str] = _DEFAULT_EVENTS):
        """Initialize the sensor."""
        self.events = events
        self.profilers = {}
        self.username = os.getlogin()

    @property
    def name(self) -> str:
        """Return the logical name of this sensor."""
        return 'perf'

    def measure_resource_utilization(
        self,
        proc: psutil.Process,
        profiler: performance_features.Profiler,
    ):
        """Measure the performance counters from one process."""
        d = {}
        d['pid'] = proc.info['pid']
        d['ppid'] = proc.info['ppid']
        d['name'] = proc.info['name']

        event_counters = profiler.read_events()
        profiler.reset_events()  # How much overhead does this add?
        event_counters = profiler._Profiler__.format_data(
            [
                event_counters,
            ],
        )
        for i, event in enumerate(self.events):
            d[event] = event_counters[0][i]

        return d

    def invoke(self):
        """Measure the performance counters of all processes."""
        process_info = []
        for proc in psutil.process_iter(['pid', 'username', 'name', 'ppid']):
            if (
                proc.info['username'] != self.username
                or proc.info['pid'] == os.getpid()
            ):
                continue

            try:
                profiler = performance_features.Profiler(
                    pid=proc.info['pid'],
                    events_groups=[[e] for e in self.events],
                )
                profiler._Profiler__initialize()
                profiler.reset_events()
                profiler.enable_events()
                self.profilers[proc.info['pid']] = profiler
            except Exception:
                # TODO: Figure out proper exceptions that could be raised?
                self.profilers[proc.info['pid']] = None
            else:
                pass

            profiler = self.profilers.get(proc.info['pid'])

            try:
                d = self.measure_resource_utilization(proc, profiler)
                process_info.append(d)
            except Exception:
                # TODO: Figure out proper exceptions that could be raised?
                pass

        return TimedMeasurement(
            datetime.datetime.now(datetime.UTC),
            polars.from_dicts(process_info),
        )
