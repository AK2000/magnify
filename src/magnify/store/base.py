from __future__ import annotations

from abc import ABC
from abc import abstractmethod

from magnify.filters.base import BaseFilter
from magnify.types import TimedMeasurement
from magnify.types import TimedTask


class BaseStore(ABC):
    """Base abstract class to provide an interface for a store."""

    def __init__(
        self,
        filters: list[BaseFilter] = (),
        includes: None | set[str] = None,
    ):
        """Initialize a store."""
        self.filters = filters
        self.includes = includes

    @abstractmethod
    def _put(self, measurements: dict[str, TimedMeasurement]):
        """Internal put method that should be overridden by concrete class."""
        pass

    def put_measurement(self, measurements: dict[str, TimedMeasurement]):
        """Public method for storing measurements to store.

        First applies filters, then calls internal _put method.
        """
        if self.includes is not None:
            measurements = {
                k: measurements[k] for k in self.includes & measurements.keys()
            }
        for f in self.filters:
            measurements = f.apply(measurements)

        self._put(measurements)

    @abstractmethod
    def put_task(self, task: TimedTask):
        """Method to store task information."""
        pass
