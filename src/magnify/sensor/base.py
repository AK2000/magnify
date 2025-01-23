from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from abc import abstractproperty

import polars

from magnify.types import TimedMeasurement


class BaseSensor(ABC):
    """Base class to implement a sensor."""

    @abstractproperty
    def name(self) -> str:
        """Return the logical name of this sensor.

        See "subscribes" for why we use a string instead of class name

        """
        pass

    @property
    def subscribes(self) -> tuple[str]:
        """Return the names of the data streams this sensor depends on.

        We use string names instead of class names because there could be
        more than one way to obatin the measurements. For instance RAPL
        could come from performance counters or sysfs or cray.
        """
        return ()

    @abstractmethod
    def invoke(
        self,
        *args: float | polars.DataFrame,
    ) -> None | TimedMeasurement:
        """Take a measurement from this sensor."""
        pass
