from __future__ import annotations

from abc import ABC
from abc import abstractmethod

from magnify.types import TimedMeasurement


class BaseFilter(ABC):
    """Base class to apply a filter to a measurement before storing it."""

    def __init__(self, to: None | set[str] = None):
        """Initialize a filter to only apply to the specified streams."""
        self.to = to

    @abstractmethod
    def _apply(
        self,
        m: dict[str, TimedMeasurement],
    ) -> dict[str, TimedMeasurement]:
        pass

    def apply(
        self,
        measurements: dict[str, TimedMeasurement],
    ) -> dict[str, TimedMeasurement]:
        """Filter a measurement based and return a new measurement."""
        if self.to is None:
            filtered = measurements
        else:
            filtered = {
                k: measurements[k] for k in measurements.keys() & self.to
            }
        filtered = self._apply(filtered)

        if self.to is None:
            return filtered

        measurements = {
            k: measurements[k] for k in measurements if k not in self.to
        }
        measurements.update(filtered)
        return measurements


class Downsample(BaseFilter):
    """Filter to downsample a measurement based on number."""

    def __init__(self, k: int, to: None | set[str] = None):
        """Initialize downsampling to take 1 of k measurements."""
        super().__init__(to)

        self.k = k
        self.current = 0

    def _apply(
        self,
        measurements: dict[str, TimedMeasurement],
    ) -> dict[str, TimedMeasurement]:
        try:
            if self.current == 0:
                return measurements
            else:
                return {}
        finally:
            self.current = (self.current + 1) % self.k
