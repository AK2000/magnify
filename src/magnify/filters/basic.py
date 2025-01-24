from __future__ import annotations

from magnify.filters.base import BaseFilter
from magnify.types import TimedMeasurement


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
