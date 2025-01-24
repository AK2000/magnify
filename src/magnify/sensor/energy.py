from __future__ import annotations

import datetime

import numpy as np
import polars
from sklearn.linear_model import ElasticNet

from magnify.sensor.base import BaseSensor
from magnify.types import TimedMeasurement

# Default events to use to predict energy on an intel x86 machine
_DEFAULT_ENERGY_EVENTS = [
    'UNHALTED_CORE_CYCLES',
    'LLC_MISSES',
    'INSTRUCTION_RETIRED',
]


class PerfEnergySensor(BaseSensor):
    """Sensor to measure energy based on RAPL plus performance counters."""

    def __init__(
        self,
        *,
        eps: float = 0.2,
        alpha: float = 0.1,
        k: int = 50,
        min_samples: int = 10,
        events: list[str] = _DEFAULT_ENERGY_EVENTS,
    ):
        """Initialize sensor to create rolling energy models."""
        self.model = None
        self.training_data = []  # TODO: Switch to circular buffer
        self.rolling_error = 0.0
        self.eps = eps
        self.alpha = alpha
        self.k = k
        self.min_samples = min_samples
        self.events = events
        self.prev_timestamp: datetime.datetime = datetime.datetime.now(
            datetime.UTC,
        )

    @property
    def name(self) -> str:
        """Return the logical name of this sensor."""
        return 'energy'

    @property
    def subscribes(self) -> tuple[str]:
        """Return the names of the data streams this sensor depends on."""
        return ('perf', 'rapl')

    def _train_model(self) -> None:
        """Retrain the model with the existing data."""
        self.model = ElasticNet(random_state=0, positive=True)
        self.model.fit(
            np.vstack([X for X, _ in self.training_data]),
            np.array([Y for _, Y in self.training_data]),
        )
        self.rolling_error = 0

    def invoke(self, perf: polars.DataFrame, rapl: float) -> TimedMeasurement:
        """Calculate energy from perf counters and Rapl measurements."""
        timestamp: datetime.datetime = datetime.datetime.now(datetime.UTC)
        duration = (timestamp - self.prev_timestamp).total_seconds()
        self.prev_timestamp = timestamp
        features = perf.select(polars.sum(self.events) / duration).to_numpy()
        power = rapl / duration

        # Only keep the last k training examples
        self.training_data.append((features.flatten(), power))
        self.training_data = self.training_data[-self.k :]
        if len(self.training_data) < self.min_samples:
            # Figure out if we need to keep samples before model is trained?
            return None

        if self.model is None:
            self._train_model()

        predicted_power = self.model.predict(features)
        error = np.abs(predicted_power - power) / power
        self.rolling_error = ((1 - self.alpha) * self.rolling_error) + (
            self.alpha * error
        )
        if self.rolling_error > self.eps:
            # Retrain model
            self._train_model()

        scale = (power - self.model.intercept_) / (
            predicted_power - self.model.intercept_
        )
        features = perf.select(polars.col(self.events) / duration).to_numpy()
        power = (features @ self.model.coef_) * scale
        process_df = perf.select(
            polars.col(['pid', 'ppid']),
            polars.Series(name='power', values=power),
        )
        return TimedMeasurement(timestamp, process_df)
