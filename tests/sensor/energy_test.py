from __future__ import annotations

import datetime
import random
from unittest import mock

import numpy as np
import polars
import pytest

from magnify.sensor.energy import _DEFAULT_ENERGY_EVENTS
from magnify.sensor.energy import PerfEnergySensor


@pytest.fixture
def fake_perf_data_factory():
    # Ensure tests are exactly repeatable
    random.seed(0)

    class PerfDataFactory:
        def get(self):
            perf_data = {'pid': [], 'ppid': []}
            for event in _DEFAULT_ENERGY_EVENTS:
                perf_data[event] = []
            for pid in range(1, 33):
                perf_data['pid'].append(pid)
                perf_data['ppid'].append(pid // 2)
                for event in _DEFAULT_ENERGY_EVENTS:
                    perf_data[event].append(random.randrange(1000))

            return polars.DataFrame(perf_data)

    return PerfDataFactory()


@pytest.fixture
def fake_perf_rapl_data_factory(fake_perf_data_factory):
    class RaplDataFactory:
        def get(self):
            perf = fake_perf_data_factory.get()
            features = (
                perf.select(polars.sum(_DEFAULT_ENERGY_EVENTS))
                .to_numpy()
                .flatten()
            )

            weights = np.array([0.001, 0.00025, 0.0005])
            power = weights @ features + 100
            return perf, power

    return RaplDataFactory()


@pytest.fixture
def mock_datetime():
    class Counter:
        def __init__(self, init):
            self.current = init

        def __iter__(self):
            return self

        def __next__(self):  # Python 2: def next(self)
            self.current += datetime.timedelta(seconds=1)
            return self.current

    init = datetime.datetime.now(datetime.UTC)
    with mock.patch('datetime.datetime') as m:
        # This is where the magic happens!
        m.now.side_effect = Counter(init)
        yield m


def test_create_energy():
    sensor = PerfEnergySensor()
    assert sensor.name == 'energy'
    assert sensor.subscribes == ('perf', 'rapl')


def test_train_model(fake_perf_rapl_data_factory):
    sensor = PerfEnergySensor()
    for _ in range(10):
        perf, rapl = fake_perf_rapl_data_factory.get()
        features = (
            perf.select(polars.sum(_DEFAULT_ENERGY_EVENTS))
            .to_numpy()
            .flatten()
        )

        # assume a 1 second interval so rapl is equivalent to power
        sensor.training_data.append((features, rapl))

    sensor._train_model()
    assert sensor.model is not None
    assert sensor.rolling_error == 0

    # Test that the model works as expected
    perf, rapl = fake_perf_rapl_data_factory.get()
    assert (
        sensor.model.predict(
            perf.select(polars.col(_DEFAULT_ENERGY_EVENTS)).to_numpy(),
        )
        >= 0
    ).all()


def test_invoke_model(fake_perf_rapl_data_factory, mock_datetime):
    sensor = PerfEnergySensor()

    for i in range(sensor.min_samples - 1):
        assert len(sensor.training_data) == i

        perf, rapl = fake_perf_rapl_data_factory.get()

        assert sensor.invoke(perf, rapl) is None

    assert len(sensor.training_data) == sensor.min_samples - 1
    perf, rapl = fake_perf_rapl_data_factory.get()
    timed_measurement = sensor.invoke(perf, rapl)
    assert len(sensor.training_data) == sensor.min_samples
    assert sensor.model is not None
    assert timed_measurement is not None

    for _ in range(2 * sensor.k):
        perf, rapl = fake_perf_rapl_data_factory.get()
        assert sensor.invoke(perf, rapl) is not None
        assert len(sensor.training_data) <= sensor.k


def test_invoke_retrain_model():
    pass
