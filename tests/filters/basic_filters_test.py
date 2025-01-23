from __future__ import annotations

import datetime

import polars
import pytest

from magnify.filters.basic import Downsample
from magnify.types import TimedMeasurement


@pytest.fixture
def fake_measurement_generator():
    class MeasurementGenerator:
        def get(self):
            measurement = {
                'perf': TimedMeasurement(
                    datetime.datetime.now(datetime.UTC),
                    polars.DataFrame({'test': [1, 2], 'data': [3, 4]}),
                ),
                'rapl': TimedMeasurement(
                    datetime.datetime.now(datetime.UTC),
                    100,
                ),
            }
            return measurement

    return MeasurementGenerator()


def test_downsample(fake_measurement_generator):
    f = Downsample(k=5)
    for i in range(10):
        m = fake_measurement_generator.get()
        copied = m.copy()
        filtered = f.apply(m)

        if i % 5 == 0:
            assert len(filtered) > 0
        else:
            assert len(filtered) == 0
            assert copied == m, 'Filter changed original measurement'


def test_downsample_to(fake_measurement_generator):
    f = Downsample(
        k=5,
        to={
            'rapl',
        },
    )
    for i in range(10):
        m = fake_measurement_generator.get()
        filtered = f.apply(m)

        if i % 5 == 0:
            assert len(filtered) == 2  # noqa: PLR2004
        else:
            assert len(filtered) == 1
