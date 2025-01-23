from __future__ import annotations

import os
from unittest import mock

import pytest


@pytest.fixture
def mock_perfcounter_profiler():
    profiler = mock.Mock()
    profiler.read_events.return_value = [100]
    profiler._Profiler__ = mock.Mock()
    profiler._Profiler__.format_data = lambda x: x
    return profiler


@pytest.fixture
def mock_performance_features_import(mock_perfcounter_profiler):
    """Patch the import so we can test without performance_features library."""
    import sys

    module = type(sys)('performance_features')
    module.Profiler = mock.Mock()
    module.Profiler.return_value = mock_perfcounter_profiler
    sys.modules['performance_features'] = module


def test_create_perf_sensor(mock_performance_features_import):
    # We must import this within the test so the import can be patched
    from magnify.sensor.perf import PerfSensor

    sensor = PerfSensor(events=['LLC_MISSES'])
    assert sensor.name == 'perf'
    assert sensor.subscribes == ()


def test_measure_resource_utilization(
    mock_performance_features_import,
    mock_perfcounter_profiler,
):
    from magnify.sensor.perf import PerfSensor

    sensor = PerfSensor(events=['LLC_MISSES'])

    proc = mock.MagicMock()
    d = sensor.measure_resource_utilization(proc, mock_perfcounter_profiler)

    assert 'LLC_MISSES' in d


def test_invoke(mock_performance_features_import):
    from magnify.sensor.perf import PerfSensor

    sensor = PerfSensor(events=['LLC_MISSES'])

    mock_process = mock.Mock()
    mock_process.info = {
        'pid': 1,
        'username': os.getlogin(),
        'name': 'test',
        'ppid': 0,
    }

    with mock.patch('psutil.process_iter') as mock_psutil:
        mock_psutil.return_value = iter([mock_process])
        timed_measurement = sensor.invoke()

        assert len(timed_measurement.measurement) == 1
