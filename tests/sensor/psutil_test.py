from __future__ import annotations

from unittest import mock

from magnify.sensor.psutil import PsutilSensor


def test_create_psutil_sensor():
    # We must import this within the test so the import can be patched

    sensor = PsutilSensor()
    assert sensor.name == 'psutil'
    assert sensor.subscribes == ()


def test_measure_resource_utilization():
    sensor = PsutilSensor()
    proc = mock.MagicMock()
    d = sensor.measure_resource_utilization(proc)

    assert 'psutil_process_time_user' in d


def test_invoke():
    sensor = PsutilSensor()
    timed_measurement = sensor.invoke()
    assert len(timed_measurement.measurement) > 0
