from __future__ import annotations

import datetime
from unittest import mock

import pytest

from magnify.monitor import MagnifyMonitor
from magnify.types import TimedMeasurement


@pytest.fixture
def mock_sensor():
    sensor = mock.Mock()
    sensor.name = 'measurement1'
    sensor.subscribes = ()
    sensor.invoke.return_value = TimedMeasurement(
        datetime.datetime.now(datetime.UTC),
        0,
    )
    return sensor


@pytest.fixture
def mock_sensor_2():
    sensor = mock.Mock()
    sensor.name = 'measurement2'
    sensor.subscribes = ('measurement1',)
    sensor.invoke.return_value = TimedMeasurement(
        datetime.datetime.now(datetime.UTC),
        0,
    )
    return sensor


@pytest.fixture
def mock_store():
    store = mock.Mock()
    store.put_measurement.return_value = None
    store.put_task.return_value = None
    return store


def test_process_task(mock_store):
    monitor = MagnifyMonitor([], [mock_store])

    task_msg = {
        'task_id': 'test_func',
        'pid': 2,
        'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
        'event': 1,
    }
    monitor.process_task(task_msg)
    mock_store.put_task.assert_called_once()


def test_task_listener(mock_store):
    pass


def test_take_measurment(mock_sensor):
    monitor = MagnifyMonitor([mock_sensor], [])
    monitor.take_measurement()
    mock_sensor.invoke.assert_called_once()


def test_sensor_subscribe(mock_sensor, mock_sensor_2):
    monitor = MagnifyMonitor([mock_sensor, mock_sensor_2], [])
    monitor.take_measurement()

    mock_sensor.invoke.assert_called_once()
    mock_sensor_2.invoke.assert_called_once_with(0)


def test_sensor_missing(mock_sensor, mock_sensor_2):
    mock_sensor.invoke.return_value = None
    monitor = MagnifyMonitor([mock_sensor, mock_sensor_2], [])
    monitor.take_measurement()

    mock_sensor_2.invoke.assert_not_called()


def test_run(mock_sensor, mock_store):
    pass
