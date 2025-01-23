from __future__ import annotations

import datetime
from unittest import mock

import polars
import pytest

from magnify.store.file import FileStore
from magnify.types import TimedMeasurement


@pytest.fixture
def fake_measurement():
    measurement = {
        'perf': TimedMeasurement(
            datetime.datetime.now(datetime.UTC),
            polars.DataFrame({'test': [1, 2], 'data': [3, 4]}),
        ),
        'rapl': TimedMeasurement(datetime.datetime.now(datetime.UTC), 100),
    }
    return measurement


NUM_DEFAULT_FILES = 1


@pytest.fixture
def mock_filter():
    f = mock.Mock()
    f.apply.side_effect = lambda x: x
    return f


def test_create_filestore(tmpdir, fake_measurement):
    store = FileStore(tmpdir)
    assert len(tmpdir.listdir()) == NUM_DEFAULT_FILES

    store.put_measurement(fake_measurement)

    sensors = 2
    assert len(tmpdir.listdir()) == (NUM_DEFAULT_FILES + sensors)


def test_filestore_includes(tmpdir, fake_measurement):
    store = FileStore(
        tmpdir,
        includes={
            'perf',
        },
    )
    assert len(tmpdir.listdir()) == NUM_DEFAULT_FILES

    store.put_measurement(fake_measurement)

    sensors = 1
    assert len(tmpdir.listdir()) == NUM_DEFAULT_FILES + sensors


def test_filestore_filter(tmpdir, fake_measurement, mock_filter):
    store = FileStore(
        tmpdir,
        filters=[
            mock_filter,
        ],
    )
    assert len(tmpdir.listdir()) == NUM_DEFAULT_FILES

    store.put_measurement(fake_measurement)

    assert mock_filter.apply.called
