from __future__ import annotations

import pathlib

import pytest

from magnify.config import FilterConfig
from magnify.config import MonitorConfig
from magnify.config import SensorConfig
from magnify.config import StoreConfig
from magnify.filters.basic import Downsample
from magnify.sensor.rapl import RaplSysfsSensor
from magnify.store.file import FileStore


@pytest.mark.parametrize(
    ('kind', 'expected'),
    (
        ('raplsysfs', RaplSysfsSensor),
        ('RAPLSysfs', RaplSysfsSensor),
        ('RaplSysfsSensor', RaplSysfsSensor),
        ('magnify.sensor.rapl.RaplSysfsSensor', RaplSysfsSensor),
    ),
)
def test_get_sensor_type(kind: str, expected: type) -> None:
    config = SensorConfig(kind=kind)
    assert config.get_sensor_type() == expected


@pytest.mark.parametrize(
    ('kind', 'expected'),
    (('file', FileStore), ('FileStore', FileStore)),
)
def test_get_store_type(kind: str, expected: type) -> None:
    config = StoreConfig(kind=kind)
    assert config.get_store_type() == expected


@pytest.mark.parametrize(
    ('kind', 'expected'),
    (
        ('downsample', Downsample),
        ('magnify.filters.basic.Downsample', Downsample),
    ),
)
def test_get_filter_type(kind: str, expected: type) -> None:
    config = FilterConfig(kind=kind)
    assert config.get_filter_type() == expected


def test_monitor_config_from_toml(tmp_path: pathlib.Path) -> None:
    config_file = tmp_path / 'config.toml'
    store_dir = tmp_path / 'cache'

    with open(config_file, 'w') as f:
        f.write(f'''\
[[sensors]]
kind = "rapl"

[[stores]]
kind = "file"
includes = [ "rapl" ]
options = {{ dir_name = "{store_dir}" }}
[[stores.filters]]
kind = "Downsample"
options = {{ k = 5 }}
''')

    config = MonitorConfig.from_toml(config_file)
    assert len(config.sensors) == 1
    assert len(config.stores) == 1

    assert config.sensors[0].kind == 'rapl'

    assert config.stores[0].kind == 'file'
    # assert config.stores[0].includes == 'rapl'
    assert config.stores[0].options['dir_name'] == str(store_dir)
