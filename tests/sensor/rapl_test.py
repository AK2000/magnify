from __future__ import annotations

import pytest

from magnify.sensor.rapl import RaplSysfsSensor

ENERGY_1 = 100
ENERGY_2 = 200
ENERGY_RANGE = 300


@pytest.fixture
def mock_rapl_filesystem(fs):
    fs.create_file('/sys/devices/system/cpu/present', contents='0-15')
    for i in range(16):
        fs.create_file(
            f'/sys/devices/system/cpu/cpu{i}/topology/physical_package_id',
            contents=str(i // 8),
        )

    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:0/name',
        contents='package-0\n',
    )
    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:1/name',
        contents='package-1\n',
    )
    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj',
        contents=str(ENERGY_1),
    )
    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:1/energy_uj',
        contents=str(ENERGY_2),
    )
    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj',
        contents=str(ENERGY_RANGE),
    )
    fs.create_file(
        '/sys/class/powercap/intel-rapl/intel-rapl:1/max_energy_range_uj',
        contents=str(ENERGY_RANGE),
    )

    return fs


def test_create_rapl(mock_rapl_filesystem):
    sensor = RaplSysfsSensor()
    assert sensor.name == 'rapl'
    assert sensor.subscribes == ()
    assert sensor.get_energy_range() == ENERGY_RANGE


def test_rapl_internal_measure(mock_rapl_filesystem):
    sensor = RaplSysfsSensor()

    devices = sensor._measure()

    assert len(devices) == 2  # noqa: PLR2004
    assert devices['package-0']['energy'] == ENERGY_1
    assert devices['package-1']['energy'] == ENERGY_2


def test_rapl_invoke(mock_rapl_filesystem):
    sensor = RaplSysfsSensor()

    timed_measurement = sensor.invoke()

    assert timed_measurement.measurement == 0


def test_rapl_dram(mock_rapl_filesystem):
    pass


def test_rapl_overflow(mock_rapl_filesystem):
    pass
