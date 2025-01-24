from __future__ import annotations

import datetime
import os
import re

from magnify.sensor.base import BaseSensor
from magnify.types import TimedMeasurement


class RaplSysfsSensor(BaseSensor):
    """Monitor energy using sysfs files created by intel RAPL.

    In a large part derived from PyRAPL[https://github.com/powerapi-ng/pyRAPL/tree/master]
    Which is under an MIT License:

    Copyright (c) 2018, INRIA
    Copyright (c) 2018, University of Lille

    All rights reserved.
    """

    def __init__(self):
        """Initialize a RAPL monitor by reading sysfs."""
        self._socket_ids = self.get_socket_ids()
        self._pkg_files = self.get_pkg_files()

        try:
            self._dram_files = self.get_dram_files()
        except Exception:
            self._dram_files = None

        self.max_energy = self.get_energy_range()
        self.prev_reading = self._measure()

    def cpu_ids(self) -> list[int]:
        """Return the cpu id of this machine."""
        with open('/sys/devices/system/cpu/present') as api_file:
            cpu_id_tmp = re.findall('\d+|-', api_file.readline().strip())
            cpu_id_list = []
            for i in range(len(cpu_id_tmp)):
                if cpu_id_tmp[i] == '-':
                    for cpu_id in range(
                        int(cpu_id_tmp[i - 1]) + 1,
                        int(cpu_id_tmp[i + 1]),
                    ):
                        cpu_id_list.append(int(cpu_id))
                else:
                    cpu_id_list.append(int(cpu_id_tmp[i]))

        return cpu_id_list

    def get_socket_ids(self) -> list[int]:
        """Return cpu socket id present on the machine."""
        socket_id_list = []
        for cpu_id in self.cpu_ids():
            with open(
                '/sys/devices/system/cpu/cpu'
                + str(cpu_id)
                + '/topology/physical_package_id',
            ) as api_file:
                socket_id_list.append(int(api_file.readline().strip()))
        return list(set(socket_id_list))

    def _get_socket_directory_names(self) -> list[tuple[str, int]]:
        """:return (str, int): directory name, rapl_id"""

        def add_to_result(directory_info, result):
            """If the directory can be added to the result list, add it."""
            dirname, _ = directory_info
            with open(dirname + '/name') as f_name:
                pkg_str = f_name.readline()

            if 'package' not in pkg_str:
                return
            package_id = int(pkg_str[:-1].split('-')[1])

            if (
                self._socket_ids is not None
                and package_id not in self._socket_ids
            ):
                return
            result.append((package_id, *directory_info))

        rapl_id = 0
        result_list = []
        while os.path.exists(
            '/sys/class/powercap/intel-rapl/intel-rapl:' + str(rapl_id),
        ):
            dirname = '/sys/class/powercap/intel-rapl/intel-rapl:' + str(
                rapl_id,
            )
            add_to_result((dirname, rapl_id), result_list)
            rapl_id += 1

        if len(result_list) != len(self._socket_ids):
            raise Exception("Can't RAPL files for all sockets.")

        # sort the result list
        result_list.sort(key=lambda t: t[0])
        # return info without socket ids
        return [(t[1], t[2]) for t in result_list]

    def get_pkg_files(self):
        """Get a list of all the sysfs files corresponding to Package RAPL."""
        directory_name_list = self._get_socket_directory_names()

        rapl_files = []
        for directory_name, _ in directory_name_list:
            rapl_files.append(open(directory_name + '/energy_uj'))  # noqa: SIM115
        return rapl_files

    def get_energy_range(self):
        """Get the maximum value of RAPL counter before looping."""
        filename = (
            '/sys/class/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj'
        )
        with open(filename) as f:
            return float(f.readline())

    def get_dram_files(self):
        """Get a list of all the sysfs files corresponding to DRAM RAPL."""
        directory_name_list = self._get_socket_directory_names()

        def get_dram_file(
            socket_directory_name,
            rapl_socket_id,
        ):
            rapl_device_id = 0
            while os.path.exists(
                socket_directory_name
                + '/intel-rapl:'
                + str(rapl_socket_id)
                + ':'
                + str(rapl_device_id),
            ):
                dirname = (
                    socket_directory_name
                    + '/intel-rapl:'
                    + str(rapl_socket_id)
                    + ':'
                    + str(rapl_device_id)
                )
                with open(dirname + '/name') as f_device:
                    if f_device.readline() == 'dram\n':
                        return open(dirname + '/energy_uj')

                rapl_device_id += 1
            raise Exception("RAPL can't open required files.")

        rapl_files = []
        for socket_directory_name, rapl_socket_id in directory_name_list:
            rapl_files.append(
                get_dram_file(socket_directory_name, rapl_socket_id),
            )

        return rapl_files

    @property
    def name(self):
        """Return the logical name of this sensor."""
        return 'rapl'

    def _measure(self):
        devices = {
            f'package-{i}': {'energy': 0, 'dram': 0} for i in self._socket_ids
        }
        for i in range(len(self._pkg_files)):
            device_file = self._pkg_files[i]
            device_file.seek(0, 0)
            devices[f'package-{self._socket_ids[i]}']['energy'] = float(
                device_file.readline(),
            )

            if self._dram_files is not None:
                dram_file = self._dram_files[i]
                dram_file.seek(0, 0)
                devices[f'package-{self._socket_ids[i]}']['dram'] = float(
                    dram_file.readline(),
                )

        return devices

    def invoke(self):
        """Read the RAPL files and create diff between previous."""
        devices = self._measure()
        result = {
            k1: {
                k2: (devices[k1][k2] - self.prev_reading[k1][k2])
                % self.max_energy
                for k2 in devices[k1]
            }
            for k1 in devices
        }
        total = sum(device['energy'] for device in result.values())
        self.prev_reading = devices
        return TimedMeasurement(datetime.datetime.now(datetime.UTC), total)
