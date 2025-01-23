"""Store configuration model."""

from __future__ import annotations

import importlib
import pathlib
from typing import Any
from typing import Self

from pydantic import AnyUrl
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from magnify import MagnifyMonitor
from magnify.filters import BaseFilter
from magnify.sensor import BaseSensor
from magnify.store import BaseStore
from magnify.utils import dump
from magnify.utils import load


def import_from_path(path: str) -> type[Any]:
    """Import object via its fully qualified path.

    Example:
        ```python
        >>> import_from_path('proxystore.connectors.protocols.Connector')
        <class 'proxystore.connectors.protocols.Connector'>
        ```

    Args:
        path: Fully qualified path of object to import.

    Returns:
        Imported object.

    Raises:
        ImportError: If an object at the `path` is not found.
    """
    module_path, _, name = path.rpartition('.')
    if len(module_path) == 0:
        raise ImportError(
            f'Object path must contain at least one module. Got {path}',
        )
    module = importlib.import_module(module_path)
    return getattr(module, name)


_KNOWN_SENSORS = {
    'magnify.sensor.energy.PerfEnergySensor',
    'magnify.sensor.perf.PerfSensor',
    'magnify.sensor.rapl.RaplSysfsSensor',
    'magnify.sensor.psutil.PsutilSensor',
}


class SensorConfig(BaseModel):
    """Sensor configuration."""

    model_config = ConfigDict(extra='forbid')

    kind: str
    options: dict[str, Any] = Field(default_factory=dict)

    def get_sensor_type(self):
        """Get the sensor type from the configuration."""
        try:
            return import_from_path(self.kind)
        except ImportError as e:
            for path in _KNOWN_SENSORS:
                _, name = path.rsplit('.', 1)
                name = name.lower()
                choices = [name, name.replace('sensor', '')]
                if self.kind.lower() in choices:
                    return import_from_path(path)
            raise ValueError(f'Unknown sensor type "{self.kind}".') from e

    def get_sensor(self) -> BaseSensor:
        """Get the sensor specified by the configuration.

        Returns:
            A [`Sensor`][leaf.sensor.BaseSensor] \
            instance.
        """
        sensor_type = self.get_sensor_type()
        return sensor_type(**self.options)


_KNOWN_FILTERS = {'magnify.filters.Downsample'}


class FilterConfig(BaseModel):
    """Configuration for filters to apply before storing measurements."""

    model_config = ConfigDict(extra='forbid')

    kind: str
    to: None | list[str] = None
    options: dict[str, Any] = Field(default_factory=dict)

    def get_filter_type(self):
        """Get the filter type specified by the configuration."""
        try:
            return import_from_path(self.kind)
        except ImportError as e:
            for path in _KNOWN_FILTERS:
                _, name = path.rsplit('.', 1)
                name = name.lower()
                choices = [name, name.replace('filter', '')]
                if self.kind.lower() in choices:
                    return import_from_path(path)
            raise ValueError(f'Unknown filter type "{self.kind}".') from e

    def get_filter(self) -> BaseFilter:
        """Get the filter specified by the configuration.

        Returns:
            A Filter instance
        """
        filter_type = self.get_filter_type()
        return filter_type(to=self.to, **self.options)


_KNOWN_STORES = {'magnify.store.file.FileStore'}


class StoreConfig(BaseModel):
    """Store configuration."""

    model_config = ConfigDict(extra='forbid')

    kind: str
    includes: None | list[str] = Field(default=None)
    options: dict[str, Any] = Field(default_factory=dict)
    filters: list[FilterConfig] = Field(default_factory=list)

    def get_store_type(self):
        """Get the store type from the configuration."""
        try:
            return import_from_path(self.kind)
        except ImportError as e:
            for path in _KNOWN_STORES:
                _, name = path.rsplit('.', 1)
                name = name.lower()
                choices = [name, name.replace('store', '')]
                if self.kind.lower() in choices:
                    return import_from_path(path)
            raise ValueError(f'Unknown store type "{self.kind}".') from e

    def get_store(self) -> BaseStore:
        """Get the store specified by the configuration.

        Returns:
            A [`Store`][leaf.store.BaseStore] \
            instance.
        """
        store_type = self.get_store_type()
        return store_type(
            filters=self.filters,
            includes=self.includes,
            **self.options,
        )


class MonitorConfig(BaseModel):
    """Overarching configuration for the monitor."""

    model_config = ConfigDict(extra='forbid')
    sensors: list[SensorConfig]
    stores: list[StoreConfig]
    monitor_address: AnyUrl = Field(default='ipc:///tmp/magnify_monitor')
    monitor_interval: int = Field(default=1)

    @classmethod
    def from_toml(cls, filepath: str | pathlib.Path) -> Self:
        """Create a configuration file from a TOML file.

        Example:
            See
            [`write_toml()`][leaf.config.MoitorConfig.write_toml].

        Args:
            filepath: Path to TOML file to load.
        """
        with open(filepath, 'rb') as f:
            return load(cls, f)

    def write_toml(self, filepath: str | pathlib.Path) -> None:
        """Write a configuration to a TOML file.

        Args:
            filepath: Path to TOML file to write.
        """
        filepath = pathlib.Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            dump(self, f)

    def get_monitor(self) -> MagnifyMonitor:
        """Get an instance of the MagnifyMonitor."""
        sensors_config = self.sensors
        sensors = [
            sensor_config.get_sensor() for sensor_config in sensors_config
        ]

        stores_config = self.stores
        stores = [store_config.get_store() for store_config in stores_config]

        return MagnifyMonitor(
            sensors,
            stores,
            self.monitor_address,
            self.monitor_interval,
        )
