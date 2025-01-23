from __future__ import annotations

import datetime
import threading
import time

import zmq
from pydantic import AnyUrl

from magnify.sensor import BaseSensor
from magnify.store import BaseStore
from magnify.types import TimedMeasurement
from magnify.types import TimedTask


class MagnifyMonitor:
    """Main class for initializing and starting resource monitoring."""

    def __init__(
        self,
        sensors: list[BaseSensor],
        stores: list[BaseStore],
        monitor_address: AnyUrl = 'ipc:///tmp/magnify_monitor',
        monitor_interval: int = 1,
    ):
        """Initialize a monitor with the configured sensors and stores."""
        self.sensors = sensors
        self.stores = stores
        self.monitor_address = monitor_address
        self.monitor_interval = monitor_interval

        self.kill_event = threading.Event()
        self.started = False

    def process_task(self, task_msg: dict[int | str]) -> None:
        """Process a single task message into the stores."""
        task_msg['timestamp'] = datetime.datetime.fromisoformat(
            task_msg['timestamp'],
        )
        task = TimedTask(**task_msg)

        for store in self.stores:
            store.put_task(task)

    def task_listener(self) -> None:
        """Listen to monitor address for incoming tasks."""
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.bind(self.monitor_address)

        while not self.kill_event.is_set():
            task_msg: dict[int | str] = socket.recv_json()
            self.process_task(task_msg)

    def take_measurement(self) -> dict[str, TimedMeasurement]:
        """Take a measurement from all of the sensors."""
        measurement: dict[str, TimedMeasurement] = {}
        for sensor in self.sensors:
            skip = False
            for dep in sensor.subscribes:
                if dep not in measurement:
                    skip = True
                    break

            if skip:
                continue

            args = (measurement[dep].measurement for dep in sensor.subscribes)
            val: TimedMeasurement | None = sensor.invoke(*args)
            if val is not None:
                measurement[sensor.name] = val

        return measurement

    def run(self) -> None:
        """Run the main monitoring loop."""
        now = time.time()
        while not self.kill_event.is_set():
            next_sleep_time = now + self.monitor_interval

            measurement = self.take_measurement()
            for store in self.stores:
                store.put_measurement(measurement)

            remaining_time = next_sleep_time - time.time()
            if remaining_time > 0:
                time.sleep(remaining_time)

            now = time.time()

    def start(self) -> None:
        """Start the task listener and monitoring loop."""
        if self.started:
            raise Exception('Cannot start previously started monitor')

        self.started = True

        # Start threads as daemon so joining them is not necessary
        self.task_listener_thread = threading.Thread(
            target=self.task_listener,
            daemon=True,
        )
        self.task_listener_thread.start()

        self.monitor_thread = threading.Thread(target=self.run, daemon=True)
        self.monitor_thread.start()

    def wait(self) -> None:
        """Wait for monitoring loops to end."""
        if not self.started:
            return

        self.task_listener_thread.join()
        self.monitor_thread.join()

    def shutdown(self) -> None:
        """Stop monitoring and wait for loops to end."""
        # Kill current threads
        self.kill_event.set()
        self.wait()

        # Prepare for next start
        self.kill_event.clear()
        self.started = False
