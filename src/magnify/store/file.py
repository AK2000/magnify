from __future__ import annotations

import csv
import os
import pathlib
from typing import TextIO

import polars

from magnify.filters.base import BaseFilter
from magnify.store.base import BaseStore
from magnify.types import TimedMeasurement
from magnify.types import TimedTask


class FileStore(BaseStore):
    """Store to write monitoring values to file system directly as csv."""

    def __init__(
        self,
        dir_name: str | os.PathLike,
        filters: tuple[BaseFilter] = (),
        includes: None | set[str] = None,
    ):
        """Initialize a file store rooted at dir_name."""
        super().__init__(filters=filters, includes=includes)

        self.streams: dict[str, TextIO] = {}
        self.parent_dir = pathlib.Path(dir_name)
        self.parent_dir.mkdir(parents=True, exist_ok=True)

        # Create the file for tasks
        task_path: pathlib.Path = self.parent_dir / 'tasks.csv'
        self.task_file = task_path.open('w')
        writer = csv.writer(self.task_file)
        writer.writerow(('task_id', 'process_id', 'start_time', 'end_time'))

    def _put(self, measurements: dict[str, TimedMeasurement]):
        for stream, timed_measurement in measurements.items():
            include_header = False
            if stream not in self.streams:
                stream_path: pathlib.Path = self.parent_dir / f'{stream}.csv'
                self.streams[stream] = stream_path.open('w')
                include_header = True

            if isinstance(timed_measurement.measurement, polars.DataFrame):
                df = timed_measurement.measurement.with_columns(
                    polars.lit(timed_measurement.time).alias('time'),
                )
                df.write_csv(
                    self.streams[stream],
                    separator=',',
                    include_header=include_header,
                )

            elif isinstance(timed_measurement.measurement, float):
                writer = csv.writer(self.streams[stream])
                if include_header:
                    writer.writerow(['time', 'value'])
                writer.writerow(timed_measurement)

    def put_task(self, timed_task: TimedTask):
        """Write task to file store."""
        writer = csv.writer(self.task_file)
        writer.writerow(timed_task)

    def __del__(self):
        """Delete file store object. Close open file pointers."""
        for fp in self.streams.values():
            fp.close()
        self.streams.clear()
        self.task_file.close()
