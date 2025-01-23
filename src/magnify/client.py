from __future__ import annotations

from enum import IntEnum
from functools import wraps
from typing import Any
from typing import Callable

from pydantic import AnyUrl


class TaskEvent(IntEnum):
    """Integer for representing task state."""

    START = 1
    COMPLETE = 2
    FAIL = 3


def execute_task(
    func: Callable,
    *args: Any,
    monitor_address: AnyUrl = 'ipc:///tmp/magnify_monitor',
    **kwargs,
) -> Any:
    """Execute a function, recording the start and end with magnify."""
    import datetime
    import os
    import uuid

    import zmq

    task_id = kwargs.pop('task_id', None)
    if task_id is None:
        task_id = f'{func.__name__}_{uuid.uuid1()}'
    # Get the PID of the current process
    current_pid = os.getpid()

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect(monitor_address)

    socket.send_json(
        {
            'task_id': task_id,
            'pid': current_pid,
            'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
            'event': TaskEvent.START,
        },
    )

    state: TaskEvent = TaskEvent.COMPLETE

    try:
        return func(*args, **kwargs)
    except Exception as e:
        # send fail message
        state = TaskEvent.FAIL
        raise e
    finally:
        socket.send_json(
            {
                'task_id': task_id,
                'pid': current_pid,
                'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
                'event': state,
            },
        )
        context.destroy()


def magnify_decorator(
    f: Any = None,
    monitor_address: AnyUrl = 'ipc:///tmp/magnify_monitor',
):
    """Wrap a functionto notify the monitor of task start and end."""

    def create_wrapped(func: Any):
        @wraps(func)
        def wrapped(*args: list[Any], **kwargs: dict[str, Any]) -> Any:
            return execute_task(
                func,
                *args,
                monitor_address=monitor_address,
                **kwargs,
            )

        return wrapped

    if f is None:
        return create_wrapped

    return create_wrapped(f)
