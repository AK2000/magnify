from __future__ import annotations

from unittest import mock

import pytest

from magnify.client import magnify_decorator
from magnify.client import TaskEvent


@pytest.fixture
def mock_socket():
    with mock.patch('zmq.Context') as mock_context:
        mock_socket = mock.Mock()
        mock_context.return_value.socket.return_value = mock_socket
        yield mock_socket


def test_task_decorator(mock_socket):
    @magnify_decorator
    def my_test_func():
        return 1

    assert my_test_func() == 1

    mock_socket.send_json.assert_called()
    calls = mock_socket.send_json.call_args_list

    msg = calls[0][0][0]  # The first positional argument of the first call
    assert msg['event'] == TaskEvent.START
    assert 'my_test_func' in msg['task_id']

    msg = calls[1][0][0]  # The first positional argument of the second call
    assert msg['event'] == TaskEvent.COMPLETE


def test_task_fail(mock_socket):
    @magnify_decorator
    def my_test_func():
        raise Exception('Test')

    with pytest.raises(Exception, match='Test'):
        my_test_func()

    mock_socket.send_json.assert_called()
    calls = mock_socket.send_json.call_args_list

    msg = calls[0][0][0]  # The first positional argument of the first call
    assert msg['event'] == TaskEvent.START

    msg = calls[1][0][0]  # The first positional argument of the second call
    assert msg['event'] == TaskEvent.FAIL
