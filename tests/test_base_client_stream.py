"""Client stream, queue, and error propagation tests without network or sleeps."""

import queue
import unittest
from unittest.mock import MagicMock, patch

import grpc

from pookiepy import message_pb2
from pookiepy.baseclient import BaseClient, _StreamError
from pookiepy.exceptions import (
    ClientExit, GrpcConnectionError, GrpcEmpty, GrpcResourceExhaustedError,
    GrpcTimeoutError, GrpcValueError, StopSpin,
)


class _RpcFailure(grpc.RpcError):
    """Concrete status-bearing transport error for receive-loop tests."""

    def __init__(self, status):
        super().__init__("stream failed")
        self.status = status

    def code(self):
        """Return the simulated gRPC status."""
        return self.status

    def details(self):
        """Return the simulated server error detail."""
        return "stream failed"


class TestClientStream(unittest.TestCase):
    """Verify stream hooks, timing metadata, queue draining, and caller errors."""

    def setUp(self):
        with patch("pookiepy.baseclient.grpc.insecure_channel"), patch.object(BaseClient, "run"):
            self.client = BaseClient(port=50099, provides=["topic"])
        self.client.logger = MagicMock()
        self.addCleanup(self.client.disconnect)
        self.addCleanup(setattr, self.client, "stream", None)

    def test_receive_welcome_bypasses_hook_and_data_extends_history(self):
        """Only post-welcome data enters hooks; filtered messages never reach callers."""
        welcome = message_pb2.PookieMessage()
        dropped = self.client.generate_message("topic", None, b"drop")
        forwarded = self.client.generate_message("topic", None, b"forward")
        forwarded.history.add(name="server", perfCounter=1.0)
        self.client.stream = iter([welcome, dropped, forwarded])
        self.client.on_receive = MagicMock(side_effect=[False, True])
        with patch("pookiepy.baseclient.time.perf_counter", return_value=12.5):
            self.client._receive_loop()  # pylint: disable=protected-access
        self.assertEqual(self.client.on_receive.call_count, 2)
        self.assertIs(self.client.get_data(timeout=0), welcome)
        self.assertIs(self.client.get_data(timeout=0), forwarded)
        self.assertTrue(self.client.receive_queue.empty())
        self.assertEqual(len(forwarded.history), 2)
        self.assertEqual(forwarded.history[-1].name, self.client.name)
        self.assertEqual(forwarded.history[-1].perfCounter, 12.5)
        self.assertTrue(forwarded.history[-1].HasField("receiveTimestamp"))

    def test_stream_errors_reach_main_thread_with_correct_types(self):
        """Cancellation stays quiet; fatal gRPC statuses map to specific public errors."""
        cases = (
            (grpc.StatusCode.CANCELLED, None),
            (grpc.StatusCode.DEADLINE_EXCEEDED, GrpcTimeoutError),
            (grpc.StatusCode.RESOURCE_EXHAUSTED, GrpcResourceExhaustedError),
            (grpc.StatusCode.PERMISSION_DENIED, GrpcConnectionError),
        )
        for status, error_type in cases:
            with self.subTest(status=status):
                failure = _RpcFailure(status)

                def responses(error=failure):
                    yield message_pb2.PookieMessage()
                    raise error

                self.client.stream = responses()
                self.client._receive_loop()  # pylint: disable=protected-access
                self.client.get_data(timeout=0)
                if error_type is None:
                    self.assertTrue(self.client.receive_queue.empty())
                else:
                    with self.assertRaisesRegex(error_type, "stream failed"):
                        self.client.get_data(timeout=0)

    def test_rejection_before_welcome_reaches_connection_check(self):
        """Admission failures surface immediately, before any welcome message."""
        def rejected():
            yield from ()
            raise _RpcFailure(grpc.StatusCode.RESOURCE_EXHAUSTED)

        self.client.stream = rejected()
        self.client._receive_loop()  # pylint: disable=protected-access
        with patch.object(self.client, "disconnect"), self.assertRaises(GrpcResourceExhaustedError):
            self.client._check_connection()  # pylint: disable=protected-access

    def test_hook_exception_reaches_blocking_get_data_unchanged(self):
        """Application errors retain their identity when transferred to the main thread."""
        failure = StopSpin("stop processing")
        self.client.on_receive = MagicMock(side_effect=failure)
        self.client.stream = iter([
            message_pb2.PookieMessage(), self.client.generate_message("topic", None, b"data")
        ])
        self.client._receive_loop()  # pylint: disable=protected-access
        self.client.get_data(timeout=0)
        with self.assertRaises(StopSpin) as raised:
            self.client.get_data(timeout=1)
        self.assertIs(raised.exception, failure)

    def test_request_history_metadata_and_completion_accounting(self):
        """Sending finalizes history and metadata; task completes after yielding to gRPC."""
        data = self.client.generate_message("topic", None, b"payload")
        with patch("pookiepy.baseclient.time.perf_counter", return_value=10.0):
            self.client.send_data(data, add_history=True)
        self.assertEqual(data.history[0].name, self.client.name)
        self.assertTrue(data.history[0].HasField("receiveTimestamp"))
        generator = self.client._request_generator()  # pylint: disable=protected-access
        with patch("pookiepy.baseclient.time.perf_counter", return_value=12.5):
            self.assertIs(next(generator), data)
        self.assertEqual(data.history[0].perfCounter, 2.5)
        self.assertTrue(data.history[0].HasField("sendTimestamp"))
        self.assertTrue(data.metaInfo.messageId)
        self.assertTrue(data.metaInfo.HasField("timestamp"))
        self.assertEqual(self.client.send_queue.unfinished_tasks, 1)
        self.client.run_event.clear()
        with self.assertRaises(StopIteration):
            next(generator)
        self.assertEqual(self.client.send_queue.unfinished_tasks, 0)

    def test_add_history_rejects_duplicate_hops_without_enqueueing(self):
        """Existing history must extend naturally instead of restarting at send_data."""
        data = self.client.generate_message("topic", None, b"payload")
        data.history.add(name="previous-hop")
        with self.assertRaisesRegex(GrpcValueError, "already has history"):
            self.client.send_data(data, add_history=True)
        self.assertTrue(self.client.send_queue.empty())

    def test_filtered_send_marks_task_done_then_retries_empty_queue(self):
        """A rejected yield is acknowledged locally and an idle poll can exit cleanly."""
        self.client.send_data(self.client.generate_message("topic", None, b"drop"))
        self.client.on_data_yield = MagicMock(return_value=False)
        original_get = self.client.send_queue.get

        def get_or_disconnect(timeout):
            if self.client.send_queue.empty():
                self.client.run_event.clear()
                raise queue.Empty
            return original_get(timeout=timeout)

        with patch.object(self.client.send_queue, "get", side_effect=get_or_disconnect):
            self.assertEqual(list(self.client._request_generator()), [])  # pylint: disable=protected-access
        self.client.on_data_yield.assert_called_once()
        self.assertEqual(self.client.send_queue.unfinished_tasks, 0)

    def test_yield_hook_failure_releases_all_unfinished_tasks(self):
        """A failed sender cannot leave wait_done blocked on abandoned queue tasks."""
        for _ in range(2):
            self.client.send_data(self.client.generate_message("topic", None, b"payload"))
        self.client.on_data_yield = MagicMock(side_effect=RuntimeError("send failed"))
        self.assertEqual(list(self.client._request_generator()), [])  # pylint: disable=protected-access
        self.assertEqual(self.client.send_queue.unfinished_tasks, 0)
        self.assertEqual(self.client.send_queue.qsize(), 1)
        self.assertEqual(self.client.logger.error.call_count, 2)

    def test_wait_done_checks_connection_delay_and_queue_join(self):
        """Wait uses queue completion plus the requested grace delay and validates inputs."""
        with patch.object(self.client.send_queue, "join") as join, patch(
            "pookiepy.baseclient.time.sleep"
        ) as sleep:
            self.client.wait_done(additional_sleep=0.25)
            join.assert_called_once_with()
            sleep.assert_called_once_with(0.25)
            sleep.reset_mock()
            self.client.wait_done(additional_sleep=0)
            sleep.assert_not_called()
            with self.assertRaises(GrpcValueError):
                self.client.wait_done(additional_sleep=-1)
            self.client.run_event.clear()
            with self.assertRaises(GrpcConnectionError):
                self.client.wait_done()

    def test_get_data_retries_idle_poll_until_disconnect(self):
        """An unbounded read polls in bounded slices and notices disconnection."""
        def disconnect_on_poll(timeout):
            self.assertEqual(timeout, 1.0)
            self.client.run_event.clear()
            raise queue.Empty

        with patch.object(self.client.receive_queue, "get", side_effect=disconnect_on_poll):
            with self.assertRaises(ClientExit):
                self.client.get_data()

    def test_get_data_keyboard_interrupt_raises_client_exit(self):
        """Interrupting a queue read returns control through the public exit exception."""
        with patch.object(self.client.receive_queue, "get", side_effect=KeyboardInterrupt):
            with self.assertRaises(ClientExit):
                self.client.get_data()
        self.client.logger.warning.assert_called_once_with("KeyboardInterrupt received")

    def test_spin_timeout_and_forever_disconnect_preserve_public_errors(self):
        """Spin propagates timeout; spin_forever stops on disconnect without looping."""
        with patch.object(self.client, "get_data", side_effect=GrpcEmpty("timeout")):
            with self.assertRaises(GrpcEmpty):
                self.client.spin(timeout=0.25)
        with patch.object(self.client, "spin", side_effect=ClientExit("disconnected")) as spin:
            self.client.spin_forever(timeout=0.25)
        spin.assert_called_once_with(timeout=0.25)

    def test_nonblocking_fatal_error_preserves_exception_identity(self):
        """The nonblocking path unwraps fatal stream sentinels rather than returning them."""
        failure = GrpcConnectionError("disconnected")
        self.client.receive_queue.put(_StreamError(failure))
        with self.assertRaises(GrpcConnectionError) as raised:
            self.client.get_data(timeout=0)
        self.assertIs(raised.exception, failure)
