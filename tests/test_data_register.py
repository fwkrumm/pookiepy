"""
Unit tests for pookiepy/data_register.py

DataRegister is pure Python (no gRPC transport) so all tests run without
starting a server or client.
"""
# pylint: disable=duplicate-code  # stdlib test-file imports are intentionally similar
import logging
import queue
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from pookiepy import message_pb2
from pookiepy.data_register import DataRegister, DeliveryResult
from pookiepy.exceptions import GrpcValueError


def _msg(name: str = "topic") -> message_pb2.PookieMessage:
    """Helper: create a minimal PookieMessage with messageName set."""
    return message_pb2.PookieMessage(
        metaInfo=message_pb2.MetaInformation(messageName=name)
    )


class TestRegisterClient(unittest.TestCase):
    """Tests for add_notification_queue_for_messageName."""

    def setUp(self):
        self.dr = DataRegister(logging.getLogger("test"))

    def test_register_single_client_no_error(self):
        """Registering a client for a topic succeeds without error."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())

    def test_register_two_clients_same_topic(self):
        """Two different clients can register for the same topic."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())
        self.dr.add_notification_queue_for_message_name("c2", "topic", queue.Queue())

    def test_register_same_client_twice_raises(self):
        """Re-registering the same client for the same topic raises ValueError."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)
        with self.assertRaises(ValueError):
            self.dr.add_notification_queue_for_message_name("c1", "topic", q)

    def test_register_same_client_multiple_topics(self):
        """A single client may subscribe to multiple independent topics."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic_a", q)
        self.dr.add_notification_queue_for_message_name("c1", "topic_b", q)


class TestAddDataFanOut(unittest.TestCase):
    """Tests for add_data_for_messageName --- broadcast and targeted delivery."""

    def setUp(self):
        self.dr = DataRegister(logging.getLogger("test"))

    def test_fanout_reaches_all_subscribers_except_sender(self):
        """Fan-out delivers to all subscribers; the sending client is skipped."""
        q1, q2, q_sender = queue.Queue(), queue.Queue(), queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)
        self.dr.add_notification_queue_for_message_name("c2", "topic", q2)
        self.dr.add_notification_queue_for_message_name("sender", "topic", q_sender)

        result = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertIn("c1", result.delivered)
        self.assertIn("c2", result.delivered)
        self.assertEqual(q1.qsize(), 1)
        self.assertEqual(q2.qsize(), 1)
        self.assertEqual(q_sender.qsize(), 0)  # sender must be skipped

    def test_sender_not_subscribed_fanout_works(self):
        """Fan-out works normally when the sender is not itself a subscriber."""
        q1 = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)

        result = self.dr.add_data_for_message_name("unrelated_sender", "topic", _msg())

        self.assertIn("c1", result.delivered)
        self.assertEqual(q1.qsize(), 1)

    def test_unknown_topic_returns_empty_tuples(self):
        """Publishing to an unregistered topic returns an empty DeliveryResult."""
        with self.assertLogs("test", level="DEBUG") as logs:
            result = self.dr.add_data_for_message_name("c1", "nonexistent", _msg("nonexistent"))
        self.assertEqual(result.delivered, ())
        self.assertEqual(result.dropped, ())
        self.assertIn(
            "Message name nonexistent has never had subscribers registered", logs.output[0]
        )

    def test_topic_with_all_subscribers_removed_logs_reason(self):
        """Previously subscribed topics report that their subscribers left."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())
        self.dr.remove_notification_queues_for_client("c1")

        with self.assertLogs("test", level="WARNING") as logs:
            result = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertEqual(result, DeliveryResult())
        self.assertIn("All subscribers have been removed for message_name: topic", logs.output[0])

    def test_wrong_data_type_raises_grpc_value_error(self):
        """Passing a non-PookieMessage value raises GrpcValueError."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())
        with self.assertRaises(GrpcValueError):
            self.dr.add_data_for_message_name("other", "topic", "not-a-message")

    def test_targeted_delivery_reaches_only_target(self):
        """targetClientId restricts delivery to exactly one subscriber."""
        q1, q2 = queue.Queue(), queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)
        self.dr.add_notification_queue_for_message_name("c2", "topic", q2)

        result = self.dr.add_data_for_message_name(
            "sender", "topic", _msg(), target_client_id="c1"
        )

        self.assertIn("c1", result.delivered)
        self.assertEqual(q1.qsize(), 1)
        self.assertEqual(q2.qsize(), 0)

    def test_named_result_fields_are_available(self):
        """DeliveryResult exposes explicit named fields."""
        q1 = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)

        result = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertIsInstance(result, DeliveryResult)
        self.assertEqual(result.delivered, ("c1",))
        self.assertEqual(result.dropped, ())

    def test_targeted_delivery_missing_client_returns_nok(self):
        """A targetClientId that is not subscribed is returned in the nok tuple."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())

        result = self.dr.add_data_for_message_name(
            "sender", "topic", _msg(), target_client_id="ghost"
        )

        self.assertIn("ghost", result.dropped)
        self.assertEqual(result.delivered, ())

    def test_full_queue_returns_client_in_nok(self):
        """A subscriber with a full queue has its client id placed in the nok tuple."""
        q = queue.Queue(maxsize=1)
        q.put("placeholder")  # fill it
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)

        result = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertIn("c1", result.dropped)
        self.assertEqual(result.delivered, ())

    def test_returned_message_is_exact_object_put(self):
        """The exact PookieMessage object put by the sender arrives in the
        subscriber's queue (no copy)."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)
        msg = _msg()
        self.dr.add_data_for_message_name("sender", "topic", msg)
        self.assertIs(q.get_nowait(), msg)

    def test_slow_consumer_drops_newest_without_blocking_healthy_queue(self):
        """Both routing modes throttle full-queue logs without hiding delivery results."""
        logger = MagicMock()
        register = DataRegister(logger)
        slow = queue.Queue(maxsize=1)
        healthy = queue.Queue()
        original = _msg()
        slow.put_nowait(original)
        register.add_notification_queue_for_message_name("slow", "topic", slow)
        register.add_notification_queue_for_message_name("healthy", "topic", healthy)
        with patch("pookiepy.data_register.time.monotonic", side_effect=[0, 1, 5]):
            first = register.add_data_for_message_name("sender", "topic", _msg())
            second = register.add_data_for_message_name(
                "sender", "topic", _msg(), target_client_id="slow"
            )
            register.add_data_for_message_name("sender", "topic", _msg())
        self.assertEqual(first, DeliveryResult(delivered=("healthy",), dropped=("slow",)))
        self.assertEqual(second.dropped, ("slow",))
        self.assertIs(slow.get_nowait(), original)
        self.assertEqual(healthy.qsize(), 2)
        self.assertEqual(logger.warning.call_count, 2)

    def test_growth_warnings_are_rate_limited(self):
        """An unbounded queue crossing its threshold cannot log for every message."""
        logger = MagicMock()
        register = DataRegister(logger, queue_warning_threshold=0)
        notifications = queue.Queue()
        register.add_notification_queue_for_message_name("slow", "topic", notifications)
        with patch("pookiepy.data_register.time.monotonic", return_value=0):
            for _ in range(10):
                register.add_data_for_message_name("sender", "topic", _msg())
        logger.warning.assert_called_once()

    def test_queue_warnings_share_one_cooldown(self):
        """Different queues and warning types share a cooldown, not delivery outcomes."""
        logger = MagicMock()
        register = DataRegister(logger, queue_warning_threshold=0)
        full = queue.Queue(maxsize=1)
        full.put_nowait(_msg())
        register.add_notification_queue_for_message_name("full", "topic", full)
        register.add_notification_queue_for_message_name("growing", "topic", queue.Queue())
        with patch("pookiepy.data_register.time.monotonic", side_effect=[0, 1, 5, 6]):
            first = register.add_data_for_message_name("sender", "topic", _msg())
            second = register.add_data_for_message_name("sender", "topic", _msg())
        expected = DeliveryResult(delivered=("growing",), dropped=("full",))
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(logger.warning.call_count, 2)

    def test_unreachable_warning_threshold_skips_queue_size_lock(self):
        """Bounded queues cannot exceed a threshold above their capacity."""
        register = DataRegister(MagicMock(), queue_warning_threshold=100_000)
        notifications = queue.Queue(maxsize=1024)
        register.add_notification_queue_for_message_name("client", "topic", notifications)
        with patch.object(notifications, "qsize") as size:
            register.add_data_for_message_name("sender", "topic", _msg())
        size.assert_not_called()


class TestRemoveClient(unittest.TestCase):
    """Tests for remove_notification_queues_for_client."""

    def setUp(self):
        self.dr = DataRegister(logging.getLogger("test"))

    def test_remove_single_subscription(self):
        """Removing a client returns a count of 1 for a single subscription."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)
        removed = self.dr.remove_notification_queues_for_client("c1")
        self.assertEqual(removed, 1)

    def test_remove_multi_topic_client(self):
        """Removing a client subscribed to N topics returns a count of N."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "t1", q)
        self.dr.add_notification_queue_for_message_name("c1", "t2", q)
        removed = self.dr.remove_notification_queues_for_client("c1")
        self.assertEqual(removed, 2)

    def test_removed_client_no_longer_receives_data(self):
        """After removal the client's queue receives no further messages."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)
        self.dr.remove_notification_queues_for_client("c1")
        result = self.dr.add_data_for_message_name("sender", "topic", _msg())
        self.assertEqual(result.delivered, ())
        self.assertEqual(q.qsize(), 0)

    def test_remove_nonexistent_client_returns_zero(self):
        """Removing an unknown client returns 0 without raising."""
        removed = self.dr.remove_notification_queues_for_client("ghost")
        self.assertEqual(removed, 0)

    def test_remaining_clients_unaffected_after_partial_remove(self):
        """Removing one client does not affect other subscribers on the same topic."""
        q1, q2 = queue.Queue(), queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)
        self.dr.add_notification_queue_for_message_name("c2", "topic", q2)

        self.dr.remove_notification_queues_for_client("c1")
        self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertEqual(q2.qsize(), 1)
        self.assertEqual(q1.qsize(), 0)


class TestConcurrentAccess(unittest.TestCase):
    """Smoke tests for DataRegister under concurrent register and fan-out."""

    def test_concurrent_register_all_receive(self):
        """20 clients registering concurrently all receive the subsequent fan-out without errors."""
        dr = DataRegister(logging.getLogger("test"))
        queues: list[queue.Queue] = []
        errors: list[Exception] = []

        def register(client_id: str):
            try:
                q = queue.Queue()
                dr.add_notification_queue_for_message_name(client_id, "topic", q)
                queues.append(q)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                errors.append(exc)

        threads = [threading.Thread(target=register, args=(f"c{i}",)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [], f"Unexpected errors: {errors}")

        result = dr.add_data_for_message_name("outsider", "topic", _msg())
        self.assertEqual(len(result.delivered), 20)
        self.assertEqual(len(result.dropped), 0)
        for q in queues:
            self.assertEqual(q.qsize(), 1)


if __name__ == "__main__":
    unittest.main()
