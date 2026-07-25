"""
Unit tests for grpchook/data_register.py

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

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from grpchook import message_pb2
from grpchook.data_register import DataRegister
from grpchook.exceptions import GrpcValueError


def _msg(name: str = "topic") -> message_pb2.Message:
    """Helper: create a minimal Message with messageName set."""
    return message_pb2.Message(
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

        ok, _ = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertIn("c1", ok)
        self.assertIn("c2", ok)
        self.assertEqual(q1.qsize(), 1)
        self.assertEqual(q2.qsize(), 1)
        self.assertEqual(q_sender.qsize(), 0)  # sender must be skipped

    def test_sender_not_subscribed_fanout_works(self):
        """Fan-out works normally when the sender is not itself a subscriber."""
        q1 = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)

        ok, _ = self.dr.add_data_for_message_name("unrelated_sender", "topic", _msg())

        self.assertIn("c1", ok)
        self.assertEqual(q1.qsize(), 1)

    def test_unknown_topic_returns_empty_tuples(self):
        """Publishing to an unregistered topic returns ((), ()) without raising."""
        result = self.dr.add_data_for_message_name("c1", "nonexistent", _msg("nonexistent"))
        self.assertEqual(result, ((), ()))

    def test_wrong_data_type_raises_grpc_value_error(self):
        """Passing a non-Message value raises GrpcValueError."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())
        with self.assertRaises(GrpcValueError):
            self.dr.add_data_for_message_name("other", "topic", "not-a-message")

    def test_targeted_delivery_reaches_only_target(self):
        """targetClientId restricts delivery to exactly one subscriber."""
        q1, q2 = queue.Queue(), queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q1)
        self.dr.add_notification_queue_for_message_name("c2", "topic", q2)

        ok, _ = self.dr.add_data_for_message_name("sender", "topic", _msg(), target_client_id="c1")

        self.assertIn("c1", ok)
        self.assertEqual(q1.qsize(), 1)
        self.assertEqual(q2.qsize(), 0)

    def test_targeted_delivery_missing_client_returns_nok(self):
        """A targetClientId that is not subscribed is returned in the nok tuple."""
        self.dr.add_notification_queue_for_message_name("c1", "topic", queue.Queue())

        ok, nok = self.dr.add_data_for_message_name(
            "sender", "topic", _msg(), target_client_id="ghost"
        )

        self.assertIn("ghost", nok)
        self.assertEqual(ok, ())

    def test_full_queue_returns_client_in_nok(self):
        """A subscriber with a full queue has its client id placed in the nok tuple."""
        q = queue.Queue(maxsize=1)
        q.put("placeholder")  # fill it
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)

        ok, nok = self.dr.add_data_for_message_name("sender", "topic", _msg())

        self.assertIn("c1", nok)
        self.assertEqual(ok, ())

    def test_returned_message_is_exact_object_put(self):
        """The exact Message object put by the sender arrives in the
        subscriber's queue (no copy)."""
        q = queue.Queue()
        self.dr.add_notification_queue_for_message_name("c1", "topic", q)
        msg = _msg()
        self.dr.add_data_for_message_name("sender", "topic", msg)
        self.assertIs(q.get_nowait(), msg)


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
        ok, _ = self.dr.add_data_for_message_name("sender", "topic", _msg())
        self.assertEqual(ok, ())
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

        ok, nok = dr.add_data_for_message_name("outsider", "topic", _msg())
        self.assertEqual(len(ok), 20)
        self.assertEqual(len(nok), 0)
        for q in queues:
            self.assertEqual(q.qsize(), 1)


if __name__ == "__main__":
    unittest.main()
