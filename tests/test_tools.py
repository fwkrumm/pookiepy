"""
Unit tests for pookiepy/tools.py

Tests cover:
- set_metadata: assigns messageId and timestamp; does not overwrite existing values.
- struct_to_json / json_to_struct: roundtrip conversion between Python dict and protobuf Struct.
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

from google.protobuf import struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from pookiepy import message_pb2
from pookiepy.exceptions import GrpcCustomInterfaceError
from pookiepy.tools import evaluate_history, generate_message, json_to_struct, set_metadata, struct_to_json


class TestSetMetadata(unittest.TestCase):
    """Tests for set_metadata() in pookiepy/tools.py."""

    def test_assigns_message_id_when_empty(self):
        """An empty message receives a non-empty messageId after set_metadata."""
        msg = message_pb2.Message()
        set_metadata(msg)
        self.assertTrue(msg.metaInfo.messageId)

    def test_assigned_message_id_is_32_char_hex(self):
        """Generated messageId is a 32-character hexadecimal string (uuid4().hex)."""
        msg = message_pb2.Message()
        set_metadata(msg)
        mid = msg.metaInfo.messageId
        self.assertEqual(len(mid), 32)
        # raises ValueError if mid is not valid hexadecimal
        int(mid, 16)

    def test_preserves_existing_message_id(self):
        """An already-set messageId is not overwritten by set_metadata."""
        msg = message_pb2.Message()
        msg.metaInfo.messageId = "fixed-id"
        set_metadata(msg)
        self.assertEqual(msg.metaInfo.messageId, "fixed-id")

    def test_preserves_existing_timestamp(self):
        """An already-set timestamp is not overwritten by set_metadata."""
        msg = message_pb2.Message()
        ts = Timestamp(seconds=1_000_000, nanos=0)
        msg.metaInfo.timestamp.CopyFrom(ts)
        set_metadata(msg)
        self.assertEqual(msg.metaInfo.timestamp.seconds, 1_000_000)

    def test_assigns_timestamp_when_unset(self):
        """A fresh message receives a non-zero timestamp after set_metadata.

        Regression: protobuf v6 bool(Timestamp()) returns True for the default-zero
        Timestamp, so the old ``if not message.metaInfo.timestamp`` guard was always
        False and the timestamp was never written.  The fix uses HasField instead.
        """
        msg = message_pb2.Message()
        self.assertFalse(msg.metaInfo.HasField("timestamp"))
        set_metadata(msg)
        self.assertTrue(
            msg.metaInfo.HasField("timestamp"),
            "timestamp must be set on fresh message"
        )
        self.assertGreater(msg.metaInfo.timestamp.seconds, 0)

    def test_each_call_produces_unique_message_id(self):
        """Successive calls on different messages generate distinct messageId values."""
        msg1 = message_pb2.Message()
        msg2 = message_pb2.Message()
        set_metadata(msg1)
        set_metadata(msg2)
        self.assertNotEqual(msg1.metaInfo.messageId, msg2.metaInfo.messageId)


class TestStructJsonRoundtrip(unittest.TestCase):
    """Roundtrip tests for json_to_struct / struct_to_json in pookiepy/tools.py."""

    def test_flat_dict_roundtrip(self):
        """A flat Python dict survives a json_to_struct → struct_to_json roundtrip unchanged."""
        data = {"key": "value", "num": 42.0}
        self.assertEqual(struct_to_json(json_to_struct(data)), data)

    def test_nested_dict_roundtrip(self):
        """A nested dict survives a roundtrip unchanged."""
        data = {"outer": {"inner": 1.0}}
        self.assertEqual(struct_to_json(json_to_struct(data)), data)

    def test_empty_dict_roundtrip(self):
        """An empty dict roundtrips to an empty dict."""
        data: dict = {}
        self.assertEqual(struct_to_json(json_to_struct(data)), data)

    def test_json_to_struct_returns_struct_type(self):
        """json_to_struct always returns a google.protobuf.Struct instance."""
        result = json_to_struct({"k": "v"})
        self.assertIsInstance(result, struct_pb2.Struct)

    def test_struct_to_json_returns_dict_type(self):
        """struct_to_json always returns a plain Python dict."""
        struct = json_to_struct({"k": "v"})
        result = struct_to_json(struct)
        self.assertIsInstance(result, dict)


class TestEvaluateHistory(unittest.TestCase):
    """Tests for evaluate_history() in pookiepy/tools.py."""

    @staticmethod
    def _ts(seconds: int) -> Timestamp:
        ts = Timestamp()
        ts.seconds = seconds
        return ts

    def _dp(self, name: str, recv_s: int = None, send_s: int = None, perf: float = 0.0):
        """Build a DataPoint with optional receive/send timestamps."""
        dp = message_pb2.DataPoint()
        dp.name = name
        if recv_s is not None:
            dp.receiveTimestamp.CopyFrom(self._ts(recv_s))
        if send_s is not None:
            dp.sendTimestamp.CopyFrom(self._ts(send_s))
        dp.perfCounter = perf
        return dp

    def _msg(self, *datapoints) -> message_pb2.Message:
        msg = message_pb2.Message()
        for dp in datapoints:
            msg.history.append(dp)
        return msg

    def test_empty_history_logs_no_history_message(self):
        """Empty history emits a 'No history available' log line."""
        logs = []
        evaluate_history(message_pb2.Message(), log_callback=logs.append)
        self.assertTrue(any("No history" in l for l in logs))

    def test_no_callback_defaults_to_print(self):
        """Omitting log_callback falls back to print without raising."""
        evaluate_history(message_pb2.Message())  # must not raise

    def test_single_hop_receive_only_shows_not_forwarded(self):
        """Single hop with receive but no send timestamp shows '<not forwarded>'."""
        msg = self._msg(self._dp("node_a", recv_s=1_000_000))
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("not forwarded" in l for l in logs))

    def test_single_hop_receive_and_send_shows_process_time(self):
        """Hop with both timestamps includes 'process=' in output."""
        msg = self._msg(self._dp("node_a", recv_s=1_000_000, send_s=1_000_001, perf=0.5))
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("process=" in l for l in logs))

    def test_multi_hop_transit_times_logged(self):
        """Multi-hop history logs transit times between consecutive hops."""
        msg = self._msg(
            self._dp("client", recv_s=1_000_000, send_s=1_000_001, perf=0.001),
            self._dp("server", recv_s=1_000_002),
        )
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("transit=" in l for l in logs))

    def test_multi_hop_missing_send_timestamp_shows_na(self):
        """First hop with no send timestamp results in 'transit=N/A' output."""
        msg = self._msg(
            self._dp("hop0", recv_s=1_000_000),  # no send
            self._dp("hop1", recv_s=1_000_002),
        )
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("N/A" in l for l in logs))

    def test_total_end_to_end_latency_logged(self):
        """Multi-hop with first and last receive timestamps logs total end-to-end."""
        msg = self._msg(
            self._dp("hop0", recv_s=1_000_000, send_s=1_000_001, perf=0.001),
            self._dp("hop1", recv_s=1_000_010),
        )
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("Total end-to-end" in l for l in logs))

    def test_end_of_history_marker_is_last_line(self):
        """'=== End of history ===' is always the final log line for non-empty history."""
        msg = self._msg(self._dp("x", recv_s=100))
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertEqual(logs[-1], "=== End of history ===")

    def test_header_contains_message_name(self):
        """Header line includes the message's messageName."""
        msg = self._msg(self._dp("n", recv_s=1))
        msg.metaInfo.messageName = "my_event"
        logs = []
        evaluate_history(msg, log_callback=logs.append)
        self.assertTrue(any("my_event" in l for l in logs))


class TestGenerateMessage(unittest.TestCase):
    """Tests for generate_message() in pookiepy/tools.py."""

    def test_default_message_name(self):
        """Default message_name is 'default_message'."""
        msg = generate_message()
        self.assertEqual(msg.metaInfo.messageName, "default_message")

    def test_custom_message_name(self):
        """Provided message_name is stored on the returned message."""
        msg = generate_message(message_name="sensor_data")
        self.assertEqual(msg.metaInfo.messageName, "sensor_data")

    def test_struct_payload_stored(self):
        """struct_payload dict is accessible via structPayload after conversion."""
        msg = generate_message(struct_payload={"key": "val"})
        result = struct_to_json(msg.payload.structPayload)
        self.assertEqual(result.get("key"), "val")

    def test_byte_payload_stored(self):
        """byte_payload bytes are set on bytePayload field."""
        msg = generate_message(byte_payload=b"\x00\x01\x02")
        self.assertEqual(msg.payload.bytePayload, b"\x00\x01\x02")

    def test_no_payload_gives_empty_bytes(self):
        """No payload arguments results in empty bytePayload."""
        msg = generate_message()
        self.assertEqual(msg.payload.bytePayload, b"")

    def test_returns_message_instance(self):
        """generate_message always returns a message_pb2.Message."""
        self.assertIsInstance(generate_message(), message_pb2.Message)

    def test_struct_and_byte_payload_both_set(self):
        """Both struct and byte payloads can coexist on the same message."""
        msg = generate_message(struct_payload={"x": 1.0}, byte_payload=b"abc")
        self.assertEqual(msg.payload.bytePayload, b"abc")
        self.assertEqual(struct_to_json(msg.payload.structPayload).get("x"), 1.0)

    def test_set_metadata_flag_populates_message_metadata(self):
        """add_metadata=True populates messageId and timestamp on generated messages."""
        msg = generate_message(message_name="sensor_data", add_metadata=True)
        self.assertEqual(msg.metaInfo.messageName, "sensor_data")
        self.assertTrue(msg.metaInfo.messageId)
        self.assertTrue(msg.metaInfo.HasField("timestamp"))

    def test_struct_payload_missing_field_raises_custom_interface_error(self):
        """Missing structPayload field raises GrpcCustomInterfaceError with guidance."""
        fake_msg = mock.MagicMock()
        fake_msg.metaInfo = mock.MagicMock()
        fake_msg.metaInfo.messageName = ""
        fake_msg.payload = object()  # no structPayload/bytePayload attributes

        interface = mock.MagicMock()
        interface.message_pb2.Message.return_value = fake_msg
        with self.assertRaises(GrpcCustomInterfaceError) as err:
            generate_message(struct_payload={"k": "v"}, proto_interface=interface)

        self.assertIn("structPayload", str(err.exception))
        self.assertIn("Custom interface mismatch", str(err.exception))

    def test_byte_payload_missing_field_raises_custom_interface_error(self):
        """Missing bytePayload field raises GrpcCustomInterfaceError with guidance."""
        fake_msg = mock.MagicMock()
        fake_msg.metaInfo = mock.MagicMock()
        fake_msg.metaInfo.messageName = ""
        fake_msg.payload = object()  # no structPayload/bytePayload attributes

        interface = mock.MagicMock()
        interface.message_pb2.Message.return_value = fake_msg
        with self.assertRaises(GrpcCustomInterfaceError) as err:
            generate_message(byte_payload=b"x", proto_interface=interface)

        self.assertIn("bytePayload", str(err.exception))
        self.assertIn("Custom interface mismatch", str(err.exception))

if __name__ == "__main__":
    unittest.main()
