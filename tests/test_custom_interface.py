"""Unit tests for explicit precompiled protobuf interface injection."""

import dataclasses
import types
import unittest

from pookiepy import message_pb2, message_pb2_grpc
from pookiepy.custom_interface import ProtoInterface
from pookiepy.exceptions import GrpcCustomInterfaceError


def _module_copy(module, name: str):
    copied = types.ModuleType(name)
    copied.__dict__.update(module.__dict__)
    copied.__name__ = name
    return copied


class TestProtoInterface(unittest.TestCase):
    """Validate the one supported custom-interface container."""

    def test_accepts_bundled_generated_modules(self):
        interface = ProtoInterface(message_pb2, message_pb2_grpc)
        self.assertIs(interface.message_pb2, message_pb2)
        self.assertIs(interface.message_pb2_grpc, message_pb2_grpc)

    def test_is_frozen(self):
        interface = ProtoInterface(message_pb2, message_pb2_grpc)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            interface.message_pb2 = message_pb2

    def test_requires_module_objects(self):
        with self.assertRaisesRegex(GrpcCustomInterfaceError, "imported message_pb2"):
            ProtoInterface(object(), object())

    def test_reports_all_missing_module_symbols(self):
        empty_pb2 = types.ModuleType("empty_pb2")
        empty_grpc = types.ModuleType("empty_pb2_grpc")
        with self.assertRaises(GrpcCustomInterfaceError) as raised:
            ProtoInterface(empty_pb2, empty_grpc)
        message = str(raised.exception)
        self.assertIn("message_pb2.Message", message)
        self.assertIn("message_pb2.MetaInformation", message)
        self.assertIn("message_pb2_grpc.StreamStub", message)
        self.assertIn("message_pb2_grpc.add_StreamServicer_to_server", message)

    def test_reports_missing_message_field(self):
        custom_pb2 = _module_copy(message_pb2, "missing_message_field_pb2")
        descriptor = types.SimpleNamespace(
            fields_by_name={"metaInfo": object(), "history": object()},
            full_name=message_pb2.Message.DESCRIPTOR.full_name,
        )
        custom_pb2.Message = type("Message", (), {"DESCRIPTOR": descriptor})
        with self.assertRaisesRegex(GrpcCustomInterfaceError, "Message.payload"):
            ProtoInterface(custom_pb2, message_pb2_grpc)

    def test_reports_missing_metadata_field(self):
        custom_pb2 = _module_copy(message_pb2, "missing_meta_field_pb2")
        fields = dict(message_pb2.MetaInformation.DESCRIPTOR.fields_by_name)
        fields.pop("responseToId")
        descriptor = types.SimpleNamespace(fields_by_name=fields)
        custom_pb2.MetaInformation = type("MetaInformation", (), {"DESCRIPTOR": descriptor})
        with self.assertRaisesRegex(GrpcCustomInterfaceError, "MetaInformation.responseToId"):
            ProtoInterface(custom_pb2, message_pb2_grpc)

    def test_requires_stream_service(self):
        custom_pb2 = _module_copy(message_pb2, "missing_service_pb2")
        custom_pb2.DESCRIPTOR = types.SimpleNamespace(services_by_name={})
        with self.assertRaisesRegex(GrpcCustomInterfaceError, "service Stream"):
            ProtoInterface(custom_pb2, message_pb2_grpc)

    def test_requires_bidirectional_data_channel(self):
        method = types.SimpleNamespace(
            client_streaming=False,
            server_streaming=False,
            input_type=message_pb2.Message.DESCRIPTOR,
            output_type=message_pb2.Message.DESCRIPTOR,
        )
        service = types.SimpleNamespace(methods_by_name={"DataChannel": method})
        custom_pb2 = _module_copy(message_pb2, "unary_service_pb2")
        custom_pb2.DESCRIPTOR = types.SimpleNamespace(services_by_name={"Stream": service})
        with self.assertRaises(GrpcCustomInterfaceError) as raised:
            ProtoInterface(custom_pb2, message_pb2_grpc)
        message = str(raised.exception)
        self.assertIn("client streaming", message)
        self.assertIn("server streaming", message)

    def test_two_interfaces_coexist(self):
        first = ProtoInterface(message_pb2, message_pb2_grpc)
        second = ProtoInterface(message_pb2, message_pb2_grpc)
        self.assertIsNot(first, second)
        self.assertIs(first.message_pb2, second.message_pb2)


if __name__ == "__main__":
    unittest.main()
