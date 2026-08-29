"""Validated protobuf interface dependencies for clients and servers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from types import ModuleType

from pookiepy.exceptions import GrpcCustomInterfaceError


_REQUIRED_MESSAGES = (
    "PookieMessage",
    "MetaInformation",
    "DataPoint",
    "ClientProvides",
    "ServerProvides",
    "Payload",
)
_REQUIRED_MESSAGE_FIELDS = ("metaInfo", "history", "payload")
_REQUIRED_META_FIELDS = (
    "timestamp",
    "messageId",
    "responseToId",
    "clientInfo",
    "serverInfo",
    "messageName",
)
_REQUIRED_GRPC_SYMBOLS = (
    "StreamStub",
    "StreamServicer",
    "add_StreamServicer_to_server",
)


def _missing_module_symbols(module: ModuleType, names: tuple[str, ...], label: str) -> list[str]:
    """Return required symbols absent from a generated module."""
    return [f"{label}.{name}" for name in names if not hasattr(module, name)]


def _missing_message_fields(message_type, names: tuple[str, ...], label: str) -> list[str]:
    """Return required protobuf fields absent from a generated message type."""
    descriptor = getattr(message_type, "DESCRIPTOR", None)
    if descriptor is None:
        return [f"{label}.DESCRIPTOR"]
    fields = getattr(descriptor, "fields_by_name", {})
    return [f"{label}.{name}" for name in names if name not in fields]


def _service_errors(message_pb2: ModuleType) -> list[str]:
    """Validate the required bidirectional Stream.DataChannel RPC descriptor."""
    descriptor = getattr(message_pb2, "DESCRIPTOR", None)
    if descriptor is None:
        return ["message_pb2.DESCRIPTOR"]

    service = descriptor.services_by_name.get("Stream")
    if service is None:
        return ["service Stream"]

    method = service.methods_by_name.get("DataChannel")
    if method is None:
        return ["rpc Stream.DataChannel"]

    errors = []
    if not method.client_streaming:
        errors.append("Stream.DataChannel client streaming")
    if not method.server_streaming:
        errors.append("Stream.DataChannel server streaming")
    if method.input_type.full_name != message_pb2.PookieMessage.DESCRIPTOR.full_name:
        errors.append("Stream.DataChannel input PookieMessage")
    if method.output_type.full_name != message_pb2.PookieMessage.DESCRIPTOR.full_name:
        errors.append("Stream.DataChannel output PookieMessage")
    return errors


def _validate_interface(message_pb2: ModuleType, message_pb2_grpc: ModuleType) -> None:
    """Raise one error containing every incompatibility in a module pair."""
    if not isinstance(message_pb2, ModuleType) or not isinstance(message_pb2_grpc, ModuleType):
        raise GrpcCustomInterfaceError(
            "ProtoInterface requires imported message_pb2 and message_pb2_grpc modules"
        )

    errors = _missing_module_symbols(message_pb2, _REQUIRED_MESSAGES, "message_pb2")
    errors.extend(
        _missing_module_symbols(message_pb2_grpc, _REQUIRED_GRPC_SYMBOLS, "message_pb2_grpc")
    )

    if hasattr(message_pb2, "PookieMessage"):
        errors.extend(
            _missing_message_fields(
                message_pb2.PookieMessage,
                _REQUIRED_MESSAGE_FIELDS,
                "message_pb2.PookieMessage",
            )
        )
        errors.extend(_service_errors(message_pb2))
    if hasattr(message_pb2, "MetaInformation"):
        errors.extend(
            _missing_message_fields(
                message_pb2.MetaInformation,
                _REQUIRED_META_FIELDS,
                "message_pb2.MetaInformation",
            )
        )

    if errors:
        raise GrpcCustomInterfaceError(
            "Incompatible protobuf interface; missing or invalid: " + ", ".join(errors)
        )


@dataclass(frozen=True, slots=True)
class ProtoInterface:
    """Immutable pair of compatible, precompiled protobuf modules."""

    message_pb2: ModuleType
    message_pb2_grpc: ModuleType

    def __post_init__(self) -> None:
        _validate_interface(self.message_pb2, self.message_pb2_grpc)


@lru_cache(maxsize=1)
def _bundled_interface() -> ProtoInterface:
    """Return the bundled interface used when no custom interface is supplied."""
    from pookiepy import message_pb2  # pylint: disable=import-outside-toplevel
    from pookiepy import message_pb2_grpc  # pylint: disable=import-outside-toplevel

    return ProtoInterface(message_pb2, message_pb2_grpc)
