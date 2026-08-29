"""
Protobuf helper utilities: message construction, metadata, struct conversion,
and history evaluation.
"""
import uuid
from datetime import datetime, timezone

from google.protobuf import struct_pb2
from google.protobuf import json_format
from google.protobuf.message import Message as ProtobufPookieMessage
from pookiepy.custom_interface import ProtoInterface, _bundled_interface
from pookiepy.exceptions import GrpcCustomInterfaceError

def set_metadata(message: ProtobufPookieMessage):
    """
    Set metadata for a message before sending

    NOTE that this function is called automatically in the request generator before sending
    any message. so in case you want to only set the messageId (and not the timestamp e.g.) then
    a) set the messageID manually or b) delete the timestamp after this function call.


    Parameters
    ----------
    message : message_pb2.PookieMessage
        The message to set metadata for
    """
    if not message.metaInfo.messageId:
        # use .hex instead of str(uuid.uuid4()) to avoid dashes in the messageId
        message.metaInfo.messageId = uuid.uuid4().hex
    # else: this case is not suggested however might be desired in certain situations
    if not message.metaInfo.HasField("timestamp"):
        message.metaInfo.timestamp = datetime.now(timezone.utc)
    # add more metadata fields here if needed

def struct_to_json(struct_msg: struct_pb2.Struct) -> dict:
    """
    convert google.protobuf.Struct to Python dict/JSON.

    Args:
        struct_msg: google.protobuf.Struct message

    Returns:
        dict: Python dictionary
    """
    return json_format.MessageToDict(struct_msg)


def json_to_struct(json_data: dict) -> struct_pb2.Struct:
    """
    convert Python dict/JSON to google.protobuf.Struct.

    Args:
        json_data: Python dictionary

    Returns:
        google.protobuf.Struct: Struct message
    """
    struct_msg = struct_pb2.Struct()
    # potentially need json_format.ParseDict(json_obj, struct_msg)
    struct_msg.update(json_data)
    return struct_msg


def _log_transit_times(history, log_callback: callable) -> None:
    """Log wall-clock transit times between consecutive history hops."""
    if len(history) <= 1:
        return
    log_callback("  --- Transit times (wall-clock, requires synchronised clocks) ---")
    for i in range(len(history) - 1):
        prev = history[i]
        nxt = history[i + 1]
        has_send_prev = prev.sendTimestamp.seconds or prev.sendTimestamp.nanos
        has_recv_next = nxt.receiveTimestamp.seconds or nxt.receiveTimestamp.nanos
        if has_send_prev and has_recv_next:
            send_dt = prev.sendTimestamp.ToDatetime()
            recv_dt = nxt.receiveTimestamp.ToDatetime()
            transit_ms = (recv_dt - send_dt).total_seconds() * 1000
            log_callback(
                f"  [{i}->{i+1}] {prev.name} -> {nxt.name}: transit={transit_ms:.3f} ms"
            )
        else:
            log_callback(
                f"  [{i}->{i+1}] {prev.name} -> {nxt.name}: transit=N/A (missing timestamps)"
            )


def evaluate_history(data: ProtobufPookieMessage, log_callback: callable = None):
    """
    AI GENERATED

    Evaluate and print the DataPoint history of a PookieMessage.

    For each hop the following is logged:
    - Node name (client identifier or "server")
    - Receive timestamp
    - Send timestamp (if the message was forwarded from that hop)
    - Processing time at that hop (time between receive and send, in ms)

    Between consecutive hops the wall-clock transit time is computed from the
    previous hop's sendTimestamp to the next hop's receiveTimestamp.  This
    requires that all participants share a reasonably synchronised clock.

    A summary line reports the total end-to-end latency (first receive →
    last receive) using wall-clock timestamps.

    Parameters
    ----------
    data : message_pb2.PookieMessage
        The message whose ``history`` field is to be evaluated.
    log_callback : callable, optional
        Function used for output.  Defaults to ``print``.
    """
    if log_callback is None:
        log_callback = print

    history = data.history
    if not history:
        log_callback("No history available for this message.")
        return

    msg_id = data.metaInfo.messageId or "N/A"
    log_callback(
        f"=== History for message '{data.metaInfo.messageName}' "
        f"(id={msg_id}) -- {len(history)} hop(s) ==="
    )

    for i, point in enumerate(history):
        has_recv = point.receiveTimestamp.seconds or point.receiveTimestamp.nanos
        has_send = point.sendTimestamp.seconds or point.sendTimestamp.nanos

        recv_str = (
            point.receiveTimestamp.ToDatetime().isoformat(timespec="milliseconds")
            if has_recv else "N/A"
        )
        send_str = (
            point.sendTimestamp.ToDatetime().isoformat(timespec="milliseconds")
            if has_send else "<not forwarded>"
        )

        if has_send:
            # perfCounter holds elapsed processing time (delta) for completed hops
            log_callback(
                f"  [{i}] {point.name:20s}  recv={recv_str}  send={send_str}"
                f"  process={point.perfCounter * 1000:.3f} ms"
            )
        else:
            # Last hop: message was not forwarded; perfCounter is still an absolute value
            log_callback(
                f"  [{i}] {point.name:20s}  recv={recv_str}  send={send_str}"
            )

    # Transit times between consecutive hops (requires synchronised clocks)
    _log_transit_times(history, log_callback)

    # Total end-to-end latency: first receive → last receive
    first = history[0]
    last = history[-1]
    has_first = first.receiveTimestamp.seconds or first.receiveTimestamp.nanos
    has_last = last.receiveTimestamp.seconds or last.receiveTimestamp.nanos
    if has_first and has_last:
        total_ms = (
            last.receiveTimestamp.ToDatetime() - first.receiveTimestamp.ToDatetime()
        ).total_seconds() * 1000
        log_callback(f"  Total end-to-end: {total_ms:.3f} ms")

    log_callback("=== End of history ===")

def generate_message(message_name: str = "default_message",
                     struct_payload: dict = None,
                     byte_payload: bytes = None,
                     add_metadata: bool = False,
                     *,
                     proto_interface: ProtoInterface = None) -> ProtobufPookieMessage:
    """
    AI GENERATED

    Generate a PookieMessage with the given payload and message name.

    The payload can be either a dictionary (which will be converted to a Struct)
    or raw bytes. The message will have a unique messageId and the current
    timestamp set in its metadata. If both payload types are empty, the message will have
    an empty payload.

    If a custom interface changes payload field names, this helper raises
    ``GrpcCustomInterfaceError`` with details on the missing field.

    Parameters
    ----------
    message_name : str, optional
        The name to set in the message's metaInfo.messageName field (default: "default_message").
    struct_payload : dict, optional
        The content to include in the message as a structured payload.  If provided, it will
        be converted to a google.protobuf.Struct.
    byte_payload : bytes, optional
        The content to include in the message as raw bytes.  If provided, it will
        be set as the bytePayload.
    add_metadata : bool, optional
        Whether to set the metadata automatically. NOTE that this is automatically also set
        if the data are sent by the client, however in certain situations or test scenarios
        it might be desired to set the metadata manually. In that case set this to True
        (default: False).
    proto_interface : ProtoInterface, optional
        Precompiled custom interface used to construct the message. Omit it to
        use the bundled interface.

    Returns
    -------
    google.protobuf.message.PookieMessage
        The generated PookieMessage object ready to be sent.

    Raises
    ------
    GrpcCustomInterfaceError
        If the active proto interface does not expose expected payload fields
        (``structPayload`` / ``bytePayload``), which commonly happens when a
        custom payload schema differs from the bundled one.
    """
    interface = proto_interface or _bundled_interface()
    msg = interface.message_pb2.PookieMessage()
    msg.metaInfo.messageName = message_name

    if struct_payload is not None:
        try:
            msg.payload.structPayload.CopyFrom(json_to_struct(struct_payload))
        except AttributeError as exc:
            raise GrpcCustomInterfaceError(
                "Custom interface mismatch in generate_message(): expected payload "
                "field 'structPayload' but active schema does not provide it. "
                "If using pookiepy.custom_interface, align Payload field names or "
                "build messages directly with your custom message_pb2 classes."
            ) from exc

    if byte_payload is not None:
        try:
            msg.payload.bytePayload = byte_payload
        except AttributeError as exc:
            raise GrpcCustomInterfaceError(
                "Custom interface mismatch in generate_message(): expected payload "
                "field 'bytePayload' but active schema does not provide it. "
                "If using pookiepy.custom_interface, align Payload field names or "
                "build messages directly with your custom message_pb2 classes."
            ) from exc

    if add_metadata:
        set_metadata(msg)

    # metadata will be set automatically by the clients (in case set_metadata is False)
    return msg
