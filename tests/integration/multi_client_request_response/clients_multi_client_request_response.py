"""Multi-client request/response test --- clients.

Scenario:
- 3 requester clients send same messageName ("request").
- 1 responder client receives requests and answers with messageName "response".
- Server must unicast each response to request initiator only.
"""

import sys
import uuid

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message
from tests.integration._interface import get_args

REQUEST_MSG = "request"
RESPONSE_MSG = "response"


class RequestClient(BaseClient):
    """Requester client that stores received responses for assertions."""

    def __init__(self, name: str, port: int, ip: str):
        super().__init__(
            port,
            name=name,
            provides=[REQUEST_MSG, "server-exit"],
            requires=[RESPONSE_MSG],
            ip=ip,
        )
        self.request_id: str | None = None
        self.received: list[message_pb2.Message] = []

    def on_receive(self, data: message_pb2.Message):
        self.received.append(data)


class ResponderClient(BaseClient):
    """Responder that turns each request into one correlated response."""

    def __init__(self, port: int, ip: str):
        super().__init__(
            port,
            name="responder",
            provides=[RESPONSE_MSG],
            requires=[REQUEST_MSG],
            ip=ip,
        )
        self.handled_request_ids: list[str] = []

    def on_receive(self, data: message_pb2.Message):
        req_id = data.metaInfo.messageId
        self.handled_request_ids.append(req_id)

        reply = generate_message(RESPONSE_MSG, byte_payload=f"reply:{req_id}".encode())
        reply.metaInfo.responseToId = req_id
        self.send_data(reply)


def send_one_request(client: RequestClient):
    """Send one request with stable id for downstream correlation assertions."""
    req_id = uuid.uuid4().hex
    msg = generate_message(REQUEST_MSG, byte_payload=client.name.encode())
    msg.metaInfo.messageId = req_id
    client.request_id = req_id
    client.send_data(msg)


if __name__ == "__main__":
    args = get_args("Multi-client request/response unicast test")

    requesters = [
        RequestClient("requester_1", args.port, args.ip),
        RequestClient("requester_2", args.port, args.ip),
        RequestClient("requester_3", args.port, args.ip),
    ]
    responder = ResponderClient(args.port, args.ip)

    try:
        for requester in requesters:
            send_one_request(requester)

        for _ in requesters:
            responder.spin(timeout=args.timeout)
        responder.wait_done()

        for requester in requesters:
            requester.spin(timeout=args.timeout)

        assert len(responder.handled_request_ids) == 3, (
            "Responder expected 3 requests, got "
            f"{len(responder.handled_request_ids)}"
        )
        assert len(set(responder.handled_request_ids)) == 3, (
            "Responder request ids are not unique"
        )

        for requester in requesters:
            assert requester.request_id, f"{requester.name}: missing request_id"
            assert len(requester.received) == 1, (
                f"{requester.name}: expected 1 response, got {len(requester.received)}"
            )
            response = requester.received[0]
            assert response.metaInfo.responseToId == requester.request_id, (
                f"{requester.name}: responseToId mismatch, expected "
                f"{requester.request_id}, got {response.metaInfo.responseToId}"
            )

        requesters[0].send_data(generate_message("server-exit"))
        requesters[0].wait_done()
    finally:
        for requester in requesters:
            requester.disconnect()
        responder.disconnect()

    sys.exit(0)
