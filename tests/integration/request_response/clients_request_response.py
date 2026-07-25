"""
Request-Response Correlation Test --- Clients
============================================

Demonstrates ``messageId``-based request/response correlation between two clients
routed through the server.

Flow
----
1. ClientA builds a ``"request"`` message with a pre-set ``messageId`` and sends it.
2. ClientB receives the request via its ``on_receive`` hook.
3. ClientB sends back ``N_EXTRA_RESPONSES`` unrelated ``"response"`` messages followed
   by one ``"response"`` whose ``messageId`` matches the original request.
4. ClientA collects all incoming responses via its ``on_receive`` hook, logs those
   that do not match as "data received but not response", and retains the matched one.

Assertions
----------
* ClientB receives exactly one request.
* ClientA receives exactly ``N_EXTRA_RESPONSES + 1`` messages in total.
* Exactly one of those messages carries a ``messageId`` that matches the request.

Run
---
    python tests/integration/request_response/clients_request_response.py
"""

import sys
import uuid

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message
from tests.integration._interface import get_args

REQUEST_MSG = "request"
RESPONSE_MSG = "response"
N_EXTRA_RESPONSES = 2
TIMEOUT = 2.0


class ClientA(BaseClient):
    """Sends a single request and collects responses, identifying the matched one by messageId.

    Args:
        port: Server port to connect to.
    """

    def __init__(self, port: int):
        super().__init__(
            port,
            name="clientA",
            provides=[REQUEST_MSG, "server-exit"],
            requires=[RESPONSE_MSG],
        )
        self.received: list[message_pb2.Message] = []
        self.request_id: str | None = None

    def on_receive(self, data: message_pb2.Message):
        """Classify incoming responses: log non-matching ones, retain all.

        Args:
            data: Received protobuf message.
        """
        if data.metaInfo.messageId == self.request_id:
            self.logger.info(
                "ClientA: matched response (messageId=%s)", self.request_id
            )
        else:
            self.logger.info(
                "ClientA: data received but not response (messageId=%s)",
                data.metaInfo.messageId,
            )
        self.received.append(data)


class ClientB(BaseClient):
    """Receives requests and replies with extra data followed by the correlated response.

    Args:
        port: Server port to connect to.
    """

    def __init__(self, port: int):
        super().__init__(
            port,
            name="clientB",
            provides=[RESPONSE_MSG],
            requires=[REQUEST_MSG],
        )
        self.requests_received: int = 0

    def on_receive(self, data: message_pb2.Message):
        """On each request send N_EXTRA_RESPONSES unrelated replies then one matched reply.

        Args:
            data: Received protobuf message (the request).
        """
        self.requests_received += 1
        req_id = data.metaInfo.messageId
        self.logger.info("ClientB: received request (messageId=%s)", req_id)

        for i in range(N_EXTRA_RESPONSES):
            extra = generate_message(RESPONSE_MSG, byte_payload=f"extra_{i}".encode())
            self.send_data(extra)
            self.logger.info("ClientB: sent extra response %d", i)

        reply = generate_message(RESPONSE_MSG, byte_payload=b"response")
        reply.metaInfo.messageId = req_id  # correlate with the request
        self.send_data(reply)
        self.logger.info("ClientB: sent matched response (messageId=%s)", req_id)


if __name__ == "__main__":
    args = get_args("Request-response correlation test")

    client_a = ClientA(args.port)
    client_b = ClientB(args.port)

    # build request with a known messageId (pre-set so set_metadata won't overwrite it)
    request_id = uuid.uuid4().hex
    request = generate_message(REQUEST_MSG, byte_payload=b"hello")
    request.metaInfo.messageId = request_id
    client_a.request_id = request_id

    # send request; ClientB processes it and replies
    client_a.send_data(request)
    client_b.spin(timeout=args.timeout)   # receives request → on_receive sends replies
    client_b.wait_done()                  # ensure replies are in gRPC pipeline

    # collect all replies on ClientA
    total_expected = N_EXTRA_RESPONSES + 1
    for _ in range(total_expected):
        client_a.spin(timeout=args.timeout)

    # --- assertions ---
    assert client_b.requests_received == 1, (
        f"ClientB expected 1 request, got {client_b.requests_received}"
    )
    assert len(client_a.received) == total_expected, (
        f"ClientA expected {total_expected} responses, got {len(client_a.received)}"
    )
    matched = [m for m in client_a.received if m.metaInfo.messageId == request_id]
    assert len(matched) == 1, (
        f"Expected exactly 1 matched response, got {len(matched)}"
    )

    client_a.logger.info(
        "OK: ClientA received %d messages, 1 matched response + %d extra",
        total_expected, N_EXTRA_RESPONSES,
    )

    # graceful shutdown
    client_a.send_data(generate_message("server-exit"))
    client_a.wait_done()

    client_a.disconnect()
    client_b.disconnect()

    client_a.logger.info("request_response: all assertions passed")
    sys.exit(0)
