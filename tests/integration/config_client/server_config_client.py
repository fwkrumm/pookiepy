"""
Config-Client Test --- Server
=============================

Demonstrates runtime server control via a dedicated "config" client.

The server stores the gRPC ``ServicerContext`` for every connected peer.
When a config message with ``{"action": "disconnect", "target": "<identifier>"}``
arrives, the server calls ``context.cancel()`` on the target peer, which
terminates that client's stream from the server side.
"""

import threading

import grpc

from grpchook import message_pb2
from grpchook.baseserver import Peer
from grpchook.tools import struct_to_json
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

CONFIG_MESSAGE = "server_config"


class ConfigServer(IntegrationServer):
    """Server that can disconnect individual clients on command."""

    def __init__(self, port: int):
        super().__init__(port)

    def on_init(self):
        """Initialise per-peer context store."""
        self._peer_contexts: dict[str, grpc.ServicerContext] = {}
        self._contexts_lock = threading.Lock()

    def on_client_connect(
        self, data: message_pb2.Message, context: grpc.ServicerContext
    ) -> bool:
        """Register the peer's gRPC context so it can be cancelled later."""
        name = data.metaInfo.clientInfo.name
        with self._contexts_lock:
            self._peer_contexts[name] = context
        self.logger.info("registered context for '%s'", name)
        return True

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        """Handle config messages; forward everything else."""
        if request.metaInfo.messageName == CONFIG_MESSAGE:
            payload = struct_to_json(request.payload.structPayload)
            action = payload.get("action")

            if action == "disconnect":
                target = payload.get("target", "")
                self.logger.info(
                    "config: disconnect request for '%s' from '%s'",
                    target,
                    peer.name,
                )
                with self._contexts_lock:
                    ctx = self._peer_contexts.pop(target, None)

                if ctx is not None:
                    ctx.cancel()
                    self.logger.info("cancelled context for '%s'", target)
                else:
                    self.logger.warning("no context found for '%s'", target)

            return False  # do not fan-out config messages

        return super().on_receive(peer, request)


if __name__ == "__main__":
    args = get_args("Config-client test: runtime disconnect of individual peers")
    s = ConfigServer(args.port)
    s.serve_forever()
