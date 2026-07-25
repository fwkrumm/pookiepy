"""
Broadcast Test --- Server
========================

Server that initiates messages to all connected clients without being triggered
by any incoming client message. A background thread pushes ``"broadcast"``
messages at a fixed rate into the DataRegister for all subscribers.
"""

import threading

from grpchook import message_pb2
from grpchook.tools import generate_message
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

BROADCAST_INTERVAL = 0.1   # seconds between broadcasts
BROADCAST_MESSAGE = "broadcast"
NUM_CLIENTS = 3  # must match clients_broadcast.py


class BroadcastServer(IntegrationServer):
    """Server that broadcasts messages to all subscribers at a fixed rate."""

    def __init__(self, port: int):
        self._broadcast_count = 0
        self._clients_ready = threading.Event()
        self._connected_count = 0
        self._connected_lock = threading.Lock()
        super().__init__(port)

    def on_client_connect(self, data: message_pb2.Message, context) -> bool:
        """Start broadcasting only after all expected clients are connected."""
        self.logger.debug("Client '%s' connecting", data.metaInfo.clientInfo.name)
        with self._connected_lock:
            self._connected_count += 1
            if self._connected_count >= NUM_CLIENTS:
                self._clients_ready.set()
        return True

    def on_init(self):
        """Start the background broadcast thread."""
        t = threading.Thread(target=self._broadcast_loop, daemon=True)
        t.start()

    def _broadcast_loop(self):
        """Push one broadcast message per interval until server shuts down."""
        # Block until all NUM_CLIENTS clients have connected and registered
        # their requires --- prevents broadcasting into an empty DataRegister.
        self._clients_ready.wait()

        while not self._global_exit_event.is_set():
            msg = generate_message(
                BROADCAST_MESSAGE,
                byte_payload=str(self._broadcast_count).encode(),
            )
            self._data_register.add_data_for_message_name(
                "server",   # non-existent clientId → no self-skip → broadcast to all
                BROADCAST_MESSAGE,
                msg,
            )
            self._broadcast_count += 1
            # use wait() instead of sleep() so shutdown is not delayed
            self._global_exit_event.wait(timeout=BROADCAST_INTERVAL)

        self.logger.info("broadcast loop stopped after %d messages", self._broadcast_count)


if __name__ == "__main__":
    args = get_args("Broadcast test: server sends to all connected clients")
    s = BroadcastServer(args.port)
    s.serve_forever()
