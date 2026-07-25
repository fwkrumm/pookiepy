"""
Wait-for-Clients Test --- Server
================================

Demonstrates a server that defers a response until all required providers are
connected.

The server waits for at least ``REQUIRED_WORKER_COUNT`` clients that include
``"worker_data"`` in their ``provides`` list before it answers a
``"check_ready"`` message.  The check is non-blocking on the receive thread:
a daemon thread waits on a ``threading.Event`` and sends the response once
the requirement is satisfied.
"""

import threading

from grpchook import message_pb2
from grpchook.baseserver import Peer
from grpchook.tools import generate_message
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

REQUIRED_WORKER_COUNT = 2
WORKER_PROVIDES = "worker_data"
CHECK_READY_MSG = "check_ready"
READY_SIGNAL_MSG = "ready_signal"


class WaitForClientsServer(IntegrationServer):
    """Defers ready_signal until enough worker providers are connected."""

    def __init__(self, port: int):
        super().__init__(port)

    def on_init(self):
        """Initialise the readiness tracking state."""
        self._worker_count = 0
        self._worker_lock = threading.Lock()
        self._ready_event = threading.Event()

    def on_client_connect(
        self, data: message_pb2.Message, context
    ) -> bool:
        """Increment worker counter when a provider of worker_data connects."""
        provides = list(data.metaInfo.clientInfo.provides)
        if WORKER_PROVIDES in provides:
            with self._worker_lock:
                self._worker_count += 1
                count = self._worker_count
            self.logger.info(
                "worker provider connected (%d/%d)",
                count,
                REQUIRED_WORKER_COUNT,
            )
            if count >= REQUIRED_WORKER_COUNT:
                self._ready_event.set()
                self.logger.info("all required workers connected --- ready!")
        return True

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        """Handle check_ready by waiting for the ready event in a daemon thread."""
        if request.metaInfo.messageName == CHECK_READY_MSG:
            client_id = peer.client_id

            def _wait_and_respond():
                self.logger.info(
                    "check_ready from '%s' --- waiting for %d workers",
                    peer.name,
                    REQUIRED_WORKER_COUNT,
                )
                self._ready_event.wait()
                response = generate_message(READY_SIGNAL_MSG)
                self._data_register.add_data_for_message_name(
                    "server",
                    READY_SIGNAL_MSG,
                    response,
                    target_client_id=client_id,
                )
                self.logger.info("ready_signal sent to '%s'", peer.name)

            threading.Thread(target=_wait_and_respond, daemon=True).start()
            return False

        return super().on_receive(peer, request)


if __name__ == "__main__":
    args = get_args(
        "Wait-for-clients test: server defers response until required providers connect"
    )
    s = WaitForClientsServer(args.port)
    s.serve_forever()
