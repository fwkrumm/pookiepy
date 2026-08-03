"""
Shared base class for integration test servers.

Provides pipeline-mode support (``--pipeline`` CLI flag) on top of
``BaseServer``.  When pipeline mode is active, a client may send a
``"server-exit"`` message to trigger a graceful server shutdown.

Usage
-----
    from tests.integration._server_base import IntegrationServer

    class MyServer(IntegrationServer):
        def __init__(self, port, args):
            super().__init__(port, args)

        def on_receive(self, peer, data):
            # domain logic here
            ...
            return super().on_receive(peer, data)  # handles pipeline/exit
"""

import threading
import time

from pookiepy.baseserver import BaseServer, Peer
from pookiepy import message_pb2


SHUTDOWN_DELAY = 1  # seconds to wait before shutting down after receiving "server-exit"
DISCONNECT_SHUTDOWN_DELAY = 0.5  # fallback delay after last client disconnect

class IntegrationServer(BaseServer):
    """``BaseServer`` subclass that adds pipeline/exit handling for integration tests.

    Args:
        port: Port number to listen on.
        args: Parsed ``argparse.Namespace`` from :func:`tests.integration._interface.get_args`.
    """

    def __init__(self, port: int, *args, ip: str = "127.0.0.1", **kwargs):
        """Initialize test server on explicit loopback by default.

        Integration tests run many short-lived subprocesses in CI. Binding to
        loopback instead of wildcard address reduces cross-process port
        collisions on shared runners while still allowing explicit overrides.
        """
        self._active_client_ids: set[str] = set()
        self._active_client_lock = threading.Lock()
        super().__init__(port, *args, ip=ip, **kwargs)

    def on_client_accepted(self, peer: Peer, request: message_pb2.Message):
        """Track accepted integration clients for disconnect-based shutdown fallback."""
        client_id = request.metaInfo.clientInfo.uuid or peer.client_id
        if client_id:
            with self._active_client_lock:
                self._active_client_ids.add(client_id)

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        """Handle ``"server-exit"`` in pipeline mode, then forward the message.

        Subclasses should call ``super().on_receive(peer, request)`` after their
        own logic to preserve this behaviour.

        Args:
            peer: The connected client that sent the message.
            request: The received protobuf message.

        Returns:
            ``True`` to fan-out the message via ``DataRegister``.
        """
        if  request.metaInfo.messageName == "server-exit":
            self.logger.info("got exit message from client %s", peer.name)

            def _shutdown():
                self.logger.info(
                    "shutting down server in %s second(s), make sure that all clients "
                    "disconnect in that time to prevent errors.",
                    SHUTDOWN_DELAY,
                )
                time.sleep(SHUTDOWN_DELAY)
                self.global_exit_event.set()

            threading.Thread(target=_shutdown, daemon=True).start()

        return True

    def on_client_disconnect(self, peer: Peer):
        """Fail-safe shutdown for CI: stop when no clients remain connected.

        Integration examples are finite scripts. If the explicit ``server-exit``
        message is lost during teardown, this prevents the server process from
        idling forever and hanging the pipeline.
        """
        should_shutdown = False
        with self._active_client_lock:
            was_tracked = peer.client_id in self._active_client_ids
            if was_tracked:
                self._active_client_ids.discard(peer.client_id)
                should_shutdown = not self._active_client_ids

        if should_shutdown and not self.global_exit_event.is_set():
            self.logger.info(
                "all clients disconnected (last=%s); triggering fallback shutdown in %.1fs",
                peer.name,
                DISCONNECT_SHUTDOWN_DELAY,
            )

            def _shutdown_after_disconnect_delay():
                time.sleep(DISCONNECT_SHUTDOWN_DELAY)
                self.global_exit_event.set()

            threading.Thread(target=_shutdown_after_disconnect_delay, daemon=True).start()
