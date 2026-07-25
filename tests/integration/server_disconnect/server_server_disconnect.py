"""
Server-Disconnect Test --- Server
================================

Auto-shuts down 1 second after the first client connects. Used together with
``clients_server_disconnect.py`` to verify that the client detects the
disconnect promptly and exits without freezing.
"""

import threading
import time

from grpchook import message_pb2
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer


SHUTDOWN_AFTER = 1.0  # seconds after first client connects


class ServerDisconnectServer(IntegrationServer):
    """Server that shuts itself down shortly after the first client connects."""

    def __init__(self, port: int):
        super().__init__(port)
        self._shutdown_scheduled = False

    def on_client_connect(self, data: message_pb2.Message, context) -> bool:
        """Schedule server shutdown on first client connection."""
        if not self._shutdown_scheduled:
            self._shutdown_scheduled = True
            self.logger.info(
                "First client '%s' connected --- scheduling shutdown in %.1fs",
                data.metaInfo.clientInfo.name,
                SHUTDOWN_AFTER,
            )
            threading.Thread(target=self._auto_shutdown, daemon=True).start()
        return True

    def _auto_shutdown(self):
        """Sleep then set the global exit event to trigger serve_forever shutdown."""
        time.sleep(SHUTDOWN_AFTER)
        self.logger.info("Auto-shutdown triggered")
        self._global_exit_event.set()


if __name__ == "__main__":
    args = get_args("Server-disconnect test: server auto-shuts after client connects")
    s = ServerDisconnectServer(args.port)
    s.serve_forever()
