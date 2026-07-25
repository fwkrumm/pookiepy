"""Integration test server for the history scenario."""
# pylint: disable=duplicate-code  # integration server files share a grpchook sys.path + import pattern
import sys
from pathlib import Path

# make sure imports can be resolved
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))  # pylint: disable=wrong-import-position
# pylint: disable=wrong-import-position  # project path must be set before importing project modules

from tests.integration._server_base import IntegrationServer
from tests.integration._interface import get_args
from grpchook.baseserver import Peer
from grpchook import message_pb2


class HistoryServer(IntegrationServer):
    """
    Simple relay server used by the history demo.

    Forwards every incoming message unchanged so that the full hop chain
    producer → server → consumer is captured in the message's history field.
    """

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        self.logger.info(
            "Forwarding '%s' with %d history hop(s) from %s",
            request.metaInfo.messageName,
            len(request.history),
            peer,
        )
        return super().on_receive(peer, request)


if __name__ == "__main__":
    args = get_args("History feature test")
    s = HistoryServer(args.port)
    s.serve_forever()
