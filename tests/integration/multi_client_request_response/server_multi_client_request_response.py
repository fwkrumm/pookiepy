"""Multi-client request/response test --- server.

Routes shared requests to responder client and unicasts each response back to
original requester using messageId -> client_id correlation.
"""

from grpchook.baseserver import Peer
from grpchook import message_pb2
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

REQUEST_MSG = "request"
RESPONSE_MSG = "response"


class MultiClientRequestResponseServer(IntegrationServer):
    """Correlates request origins and unicasts responses to request initiators."""

    def on_init(self):
        self._origin_by_request_id: dict[str, str] = {}

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        name = request.metaInfo.messageName

        if name == REQUEST_MSG:
            self._origin_by_request_id[request.metaInfo.messageId] = peer.client_id
            return super().on_receive(peer, request)

        if name == RESPONSE_MSG:
            origin_client_id = self._origin_by_request_id.pop(
                request.metaInfo.responseToId,
                None,
            )
            if not origin_client_id:
                self.logger.warning(
                    "Dropping response with unknown responseToId=%s",
                    request.metaInfo.responseToId,
                )
                return False

            self._data_register.add_data_for_message_name(
                peer.client_id,
                RESPONSE_MSG,
                request,
                target_client_id=origin_client_id,
            )
            return False

        return super().on_receive(peer, request)


if __name__ == "__main__":
    args = get_args("Multi-client request/response unicast test")
    s = MultiClientRequestResponseServer(args.port)
    s.serve_forever()
