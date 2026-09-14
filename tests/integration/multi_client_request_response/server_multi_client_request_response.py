"""Multi-client request/response test --- server.

Routes shared requests to responder client and unicasts each response back to
original requester using messageId -> client_id correlation.

Alternative design: requester_i sends dedicated request/response topics (for
example response_client1/response_client2/response_client3) and server routes
by messageName only. This example shows the shared-topic + correlation-id model.
"""

from pookiepy.baseserver import Peer
from pookiepy import message_pb2
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

REQUEST_MSG = "request"
RESPONSE_MSG = "response"


class MultiClientRequestResponseServer(IntegrationServer):
    """Correlates request origins and unicasts responses to request initiators."""

    def on_init(self):
        # request messageId -> origin requester client_id
        self._origin_by_request_id: dict[str, str] = {}

    def on_receive(self, peer: Peer, request: message_pb2.PookieMessage) -> bool:
        name = request.metaInfo.messageName

        if name == REQUEST_MSG:
            # Shared request topic: remember who originated this request id.
            self._origin_by_request_id[request.metaInfo.messageId] = peer.client_id
            return super().on_receive(peer, request)

        if name == RESPONSE_MSG:
            # Response carries responseToId; use it to find exact requester.
            origin_client_id = self._origin_by_request_id.pop(
                request.metaInfo.responseToId,
                None,
            )
            if not origin_client_id:
                self.logger.warning(
                    "Dropping response with unknown responseToId=%s",
                    request.metaInfo.responseToId,
                )
                # Returning False prevents BaseServer fan-out for invalid correlation.
                return False

            # Unicast to exactly one target requester, no broadcast.
            self._data_register.add_data_for_message_name(
                peer.client_id,
                RESPONSE_MSG,
                request,
                target_client_id=origin_client_id,
            )
            # Returning False avoids default broadcast; response already unicasted above.
            return False

        return super().on_receive(peer, request)


if __name__ == "__main__":
    args = get_args("Multi-client request/response unicast test")
    s = MultiClientRequestResponseServer(args.port)
    s.serve_forever()
