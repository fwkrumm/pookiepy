"""Custom-interface integration test using precompiled protobuf modules."""
import importlib

from pookiepy.custom_interface import ProtoInterface
from tests.integration._server_base import IntegrationServer
from tests.integration._interface import get_args


message_pb2 = importlib.import_module("custom_if.message_pb2")
message_pb2_grpc = importlib.import_module("custom_if.message_pb2_grpc")
PROTO_INTERFACE = ProtoInterface(message_pb2, message_pb2_grpc)


class GrpcServerCustom(IntegrationServer):
    def __init__(self, port: int):
        super().__init__(port, proto_interface=PROTO_INTERFACE)
        self.logger.info("initialized GrpcServerCustom")

    def on_receive(self, peer, request):
        # add info log here since it is easier to verify that the
        # custom interfaces has been used
        self.logger.info("from %s received data: %s", peer, request)
        return super().on_receive(peer, request)  # handles pipeline/exit

if __name__ == "__main__":
    args = get_args("Custom interface test")
    s = GrpcServerCustom(args.port)
    s.serve_forever()
