"""Custom-interface integration test --- server using a runtime-compiled proto."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))  # enable: import _proto_setup
# pylint: disable=wrong-import-position
import _proto_setup  # side-effect: compiles + registers custom proto
from tests.integration._server_base import IntegrationServer
from tests.integration._interface import get_args
# pylint: enable=wrong-import-position

_proto_setup.ensure_loaded()


class GrpcServerCustom(IntegrationServer):
    def __init__(self, port: int):
        super().__init__(port)
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
