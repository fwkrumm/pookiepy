"""Basic connectivity integration test --- server."""
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer


class GrpcServer(IntegrationServer):
    """Basic gRPC server for integration tests."""

    def __init__(self, port):
        super().__init__(port)
        self.logger.info("initialized GrpcServer")
        self.logger.debug("this should only appear in log file: initialized GrpcServer")


if __name__ == "__main__":
    args = get_args("Basic connectivity test")
    s = GrpcServer(args.port)
    s.serve_forever()
