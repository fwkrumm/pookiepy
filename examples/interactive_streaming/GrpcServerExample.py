"""Minimal gRPC server for the interactive streaming example."""
from pookiepy.baseserver import BaseServer


class GrpcServer(BaseServer):
    """
    Minimal server for interactive streaming example.
    """

    def __init__(self, port: int = 49999):
        super().__init__(port)


if __name__ == "__main__":
    s = GrpcServer(49999)
    s.serve_forever()
