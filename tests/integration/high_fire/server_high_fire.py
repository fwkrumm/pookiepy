"""High-Fire Test --- Server
==========================
Plain relay server; all stress-test logic lives in the client.
"""
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

if __name__ == "__main__":
    args = get_args("High-fire test: maximum-throughput stress test on localhost")
    s = IntegrationServer(args.port)
    s.serve_forever()
