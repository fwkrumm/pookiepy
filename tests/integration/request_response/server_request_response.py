"""Request-response correlation test --- server.

Passthrough server: routes messages between clients without modification.
"""

from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer


class RequestResponseServer(IntegrationServer):
    """Passthrough server for the request-response correlation test."""


if __name__ == "__main__":
    args = get_args("Request-response correlation test")
    s = RequestResponseServer(args.port)
    s.serve_forever()
