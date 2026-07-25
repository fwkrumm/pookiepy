"""
Exception-Handling Test --- Server
==================================

Basic server for the exception-handling integration test.
Passes all messages through unchanged via IntegrationServer.
"""

from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer


class ExceptionHandlingServer(IntegrationServer):
    """Plain pass-through server --- exception handling is client-side."""

    def __init__(self, port: int):
        super().__init__(port)
        self.logger.info("initialized ExceptionHandlingServer")


if __name__ == "__main__":
    args = get_args("Exception-handling test: no freeze, no ghost threads")
    s = ExceptionHandlingServer(args.port)
    s.serve_forever()
