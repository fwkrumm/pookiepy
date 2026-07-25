"""Plain gRPC relay server for the mcp_server example."""
from grpchook.baseserver import BaseServer


class McpGrpcServer(BaseServer):
    """
    Plain gRPC server for the mcp_server example.

    No custom routing logic needed --- the default on_receive() returns True
    so every message is fanned out via DataRegister to all subscribers.
    """

    def __init__(self, port: int):
        super().__init__(port, name="McpGrpcServer")

    def on_init(self):
        self.logger.info("McpGrpcServer ready on port %d", self._port)


if __name__ == "__main__":
    server = McpGrpcServer(49998)
    server.serve_forever()
