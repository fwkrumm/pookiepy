"""Timer Test --- Server
====================
Plain relay server; all timer-driving logic lives in the client.
"""
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

if __name__ == "__main__":
    args = get_args("Timer test: timer-driven client sends tick messages to a subscriber")
    s = IntegrationServer(args.port)
    s.serve_forever()
