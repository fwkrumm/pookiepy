"""Timer Test --- Server
====================
Plain relay server; all timer-driving logic lives in the client.
"""
import logging

from pookiepy.baseserver import ServerConfig
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer

MAX_TIMER_CLIENTS = 64

if __name__ == "__main__":
    args = get_args("Timer test: 200 timed messages broadcast to 50 subscribers")
    s = IntegrationServer(
        args.port,
        config=ServerConfig(max_workers=MAX_TIMER_CLIENTS),
    )
    # Exclude synchronous per-message debug file writes from timing results;
    # slow CI storage otherwise measures logging throughput instead of delivery.
    s.logger.setLevel(logging.INFO)
    s.serve_forever()
