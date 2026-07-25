"""
History Feature Demo -- Clients
=================================

Demonstrates multi-hop message tracing via the ``history`` field.

A single message is bounced back and forth between two clients through the
server.  Each round trip adds 4 DataPoints (and 4 transits) to the history.

    ping  --[ping]--> server --[ping]--> pong
    pong  --[pong]--> server --[pong]--> ping
    ... repeated N_ROUNDS times ...

Run
---

    python examples/history/HistoryServer.py
    python examples/history/HistoryClients.py
"""

import sys
import time
from datetime import datetime
from pathlib import Path



project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))  # pylint: disable=wrong-import-position
# pylint: disable=wrong-import-position  # project path must be set before importing project modules
from grpchook.exceptions import GrpcValueError
from grpchook.tools import evaluate_history, generate_message
from grpchook.baseclient import BaseClient
from grpchook import message_pb2
from tests.integration._interface import get_args

N_ROUNDS = 3  # each round trip adds 4 transits; 3 rounds -> 12 transits
TIMEOUT = 1.0 # seconds

class PingClient(BaseClient):
    def __init__(self, port: int):
        super().__init__(port, name="ping",
                         provides=["ping", "server-exit"], requires=["pong"])


class PongClient(BaseClient):
    def __init__(self, port: int):
        super().__init__(port, name="pong", provides=["pong"], requires=["ping"])


if __name__ == "__main__":
    args = get_args("History feature test")
    ping = PingClient(args.port)
    pong = PongClient(args.port)

    expected_hops = 1 + 4 * N_ROUNDS
    expected_transits = 4 * N_ROUNDS

    ping.logger.info(
        "History demo --- %d round trips --- ping <-> server <-> pong  "
        "(expected hops: %d  transits: %d)",
        N_ROUNDS, expected_hops, expected_transits,
    )

    try:

        # try to manually add history -> not the desired workflow, check that this casues an error.
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="ping"),
            history=[ # dont do that
                message_pb2.DataPoint(
                    name="ping",
                    receiveTimestamp=datetime.now(),
                    perfCounter=time.perf_counter(),
                )
            ],
        )
        try:
            ping.send_data(msg, add_history=True)
            assert False, "Expected ValueError when sending data with history and add_history=True"
        except GrpcValueError:
            pass  # expected

        # this is how it should be done.
        ping.send_data(generate_message(message_name="ping"), add_history=True)

        for r in range(N_ROUNDS):
            # pong side: receive the ping, echo back as pong
            msg = pong.get_data(timeout=TIMEOUT)
            msg.metaInfo.messageName = "pong"
            pong.send_data(msg)

            # ping side: receive the pong
            msg = ping.get_data(timeout=TIMEOUT)

            if r < N_ROUNDS - 1:
                # send back as ping for the next round
                msg.metaInfo.messageName = "ping"
                ping.send_data(msg)

        # msg now holds the final received message with the full hop chain
        ping.logger.info("Final message after %d round trips:", N_ROUNDS)

        assert len(msg.history) == expected_hops,\
            f"Expected {expected_hops} hops in history, got {len(msg.history)}"

        evaluate_history(msg, log_callback=ping.logger.info)

    finally:

        # trigger server shutdown if pipeline mode is active
        ping.send_data(generate_message(message_name="server-exit"))
        ping.wait_done()

        ping.disconnect()
        pong.disconnect()
