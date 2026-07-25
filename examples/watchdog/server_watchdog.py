"""Watchdog Demo Server
=======================
Standalone gRPC server for the Aero dashboard demo.

Tracks per-client connection statistics and a rolling event log; responds to
``"watchdog_request"`` messages with a full snapshot.

Unlike the integration-test server this one does **not** shut down on
``"server-exit"`` --- it runs until you press Ctrl+C.

Run
---
    python examples/watchdog/server_watchdog.py [--port 49999]
"""
import argparse
import json
import threading
from collections import deque
from datetime import datetime, timezone

from grpchook.baseserver import BaseServer, Peer, ServerConfig
from grpchook.tools import generate_message
from grpchook import message_pb2

WATCHDOG_REQUEST = "watchdog_request"
WATCHDOG_STATS   = "watchdog_stats"
MAX_EVENTS       = 50


class WatchdogServer(BaseServer):
    """Tracks client activity and answers ``"watchdog_request"`` stat polls.

    Attributes:
        _client_stats: Mapping from client identifier to connection metadata.
        _events: Rolling deque of the last ``MAX_EVENTS`` messages received.
        _total_events: Monotonically increasing count for client-side diffing.
    """

    def on_init(self) -> None:
        """Initialise shared state before any client connects."""
        self._client_stats: dict[str, dict] = {}
        self._events: deque = deque(maxlen=MAX_EVENTS)
        self._total_events: int = 0
        self._lock = threading.Lock()

    def on_client_connect(self, data: message_pb2.Message, context) -> bool:
        """Record a new client in the stats table.

        Args:
            data: First message carrying clientInfo.
            context: gRPC servicer context.

        Returns:
            Always True --- all clients are accepted.
        """
        name = data.metaInfo.clientInfo.name
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._client_stats[name] = {
                "msg_count": 0,
                "connected_at": now,
                "last_seen": now,
            }
        return True

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        """Dispatch incoming messages; reply to stat requests with unicast.

        Args:
            peer: The sending client.
            request: Received protobuf message.

        Returns:
            True to fan-out the message; False when handled privately.
        """
        name = request.metaInfo.messageName

        if name == WATCHDOG_REQUEST:
            self._send_stats(peer)
            return False

        # extract a short human-readable preview of the payload
        preview = _payload_preview(request)

        with self._lock:
            if peer.name in self._client_stats:
                self._client_stats[peer.name]["msg_count"] += 1
                self._client_stats[peer.name]["last_seen"] = (
                    datetime.now(timezone.utc).isoformat()
                )
            self._events.append({
                "id":      self._total_events,
                "ts":      datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
                "sender":  peer.name,
                "name":    name,
                "preview": preview,
            })
            self._total_events += 1

        return True

    def _send_stats(self, peer: Peer) -> None:
        """Build and unicast a stats snapshot to the requesting client.

        Args:
            peer: Client to receive the snapshot.
        """
        with self._lock:
            clients  = {k: dict(v) for k, v in self._client_stats.items()}
            events   = list(self._events)
            total    = self._total_events

        payload = {
            "clients":       clients,
            "events_json":   json.dumps(events),
            "total_events":  total,
        }
        response = generate_message(WATCHDOG_STATS, struct_payload=payload)
        self._data_register.add_data_for_message_name(
            "server", WATCHDOG_STATS, response, target_client_id=peer.client_id
        )


def _payload_preview(data: message_pb2.Message) -> str:
    """Return a short UTF-8 preview of whichever payload field is populated.

    Args:
        data: Message whose payload to inspect.

    Returns:
        Up to 80-character preview string, or empty string if no payload.
    """
    if data.payload.bytePayload:
        return data.payload.bytePayload[:80].decode("utf-8", errors="replace")
    if data.payload.structPayload.fields:
        fields = data.payload.structPayload.fields
        if "text" in fields:
            return fields["text"].string_value[:80]
        keys = list(fields.keys())[:3]
        return "{" + ", ".join(keys) + ("…" if len(fields) > 3 else "") + "}"
    return ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Watchdog Aero demo server --- press Ctrl+C to stop"
    )
    parser.add_argument("--port", type=int, default=49999)
    args = parser.parse_args()

    print(f"Watchdog server starting on port {args.port} …")
    print("Start the dashboard:  python examples/watchdog/watchdog_ui.py")
    print("Press Ctrl+C to stop.\n")

    # The UI spawns 1 WatchdogPoller + 26 AlphabetClients, all of which hold a
    # permanent gRPC stream.  Each stream occupies one ThreadPoolExecutor worker
    # for its entire lifetime, so we need at least 27 threads.  32 adds a buffer
    # for any additional tooling connections.
    server = WatchdogServer(args.port, config=ServerConfig(max_workers=32))
    server.serve_forever()
