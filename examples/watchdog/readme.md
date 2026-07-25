# watchdog

Live server-monitoring dashboard. A gRPC server tracks per-client connection stats and a rolling event log. A Flask web UI polls those stats and displays them in a browser dashboard.

## Architecture

```
WatchdogPoller (BaseClient)  ──watchdog_request──►  WatchdogServer (BaseServer)
                              ◄─watchdog_stats────────
        │
     Flask app → http://localhost:5000
```

- **WatchdogServer** --- tracks client connect/disconnect events, message counts, and timestamps. Replies to `watchdog_request` with a full stats snapshot.
- **watchdog_ui** --- runs a `WatchdogPoller` client + a Flask HTTP server. Opens the dashboard automatically in your browser. Keyboard keys send live `ui_event` messages to the server.

## Requirements

```
pip install flask
```

Or: `pip install -r requirements_examples.txt`

## How to run

```
# Terminal 1
pyton examples/watchdog/server_watchdog.py

# Terminal 2
pyton examples/watchdog/watchdog_ui.py
```

Then open http://localhost:5000 in a browser.

Optional flags:
- `--port 49999` --- gRPC server port (default: 49999)
- `--ui-port 5000` --- Flask dashboard port (default: 5000)


Press keys in the UI terminal to send live events.
