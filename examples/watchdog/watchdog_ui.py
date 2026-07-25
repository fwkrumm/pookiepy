"""Watchdog Aero Dashboard
==========================
Interactive Windows-Aero-styled live web dashboard for the gRPC Watchdog demo.

Architecture
------------
* ``WatchdogPoller`` --- gRPC client that periodically requests stats and
  exposes a ``send_ui_event(text)`` method for keyboard-driven messages.
* Flask app --- serves the interactive dashboard at ``/`` and two JSON APIs.

Usage
-----
Start the server first, then run this script::

    python examples/watchdog/server_watchdog.py
    python examples/watchdog/watchdog_ui.py [--port 49999] [--ui-port 5000]

Then open http://localhost:5000 in a browser.
"""
import argparse
import json
import logging
import random
import sys
import threading
import time
import webbrowser

from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcConnectionError
from grpchook.tools import generate_message, struct_to_json
from grpchook import message_pb2

try:
    from flask import Flask, jsonify, request as flask_request
except ImportError:
    print("Flask is required.  Install: pip install -r requirements_examples.txt")
    sys.exit(1)

# ── platform keyboard helper ──────────────────────────────────────────────────
if sys.platform == "win32":
    import msvcrt  # pylint: disable=import-error

    def _getch() -> str:
        """Block until a keypress, then discard any buffered repeat events.

        Reads one character from the console input buffer (blocking), then
        flushes every additional event that the OS already queued (key-repeat
        backlog) so that the next call blocks again on a fresh press.
        Returns the first character as a str, or "" for non-ASCII keys.
        """
        ch = msvcrt.getch()
        # drain every event the OS buffered while we were processing
        while msvcrt.kbhit():
            msvcrt.getch()
        try:
            return ch.decode("ascii")
        except UnicodeDecodeError:
            return ""
else:
    import tty
    import termios  # pylint: disable=import-error

    def _getch() -> str:
        """Read one keypress without Enter (Unix)."""
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            return sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

WATCHDOG_REQUEST = "watchdog_request"
WATCHDOG_STATS   = "watchdog_stats"
UI_EVENT         = "ui_event"
POLL_INTERVAL    = 0.25  # seconds between stat requests

_poll_log = logging.getLogger(__name__)


# ── gRPC client ───────────────────────────────────────────────────────────────

class WatchdogPoller(BaseClient):
    """Background gRPC client: polls for stats and forwards user-typed events.

    Thread-safety: all shared state is guarded by ``_lock``.

    Args:
        port: gRPC server port.
        ip: gRPC server host.
    """

    def __init__(self, port: int, ip: str = "localhost"):
        self._latest_clients: dict = {}
        self._latest_events: list  = []
        self._total_events: int    = 0
        self._lock = threading.Lock()
        super().__init__(
            port,
            name="dashboard_ui",
            ip=ip,
            provides=[WATCHDOG_REQUEST, UI_EVENT],
            requires=[WATCHDOG_STATS],
        )

    def on_receive(self, data: message_pb2.Message) -> bool:
        """Parse incoming stats snapshot and store for the API to serve."""
        raw = struct_to_json(data.payload.structPayload)
        clients = raw.get("clients", {})
        events  = json.loads(raw.get("events_json", "[]"))
        total   = int(raw.get("total_events", 0))
        with self._lock:
            self._latest_clients = clients
            self._latest_events  = events
            self._total_events   = total
        return True

    def latest(self) -> tuple[dict, list, int]:
        """Return a thread-safe snapshot of (clients, events, total_count)."""
        with self._lock:
            return (
                dict(self._latest_clients),
                list(self._latest_events),
                self._total_events,
            )

    def send_ui_event(self, text: str) -> None:
        """Send a user-typed message through the gRPC stream.

        Args:
            text: Message text to deliver as ``"ui_event"`` struct payload.
        """
        self.send_data(generate_message(UI_EVENT, struct_payload={"text": text}))


def _poll_loop(watcher: WatchdogPoller) -> None:
    """Daemon thread: periodically request fresh stats from the server."""
    while watcher.run_event.is_set():
        try:
            watcher.send_data(generate_message(WATCHDOG_REQUEST))
        except (OSError, RuntimeError) as exc:
            _poll_log.debug("poll send failed: %s", exc)
        time.sleep(POLL_INTERVAL)


# ── Flask app ─────────────────────────────────────────────────────────────────

def create_app(stats_client: WatchdogPoller, alpha_clients: dict) -> Flask:
    """Build and return the configured Flask application.

    Args:
        stats_client: Live :class:`WatchdogPoller` instance to serve data from.
        alpha_clients: Shared dict of letter → :class:`AlphabetClient`; may
            be empty at call time and populated later before requests arrive.

    Returns:
        Flask application ready to run.
    """
    flask_app = Flask(__name__)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    @flask_app.route("/")
    def index():
        """Serve the interactive Aero dashboard HTML."""
        return _HTML, 200, {"Content-Type": "text/html; charset=utf-8"}

    @flask_app.route("/api/stats")
    def api_stats():
        """Return a JSON snapshot of current clients and event feed."""
        clients, events, total = stats_client.latest()
        return jsonify({"clients": clients, "events": events, "total": total})

    @flask_app.route("/api/send", methods=["POST"])
    def api_send():
        """Accept a user-typed message and forward it as a gRPC ui_event.

        Expected JSON body: ``{"text": "<message content>"}``
        """
        body = flask_request.get_json(force=True, silent=True) or {}
        text = str(body.get("text", "")).strip()[:200]
        if not text:
            return jsonify({"error": "empty message"}), 400
        try:
            stats_client.send_ui_event(text)
            print(f"  📨 [UI] transmitted: \"{text}\"")
        except (OSError, RuntimeError) as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"ok": True})

    @flask_app.route("/api/fire", methods=["POST"])
    def api_fire():
        """Fire a random AlphabetClient or a specific letter.

        Optional JSON body: ``{"letter": "a"}`` --- omit for a random pick.
        """
        if not alpha_clients:
            return jsonify({"error": "clients not ready"}), 503
        body          = flask_request.get_json(force=True, silent=True) or {}
        chosen_letter = str(body.get("letter", "")).lower()
        if chosen_letter not in alpha_clients:
            chosen_letter = random.choice(list(alpha_clients))
        text = _random_payload(chosen_letter)
        try:
            alpha_clients[chosen_letter].fire(text)
        except (OSError, RuntimeError) as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"ok": True, "letter": chosen_letter, "text": text})

    return flask_app


# ── HTML / CSS / JS ───────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>gRPC Watchdog · Aero Live</title>
<style>
/* ── reset ── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

/* ── base ── */
:root{
  --bg:       #08000e;
  --panel:    rgba(255,255,255,.048);
  --border:   rgba(255,80,210,.14);
  --accent:   #e0109a;
  --cyan:     #ff55d9;
  --green:    #00e676;
  --gold:     #ffd600;
  --muted:    rgba(240,170,255,.5);
  --text:     #f0d0f5;
  --radius:   14px;
  --font:     "Segoe UI",Tahoma,Geneva,Verdana,sans-serif;
}

html,body{
  min-height:100vh; height:100%;
  background:
    radial-gradient(ellipse at 18% 15%,rgba(180,0,120,.22) 0%,transparent 52%),
    radial-gradient(ellipse at 82% 85%,rgba(120,0,180,.18) 0%,transparent 52%),
    linear-gradient(155deg,#070009 0%,#180030 55%,#07000e 100%);
  font-family:var(--font);
  color:var(--text);
  display:flex; flex-direction:column;
}

/* ── glass mixin (applied via .glass) ── */
.glass{
  background:var(--panel);
  backdrop-filter:blur(16px) saturate(170%);
  -webkit-backdrop-filter:blur(16px) saturate(170%);
  border:1px solid var(--border);
  border-radius:var(--radius);
  box-shadow:0 10px 48px rgba(0,0,0,.5),
             inset 0 1px 0 rgba(255,255,255,.11),
             inset 0 -1px 0 rgba(0,0,0,.2);
  position:relative;
}
/* top reflection stripe */
.glass::before{
  content:"";display:block;height:1px;border-radius:var(--radius) var(--radius) 0 0;
  background:linear-gradient(90deg,transparent 0%,rgba(255,255,255,.3) 35%,
             rgba(255,255,255,.45) 50%,rgba(255,255,255,.3) 65%,transparent 100%);
}

/* ── header ── */
header{
  flex-shrink:0;
  margin:.9rem .9rem 0;
  border-radius:var(--radius);
  background:linear-gradient(180deg,rgba(200,0,130,.42) 0%,rgba(100,0,180,.22) 100%);
  border:1px solid rgba(220,0,150,.28);
  box-shadow:0 4px 30px rgba(0,0,0,.45);
  padding:.8rem 1.3rem;
  display:flex; align-items:center; gap:1rem;
  position:relative; overflow:hidden;
}
/* animated shimmer on header */
header::after{
  content:"";position:absolute;inset:0;
  background:linear-gradient(105deg,transparent 30%,rgba(255,255,255,.06) 50%,transparent 70%);
  background-size:200% 100%;
  animation:shimmer 4s linear infinite;
  pointer-events:none;
}
@keyframes shimmer{from{background-position:200% 0}to{background-position:-200% 0}}

.header-title{font-size:1.22rem;font-weight:700;letter-spacing:.06em;
  text-shadow:0 0 18px rgba(255,0,180,.8)}
.header-sub{font-size:.72rem;opacity:.5;letter-spacing:.1em;text-transform:uppercase;
  margin-left:.1rem;margin-top:.18rem}

/* status dot */
.dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;transition:background .4s,box-shadow .4s}
.dot.ok{background:var(--green);box-shadow:0 0 10px var(--green)}
.dot.err{background:#ff5252;box-shadow:0 0 10px #ff5252}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.dot{animation:pulse 2.4s ease-in-out infinite}

/* LIVE badge */
.live-badge{
  margin-left:auto; display:flex; align-items:center; gap:.4rem;
  font-size:.68rem; font-weight:700; letter-spacing:.14em; text-transform:uppercase;
  background:rgba(255,40,40,.18); border:1px solid rgba(255,80,80,.4);
  padding:.22rem .65rem; border-radius:20px; color:#ff6b6b;
}
.live-dot{width:6px;height:6px;border-radius:50%;background:#ff5252;
  box-shadow:0 0 8px #ff5252;animation:pulse 1.4s ease-in-out infinite}

/* header chips */
.hchip{
  display:flex;flex-direction:column;align-items:center;
  padding:.2rem .8rem; border-radius:8px;
  background:rgba(200,0,130,.1); border:1px solid rgba(220,0,160,.2);
  min-width:70px;
}
.hchip .v{font-size:1.25rem;font-weight:700;color:var(--cyan);line-height:1}
.hchip .l{font-size:.6rem;opacity:.55;letter-spacing:.1em;text-transform:uppercase;margin-top:.15rem}

/* ── main grid ── */
main{
  flex:1; display:grid;
  grid-template-columns:340px 1fr;
  gap:.7rem; padding:.7rem .9rem;
  min-height:0;
}

/* ── panel header strip ── */
.ph{
  padding:.65rem 1rem; border-radius:var(--radius) var(--radius) 0 0;
  background:linear-gradient(180deg,rgba(200,0,140,.3) 0%,rgba(100,0,160,.15) 100%);
  border-bottom:1px solid rgba(255,60,200,.22);
  display:flex; align-items:center; gap:.55rem;
  font-size:.72rem; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:#ff99e0;
}
.ph-count{
  margin-left:auto; background:rgba(200,0,140,.22); border:1px solid rgba(230,0,170,.3);
  font-size:.65rem; padding:.1rem .5rem; border-radius:10px;
}

/* ── clients panel ── */
#clients-panel{display:flex;flex-direction:column;overflow:hidden}
#clients-body{flex:1;overflow-y:auto;padding:.7rem}
#clients-body::-webkit-scrollbar{width:4px}
#clients-body::-webkit-scrollbar-thumb{background:rgba(220,0,160,.35);border-radius:4px}

/* client card */
.ccard{
  background:rgba(200,0,120,.07); border:1px solid rgba(220,0,150,.18);
  border-radius:10px; padding:.65rem .9rem; margin-bottom:.55rem;
  transition:background .2s;
}
.ccard:hover{background:rgba(200,0,120,.14)}
.ccard:last-child{margin-bottom:0}
.ccard-name{font-size:.88rem;font-weight:600;color:#f5d0f8}
.ccard-meta{font-size:.68rem;color:var(--muted);margin-top:.2rem}
.ccard-stats{display:flex;align-items:center;gap:.6rem;margin-top:.5rem}
.msg-count{font-size:1.05rem;font-weight:700;color:var(--cyan);min-width:28px}
.bar-bg{flex:1;height:5px;border-radius:3px;background:rgba(255,255,255,.07)}
.bar-fill{height:100%;border-radius:3px;
  background:linear-gradient(90deg,var(--accent),var(--cyan));
  transition:width .5s ease; min-width:2px}
.ts-tag{font-size:.62rem;color:var(--muted)}

/* ── events panel ── */
#events-panel{display:flex;flex-direction:column;overflow:hidden}
#event-feed{
  flex:1; overflow-y:auto;
  padding:.45rem .7rem;
  font-family:"Cascadia Code","Consolas","Courier New",monospace;
  font-size:.8rem;
  display:flex; flex-direction:column; gap:.28rem;
}
#event-feed::-webkit-scrollbar{width:4px}
#event-feed::-webkit-scrollbar-thumb{background:rgba(220,0,160,.35);border-radius:4px}

.ev-row{
  display:grid;
  grid-template-columns:70px 110px 1fr auto;
  gap:.5rem; align-items:baseline;
  padding:.3rem .55rem; border-radius:7px;
  border:1px solid transparent;
  transition:background .15s;
}
.ev-row:hover{background:rgba(255,255,255,.04)}

/* new event animation */
@keyframes ev-in{
  from{opacity:0;transform:translateX(18px)}
  to  {opacity:1;transform:translateX(0)}
}
.ev-new{animation:ev-in .3s ease-out forwards}

/* sender-colour highlight */
.ev-row.src-ui{
  background:rgba(255,214,0,.07);
  border-color:rgba(255,214,0,.2);
}
.ev-row.src-ui:hover{background:rgba(255,214,0,.12)}

.ev-ts{color:rgba(200,120,220,.6);font-size:.72rem}
.ev-sender{
  display:inline-block; padding:.08rem .42rem; border-radius:12px;
  font-size:.68rem; font-weight:600; white-space:nowrap; overflow:hidden;
  text-overflow:ellipsis; max-width:108px;
  border:1px solid currentColor;
}
.ev-name{color:#e8aaf0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ev-preview{color:var(--muted);text-align:right;font-size:.72rem;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:180px}

/* sender palette (cycling, assigned by JS) */
.sc0{color:#ff2d9a;border-color:rgba(255,45,154,.4);background:rgba(255,45,154,.08)}
.sc1{color:#b44dff;border-color:rgba(180,77,255,.4);background:rgba(180,77,255,.08)}
.sc2{color:#00d4ff;border-color:rgba(0,212,255,.4);background:rgba(0,212,255,.08)}
.sc3{color:#ff8a65;border-color:rgba(255,138,101,.4);background:rgba(255,138,101,.08)}
.sc4{color:#c8ff57;border-color:rgba(200,255,87,.4);background:rgba(200,255,87,.08)}
.sc-ui{color:var(--gold);border-color:rgba(255,214,0,.5);background:rgba(255,214,0,.1)}

/* empty state */
.empty-state{
  flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:.5rem;opacity:.28;font-size:.82rem;
}
.empty-icon{font-size:2.5rem}

/* ── command bar ── */
footer{
  flex-shrink:0;
  margin:0 .9rem .9rem;
  padding:.75rem 1rem;
  display:flex; align-items:center; gap:.7rem;
  border-radius:var(--radius);
  background:rgba(10,0,18,.55);
  border:1px solid rgba(200,0,150,.25);
  box-shadow:0 -2px 20px rgba(0,0,0,.35);
}
.cmd-label{
  font-size:.65rem; font-weight:700; letter-spacing:.1em; text-transform:uppercase;
  color:var(--accent); opacity:.8; white-space:nowrap;
}
#msg-input{
  flex:1;
  background:rgba(255,255,255,.05); border:1px solid rgba(200,0,150,.28);
  border-radius:24px; padding:.5rem 1.1rem;
  font-family:var(--font); font-size:.88rem; color:var(--text);
  outline:none; transition:border-color .2s, box-shadow .2s;
}
#msg-input:focus{
  border-color:rgba(230,0,170,.65);
  box-shadow:0 0 0 3px rgba(200,0,130,.18),0 0 14px rgba(200,0,130,.12);
}
#msg-input::placeholder{color:rgba(240,160,255,.35)}
#char-count{font-size:.65rem;color:var(--muted);min-width:38px;text-align:right}
#send-btn{
  padding:.5rem 1.3rem; border-radius:24px; border:none; cursor:pointer;
  background:linear-gradient(135deg,var(--accent) 0%,var(--cyan) 100%);
  color:#1a0010; font-family:var(--font); font-size:.82rem; font-weight:700;
  letter-spacing:.06em; transition:transform .12s,opacity .12s;
  white-space:nowrap;
}
#send-btn:hover{opacity:.9}
#send-btn:active{transform:scale(.95)}
#send-btn:disabled{opacity:.4;cursor:not-allowed}
#fire-btn{
  padding:.5rem 1.1rem; border-radius:24px; border:none; cursor:pointer;
  background:linear-gradient(135deg,#ffab00 0%,#ffd600 100%);
  color:#1a0e00; font-family:var(--font); font-size:.82rem; font-weight:700;
  letter-spacing:.06em; transition:transform .12s,opacity .12s;
  white-space:nowrap;
}
#fire-btn:hover{opacity:.9}
#fire-btn:active{transform:scale(.95)}
.hint{font-size:.62rem;color:var(--muted);opacity:.6;white-space:nowrap}
#status-msg{
  font-size:.7rem; min-width:80px; text-align:center;
  transition:opacity .3s;
}
</style>
</head>
<body>

<!-- ── header ─────────────────────────────────────────── -->
<header>
  <div class="dot ok" id="status-dot"></div>
  <div>
    <div class="header-title">gRPC Watchdog</div>
    <div class="header-sub">Aero Live Dashboard</div>
  </div>
  <div class="hchip" style="margin-left:.6rem">
    <span class="v" id="h-clients">---</span>
    <span class="l">clients</span>
  </div>
  <div class="hchip">
    <span class="v" id="h-events">---</span>
    <span class="l">events</span>
  </div>
  <div class="hchip">
    <span class="v" id="h-msgs">---</span>
    <span class="l">messages</span>
  </div>
  <div class="hchip">
    <span class="v" id="h-rate">---</span>
    <span class="l">msg/s</span>
  </div>
  <div class="live-badge" style="margin-left:auto">
    <div class="live-dot"></div>LIVE
  </div>
</header>

<!-- ── main grid ──────────────────────────────────────── -->
<main>

  <!-- left: clients panel -->
  <div class="glass" id="clients-panel">
    <div class="ph">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2.5"><circle cx="12" cy="8" r="4"/><path d="M4 20c0-4 3.6-7 8-7s8 3 8 7"/></svg>
      Connected Clients
      <span class="ph-count" id="client-count-badge">0</span>
    </div>
    <div id="clients-body">
      <div class="empty-state" id="clients-empty">
        <div class="empty-icon">⟳</div>
        <div>Waiting for clients…</div>
      </div>
    </div>
  </div>

  <!-- right: events panel -->
  <div class="glass" id="events-panel">
    <div class="ph">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2.5"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      Live Event Feed
      <span class="ph-count" id="event-count-badge">0</span>
    </div>
    <div id="event-feed">
      <div class="empty-state" id="events-empty">
        <div class="empty-icon">📡</div>
        <div>No events yet</div>
      </div>
    </div>
  </div>

</main>

<!-- ── command bar ────────────────────────────────────── -->
<footer>
  <span class="cmd-label">⌨ Transmit</span>
  <input id="msg-input" type="text" maxlength="200"
         placeholder="type a ui_event message and press Enter …" autocomplete="off">
  <span id="char-count">0/200</span>
  <span id="status-msg" style="opacity:0"> </span>
  <span class="hint">Enter ↵ to send</span>
  <button id="send-btn"
          title="Send the text as a 'ui_event' gRPC message --- appears as a gold row in the event feed">TRANSMIT ▶</button>
  <button id="fire-btn"
          title="Pick a random alphabet client (a–z) and fire it with a generated payload --- simulates sensor/agent traffic">⚡ RANDOM</button>
</footer>

<script>
// ── state ────────────────────────────────────────────────────────
let lastTotal      = -1;
let isRefreshing   = false;
let senderColorMap = {};
let colorIdx       = 0;
const COLOR_COUNT  = 5;
const MAX_ROWS     = 80;   // prune DOM to this many event rows
let prevTotal      = 0;
let prevTime       = Date.now();

function senderClass(sender) {
  if (sender === 'dashboard_ui') return 'sc-ui';
  if (!(sender in senderColorMap)) {
    senderColorMap[sender] = 'sc' + (colorIdx++ % COLOR_COUNT);
  }
  return senderColorMap[sender];
}

// ── client cards ─────────────────────────────────────────────────
let maxMsgs = 1;

function renderClients(clients) {
  const keys  = Object.keys(clients);
  const body  = document.getElementById('clients-body');
  const empty = document.getElementById('clients-empty');
  document.getElementById('h-clients').textContent          = keys.length;
  document.getElementById('client-count-badge').textContent = keys.length;
  [...body.querySelectorAll('.ccard')].forEach(n => n.remove());

  if (!keys.length) {
    empty.style.display = 'flex';
    return;
  }
  empty.style.display = 'none';
  maxMsgs = Math.max(1, ...keys.map(k => clients[k].msg_count || 0));

  keys.sort().forEach(id => {
    const info    = clients[id];
    const count   = info.msg_count  || 0;
    const connAt  = info.connected_at ? new Date(info.connected_at).toLocaleTimeString() : '---';
    const lastAt  = info.last_seen    ? new Date(info.last_seen).toLocaleTimeString()    : '---';
    const pct     = Math.round((count / maxMsgs) * 100);
    const sClass  = senderClass(id);

    const card = document.createElement('div');
    card.className = 'ccard';
    card.innerHTML = `
      <div style="display:flex;align-items:center;gap:.45rem">
        <span class="ev-sender ${sClass}" style="font-size:.72rem">${esc(id)}</span>
      </div>
      <div class="ccard-meta">
        connected&nbsp;<strong>${connAt}</strong>&nbsp;·&nbsp;last&nbsp;<strong>${lastAt}</strong>
      </div>
      <div class="ccard-stats">
        <span class="msg-count">${count}</span>
        <div class="bar-bg"><div class="bar-fill" style="width:${pct}%"></div></div>
        <span class="ts-tag">msgs</span>
      </div>`;
    body.appendChild(card);
  });

  const total = keys.reduce((s, k) => s + (clients[k].msg_count || 0), 0);
  document.getElementById('h-msgs').textContent = total;
}

// ── event feed ───────────────────────────────────────────────────
function appendEventRow(ev, animate) {
  const feed   = document.getElementById('event-feed');
  const isUI   = ev.sender === 'dashboard_ui';
  const sClass = senderClass(ev.sender);

  const row = document.createElement('div');
  row.className = 'ev-row' + (isUI ? ' src-ui' : '') + (animate ? ' ev-new' : '');
  row.innerHTML = `
    <span class="ev-ts">${esc(ev.ts)}</span>
    <span class="ev-sender ${sClass}">${esc(ev.sender)}</span>
    <span class="ev-name">${esc(ev.name)}</span>
    <span class="ev-preview">${esc(ev.preview || '')}</span>`;
  feed.appendChild(row);

  // prune old rows to keep DOM lean
  // NOTE: use firstElementChild, NOT firstChild --- firstChild can be a text node
  // (whitespace between tags) which has no classList and would throw a TypeError.
  while (feed.children.length > MAX_ROWS + 1) {
    const first = feed.firstElementChild;
    if (first && !first.classList.contains('empty-state')) feed.removeChild(first);
    else if (feed.children.length > MAX_ROWS + 1) feed.removeChild(feed.children[1]);
    else break;
  }
}

function renderEvents(events, total) {
  const feed  = document.getElementById('event-feed');
  const empty = document.getElementById('events-empty');

  document.getElementById('h-events').textContent        = total;
  document.getElementById('event-count-badge').textContent = events.length;

  if (!events.length) {
    empty.style.display = 'flex';
    return;
  }
  empty.style.display = 'none';

  // Capture scroll state before DOM changes --- scrollHeight grows when rows are added
  const atBottom = lastTotal < 0 ||
    (feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80);

  // Capture and advance lastTotal BEFORE DOM operations so it is always
  // updated even if appendEventRow throws (e.g. from a pruning edge-case).
  const prev = lastTotal;
  lastTotal = total;

  if (prev < 0) {
    // first render --- populate without animation
    [...feed.querySelectorAll('.ev-row')].forEach(n => n.remove());
    events.forEach(ev => appendEventRow(ev, false));
  } else if (total > prev) {
    // new events arrived
    const newCount = Math.min(total - prev, events.length);
    events.slice(events.length - newCount).forEach(ev => appendEventRow(ev, true));
  }
  if (atBottom) feed.scrollTop = feed.scrollHeight;
}

// ── polling ──────────────────────────────────────────────────────
async function refresh() {
  if (isRefreshing) return;
  isRefreshing = true;
  const dot = document.getElementById('status-dot');
  try {
    const res  = await fetch('/api/stats');
    if (!res.ok) throw new Error(res.statusText);
    const data = await res.json();
    dot.className = 'dot ok';
    renderClients(data.clients || {});
    renderEvents(data.events   || [], data.total || 0);
    const now = Date.now();
    if (now - prevTime >= 400) {
      document.getElementById('h-rate').textContent =
        Math.round((data.total - prevTotal) / ((now - prevTime) / 1000));
      prevTotal = data.total;
      prevTime  = now;
    }
  } catch {
    dot.className = 'dot err';
  } finally {
    isRefreshing = false;
  }
}

setInterval(refresh, 300);
refresh();

// ── command bar ──────────────────────────────────────────────────
const input     = document.getElementById('msg-input');
const sendBtn   = document.getElementById('send-btn');
const charCount = document.getElementById('char-count');
const statusMsg = document.getElementById('status-msg');
let statusTimer = null;

input.addEventListener('input', () => {
  charCount.textContent = input.value.length + '/200';
});

function setStatus(text, color) {
  statusMsg.textContent = text;
  statusMsg.style.color   = color;
  statusMsg.style.opacity = '1';
  clearTimeout(statusTimer);
  statusTimer = setTimeout(() => { statusMsg.style.opacity = '0'; }, 2200);
}

async function sendMessage() {
  const text = input.value.trim();
  if (!text) return;
  sendBtn.disabled = true;
  try {
    const res = await fetch('/api/send', {
      method:  'POST',
      headers: {'Content-Type': 'application/json'},
      body:    JSON.stringify({text}),
    });
    const data = await res.json();
    if (data.ok) {
      input.value = '';
      charCount.textContent = '0/200';
      setStatus('● Delivered', '#00e676');
      refresh();  // immediate refresh (may still be stale)
      // second pass after the poll loop has had time to fetch fresh stats;
      // also force-scroll to bottom so the sent event is visible
      setTimeout(() => {
        document.getElementById('event-feed').scrollTop = 999999;
        refresh();
      }, 350);
    } else {
      setStatus('✖ ' + (data.error || 'error'), '#ff5252');
    }
  } catch {
    setStatus('✖ network error', '#ff5252');
  } finally {
    sendBtn.disabled = false;
    input.focus();
  }
}

sendBtn.addEventListener('click', sendMessage);
input.addEventListener('keydown', e => {
  if (e.key === 'Enter')  sendMessage();
  if (e.key === 'Escape') { input.value = ''; charCount.textContent = '0/200'; }
});

document.getElementById('fire-btn').addEventListener('click', async () => {
  try {
    const res  = await fetch('/api/fire', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'
    });
    const data = await res.json();
    if (data.ok) {
      setStatus(`⚡ [${data.letter.toUpperCase()}] fired`, '#ffd600');
      setTimeout(refresh, 250);
    } else {
      setStatus('✖ ' + (data.error || 'error'), '#ff5252');
    }
  } catch {
    setStatus('✖ network error', '#ff5252');
  }
});

// ── helpers ──────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
                  .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// auto-focus the input on load
window.addEventListener('load', () => input.focus());
</script>
</body>
</html>
"""


# ── alphabet clients ─────────────────────────────────────────────────────────

_ADJECTIVES = [
    "cosmic", "electric", "phantom", "quantum", "solar",
    "neon", "binary", "radiant", "digital", "plasma",
    "spectral", "aurora", "zenith", "cipher", "delta",
]
_NOUNS = [
    "beacon", "circuit", "signal", "pulse", "vector",
    "matrix", "nexus", "stream", "relay", "node",
    "flux", "echo", "burst", "spike", "trace",
]


def _random_payload(letter: str) -> str:
    """Return a random flavour-text message tagged with the client letter.

    Args:
        letter: Single lowercase letter identifying the sending client.

    Returns:
        Human-readable payload string, e.g. ``'cosmic beacon [A:742]'``.
    """
    adj  = random.choice(_ADJECTIVES)
    noun = random.choice(_NOUNS)
    seq  = random.randint(100, 999)
    return f"{adj} {noun} [{letter.upper()}:{seq}]"


class AlphabetClient(BaseClient):
    """Single-letter gRPC client that fires a random payload when triggered.

    One instance is created for each letter a–z.  The ``messageName`` it
    publishes is ``alpha_<letter>``; the server logs every message in its
    rolling event feed.

    Args:
        letter: Single lowercase letter ``'a'``–``'z'``.
        port: gRPC server port.
        ip: gRPC server host.
    """

    def __init__(self, letter: str, port: int, ip: str = "localhost") -> None:
        self._letter = letter
        super().__init__(
            port,
            name=f"alpha_{letter}",
            ip=ip,
            provides=[f"alpha_{letter}"],
            requires=[],
        )

    def fire(self, text: str) -> None:
        """Send a random-payload message with this client's messageName.

        Args:
            text: Pre-generated random payload text.
        """
        self.send_data(
            generate_message(f"alpha_{self._letter}", struct_payload={"text": text})
        )


def _print_banner(url: str) -> None:
    """Print the keyboard-controls help banner to stdout.

    Args:
        url: Dashboard URL shown at the bottom of the banner.
    """
    print()
    print("═" * 56)
    print("  gRPC Watchdog · Alphabet Clients [a–z] Ready")
    print("  [a–z]  fire that client    [SPACE]  fire random")
    print("  [!]    burst all 26        [?]      show this help")
    print("  [Q / Esc]  quit            [Ctrl+C] force stop")
    print(f"  Dashboard → {url}")
    print("═" * 56)
    print()


def _keyboard_loop(
    clients: dict[str, AlphabetClient],
    kbd_stop: threading.Event,
    banner_fn,
) -> None:
    """Block-read single keypresses and fire the matching AlphabetClient.

    Keys:
        a–z    fire the named client with a random payload
        SPACE  fire a random client
        !      fire all 26 clients at once (burst)
        ?      reprint the help banner
        Q / Esc / Ctrl+C  quit

    Args:
        clients: Mapping from lowercase letter to :class:`AlphabetClient`.
        kbd_stop: Set to signal the shutdown path.
        banner_fn: Zero-argument callable that reprints the help banner.
    """
    while not kbd_stop.is_set():
        ch = _getch()
        if not ch:
            continue
        if ch in ("q", "Q", "\x03", "\x1b"):
            kbd_stop.set()
            break
        if ch == "?":
            banner_fn()
            continue
        if ch == " ":
            ch_letter = random.choice(list(clients))
            text = _random_payload(ch_letter)
            try:
                clients[ch_letter].fire(text)
                print(f"  → [RND → {ch_letter.upper()}] alpha_{ch_letter}: \"{text}\"")
            except (OSError, RuntimeError) as exc:
                print(f"  ✖ [RND] error: {exc}")
            continue
        if ch == "!":
            burst = list(clients)
            random.shuffle(burst)
            print(f"  ⚡ BURST --- firing all {len(burst)} clients …")
            for ch_burst in burst:
                try:
                    clients[ch_burst].fire(_random_payload(ch_burst))
                except (OSError, RuntimeError):
                    pass
            continue
        ch_lower = ch.lower()
        if ch_lower in clients:
            text = _random_payload(ch_lower)
            try:
                clients[ch_lower].fire(text)
                print(f"  → [{ch_lower.upper()}] alpha_{ch_lower}: \"{text}\"")
            except (OSError, RuntimeError) as exc:
                print(f"  ✖ [{ch_lower.upper()}] error: {exc}")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Watchdog Aero dashboard --- interactive gRPC UI demo"
    )
    parser.add_argument("--port",    type=int,  default=49999,       help="gRPC server port")
    parser.add_argument("--ip",      type=str,  default="localhost",  help="gRPC server host")
    parser.add_argument("--ui-port", type=int,  default=5000,        help="Flask HTTP port")
    args = parser.parse_args()

    # ── connect the watchdog poller ───────────────────────────────────────────
    print(f"Connecting to watchdog server at {args.ip}:{args.port} …")
    try:
        poller = WatchdogPoller(port=args.port, ip=args.ip)
    except GrpcConnectionError:
        print(f"\nERROR: Cannot connect to {args.ip}:{args.port}")
        print("       Start the server first:")
        print("         python examples/watchdog/server_watchdog.py")
        sys.exit(1)

    spin_thread = threading.Thread(target=poller.spin_forever, daemon=True)
    spin_thread.start()

    poll_thread = threading.Thread(target=_poll_loop, args=(poller,), daemon=True)
    poll_thread.start()

    # ── alphabet_clients dict --- created early so Flask can reference it ──────
    alphabet_clients: dict[str, AlphabetClient] = {}

    # ── Flask in a background thread (main thread drives keyboard input) ──────
    dashboard_app = create_app(poller, alphabet_clients)
    dashboard_url = f"http://localhost:{args.ui_port}"
    flask_thread = threading.Thread(
        target=lambda: dashboard_app.run(
            host="0.0.0.0", port=args.ui_port,
            debug=False, use_reloader=False, threaded=True,
        ),
        daemon=True,
    )
    flask_thread.start()

    # ── connect one gRPC client per letter a–z ────────────────────────────────
    print("Connecting alphabet clients a–z ", end="", flush=True)
    try:
        for ch_init in "abcdefghijklmnopqrstuvwxyz":
            alphabet_clients[ch_init] = AlphabetClient(ch_init, port=args.port, ip=args.ip)
            print(".", end="", flush=True)
    except GrpcConnectionError as exc:
        print(f"\nERROR: failed to connect alphabet client: {exc}")
        poller.disconnect()
        sys.exit(1)
    print(" done.")

    webbrowser.open(dashboard_url)
    _print_banner(dashboard_url)

    kbd_stop_event = threading.Event()
    try:
        _keyboard_loop(alphabet_clients, kbd_stop_event,
                       banner_fn=lambda: _print_banner(dashboard_url))
    except KeyboardInterrupt:
        pass
    finally:
        print("\nDisconnecting …")
        for client in alphabet_clients.values():
            client.disconnect()
        poller.disconnect()
        print("Done.")
