"""HTTP helpers for LMProxyClient: streaming/sync LM Studio calls."""

import json
import logging
import os
import re
import time

try:
    import requests
    from requests.adapters import HTTPAdapter as _HTTPAdapter
except ImportError:
    requests = None
    _HTTPAdapter = None

from grpchook.logger import get_logger

SYSTEM_PROMPT = os.environ.get(
    "LMSTUDIO_SYSTEM_PROMPT",
    "Use only printable UTF-8 characters. Reply in plain text without special symbols or emojis.",
)

# Set by LMProxyClient.__init__() to a persistent requests.Session.
_session = None


def _sanitize_text(s: object) -> str:
    if s is None:
        return ""
    if isinstance(s, bytes):
        s = s.decode("utf-8", errors="ignore")
    s = str(s)
    s = s.replace("\ufffd", "")
    s = re.sub(r"[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]", "", s)
    return s


def _build_messages(prompt: str) -> list:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": prompt},
    ]


def _extract_text(j: dict) -> str | None:
    """Return the text content from a parsed SSE JSON object, or None."""
    if "choices" in j and j["choices"]:
        c = j["choices"][0]
        return c.get("delta", {}).get("content") or c.get("text")
    return j.get("text") or j.get("token")


def _iter_stream(prompt: str, base_url: str, model: str):
    """Stream from OpenAI-compatible /chat/completions. Yields sanitized text chunks."""
    api_key = os.environ.get("LMSTUDIO_API_KEY") or os.environ.get("OPENAI_API_KEY")
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Accept": "text/event-stream", "Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {"model": model, "messages": _build_messages(prompt), "stream": True}
    log = get_logger("LMProxyClient", console_log_level=logging.DEBUG)
    sess = _session or requests
    log.debug("stream POST %s model=%s", url, model)
    n_chunks = 0
    with sess.post(url, json=payload, stream=True, headers=headers, timeout=(5, None)) as r:
        log.debug("stream response status=%s", r.status_code)
        if not 200 <= r.status_code < 300:
            log.warning("stream got non-2xx status=%s", r.status_code)
            return
        for raw in r.iter_lines(decode_unicode=False):
            if not raw:
                continue
            line = (
                raw.decode("utf-8", errors="ignore").strip()
                if isinstance(raw, bytes)
                else str(raw).strip()
            )
            if line.startswith("data: "):
                line = line[6:]
            if line == "[DONE]":
                log.debug("stream [DONE] after %d chunks", n_chunks)
                return
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                yield _sanitize_text(line)
                n_chunks += 1
                continue
            text = _extract_text(j)
            if text:
                if n_chunks == 0:
                    log.debug("stream first chunk received")
                n_chunks += 1
                yield _sanitize_text(text)


def _fetch_sync(prompt: str, base_url: str, model: str) -> str:
    """Synchronous /chat/completions fallback. Returns text or raises."""
    api_key = os.environ.get("LMSTUDIO_API_KEY") or os.environ.get("OPENAI_API_KEY")
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {"model": model, "messages": _build_messages(prompt)}
    log = get_logger("LMProxyClient", console_log_level=logging.DEBUG)
    sess = _session or requests
    log.debug("sync POST %s model=%s", url, model)
    r = sess.post(url, json=payload, headers=headers, timeout=30)
    log.debug("sync response status=%s", r.status_code)
    r.raise_for_status()
    j = r.json()
    if "choices" in j and j["choices"]:
        c = j["choices"][0]
        text = (c.get("message") or {}).get("content") or c.get("text")
        if text:
            log.debug("sync got text len=%d", len(text))
            return _sanitize_text(text)
    raise RuntimeError("No text in sync response")


def _offline_stream(_prompt: str):
    """Multi-chunk offline fallback when LM Studio is unreachable."""
    chunks = [
        "LM Studio is not available. This is a static offline response.",
        " It simulates streaming by sending multiple chunks.",
        " Use it to test client rendering and chunk reassembly.",
    ]
    for chunk in chunks:
        yield _sanitize_text(chunk)
        time.sleep(0.05)


def make_http_session():
    """Create a persistent requests.Session with connection pooling.

    Returns None when requests is not installed.
    """
    if requests is None or _HTTPAdapter is None:
        return None
    sess = requests.Session()
    adapter = _HTTPAdapter(pool_connections=4, pool_maxsize=16)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess
