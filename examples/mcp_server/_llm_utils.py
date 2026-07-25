"""JSON parsing and text sanitization helpers for LlmBridgeClient."""

import re

# Regex to find all <tool>…</tool> blocks (content may span multiple lines)
_TOOL_RE = re.compile(r'<tool>(.*?)</tool>', re.DOTALL)

# Characters that form valid single-char JSON escape sequences (excluding 'u')
_VALID_JSON_ESCAPES = frozenset('"' + r'\/' + 'bfnrt')


def _sanitize(s: object) -> str:
    if s is None:
        return ''
    s = s.decode('utf-8', errors='ignore') if isinstance(s, bytes) else str(s)
    s = s.replace('\ufffd', '')
    return re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', s)


def _process_escape(raw: str, i: int, out: list) -> int:
    """Process a backslash-escape sequence starting at raw[i].

    Appends the sanitized form to *out* and returns the new position.
    """
    nxt = raw[i + 1] if i + 1 < len(raw) else ''
    if nxt in _VALID_JSON_ESCAPES:
        out.append('\\')
        out.append(nxt)
        return i + 2
    if nxt == 'u':
        hex4 = raw[i + 2:i + 6]
        if len(hex4) == 4 and all(c in '0123456789abcdefABCDEF' for c in hex4):
            out.append('\\u')
            out.append(hex4)
            return i + 6
    # Malformed \uXXXX or completely invalid escape --- escape the backslash
    out.append('\\\\')
    return i + 1


# Fast dispatch table for control characters grpchookly appearing inside JSON strings
_CTRL_ESCAPES: dict[str, str] = {'\n': '\\n', '\r': '\\r', '\t': '\\t'}


def _process_string_char(raw: str, i: int, out: list) -> tuple[int, bool]:
    """Process one character inside a JSON string.

    Returns (new_i, still_in_string).
    """
    ch = raw[i]
    if ch == '\\':
        return _process_escape(raw, i, out), True
    if ch == '"':
        out.append(ch)
        return i + 1, False
    ctrl = _CTRL_ESCAPES.get(ch)
    if ctrl is not None:
        out.append(ctrl)
        return i + 1, True
    if ord(ch) < 0x20:
        out.append(f'\\u{ord(ch):04x}')
        return i + 1, True
    out.append(ch)
    return i + 1, True


def _fix_json_strings(raw: str) -> str:
    """Fix control characters AND invalid backslash escapes inside JSON string values.

    Walks the text char-by-char tracking whether we are inside a JSON string.
    Inside a string:
      - Bare \\n / \\r / \\t → proper JSON escapes
      - Other control characters (< 0x20) → \\uXXXX
      - \\<valid_escape>  (one of: \\" \\\\ \\/ \\b \\f \\n \\r \\t) → passed through
      - \\uXXXX with exactly 4 hex digits → passed through
      - \\<anything_else> (e.g. \\d \\s \\C from JS regex / CSS) → backslash
        is itself escaped to \\\\ so json.loads sees a literal backslash
    Between values: structural whitespace is left untouched.
    """
    out: list[str] = []
    in_str = False
    i = 0
    while i < len(raw):
        ch = raw[i]
        if in_str:
            i, in_str = _process_string_char(raw, i, out)
        else:
            if ch == '"':
                in_str = True
            out.append(ch)
            i += 1
    return ''.join(out)
