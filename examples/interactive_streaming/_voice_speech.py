"""Text buffering helpers for the interactive voice client."""
from __future__ import annotations

import re


_SENTENCE_END_RE = re.compile(r"([.!?]+[\'\")\]]*)(\s+|$)")
DEFAULT_MIN_UTTERANCE_CHARS = 100


class SpeechChunkBuffer:
    """Collect streamed text and release complete sentences for speech."""

    def __init__(self, min_utterance_chars: int = DEFAULT_MIN_UTTERANCE_CHARS):
        if min_utterance_chars < 1:
            raise ValueError("min_utterance_chars must be at least 1")
        self._min_utterance_chars = min_utterance_chars
        self._pending_text = ""

    def add(self, text: str, done: bool = False) -> list[str]:
        """Append streamed text and return utterances ready for speech."""
        if text:
            self._pending_text += text

        if done:
            trailing = self._pending_text.strip()
            self._pending_text = ""
            if trailing:
                return [trailing]
            return []

        utterances: list[str] = []
        while len(self._pending_text.strip()) >= self._min_utterance_chars:
            match = next(
                (
                    candidate
                    for candidate in _SENTENCE_END_RE.finditer(self._pending_text)
                    if candidate.end(1) >= self._min_utterance_chars
                ),
                None,
            )
            if match is None:
                break

            utterance = self._pending_text[:match.end(1)].strip()
            if utterance:
                utterances.append(utterance)
            self._pending_text = self._pending_text[match.end():]

        return utterances

    def reset(self):
        """Discard all buffered text."""
        self._pending_text = ""
