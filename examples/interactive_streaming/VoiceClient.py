"""Sprachclient für das LM-Studio-Streaming-Beispiel."""
from __future__ import annotations

import queue
import threading

from pookiepy.baseclient import BaseClient
from pookiepy.tools import struct_to_json
from examples.interactive_streaming._voice_backends import (
    DEFAULT_GERMAN_VOICE,
    DEFAULT_SPEECH_RATE,
    SpeechBackend,
    create_speech_backend,
)
from examples.interactive_streaming._voice_speech import SpeechChunkBuffer


class _SpeechWorker:
    """Serialize speech requests so chunks do not overlap."""

    def __init__(self, backend: SpeechBackend):
        self._backend = backend
        self._queue: queue.Queue[str | None] = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._stop_event = threading.Event()

    def start(self):
        """Start background speech worker."""
        self._thread.start()

    def speak(self, text: str):
        """Queue text for serialized speech output."""
        self._queue.put(text)

    def stop(self):
        """Stop worker and release speech backend resources."""
        self._stop_event.set()
        self._queue.put(None)
        self._thread.join(timeout=5.0)
        self._backend.close()

    def _run(self):
        """Process queued speech until stop sentinel arrives."""
        while True:
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue

            try:
                if item is None:
                    break
                self._backend.speak(item)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                print(f"Sprachausgabe fehlgeschlagen: {exc}", flush=True)
            finally:
                self._queue.task_done()


class VoiceClient(BaseClient):
    """Interaktiver Client, der LLM-Ausgabe laut vorliest."""

    def __init__(self, identifier: str, port: int,
                 voice: str = DEFAULT_GERMAN_VOICE,
                 speech_rate: str = DEFAULT_SPEECH_RATE):
        self._speech_buffer = SpeechChunkBuffer()
        self._speech_worker: _SpeechWorker | None = None
        self._voice = voice
        self._speech_rate = speech_rate
        super().__init__(port, name=identifier, requires=["lm_response_stream"])
        self.logger.setLevel("WARNING")

    def on_init(self):
        """Create and start configured German speech backend."""
        if self._speech_worker is not None:
            return

        self.logger.warning(
            "Sprachfokus aktiv: Deutsch (de-DE), Stimme=%s, Tempo=%s",
            self._voice,
            self._speech_rate,
        )
        backend = create_speech_backend(
            logger=self.logger,
            voice=self._voice,
            rate=self._speech_rate,
        )
        self._speech_worker = _SpeechWorker(backend)
        self._speech_worker.start()

    def on_receive(self, data) -> bool:
        """Buffer incoming LLM chunks and queue complete utterances."""
        try:
            payload = (
                struct_to_json(data.payload.structPayload)
                if data.payload and data.payload.structPayload
                else {}
            )
        except (ValueError, TypeError, AttributeError):
            payload = {}

        chunk = payload.get("chunk", "")
        done = payload.get("done", False)

        if self._speech_worker is None:
            return True

        for utterance in self._speech_buffer.add(chunk, done=done):
            self._speech_worker.speak(utterance)

        return True

    def on_shutdown(self):
        """Stop speech output and clear pending text."""
        if self._speech_worker is None:
            return

        self._speech_worker.stop()
        self._speech_worker = None
        self._speech_buffer.reset()

    def listen_forever(self):
        """Process incoming response chunks until disconnected."""
        self.spin_forever()


if __name__ == "__main__":
    client = VoiceClient("voice-ui", 49999)
    try:
        client.listen_forever()
    except KeyboardInterrupt:
        client.disconnect()
