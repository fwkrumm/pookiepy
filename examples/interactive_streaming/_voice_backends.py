"""Speech synthesis backends for the interactive voice client."""
from __future__ import annotations

import asyncio
import os
import tempfile
from abc import ABC, abstractmethod


DEFAULT_GERMAN_VOICE = "de-DE-KatjaNeural"
DEFAULT_SPEECH_RATE = "+15%"


class SpeechBackend(ABC):
    """Common interface for synchronous speech playback."""

    @abstractmethod
    def speak(self, text: str):
        """Speak text and block until playback finishes."""

    def close(self):
        """Release backend resources."""


class EdgeNeuralSpeechBackend(SpeechBackend):
    """Microsoft Edge neural synthesis with local audio playback."""

    def __init__(self, voice: str, rate: str):
        os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")
        import edge_tts
        import pygame

        self._edge_tts = edge_tts
        self._pygame = pygame
        self._voice = voice
        self._rate = rate
        self._pygame.mixer.init()

    def speak(self, text: str):
        audio_path = self._synthesize(text)
        try:
            sound = self._pygame.mixer.Sound(audio_path)
            channel = sound.play()
            if channel is None:
                raise RuntimeError("Audiowiedergabe konnte nicht gestartet werden")

            clock = self._pygame.time.Clock()
            while channel.get_busy():
                clock.tick(30)
        finally:
            os.unlink(audio_path)

    def _synthesize(self, text: str) -> str:
        file_descriptor, audio_path = tempfile.mkstemp(suffix=".mp3")
        os.close(file_descriptor)
        try:
            communicator = self._edge_tts.Communicate(
                text=text,
                voice=self._voice,
                rate=self._rate,
            )
            asyncio.run(communicator.save(audio_path))
        except Exception:
            os.unlink(audio_path)
            raise
        return audio_path

    def close(self):
        self._pygame.mixer.quit()


class SystemSpeechBackend(SpeechBackend):
    """Offline fallback using an installed German system voice."""

    def __init__(self, preferred_language: str = "de"):
        import pyttsx3

        self._engine = pyttsx3.init()
        self._engine.setProperty("rate", 168)
        self._select_voice(preferred_language)

    @staticmethod
    def _voice_score(voice, preferred_language: str) -> tuple[int, str]:
        name = (getattr(voice, "name", "") or "").lower()
        languages = getattr(voice, "languages", []) or []
        language_text = " ".join(
            item.decode("utf-8", errors="ignore") if isinstance(item, bytes) else str(item)
            for item in languages
        ).lower()

        score = 0
        if preferred_language.lower() in language_text:
            score += 2
        if preferred_language.lower() in name:
            score += 1
        return score, name

    def _select_voice(self, preferred_language: str):
        voices = self._engine.getProperty("voices") or []
        if voices:
            voice = max(voices, key=lambda item: self._voice_score(item, preferred_language))
            self._engine.setProperty("voice", voice.id)

    def speak(self, text: str):
        self._engine.say(text)
        self._engine.runAndWait()


class ConsoleSpeechBackend(SpeechBackend):
    """Last-resort backend that preserves output as text."""

    def speak(self, text: str):
        print(text, flush=True)


class FallbackSpeechBackend(SpeechBackend):
    """Disable failing online synthesis and continue with offline speech."""

    def __init__(self, primary: SpeechBackend, fallback: SpeechBackend,
                 fallback_name: str, logger):
        self._primary = primary
        self._fallback = fallback
        self._fallback_name = fallback_name
        self._logger = logger
        self._primary_available = True

    def speak(self, text: str):
        if self._primary_available:
            try:
                self._primary.speak(text)
                return
            except Exception as exc:  # pylint: disable=broad-exception-caught
                self._primary_available = False
                self._logger.error("Edge Neural-TTS ausgefallen: %s", exc)
                self._logger.warning(
                    "Fallback aktiv: %s. Sprachausgabe läuft weiter, "
                    "klingt aber weniger natürlich.",
                    self._fallback_name,
                )
        self._fallback.speak(text)

    def close(self):
        self._primary.close()
        self._fallback.close()


def create_speech_backend(logger, voice: str, rate: str) -> SpeechBackend:
    """Build neural speech with graceful system/console fallback."""
    try:
        fallback: SpeechBackend = SystemSpeechBackend(preferred_language="de")
        fallback_name = "pyttsx3-Offline-Systemstimme"
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("pyttsx3-Offline-Fallback nicht verfügbar: %s", exc)
        logger.warning("Letzter Fallback aktiv: reine Textausgabe ohne Sprache.")
        fallback = ConsoleSpeechBackend()
        fallback_name = "Textausgabe ohne Sprache"

    try:
        primary = EdgeNeuralSpeechBackend(voice=voice, rate=rate)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Edge Neural-TTS konnte nicht gestartet werden: %s", exc)
        logger.warning(
            "Fallback aktiv: %s. Sprachausgabe läuft weiter, "
            "klingt aber weniger natürlich.",
            fallback_name,
        )
        return fallback

    logger.warning("Neural-TTS aktiv: %s, Sprechtempo %s", voice, rate)
    return FallbackSpeechBackend(primary, fallback, fallback_name, logger)