"""Unit tests for pookiepy/timer.py public API."""
import inspect
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from pookiepy.timer import TimedEvent, timer


class TestTimedEvent(unittest.TestCase):
    """Ensure TimedEvent class is directly constructible."""

    def test_timed_event_instance_creation(self):
        """TimedEvent constructor returns a TimedEvent context manager instance."""
        timer_ctx = TimedEvent(s=0.01, n=1, compensation=False)
        self.assertIsInstance(timer_ctx, TimedEvent)

    def test_thread_backend_emits_all_ticks(self):
        """Thread backend emits requested ticks without creating a process."""
        with TimedEvent(s=0.01, n=3, compensation=False, backend="thread") as timer_ctx:
            self.assertEqual(list(timer_ctx), [0, 1, 2])


class TestTimer(unittest.TestCase):
    """Ensure low-level timer keeps its established positional API."""

    def test_only_stop_event_is_keyword_only(self):
        """New shutdown event must not expand positional API."""
        parameters = inspect.signature(timer).parameters

        self.assertEqual(
            parameters["enable_compensation"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertEqual(
            parameters["logger_level"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertEqual(parameters["stop_event"].kind, inspect.Parameter.KEYWORD_ONLY)

    def test_existing_five_positional_arguments_remain_valid(self):
        """Established positional arguments still invoke timer unchanged."""
        tick_event = threading.Event()

        timer(0, 0.01, tick_event, False, None)


if __name__ == "__main__":
    unittest.main()
