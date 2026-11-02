"""Timer context lifecycle tests with mocked workers and tick events."""

import logging
import unittest
from unittest.mock import Mock, patch

import psutil

from pookiepy.timer import TimedEvent, timer


class TestTimerLifecycle(unittest.TestCase):
    """Verify worker setup, platform priority routing, cleanup, and iteration exits."""

    def test_invalid_backend_is_rejected(self):
        """Unknown backend names fail before any worker is constructed."""
        with self.assertRaisesRegex(ValueError, "Unsupported timer backend"):
            TimedEvent(s=0.01, n=1, backend="invalid")

    def test_process_backend_priority_and_cleanup(self):
        """Process setup forwards configuration and chooses platform-specific priority."""
        priorities = (
            ("win32", getattr(psutil, "REALTIME_PRIORITY_CLASS", 256)),
            ("linux", getattr(psutil, "IOPRIO_CLASS_RT", 1)),
            ("darwin", 20),
        )
        for platform, priority in priorities:
            with self.subTest(platform=platform), patch(
                "pookiepy.timer._SPAWN_CONTEXT"
            ) as spawn, patch("pookiepy.timer.psutil.Process") as process, patch(
                "pookiepy.timer.sys.platform", platform
            ), patch.object(psutil, "REALTIME_PRIORITY_CLASS", 256, create=True), patch.object(
                psutil, "IOPRIO_CLASS_RT", 1, create=True
            ):
                logger = Mock(level=logging.WARNING)
                context = TimedEvent(s=0.02, n=3, compensation=False, logger=logger)
                with context as entered:
                    self.assertIs(entered, context)
                    spawn.Process.assert_called_once_with(
                        target=timer, args=(3, 0.02, spawn.Event.return_value),
                        kwargs={
                            "enable_compensation": False,
                            "logger_level": logging.WARNING,
                            "stop_event": None,
                        }, daemon=True,
                    )
                    spawn.Process.return_value.start.assert_called_once_with()
                    process.assert_called_once_with(spawn.Process.return_value.pid)
                    process.return_value.nice.assert_called_once_with(priority)
                spawn.Process.return_value.terminate.assert_called_once_with()
                spawn.Process.return_value.join.assert_called_once_with()

    def test_thread_cleanup_signals_stop_joins_and_preserves_error(self):
        """Application failure still requests thread shutdown and is never suppressed."""
        context = TimedEvent(s=0.01, n=-1, backend="thread")
        with patch("pookiepy.timer.threading.Thread") as worker, patch(
            "pookiepy.timer.psutil.Process"
        ) as process:
            with self.assertRaisesRegex(RuntimeError, "task failed"):
                with context:
                    raise RuntimeError("task failed")
        self.assertTrue(context._stop_event.is_set())  # pylint: disable=protected-access
        worker.return_value.join.assert_called_once_with()
        worker.return_value.terminate.assert_not_called()
        process.assert_not_called()

    def test_exit_without_worker_does_not_suppress_exception(self):
        """Cleanup is safe even when context entry never created a worker."""
        context = TimedEvent(s=0.01, n=1, backend="thread")
        self.assertFalse(context.__exit__(None, None, None))

    def test_dead_worker_reports_missed_ticks(self):
        """A finished worker with missing ticks ends iteration rather than deadlocking."""
        logger = Mock()
        context = TimedEvent(s=0.02, n=3, logger=logger, backend="thread")
        with patch.object(context, "_event") as event, patch.object(context, "_worker") as worker:
            event.wait.side_effect = [True, False, False]
            worker.is_alive.side_effect = [True, False]
            self.assertEqual(list(context), [0])
        event.wait.assert_called_with(timeout=0.2)
        event.clear.assert_called_once_with()
        logger.warning.assert_called_once_with(
            "Timer process ended after %d ticks; %d tick(s) missed.", 1, 2
        )

    def test_infinite_iteration_stops_when_worker_dies_without_logger(self):
        """Infinite mode emits consecutive indices until its worker stops producing."""
        context = TimedEvent(s=0.01, n=-1, backend="thread")
        with patch.object(context, "_event") as event, patch.object(context, "_worker") as worker:
            event.wait.side_effect = [True, True, False]
            worker.is_alive.return_value = False
            self.assertEqual(list(context), [0, 1])

    def test_interrupt_is_logged_and_reraised(self):
        """Iteration never swallows Ctrl+C, with or without an application logger."""
        for logger in (None, Mock()):
            with self.subTest(logger=logger):
                context = TimedEvent(s=0.01, n=1, logger=logger, backend="thread")
                with patch.object(context, "_event") as event:
                    event.wait.side_effect = KeyboardInterrupt
                    with self.assertRaises(KeyboardInterrupt):
                        next(iter(context))
                if logger is not None:
                    logger.info.assert_called_once_with("strg+c")
