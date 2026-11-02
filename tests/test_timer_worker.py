"""Deterministic timer worker tests without sleeps or child processes."""

import itertools
import logging
import threading
import unittest
from unittest.mock import Mock, call, patch

from pookiepy.timer import (
    CLOCK_COMPENSATION_STRENGTH, TIME_NS_OFFSET, _cycles, _wait_for_next_tick,
    map_digit_to_compensation, timer,
)


class TestTimerWorker(unittest.TestCase):
    """Verify interval calculation, drift correction, overruns, and early stops."""

    def test_cycle_sequences_include_infinite_mode(self):
        """Finite counts stop exactly; infinite mode yields consecutive indices."""
        self.assertEqual(list(_cycles(0)), [])
        self.assertEqual(list(_cycles(3)), [0, 1, 2])
        self.assertEqual(list(itertools.islice(_cycles(-1), 4)), [0, 1, 2, 3])

    def test_interval_wait_uses_sleep_or_interruptible_event(self):
        """Process waits sleep, while thread waits report shutdown requests."""
        with patch("pookiepy.timer.time.sleep") as sleep:
            self.assertFalse(_wait_for_next_tick(0.25))
        sleep.assert_called_once_with(0.25)
        stop = Mock()
        stop.wait.return_value = True
        self.assertTrue(_wait_for_next_tick(0.25, stop))
        stop.wait.assert_called_once_with(timeout=0.25)

    def test_compensation_digit_mapping(self):
        """Every decimal digit retains its documented correction strength."""
        for digit, strength in CLOCK_COMPENSATION_STRENGTH.items():
            with self.subTest(digit=digit):
                self.assertEqual(map_digit_to_compensation(digit), strength)

    def test_compensation_adjusts_next_interval_and_reports_overrun(self):
        """An overrun logs its cycle and measured drift changes the next interval."""
        event = Mock()
        event.is_set.return_value = True
        timestamp = int("1" * (TIME_NS_OFFSET + 2) + "3")
        with patch("pookiepy.timer.get_logger") as logger, patch(
            "pookiepy.timer.time.time", return_value=0
        ), patch("pookiepy.timer.time.time_ns", return_value=timestamp), patch(
            "pookiepy.timer._wait_for_next_tick", return_value=False
        ) as wait:
            timer(2, 0.01, event, logger_level=logging.WARNING)
        logger.assert_called_once_with(
            "timer", console_log_level=logging.WARNING, file_log_level=None
        )
        self.assertEqual(wait.call_args_list, [call(0.01, None), call(0.008, None)])
        self.assertEqual(event.set.call_count, 2)
        self.assertEqual(logger.return_value.warning.call_count, 2)
        self.assertEqual(logger.return_value.warning.call_args.args[1], 2)

    def test_short_nanosecond_timestamp_falls_back_to_zero_drift(self):
        """An unexpectedly short timestamp does not crash or alter the next period."""
        event = Mock()
        event.is_set.return_value = False
        with patch("pookiepy.timer.time.time", return_value=0), patch(
            "pookiepy.timer.time.time_ns", return_value=1
        ), patch("pookiepy.timer._wait_for_next_tick", return_value=False) as wait:
            timer(2, 0.01, event)
        self.assertEqual(wait.call_args_list, [call(0.01, None), call(0.01, None)])

    def test_precision_validation_and_scientific_notation(self):
        """Unsupported fractional precision fails before ticks; large periods parse."""
        event = Mock()
        with self.assertRaisesRegex(AssertionError, "precision"):
            timer(1, 0.0001, event)
        event.set.assert_not_called()
        timer(0, 1e20, event)
        event.set.assert_not_called()

    def test_shutdown_before_tick_and_during_wait(self):
        """Thread shutdown prevents new ticks or interrupts the current wait."""
        stop = threading.Event()
        stop.set()
        event = Mock()
        timer(-1, 0.01, event, False, stop_event=stop)
        event.set.assert_not_called()
        stop.clear()
        with patch("pookiepy.timer.time.time", side_effect=[10.0, 11.0]), patch(
            "pookiepy.timer._wait_for_next_tick", return_value=True
        ) as wait:
            timer(-1, 0.01, event, False, stop_event=stop)
        event.set.assert_called_once_with()
        wait.assert_called_once_with(0, stop)
