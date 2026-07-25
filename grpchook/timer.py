"""
High-precision periodic timer backed by a ``multiprocessing.Process``.

Public surface:
- :func:`timer` --- fire an event *n* times at interval *s*.
- :class:`TimedEvent` --- context manager that iterates once per tick.
"""
import itertools
import logging
import multiprocessing
import sys
import threading
import time
from multiprocessing import synchronize
from typing import Union

import psutil

from grpchook.logger import get_logger

TIME_NS_OFFSET = len(str(int(time.time())))
# the latter  will cause a problem on Sat Nov 20 2286 17:46:39 GMT+0000

# the following variable determines the max relevant digit position.
# e.g. 0.01 leads to relevant digit at 0.01X where X is the position
# because we try to compensate on this bit. thus the relevant digit is 2.
# for 0.001 leads to digit 3 etc.
# empirically this is the lowest value we allow, however we only test for
# relevant digit 2, the period time is 0.01 at lowest.
MAX_RELEVANT_DIGIT_VALUE = 3


# the following dict determines the ''strength'' of the clock drift compensation.
CLOCK_COMPENSATION_STRENGTH = \
{
    "0":0,
    "1":-1,
    "2":-1,
    "3":-2,
    "4":-2,
    "5":-2,
    "6":2,
    "7":1,
    "8":1,
    "9":1
}

def map_digit_to_compensation(key: str):
    """
    map a digit x to compensation strength according to
    CLOCK_COMPENSATION_STRENGTH

    Parameters
    ----------
    x : int
        digit which will be mapped to a compensation strength
        multiplier

    Returns
    -------
    int
        returns an int which will be multiplied with a compensation strength
    """
    return CLOCK_COMPENSATION_STRENGTH[key]

def _cycles(n: int):
    """Yield cycle indices; runs indefinitely when n == -1."""
    if n == -1:
        yield from itertools.count()
    else:
        yield from range(n)



def timer(n: int, s: float, event: Union[synchronize.Event, threading.Event],
          enable_compensation: bool = True, logger_level: int = None):
    """
    Timer function that sets an event periodically.

    TODO
    - add log level
    - add parameter for set priority


    Args:
        n: Number of cycles to run
        s: Timing interval in seconds (float)
        event: multiprocessing.synchronize.Event or threading.Event which will be set periodically
            based on the timing interval. The event is expected to be cleared by the caller
            after each tick.
        enable_compensation: Whether to enable clock drift compensation based on the current time.
            NOTE that the compensation may cause some ticks to be shorter than the specified
            period time, and is quite simple.
            i.e. with compensation the timer is more stable w.r.t. number of cycles within
            a specific time interval (e.g. within 1 second there are 100 cycles at 0.01s period
            time; still not exact of course), however the compensation can cause some ticks to be
            shortened e.g. 0.008s instead of 0.01s.

    Prints warning if event is still set from previous cycle (timer overrun).
    """

    # only required if compensation is enabled
    s_orig = None
    relevant_digit = None
    compensation_strength = None

    if logger_level is not None:
        process_logger = get_logger("timer", console_log_level=logger_level, file_log_level=None)
    else:
        process_logger = None

    if enable_compensation:

        # store original period time
        s_orig = s

        # relevant digit is used for drift compensation (if enabled), e.g.
        # for 0.01 the relevant digit is
        # 0.01
        #     ^ this is the relevant digit because we will try to
        # compensate with respect to that digit so that there
        # should be a zero.
        # NOTE we cast any int to float to that we surely can extract
        # a relevant digit, e.g. rate=1 yields a relevant digit of -1
        relevant_digit = str(float(s))[::-1].find(".")
        if relevant_digit == -1:
            relevant_digit = 0

        assert 0 <= relevant_digit <= MAX_RELEVANT_DIGIT_VALUE,\
                f"relevant digit is {relevant_digit} "\
                f"and thus too low at precision. period_time = {s}. "\
                "The latter has to be increased or at least to be adjusted to have "\
                f"at max {MAX_RELEVANT_DIGIT_VALUE} digits."

        compensation_strength = round(1e-1 ** (relevant_digit + 1), relevant_digit + 1)

    def _tick(s: float):
        """
        provides timing; ''inspired'' from stackoverflow
        https://stackoverflow.com/questions/8600161/executing-periodic-actions
        comment from watsonic
        """
        t = time.time()
        while True:
            t += s
            yield max(t - time.time(), 0)

    for i in _cycles(n):

        event.set()

        time.sleep(next(_tick(s))) # in newer Python versions we can use time.sleep directly,
        # in older versions this did not work well because of clock precision.

        if event.is_set():
            # not optimal but using log callback here would require passing a logger to the
            # timer function and we want to avoid that for now
            if process_logger:
                process_logger.warning(
                    "Warning: Timer overrun at cycle %d. Event is still set.", i + 1
                )

        if enable_compensation:
            # compensation for clock drift, we use the current time in nanoseconds to extract a
            # digit which is used to determine the compensation strength.
            try:
                relevant_number = str(time.time_ns())[relevant_digit + TIME_NS_OFFSET]
            except IndexError:
                # just a fail safe, if this occurs frequently will likely cause hick-ups
                relevant_number = "0"

            # round to sleep time for next iteration
            s = round(s_orig + map_digit_to_compensation(relevant_number) * compensation_strength,
                    relevant_digit + 1)
            # process exits naturally here when _cycles(n) is exhausted

class TimedEvent:
    """
    Context manager that drives periodic task execution using the existing
    timer function.

    Usage::

        with timedevent(s=0.01, n=100) as te:
            for tick in te:
                # executed exactly n times, once per timer tick

    Parameters
    ----------
    s : float
        Period time in seconds between ticks.
    n : int
        Number of ticks (cycles) to run.

    Notes
    -----
    The timer runs in a daemon thread.  The context manager blocks in
    ``__exit__`` until all ``n`` ticks have been issued or the ``for``
    loop inside the ``with`` block has been exhausted.
    """

    def __init__(self, s: float, n: int, compensation: bool = True, logger: logging.Logger = None):
        self.s = s
        self.n = n
        self.compensation = compensation
        self._event = multiprocessing.Event()
        self._process: multiprocessing.Process = None
        self.overrun_count: int = 0  # number of cycles where task exceeded the tick period
        self.logger = logger

    def __enter__(self) -> "TimedEvent":
        self._process = multiprocessing.Process(
            target=timer,
            args=(self.n, self.s, self._event, self.compensation,
                  self.logger.level if self.logger else None),
            daemon=True,
        )

        # set timer process to high priority to minimize the risk of timer
        # overruns due to scheduling delays (not yet optimal but this is not
        #  supposed to be a real-time application)
        psutil_process = psutil.Process(self._process.pid)
        if sys.platform == "win32":  # Windows (either 32-bit or 64-bit)
            psutil_process.nice(psutil.REALTIME_PRIORITY_CLASS)
        elif sys.platform == "linux":  # Linux
            psutil_process.nice(psutil.IOPRIO_CLASS_RT)
        else:  # MAC OS X or other (potentially)
            psutil_process.nice(20)

        self._process.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._process is not None:
            self._process.terminate()
            self._process.join()  # reap the process so no zombie is left on Unix/macOS
        return False  # do not suppress exceptions

    def __iter__(self):
        """
        Yield the cycle index (0-based) once per timer tick.

        Each iteration waits for the timer to set the event, then clears it
        before yielding, so the task code runs synchronously between ticks.

        After each yield (i.e. after the task body has finished), the event
        is checked again.  If it is already set the timer has fired the next
        tick while the task was still running --- a timer overrun.  A warning
        is printed and ``self.overrun_count`` is incremented.

        Deadlock prevention
        -------------------
        ``threading.Event.set()`` is idempotent: if the task overruns by more
        than one tick, several timer fires collapse into a single set signal.
        ``__iter__`` would then wait forever on a tick that the (now-finished)
        timer thread will never fire.  To guard against this, every wait uses a
        finite timeout (10 × period); if the timeout expires *and* the timer
        thread is no longer alive, iteration stops early and the missed cycles
        are reported.
        """
        try:
            i = 0
            while True:
                # Wait for the next tick with a generous timeout so we never
                # block permanently.  Only bail out when the timeout expires
                # AND the process is already dead (covers normal exit and crashes).
                while not self._event.wait(timeout=10 * self.s):
                    if not self._process.is_alive():
                        if self.logger:
                            self.logger.warning(
                                "Timer process ended after %d ticks; %d tick(s) missed.",
                                i,
                                self.n - i,
                            )
                        return
                self._event.clear()
                yield i
                i += 1
                # Stop after n ticks (mirrors the timer process cycle count).
                # Checked here --- after the task body returns --- so the last
                # yield is never skipped even if the process has already exited.
                if self.n != -1 and i >= self.n:
                    return
        except KeyboardInterrupt:
            if self.logger:
                self.logger.info("strg+c")
            raise  # re-raise so the caller's signal handling is not suppressed
        # cleanup is handled solely by __exit__ (terminate + join)


if __name__ == "__main__":



    demo_logger = get_logger("timer_demo")

    demo_logger.info("Starting timer demo with compensation enabled...")

    with TimedEvent(s=0.01, n=10, compensation=True, logger=demo_logger) as te:
        for tick in te:
            demo_logger.info("Tick %d at %s", tick, time.time())
            time.sleep(2.1)
