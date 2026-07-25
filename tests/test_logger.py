"""
Unit tests for grpchook/logger.py

Tests cover branches missed in previous coverage runs:
- GrpcLogger.setLevel propagates level to all attached handlers (line 34)
- get_logger custom log_dir else-branch (line 90)
- get_logger use_colored_output=False StreamHandler path (lines 126-129)
- get_logger file-logging PermissionError/OSError warning path (lines 153-154)
- get_logger returns cached logger on second call (hasHandlers guard)
- GrpcLogger is the concrete type returned
"""
import logging
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from grpchook.logger import GrpcLogger, get_logger


def _unique(prefix: str = "test") -> str:
    """Return a unique logger name for each test to avoid handler-cache collisions."""
    return f"{prefix}_{uuid.uuid4().hex}"


def _close_handlers(logger: logging.Logger) -> None:
    """Close and remove all handlers from a logger (needed on Windows before temp dir cleanup)."""
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)


class TestGrpcLoggerSetLevel(unittest.TestCase):
    """Tests for GrpcLogger.setLevel() handler propagation."""

    def test_set_level_propagates_to_all_handlers(self):
        """setLevel() updates every attached handler to the same level."""
        tmpdir = tempfile.mkdtemp()
        logger = None
        try:
            logger = get_logger(name=_unique("setlevel"), log_dir=tmpdir,
                                console_log_level=None)
            self.assertTrue(logger.handlers, "Expected at least one handler after get_logger")
            logger.setLevel(logging.DEBUG)
            for handler in logger.handlers:
                self.assertEqual(handler.level, logging.DEBUG)
        finally:
            if logger:
                _close_handlers(logger)
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_set_level_no_handlers_does_not_raise(self):
        """setLevel() on a handleerless GrpcLogger does not raise."""
        logging.setLoggerClass(GrpcLogger)
        logger = logging.getLogger(_unique("bare"))
        logger.setLevel(logging.WARNING)  # must not raise


class TestGetLoggerCustomDir(unittest.TestCase):
    """Tests for get_logger log_dir parameter (the else-branch)."""

    def test_custom_log_dir_creates_file_handler_in_that_dir(self):
        """File handler base path is inside the provided log_dir."""
        tmpdir = tempfile.mkdtemp()
        logger = None
        try:
            logger = get_logger(name=_unique("customdir"), log_dir=tmpdir,
                                console_log_level=None)
            file_handlers = [h for h in logger.handlers
                             if hasattr(h, "baseFilename")]
            self.assertTrue(file_handlers, "Expected a file handler when log_dir is provided")
            self.assertTrue(
                any(tmpdir in h.baseFilename for h in file_handlers),
                f"Expected file handler inside {tmpdir!r}, got: "
                f"{[h.baseFilename for h in file_handlers]}"
            )
        finally:
            if logger:
                _close_handlers(logger)
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_custom_log_dir_returns_grpc_logger(self):
        """get_logger with log_dir still returns a GrpcLogger instance."""
        tmpdir = tempfile.mkdtemp()
        logger = None
        try:
            logger = get_logger(name=_unique("customdir_type"), log_dir=tmpdir,
                                console_log_level=None)
            self.assertIsInstance(logger, GrpcLogger)
        finally:
            if logger:
                _close_handlers(logger)
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestGetLoggerNoColor(unittest.TestCase):
    """Tests for get_logger use_colored_output=False branch."""

    def test_no_color_installs_plain_stream_handler(self):
        """use_colored_output=False adds a plain StreamHandler (not coloredlogs)."""
        logger = get_logger(name=_unique("nocolor"), use_colored_output=False,
                            file_log_level=None)
        stream_handlers = [
            h for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not hasattr(h, "baseFilename")
        ]
        self.assertTrue(
            stream_handlers,
            "Expected a plain StreamHandler with use_colored_output=False"
        )

    def test_no_color_handler_level_matches_log_level(self):
        """The plain StreamHandler's level equals the requested console_log_level."""
        logger = get_logger(name=_unique("nocolor_level"), use_colored_output=False,
                    file_log_level=None, console_log_level=logging.WARNING)
        stream_handlers = [
            h for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not hasattr(h, "baseFilename")
        ]
        self.assertTrue(stream_handlers)
        self.assertEqual(stream_handlers[0].level, logging.WARNING)


class TestGetLoggerPermissionError(unittest.TestCase):
    """Tests for get_logger file-logging PermissionError/OSError warning path."""

    def test_permission_error_on_mkdir_emits_user_warning(self):
        """PermissionError during log dir creation emits a UserWarning."""
        with patch.object(Path, "mkdir", side_effect=PermissionError("access denied")):
            with self.assertWarns(UserWarning):
                get_logger(name=_unique("permerr"), log_dir="/fake/no/permission",
                           console_log_level=None)

    def test_os_error_on_mkdir_emits_user_warning(self):
        """OSError during log dir creation emits a UserWarning."""
        with patch.object(Path, "mkdir", side_effect=OSError("disk full")):
            with self.assertWarns(UserWarning):
                get_logger(name=_unique("oserr"), log_dir="/fake/disk/full",
                           console_log_level=None)


class TestGetLoggerCaching(unittest.TestCase):
    """Tests for get_logger hasHandlers guard (returns same instance on re-call)."""

    def test_second_call_returns_same_logger(self):
        """Calling get_logger twice with the same name returns the identical object."""
        name = _unique("cached")
        l1 = get_logger(name=name, file_log_level=None)
        with self.assertWarns(UserWarning):
            l2 = get_logger(name=name, file_log_level=None)
        self.assertIs(l1, l2)

    def test_second_call_does_not_add_extra_handlers(self):
        """Second call with the same name does not duplicate handlers."""
        name = _unique("no_dup")
        l1 = get_logger(name=name, file_log_level=None)
        count_after_first = len(l1.handlers)
        with self.assertWarns(UserWarning):
            get_logger(name=name, file_log_level=None)
        self.assertEqual(len(l1.handlers), count_after_first)


if __name__ == "__main__":
    unittest.main()
