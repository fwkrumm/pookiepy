"""Failure-path tests for the skill installer that do not require a package build."""

import contextlib
import io
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pookiepy import __main__ as cli


class TestSkillInstallFailures(unittest.TestCase):
    """Require complete bundled assets and explicit CLI filesystem errors."""

    def test_missing_reference_writes_nothing(self):
        """A missing bundled reference must fail before installing SKILL.md."""
        with tempfile.TemporaryDirectory(prefix="pookiepy_missing_skill_") as raw_root:
            root = Path(raw_root)
            source = root / "package" / "assets" / "skills" / "pookiepy"
            source.mkdir(parents=True)
            bundled = Path(cli.__file__).parent / "assets" / "skills" / "pookiepy"
            shutil.copyfile(bundled / "SKILL.md", source / "SKILL.md")
            output = root / "output"
            output.mkdir()
            with patch.object(cli, "_PACKAGE_DIR", root / "package"):
                with self.assertRaises(FileNotFoundError):
                    cli.install_skill(output, ("github", "claude", "cursor", "codex"))
            self.assertEqual(list(output.iterdir()), [])

    def test_cli_reports_write_error(self):
        """A filesystem failure must produce stderr diagnostics and a nonzero exit."""
        stderr = io.StringIO()
        with patch("sys.argv", ["pookiepy", "--install-skills"]):
            with patch.object(cli, "install_skill", side_effect=PermissionError("read-only")):
                with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(SystemExit) as caught:
                        cli.main()
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("[error] Skill installation failed: read-only", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
