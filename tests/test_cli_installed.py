"""
Integration tests for the grpchook CLI.

These tests install grpchook into a dedicated virtual environment (created
once per test run in ``setUpClass``) and then invoke ``python -m grpchook``
from a temporary working directory that is completely outside the project
source tree.  This guarantees that the CLI is exercised against the *installed*
package, not imported directly from sources.

Run with:
    python -m unittest tests.test_cli_installed -v

Test coverage
-------------
- No-args invocation prints help text and exits 0.
- ``--generate-server``     produces a syntactically valid server_skeleton.py.
- ``--generate-client``     produces a syntactically valid client_skeleton.py.
- ``--generate-skeletons``  produces both skeleton files (and both are valid).
- ``--generate-how-to``     produces HOW_TO.md whose content matches the bundled source.
- ``--generate``            produces all three files; skeletons are valid.
- ``--generate-interface``  produces message.proto and prints
  "Next steps" instructions.
- ``--generate-interface-with-skeletons`` produces message.proto + skeletons using
  compile_and_register.
- Re-running a generate flag does NOT overwrite an existing file ([skip] path).

Notes
-----
- If ``uv`` is on PATH it is used for both venv creation and package install
  (``uv venv`` + ``uv pip install``), which is significantly faster than the
  standard ``venv`` + ``pip`` path.
- Fallback: ``venv.create(with_pip=True)`` + ``pip install``.
  This triggers a PEP 517 build via pdm-backend; first run requires network
  to fetch build dependencies that are not yet cached.
  To run offline with the pip path, ensure ``pdm-backend`` is installed in the
  outer environment and set ``PIP_EXTRA_ARGS=--no-build-isolation``.
- ``venv.create(with_pip=True)`` requires ``ensurepip``.  On Debian-based CI
  images, install the ``python3-venv`` apt package.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import venv
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Root of the project source tree (two levels above this file).
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# HOW_TO.md as bundled inside the grpchook package directory.
# This is the authoritative source used for byte-for-byte comparison.
_BUNDLED_HOW_TO = _PROJECT_ROOT / "grpchook" / "assets" / "HOW_TO.md"

# The bundled message.proto used for byte-for-byte comparison.
_BUNDLED_PROTO = _PROJECT_ROOT / "grpchook" / "message.proto"

# Optional extra args passed to pip install (e.g. "--no-build-isolation").
# Set the PIP_EXTRA_ARGS environment variable to override.
_PIP_EXTRA_ARGS: list[str] = os.environ.get("PIP_EXTRA_ARGS", "").split() or []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _venv_python(venv_dir: Path) -> Path:
    """
    Return the path to the Python executable inside *venv_dir*.

    The location differs between Windows (Scripts/) and POSIX (bin/).
    """
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _find_uv() -> str | None:
    """
    Return the absolute path to the ``uv`` executable if it is available on
    PATH, or ``None`` if it is not installed.

    ``uv`` creates venvs and installs packages significantly faster than the
    standard ``venv`` + ``pip`` combination, so it is used preferentially when
    present.
    """
    return shutil.which("uv")


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestInstalledCLI(unittest.TestCase):
    """
    Smoke-tests for the ``python -m grpchook`` CLI, run against an installed
    wheel rather than the source tree.
    """

    # ------------------------------------------------------------------
    # Class-level fixtures --- venv created / destroyed once per test run
    # ------------------------------------------------------------------

    _venv_dir: Path
    _python: Path

    @classmethod
    def setUpClass(cls) -> None:
        """
        Create a temporary virtual environment and install grpchook into it.

        This runs once before all test methods in this class.  The install step
        invokes a PEP 517 build (pdm-backend), so it may take ~10–30 s on first
        run when the build backend is not yet cached.
        """
        raw_venv_dir = tempfile.mkdtemp(prefix="grpchook_test_venv_")
        cls._venv_dir = Path(raw_venv_dir)

        uv = _find_uv()
        if uv:
            cls._setup_with_uv(uv)
        else:
            cls._setup_with_pip()

        if not cls._python.exists():
            raise RuntimeError(
                f"venv Python not found at {cls._python}. "
                "Ensure ensurepip / python3-venv is available."
            )

    @classmethod
    def _setup_with_uv(cls, uv: str) -> None:
        """
        Create the virtual environment and install grpchook using ``uv``.

        ``uv venv`` skips the ensurepip bootstrap entirely and is typically
        10x faster than ``venv.create(with_pip=True)``.
        ``uv pip install`` resolves and installs packages faster than pip.
        """
        print(
            f"\n[setUpClass] uv found ({uv}) --- "
            f"creating venv in {cls._venv_dir} ...",
            flush=True,
        )
        t0 = time.monotonic()
        result = subprocess.run(
            [uv, "venv", str(cls._venv_dir)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"uv venv failed (exit {result.returncode}):\n"
                f"{result.stdout}\n{result.stderr}"
            )
        print(f"[setUpClass] venv created ({time.monotonic() - t0:.1f}s)", flush=True)

        cls._python = _venv_python(cls._venv_dir)

        print(f"[setUpClass] Running uv pip install {_PROJECT_ROOT} ...", flush=True)
        t1 = time.monotonic()
        result = subprocess.run(
            [uv, "pip", "install", "--python", str(cls._python), str(_PROJECT_ROOT), "--quiet"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"uv pip install failed (exit {result.returncode}):\n"
                f"{result.stdout}\n{result.stderr}"
            )
        print(f"[setUpClass] uv pip install done ({time.monotonic() - t1:.1f}s)", flush=True)

    @classmethod
    def _setup_with_pip(cls) -> None:
        """
        Create the virtual environment and install grpchook using the standard
        ``venv`` + ``pip`` toolchain.

        This is the fallback path when ``uv`` is not available.  The install
        step triggers a PEP 517 build via pdm-backend, which may take ~10–30 s
        on first run when build dependencies are not yet cached.
        """
        print(
            f"\n[setUpClass] uv not found --- falling back to venv+pip. "
            f"Creating venv in {cls._venv_dir} ...",
            flush=True,
        )
        t0 = time.monotonic()
        venv.create(str(cls._venv_dir), with_pip=True, clear=True)
        print(f"[setUpClass] venv created ({time.monotonic() - t0:.1f}s)", flush=True)

        cls._python = _venv_python(cls._venv_dir)

        # The PEP 517 build via pdm-backend is the dominant cost here.
        print(f"[setUpClass] Running pip install {_PROJECT_ROOT} ...", flush=True)
        t1 = time.monotonic()
        result = subprocess.run(
            [
                str(cls._python), "-m", "pip", "install",
                str(_PROJECT_ROOT),
                "--quiet",
                *_PIP_EXTRA_ARGS,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"pip install failed (exit {result.returncode}):\n"
                f"{result.stdout}\n{result.stderr}"
            )
        print(f"[setUpClass] pip install done ({time.monotonic() - t1:.1f}s)", flush=True)

    @classmethod
    def tearDownClass(cls) -> None:
        """Remove the temporary virtual environment after all tests complete."""
        shutil.rmtree(cls._venv_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Per-test fixtures --- fresh working directory for every test method
    # ------------------------------------------------------------------

    def setUp(self) -> None:
        """
        Create a temporary working directory outside the project source tree.

        Each test method gets its own empty directory so generated files never
        bleed between tests.
        """
        self._work_dir = Path(tempfile.mkdtemp(prefix="grpchook_test_work_"))

    def tearDown(self) -> None:
        """Remove the per-test working directory."""
        shutil.rmtree(self._work_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Private helper
    # ------------------------------------------------------------------

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        """
        Invoke ``python -m grpchook *args`` inside *self._work_dir*.

        The working directory is always the temporary directory created in
        ``setUp``, so generated files land there and never pollute the project.
        """
        return subprocess.run(
            [str(self._python), "-m", "grpchook", *args],
            cwd=str(self._work_dir),
            capture_output=True,
            text=True,
            check=False,
        )

    @staticmethod
    def _assert_valid_python(path: Path, *, label: str) -> None:
        """
        Assert that the file at *path* is syntactically valid Python by
        compiling it with ``compile()``.

        Raises AssertionError on syntax errors; the error message includes
        *label* so failures are easy to identify in the test output.
        """
        source = path.read_text(encoding="utf-8")
        try:
            compile(source, str(path), "exec")
        except SyntaxError as exc:
            raise AssertionError(
                f"{label} contains a syntax error: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Test methods
    # ------------------------------------------------------------------

    def test_no_args_prints_help(self):
        """Invoking with no arguments prints usage text and exits cleanly."""
        result = self._run()
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("usage:", result.stdout.lower())

    def test_generate_server(self):
        """--generate-server creates a syntactically valid server_skeleton.py."""
        result = self._run("--generate-server")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        skeleton = self._work_dir / "server_skeleton.py"
        self.assertTrue(skeleton.exists(), "server_skeleton.py was not created")
        self._assert_valid_python(skeleton, label="server_skeleton.py")

    def test_generate_client(self):
        """--generate-client creates a syntactically valid client_skeleton.py."""
        result = self._run("--generate-client")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        skeleton = self._work_dir / "client_skeleton.py"
        self.assertTrue(skeleton.exists(), "client_skeleton.py was not created")
        self._assert_valid_python(skeleton, label="client_skeleton.py")

    def test_generate_skeletons(self):
        """--generate-skeletons creates both skeleton files and both are valid Python."""
        result = self._run("--generate-skeletons")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        server = self._work_dir / "server_skeleton.py"
        client = self._work_dir / "client_skeleton.py"
        self.assertTrue(server.exists(), "server_skeleton.py was not created")
        self.assertTrue(client.exists(), "client_skeleton.py was not created")
        self._assert_valid_python(server, label="server_skeleton.py")
        self._assert_valid_python(client, label="client_skeleton.py")

    def test_generate_how_to(self):
        """
        --generate-how-to creates HOW_TO.md whose content matches the bundled
        source file byte-for-byte.
        """
        result = self._run("--generate-how-to")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        generated = self._work_dir / "HOW_TO.md"
        self.assertTrue(generated.exists(), "HOW_TO.md was not created")

        # Content must match the HOW_TO.md that is bundled inside the package.
        expected = _BUNDLED_HOW_TO.read_bytes()
        actual = generated.read_bytes()
        self.assertEqual(
            expected, actual,
            "Generated HOW_TO.md content does not match grpchook/HOW_TO.md"
        )

    def test_generate(self):
        """
        --generate creates server_skeleton.py, client_skeleton.py, and
        HOW_TO.md in one call.  Both Python files must be syntactically valid.
        """
        result = self._run("--generate")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        server = self._work_dir / "server_skeleton.py"
        client = self._work_dir / "client_skeleton.py"
        how_to = self._work_dir / "HOW_TO.md"

        self.assertTrue(server.exists(), "server_skeleton.py was not created")
        self.assertTrue(client.exists(), "client_skeleton.py was not created")
        self.assertTrue(how_to.exists(), "HOW_TO.md was not created")

        self._assert_valid_python(server, label="server_skeleton.py")
        self._assert_valid_python(client, label="client_skeleton.py")

    def test_no_overwrite_existing_file(self):
        """
        Running --generate-server twice must NOT overwrite the existing file.

        On the second run the CLI must print "[skip]" and leave the file
        contents unchanged.
        """
        # First run --- file is created.
        first = self._run("--generate-server")
        self.assertEqual(first.returncode, 0, msg=first.stderr)

        skeleton = self._work_dir / "server_skeleton.py"

        # Overwrite the file with sentinel content so we can detect any change.
        sentinel = "# sentinel --- must not be overwritten\n"
        skeleton.write_text(sentinel, encoding="utf-8")

        # Second run --- must skip the file.
        second = self._run("--generate-server")
        self.assertEqual(second.returncode, 0, msg=second.stderr)
        self.assertIn(
            "[skip]", second.stdout,
            "Expected '[skip]' in output on second run, but it was not found"
        )

        # File content must still be the sentinel, not the original template.
        after_content = skeleton.read_text(encoding="utf-8")
        self.assertEqual(
            sentinel, after_content,
            "server_skeleton.py was overwritten on the second run"
        )

    def test_generate_interface(self):
        """
        --generate-interface creates message.proto whose content matches the
        bundled source, and prints "Next steps" instructions to stdout.
        """
        result = self._run("--generate-interface")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        proto = self._work_dir / "message.proto"
        self.assertTrue(proto.exists(), "message.proto was not created")

        # Content must match the proto bundled inside the package.
        expected = _BUNDLED_PROTO.read_bytes()
        actual = proto.read_bytes()
        self.assertEqual(
            expected, actual,
            "Generated message.proto content does not match grpchook/message.proto"
        )

        # Printed instructions must contain the key heading.
        self.assertIn("Next steps", result.stdout)

    def test_generate_interface_with_skeletons(self):
        """
        --generate-interface-with-skeletons creates message.proto, server_skeleton.py,
        and client_skeleton.py.  The Python files must be syntactically valid and
        must reference compile_and_register so we know they use the custom interface
        path rather than the plain skeleton path.
        """
        result = self._run("--generate-interface-with-skeletons")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        proto = self._work_dir / "message.proto"
        server = self._work_dir / "server_skeleton.py"
        client = self._work_dir / "client_skeleton.py"

        self.assertTrue(proto.exists(),   "message.proto was not created")
        self.assertTrue(server.exists(),  "server_skeleton.py was not created")
        self.assertTrue(client.exists(),  "client_skeleton.py was not created")

        self._assert_valid_python(server, label="server_skeleton.py")
        self._assert_valid_python(client, label="client_skeleton.py")

        # Both skeletons must use the custom-interface bootstrap, not the plain import.
        server_src = server.read_text(encoding="utf-8")
        client_src = client.read_text(encoding="utf-8")
        self.assertIn("compile_and_register", server_src,
                      "server_skeleton.py does not call compile_and_register")
        self.assertIn("compile_and_register", client_src,
                      "client_skeleton.py does not call compile_and_register")

        # Instructions must also be printed.
        self.assertIn("Next steps", result.stdout)


if __name__ == "__main__":
    unittest.main()
