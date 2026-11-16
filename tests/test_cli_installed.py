"""
Integration tests for the pookiepy CLI.

These tests install pookiepy into a dedicated virtual environment (created
once per test run in ``setUpClass``) and then invoke ``python -m pookiepy``
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
- Skill commands install complete byte-identical assets for all or selected tools.
- Skill installation is idempotent and rejects conflicts before writing any files.
- ``--generate`` produces both skeletons and all skills.
- ``--generate-interface``  produces message.proto and prints
  "Next steps" instructions.
- ``--generate-interface-with-skeletons`` produces message.proto + skeletons using
    explicit ProtoInterface injection.
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
import re
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

_BUNDLED_SKILL = _PROJECT_ROOT / "pookiepy" / "assets" / "skills" / "pookiepy"
_SKILL_TARGETS = {
    "github": Path(".github") / "skills" / "pookiepy",
    "claude": Path(".claude") / "skills" / "pookiepy",
    "cursor": Path(".cursor") / "skills" / "pookiepy",
    "codex": Path(".agents") / "skills" / "pookiepy",
}

# The bundled message.proto used for byte-for-byte comparison.
_BUNDLED_PROTO = _PROJECT_ROOT / "pookiepy" / "message.proto"

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

class TestInstalledCLI(unittest.TestCase):  # pylint: disable=too-many-public-methods
    """
    Smoke-tests for the ``python -m pookiepy`` CLI, run against an installed
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
        Create a temporary virtual environment and install pookiepy into it.

        This runs once before all test methods in this class.  The install step
        invokes a PEP 517 build (pdm-backend), so it may take ~10–30 s on first
        run when the build backend is not yet cached.
        """
        raw_venv_dir = tempfile.mkdtemp(prefix="pookiepy_test_venv_")
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
        Create the virtual environment and install pookiepy using ``uv``.

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
        Create the virtual environment and install pookiepy using the standard
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
        self._work_dir = Path(tempfile.mkdtemp(prefix="pookiepy_test_work_"))

    def tearDown(self) -> None:
        """Remove the per-test working directory."""
        shutil.rmtree(self._work_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Private helper
    # ------------------------------------------------------------------

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        """
        Invoke ``python -m pookiepy *args`` inside *self._work_dir*.

        The working directory is always the temporary directory created in
        ``setUp``, so generated files land there and never pollute the project.
        """
        return subprocess.run(
            [str(self._python), "-m", "pookiepy", *args],
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

    def _assert_skills(self, targets: tuple[str, ...], *, skeletons: bool = False) -> None:
        """Check exact installed file paths, content, and absence of other targets."""
        expected = set()
        for target in targets:
            for relative in (Path("SKILL.md"), Path("references") / "API_REFERENCE.md"):
                destination = _SKILL_TARGETS[target] / relative
                expected.add(destination)
                self.assertEqual(
                    (self._work_dir / destination).read_bytes(),
                    (_BUNDLED_SKILL / relative).read_bytes(),
                    msg=str(destination),
                )
        if skeletons:
            expected.update((Path("server_skeleton.py"), Path("client_skeleton.py")))
        actual = {
            path.relative_to(self._work_dir)
            for path in self._work_dir.rglob("*") if path.is_file()
        }
        self.assertEqual(actual, expected)

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

    def test_install_skills(self):
        """All-tool installation provides complete assets with valid local links."""
        result = self._run("--install-skills")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self._assert_skills(tuple(_SKILL_TARGETS))

        for target in _SKILL_TARGETS.values():
            skill_file = self._work_dir / target / "SKILL.md"
            skill = skill_file.read_text(encoding="utf-8")
            frontmatter = skill.split("---", 2)[1]
            self.assertRegex(frontmatter, r"(?m)^name: pookiepy$")
            description = re.search(r"(?m)^description: (.+)$", frontmatter)
            if description is None:
                self.fail("Skill description is missing")
            self.assertLessEqual(len(description.group(1)), 1024)
            self.assertLess(len(skill.splitlines()), 500)
            for link in re.findall(r"\]\(([^)]+)\)", skill):
                reference = skill_file.parent / link.split("#", 1)[0]
                self.assertTrue(reference.is_file(), msg=link)
            for code in re.findall(r"```python\n(.*?)```", skill, re.DOTALL):
                compile(code, str(skill_file), "exec")

    def test_removed_skill_commands(self):
        """Superseded skill command names fail without writing files."""
        for flag in ("--generate-skills", "--install-skill", *(
            f"--generate-{target}" for target in _SKILL_TARGETS
        )):
            with self.subTest(flag=flag):
                result = self._run(flag)
                self.assertEqual(result.returncode, 2, msg=result.stderr)
                self.assertEqual(list(self._work_dir.iterdir()), [])

    def test_deprecated_skill_alias(self):
        """The legacy flag warns and installs skills without creating the old guide."""
        result = self._run("--generate-how-to")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("deprecated", result.stderr)
        self._assert_skills(tuple(_SKILL_TARGETS))

    def test_install_skills_github(self):
        """The GitHub flag installs only in Copilot's project directory."""
        result = self._run("--install-skills-github")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self._assert_skills(("github",))

    def test_install_skills_claude(self):
        """The Claude flag installs only in Claude Code's project directory."""
        result = self._run("--install-skills-claude")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self._assert_skills(("claude",))

    def test_install_skills_cursor(self):
        """The Cursor flag installs only in Cursor's project directory."""
        result = self._run("--install-skills-cursor")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self._assert_skills(("cursor",))

    def test_install_skills_codex(self):
        """The Codex flag installs only in the shared agents project directory."""
        result = self._run("--install-skills-codex")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self._assert_skills(("codex",))

    def test_combining_skill_flags(self):
        """Combined and repeated per-tool flags install each selected target once."""
        result = self._run(
            "--install-skills-github", "--install-skills-cursor", "--install-skills-github"
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.count("[ok]"), 4)
        self._assert_skills(("github", "cursor"))

    def test_skills_are_idempotent(self):
        """An identical reinstall leaves every file's content and timestamp intact."""
        first = self._run("--install-skills")
        self.assertEqual(first.returncode, 0, msg=first.stderr)
        before = {
            path: (path.read_bytes(), path.stat().st_mtime_ns)
            for path in self._work_dir.rglob("*") if path.is_file()
        }
        second = self._run("--install-skills", "--install-skills-cursor")
        self.assertEqual(second.returncode, 0, msg=second.stderr)
        self.assertIn("[skip]", second.stdout)
        self.assertNotIn("[ok]", second.stdout)
        self._assert_skills(tuple(_SKILL_TARGETS))
        for path, state in before.items():
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), state)

    def test_skill_conflict_prevents_partial_generation(self):
        """A last-target reference conflict blocks other targets and skeletons."""
        conflict = self._work_dir / _SKILL_TARGETS["codex"] / "references" / "API_REFERENCE.md"
        conflict.parent.mkdir(parents=True)
        conflict.write_text("custom reference\n", encoding="utf-8")
        result = self._run("--generate")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[error]", result.stderr)
        self.assertIn(str(conflict), result.stderr)
        self.assertNotIn("[ok]", result.stdout)
        self.assertEqual(conflict.read_text(encoding="utf-8"), "custom reference\n")
        self.assertEqual(
            [path for path in self._work_dir.rglob("*") if path.is_file()], [conflict]
        )

    def test_skill_parent_file_conflict(self):
        """A file blocking a target directory fails before installing any target."""
        conflict = self._work_dir / ".agents"
        conflict.write_text("not a directory\n", encoding="utf-8")
        result = self._run("--install-skills")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a directory", result.stderr)
        self.assertEqual(list(self._work_dir.iterdir()), [conflict])

    def test_skill_symlink_destination(self):
        """Destination symlinks must not redirect installs outside the project."""
        with tempfile.TemporaryDirectory(prefix="pookiepy_skill_link_") as raw_target:
            target = Path(raw_target)
            link = self._work_dir / ".github"
            try:
                link.symlink_to(target, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"Creating symlinks is unavailable: {exc}")
            result = self._run("--install-skills")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("symbolic link", result.stderr)
            self.assertEqual(list(target.iterdir()), [])
            self.assertEqual(list(self._work_dir.iterdir()), [link])

    def test_skill_help(self):
        """Help exposes supported flags and not the deprecated alias."""
        result = self._run("--help")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        for flag in ("--install-skills", *(
            f"--install-skills-{target}" for target in _SKILL_TARGETS
        )):
            self.assertIn(flag, result.stdout)
        self.assertNotIn("--generate-how-to", result.stdout)
        options = set(re.findall(r"--[a-z][a-z-]+", result.stdout))
        self.assertNotIn("--generate-skills", options)
        self.assertNotIn("--install-skill", options)
        for target in _SKILL_TARGETS:
            self.assertNotIn(f"--generate-{target}", options)

    def test_generate(self):
        """
        --generate creates both skeletons and all supported agent skills.
        Both Python files must be syntactically valid.
        """
        result = self._run("--generate")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

        server = self._work_dir / "server_skeleton.py"
        client = self._work_dir / "client_skeleton.py"

        self.assertTrue(server.exists(), "server_skeleton.py was not created")
        self.assertTrue(client.exists(), "client_skeleton.py was not created")
        self._assert_skills(tuple(_SKILL_TARGETS), skeletons=True)

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
            "Generated message.proto content does not match pookiepy/message.proto"
        )

        # Printed instructions must contain the key heading.
        self.assertIn("Next steps", result.stdout)

    def test_generate_interface_with_skeletons(self):
        """
        --generate-interface-with-skeletons creates message.proto, server_skeleton.py,
        and client_skeleton.py.  The Python files must be syntactically valid and
        must reference ProtoInterface so we know they use the custom interface
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

        # Both skeletons must inject precompiled modules without runtime compilation.
        server_src = server.read_text(encoding="utf-8")
        client_src = client.read_text(encoding="utf-8")
        self.assertIn("ProtoInterface", server_src)
        self.assertIn("proto_interface: ProtoInterface = PROTO_INTERFACE", server_src)
        self.assertIn("proto_interface=proto_interface", server_src)
        self.assertIn("ProtoInterface", client_src)
        self.assertIn("proto_interface: ProtoInterface = PROTO_INTERFACE", client_src)
        self.assertIn("proto_interface=proto_interface", client_src)
        self.assertNotIn("compile_and_register", server_src)
        self.assertNotIn("compile_and_register", client_src)

        compile_result = subprocess.run(
            [
                str(self._python),
                "-m",
                "grpc_tools.protoc",
                "-I.",
                "--python_out=.",
                "--grpc_python_out=.",
                "--pyi_out=.",
                "message.proto",
            ],
            cwd=str(self._work_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(compile_result.returncode, 0, msg=compile_result.stderr)

        import_result = subprocess.run(
            [
                str(self._python),
                "-c",
                (
                    "import inspect, server_skeleton, client_skeleton; "
                    "assert inspect.signature(server_skeleton.MyServer).parameters"
                    "['proto_interface'].default is server_skeleton.PROTO_INTERFACE; "
                    "assert inspect.signature(client_skeleton.MyClient).parameters"
                    "['proto_interface'].default is client_skeleton.PROTO_INTERFACE"
                ),
            ],
            cwd=str(self._work_dir),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(import_result.returncode, 0, msg=import_result.stderr)

        # Instructions must also be printed.
        self.assertIn("Next steps", result.stdout)


if __name__ == "__main__":
    unittest.main()
