"""
RunnerClient --- gRPC client that builds and runs the agent-generated main.py.

Detection order:
  1. uv   → uv venv .venv  +  uv pip install -r requirements.txt  +  venv python
  2. python / python3 → python -m venv .venv  +  pip install -r  +  venv python
  3. neither found → return a descriptive error, do not crash

Setup (venv + install) runs with SETUP_TIMEOUT; the Flask server itself is then
launched and given RUN_TIMEOUT seconds:
  - still alive → assumed to have started cleanly  (ok=True,  timed_out=True)
  - exited early → crash or immediate error          (ok=False, timed_out=False)
"""

import sys
import shutil as _shutil
import subprocess
import threading
from pathlib import Path

from grpchook.baseclient import BaseClient
from grpchook import message_pb2
from grpchook.tools import json_to_struct
from examples.mcp_server.FileOperationClient import BASE_DIR

# ── Constants ─────────────────────────────────────────────────────────────────

RUN_REQUEST    = "mcp.run.execute"
RUN_RESPONSE   = "mcp.run.response"

SETUP_TIMEOUT  = 120  # seconds for venv creation + package install
RUN_TIMEOUT    = 10   # seconds --- time allowed before we assume the server started
MAX_OUTPUT     = 8192  # chars kept from stdout / stderr (tail)


# ── RunnerClient ──────────────────────────────────────────────────────────────

class RunnerClient(BaseClient):
    """gRPC client that builds a venv and runs the agent-generated main.py."""

    def __init__(self, port: int):
        super().__init__(
            name="RunnerClient",
            port=port,
            requires=[RUN_REQUEST],
            provides=[RUN_RESPONSE],
        )

    def on_init(self):
        self.logger.info(
            "RunnerClient ready --- cwd: %s  setup_timeout: %ds  run_timeout: %ds",
            BASE_DIR.resolve(), SETUP_TIMEOUT, RUN_TIMEOUT,
        )
        threading.Thread(target=self.spin_forever, daemon=True).start()

    # ── Runner detection ──────────────────────────────────────────────────────

    def _detect_runner(self) -> tuple[str, str] | None:
        """Return ('uv', path) or ('python', path), or None if neither found."""
        uv = _shutil.which("uv")
        if uv:
            return ("uv", uv)
        py = _shutil.which("python") or _shutil.which("python3")
        if py:
            return ("python", py)
        return None

    def _venv_python(self) -> Path:
        venv = BASE_DIR / ".venv"
        if sys.platform == "win32":
            return venv / "Scripts" / "python.exe"
        return venv / "bin" / "python"

    # ── Setup phase ───────────────────────────────────────────────────────────

    def _run_setup(self, runner_type: str, runner_path: str) -> tuple[bool, str]:
        """Create .venv and install requirements.txt.

        Returns (ok, error_message).
        """
        venv_dir  = BASE_DIR / ".venv"
        req_file  = BASE_DIR / "requirements.txt"
        py_bin    = self._venv_python()

        if runner_type == "uv":
            setup_cmds = [["uv", "venv", "--allow-existing", str(venv_dir)]]
            if req_file.exists():
                setup_cmds.append(
                    ["uv", "pip", "install", "--python", str(py_bin), "-r", str(req_file)]
                )
        else:
            setup_cmds = [[runner_path, "-m", "venv", str(venv_dir)]]
            if req_file.exists():
                if sys.platform == "win32":
                    pip_bin = str(venv_dir / "Scripts" / "pip.exe")
                else:
                    pip_bin = str(venv_dir / "bin" / "pip")
                setup_cmds.append([pip_bin, "install", "-r", str(req_file)])

        for cmd in setup_cmds:
            self.logger.info("Setup: %s", " ".join(cmd))
            try:
                result = subprocess.run(
                    cmd, cwd=BASE_DIR,
                    capture_output=True, text=True,
                    timeout=SETUP_TIMEOUT,
                    check=False,
                )
                if result.returncode != 0:
                    return False, f"Setup failed ({cmd[0]}): {result.stderr[-MAX_OUTPUT:]}"
            except subprocess.TimeoutExpired:
                cmd_str = " ".join(cmd)
                return False, f"Setup timed out after {SETUP_TIMEOUT}s running: {cmd_str}"
            except FileNotFoundError as exc:
                return False, str(exc)

        return True, ""

    # ── Run phase ─────────────────────────────────────────────────────────────

    def _execute(self) -> dict:
        """
        Full pipeline: detect runner → setup venv + install → launch Flask app.

        Returns a result dict with keys:
          ok (bool), stdout (str), stderr (str),
          timed_out (bool), exit_code (int|None)
        """
        if not BASE_DIR.is_dir():
            return {
                "ok": False, "stdout": "", "timed_out": False,
                "stderr": f"Working directory does not exist: {BASE_DIR}",
                "exit_code": None,
            }

        runner = self._detect_runner()
        if runner is None:
            msg = "No uv or Python installation found --- test yourself."
            self.logger.warning(msg)
            return {"ok": False, "stdout": "", "timed_out": False,
                    "stderr": msg, "exit_code": None}

        runner_type, runner_path = runner
        self.logger.info("Runner detected: %s (%s)", runner_type, runner_path)

        ok, err = self._run_setup(runner_type, runner_path)
        if not ok:
            return {"ok": False, "stdout": "", "timed_out": False,
                    "stderr": err, "exit_code": 1}

        py_bin = self._venv_python()
        self.logger.debug("Launching: %s main.py  cwd=%s", py_bin, BASE_DIR)
        try:
            with subprocess.Popen(
                [str(py_bin), "main.py"],
                cwd=BASE_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ) as proc:
                try:
                    stdout, stderr = proc.communicate(timeout=RUN_TIMEOUT)
                    self.logger.warning("Process exited early (rc=%d)", proc.returncode)
                    return {
                        "ok": False,
                        "stdout": stdout[-MAX_OUTPUT:],
                        "stderr": stderr[-MAX_OUTPUT:],
                        "timed_out": False,
                        "exit_code": proc.returncode,
                    }
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    self.logger.info(
                        "Process still alive after %ds --- clean start assumed", RUN_TIMEOUT
                    )
                    return {
                        "ok": True,
                        "stdout": stdout[-MAX_OUTPUT:],
                        "stderr": stderr[-MAX_OUTPUT:],
                        "timed_out": True,
                        "exit_code": None,
                    }
        except FileNotFoundError as exc:
            self.logger.error("Python binary not found: %s", exc)
            return {"ok": False, "stdout": "", "timed_out": False,
                    "stderr": str(exc), "exit_code": None}

    # ── Hook ──────────────────────────────────────────────────────────────────

    def on_receive(self, data: message_pb2.Message) -> bool:
        """Execute the run pipeline and reply with the result."""
        self.logger.info("Run request received")
        result = self._execute()

        response = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName=RUN_RESPONSE),
            payload=message_pb2.Payload(structPayload=json_to_struct(result)),
        )
        response.metaInfo.messageId = data.metaInfo.messageId
        self.send_data(response)

        status = "OK" if result["ok"] else "FAIL"
        self.logger.info("Run %s --- timed_out=%s  exit_code=%s",
                         status, result["timed_out"], result.get("exit_code"))
        return True


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = RunnerClient(49998)
    try:
        client.spin_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
