"""
run_example.py --- starts all four processes in-process for the mcp_server example.

Start order:
  1. GrpcServer      --- routing / monitoring choke point
  2. FileOperationClient --- executes sandboxed file ops, replies via gRPC
  3. RunnerClient    --- executes `uv run python main.py`, replies via gRPC
  4. LlmBridgeClient --- drives the LLM agentic loop; disconnects when done

The main thread blocks until LlmBridgeClient finishes or Ctrl+C is pressed.
"""

import shutil
import tempfile
import time
import threading
from pathlib import Path

from examples.mcp_server.GrpcServer import McpGrpcServer
from examples.mcp_server.FileOperationClient import FileOperationClient, BASE_DIR
from examples.mcp_server.RunnerClient import RunnerClient
from examples.mcp_server.LlmBridgeClient import LlmBridgeClient

PORT = 49998


def _clean_base_dir() -> None:
    """Remove only the agent working directory.

    Hard-guards against accidental deletion of non-temp paths: BASE_DIR must
    resolve to a direct child of the system temp directory.  Any deviation
    raises RuntimeError instead of deleting anything.
    """
    system_tmp = Path(tempfile.gettempdir()).resolve()
    resolved   = BASE_DIR.resolve()
    if not resolved.is_relative_to(system_tmp):
        raise RuntimeError(
            f"Refusing to wipe '{resolved}': not inside system temp dir '{system_tmp}'"
        )
    if resolved == system_tmp:
        raise RuntimeError(
            f"Refusing to wipe system temp dir itself: '{resolved}'"
        )
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)


def main():
    # ── Clean working directory from previous runs ────────────────────────────
    _clean_base_dir()

    # ── Server ────────────────────────────────────────────────────────────────
    server = McpGrpcServer(PORT)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    time.sleep(0.3)  # let the server bind the port before clients connect

    # ── Executor clients ──────────────────────────────────────────────────────
    file_client   = FileOperationClient(PORT)
    runner_client = RunnerClient(PORT)

    # ── LLM agent --- starts its agentic loop in on_init ────────────────────────
    bridge = LlmBridgeClient(PORT)

    # Block until the bridge finishes (it calls disconnect() → clears run_event)
    try:
        while bridge.run_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        bridge.disconnect()
        runner_client.disconnect()
        file_client.disconnect()
        server.shutdown()

    if bridge.run_succeeded:
        print()
        print("=" * 60)
        print("  Task finished --- Flask app is running.")
        print("  Open http://localhost:5000 in your browser.")
        print(f"  Generated files are in: {BASE_DIR}")
        print("=" * 60)
        print()
    else:
        print()
        print("=" * 60)
        print("  Task did not complete --- agent aborted or was interrupted.")
        print(f"  Generated files (if any) are in: {BASE_DIR}")
        print("=" * 60)
        print()


if __name__ == "__main__":
    main()
