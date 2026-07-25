"""
this is a helper script to execute the integration tests. It will sequentially run the
server and client scripts for each example. You can stop each example by pressing Ctrl+C,
which will terminate both the server and client processes before moving on to the next example.

NOTE that this is just a helper script and if there are errors there might be dangling processes.
NOTE that logs will be scrambled due to subprocess usage. You can however still run the examples
    individually to get clean logs for debugging.
"""
import os
import sys
import time
import socket
import subprocess
from pathlib import Path


ROOT = Path(__file__).parent
PROJECT_ROOT = ROOT.parent.parent

# Ensure subprocesses can import project packages (baseclasses, grpchook, interface, tests)
_env = os.environ.copy()
_existing = _env.get("PYTHONPATH", "")
_env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + _existing if _existing else "")

# Hard cap for one example pair. Integration examples are short; if a pair does
# not finish within this window, treat it as a hang and fail fast instead of
# letting CI spin forever.
EXAMPLE_TIMEOUT_S = 120
SERVER_STARTUP_WAIT_S = 1.0

# examples/integration tests to execute
EXAMPLES = [
    {
        "name": "basic",
        "server": ROOT / "basic" / "server_basic.py",
        "client": ROOT / "basic" / "clients_basic.py",
    },
    {
        "name": "history",
        "server": ROOT / "history" / "server_history.py",
        "client": ROOT / "history" / "clients_history.py",
    },
    {
        "name": "custom_interface",
        "server": ROOT / "custom_interface" / "server_custom_interface.py",
        "client": ROOT / "custom_interface" / "clients_custom_interface.py",
    },
    {
        "name": "static_data",
        "server": ROOT / "static_data" / "server_static_data.py",
        "client": ROOT / "static_data" / "clients_static_data.py",
    },
    # --- new examples ---
    {
        "name": "server_off",
        "server": None,  # no server --- client must raise GrpcConnectionError cleanly
        "client": ROOT / "server_off" / "clients_server_off.py",
    },
    {
        "name": "server_disconnect",
        "server": ROOT / "server_disconnect" / "server_server_disconnect.py",
        "client": ROOT / "server_disconnect" / "clients_server_disconnect.py",
    },
    {
        "name": "exception_handling",
        "server": ROOT / "exception_handling" / "server_exception_handling.py",
        "client": ROOT / "exception_handling" / "clients_exception_handling.py",
    },
    {
        "name": "broadcast",
        "server": ROOT / "broadcast" / "server_broadcast.py",
        "client": ROOT / "broadcast" / "clients_broadcast.py",
    },
    {
        "name": "config_client",
        "server": ROOT / "config_client" / "server_config_client.py",
        "client": ROOT / "config_client" / "clients_config_client.py",
    },
    {
        "name": "wait_for_clients",
        "server": ROOT / "wait_for_clients" / "server_wait_for_clients.py",
        "client": ROOT / "wait_for_clients" / "clients_wait_for_clients.py",
    },
    {
        "name": "timer",
        "server": ROOT / "timer" / "server_timer.py",
        "client": ROOT / "timer" / "clients_timer.py",
    },
    {
        "name": "password",
        "server": ROOT / "password" / "server_password.py",
        "client": ROOT / "password" / "clients_password.py",
    },
    {
        "name": "high_fire",
        "server": ROOT / "high_fire" / "server_high_fire.py",
        "client": ROOT / "high_fire" / "clients_high_fire.py",
    },
    {
        "name": "request_response",
        "server": ROOT / "request_response" / "server_request_response.py",
        "client": ROOT / "request_response" / "clients_request_response.py",
    },
    {
        "name": "compression",
        "server": ROOT / "compression" / "server_compression.py",
        "client": ROOT / "compression" / "clients_compression.py",
    },
    {
        "name": "payload_limits_sweep",
        "server": None,  # client script orchestrates per-case server subprocesses
        "client": ROOT / "payload_limits_sweep" / "clients_payload_limits_sweep.py",
    },
]

def _exists(p: Path) -> bool:
    """
    check if file exists

    Parameters
    ----------
    p : Path
        path to file

    Returns
    -------
    bool
        True if file exists, False otherwise
    """
    return p.exists() and p.is_file()


def _terminate(proc: subprocess.Popen, name: str):
    """
    terminate process

    Parameters
    ----------
    proc : subprocess.Popen
        process to terminate
    name : str
        process name for logging purposes
    """
    if proc is None:
        return
    if proc.poll() is not None:
        return
    print(f"Terminating {name} (pid={proc.pid})")
    try:
        proc.terminate()
        proc.wait(timeout=3)
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            proc.kill()
        except Exception:  # pylint: disable=broad-exception-caught
            pass


def _pick_unused_port() -> int:
    """Ask OS for a currently unused TCP port.

    This avoids reusing a single fixed port across sequential integration
    subprocesses, which is flaky on Windows because a recently closed port can
    stay temporarily unavailable for rebinding.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server_startup(server_path: Path, srv: subprocess.Popen, port: int):
    """Wait briefly for server to either stay alive or fail fast.

    Many startup errors, including bind failures, surface immediately. Catch
    them before launching the client so the harness reports the real server
    failure instead of a downstream client error.
    """
    time.sleep(SERVER_STARTUP_WAIT_S)
    if srv.poll() is not None:
        raise RuntimeError(
            f"Server '{server_path.name}' exited during startup with code {srv.returncode} "
            f"on port {port}."
        )


def run_example_pair(server_path: Path, client_path: Path):
    """
    run examples and check if they terminate cleanly. If one example
    fails we immediately exit to not waste pipeline time.

    Parameters
    ----------
    server_path : Path or None
        path to the server script, or None for client-only tests
    client_path : Path
        path to the client script
    """
    print(f"\n=== Example: {client_path.parent.name} ===")

    srv = None
    port = _pick_unused_port()
    if server_path is not None:
        if not _exists(server_path):
            # usually should not happen
            print(f"Server script not found: {server_path}, skipping example")
            return

        srv_cmd = [sys.executable, str(server_path), "--port", str(port)]
        print(f"Starting server: {srv_cmd}")
        srv = subprocess.Popen(  # pylint: disable=consider-using-with
            srv_cmd,
            cwd=str(ROOT),
            env=_env,
        )
        _wait_for_server_startup(server_path, srv, port)
    else:
        print("(client-only test --- no server started)")

    if not _exists(client_path):
        # usually should not happen
        print(f"Client script not found: {client_path}, skipping client startup "\
              "(server kept running).")
        _terminate(srv, "server")
        return

    cli_cmd = [sys.executable, str(client_path), "--port", str(port)]
    print(f"Starting client: {cli_cmd}")
    cli = subprocess.Popen(  # pylint: disable=consider-using-with
        cli_cmd,
        cwd=str(ROOT),
        env=_env,
    )

    try:
        print("Press Ctrl+C to stop this example and move to the next one.")
        deadline = time.monotonic() + EXAMPLE_TIMEOUT_S

        while True:
            time.sleep(1.0)

            if time.monotonic() > deadline:
                _terminate(cli, "client")
                _terminate(srv, "server")
                sys.exit(
                    f"ERROR: Example '{client_path.parent.name}' did not finish within "
                    f"{EXAMPLE_TIMEOUT_S}s; treating it as a hang."
                )

            # check if server and client processes terminated CLEANLY without errors
            # server_done starts True when there is no server process (client-only test)
            server_done = srv is None
            client_done = False

            # check if server process is still alive
            if srv is not None and srv.poll() is not None:

                # check return value
                if srv.returncode != 0:
                    # exit early and terminate client if server failed
                    _terminate(cli, "client")
                    sys.exit(f"ERROR: Server exited with code {srv.returncode}")

                print("server process done.")
                server_done = True

            # check client process
            if cli.poll() is not None:
                if cli.returncode != 0:
                    # exit early and terminate server if client failed
                    _terminate(srv, "server")
                    sys.exit(f"ERROR: Client exited with code {cli.returncode}")
                client_done = True

            if server_done and client_done:
                print("Example finished successfully, moving to the next one...")
                break

    except KeyboardInterrupt:
        # fallback if there is any freeze or early exit desired.
        print("Keyboard interrupt received --- shutting down example processes...")
        _terminate(cli, "client")
        _terminate(srv, "server")
        # small pause to allow clean shutdown
        time.sleep(0.5)


def main():
    print("run_examples.py --- sequentially runs bundled examples")
    for ex in EXAMPLES:
        run_example_pair(ex.get("server"), ex["client"])

    print("All examples processed. Exiting.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Fatal error: {e}")
        sys.exit(1)
