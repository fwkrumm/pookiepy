"""Password Auth Test --- Clients
================================
.. warning::

    Plaintext password in gRPC metadata is **NOT best practice**.
    Use TLS certificate-based auth for production workloads.

Scenario
--------
1. An unauthorised client connects with the wrong password
   → ``GrpcConnectionError`` is raised.
2. An authorised client connects with the correct password
   → succeeds, sends one message, then triggers server shutdown.

Run
---
    python tests/integration/password/clients_password.py
"""
from grpchook.baseclient import BaseClient, ClientConfig
from grpchook.exceptions import GrpcConnectionError
from grpchook.logger import get_logger as _get_logger
from grpchook.tools import generate_message
from tests.integration._interface import get_args

CORRECT_PASSWORD = "s3cr3t"
PASSWORD_METADATA_KEY = "x-password"


class PasswordClient(BaseClient):
    """BaseClient subclass that appends a plaintext password to gRPC call metadata.

    Args:
        name: Client name used for logging and routing.
        port: gRPC server port.
        provides: Message names this client will send.
        requires: Message names this client wants to receive.
        password: Plaintext password sent as ``x-password`` gRPC call metadata.
        config: Optional :class:`ClientConfig` override.
    """

    def __init__(
        self,
        name: str,
        port: int,
        provides: list,
        requires: list,
        *,
        password: str,
        config: ClientConfig = None,
    ):
        cfg = config or ClientConfig()
        cfg.ext_metadata = [(PASSWORD_METADATA_KEY, password)]
        super().__init__(port, name=name, provides=provides, requires=requires, config=cfg)


if __name__ == "__main__":
    args = get_args("Password auth test: gRPC call-metadata password check")

    _log = _get_logger("password")
    _log.warning(
        "Plaintext password in gRPC metadata is NOT best practice. "
        "Use TLS certificate-based auth for production workloads."
    )

    # --- unauthorised attempt ---
    _log.info("password: attempting unauthorised connection (wrong password)...")
    bad_client = None
    try:
        bad_client = PasswordClient(
            "hacker",
            args.port,
            provides=["ping"],
            requires=[],
            password="wrong_password",
            config=ClientConfig(connection_check_timeout=1.5),
        )
        assert False, "Expected GrpcConnectionError for wrong password --- none raised"
    except GrpcConnectionError as exc:
        _log.info("OK: unauthorised client rejected as expected: %s", exc)
    finally:
        if bad_client is not None:
            bad_client.disconnect()

    # --- authorised connection ---
    _log.info("password: attempting authorised connection (correct password)...")
    auth_client = None
    try:
        auth_client = PasswordClient(
            "trusted_worker",
            args.port,
            provides=["server-exit"],
            requires=[],
            password=CORRECT_PASSWORD,
        )
        assert auth_client.server_session_id, (
            "No server_session_id --- authorised connection failed"
        )
        assert auth_client.run_event.is_set(), (
            "run_event not set --- client not properly connected"
        )
        auth_client.logger.info(
            "OK: authorised client connected (sessionId=%s...)",
            auth_client.server_session_id[:8],
        )

        auth_client.send_data(generate_message("server-exit"))
        auth_client.wait_done()
    finally:
        if auth_client is not None:
            auth_client.disconnect()

    assert not auth_client.run_event.is_set(), (
        "run_event still set after disconnect --- client not properly shut down"
    )
    _log.info("password: all assertions passed")
