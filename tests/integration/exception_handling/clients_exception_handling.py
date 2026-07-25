"""
Exception-Handling Test --- Clients
===================================

Demonstrates that when ``on_receive`` raises an exception:

* ``spin()`` propagates the exception to the caller (not swallowed).
* The client can still be ``disconnect()``-ed cleanly afterwards.
* ``receive_thread`` is joined --- no ghost threads left behind.
* The process exits with code 0 (everything was handled properly).

Flow
----
1. SenderClient sends a ``"test_message"``.
2. Server fans it out to ReceiverClient (which ``requires=["test_message"]``).
3. ReceiverClient's ``on_receive`` raises ``ValueError`` intentionally.
4. ``spin()`` propagates that ValueError.
5. Test catches it, asserts correct type/message.
6. ``disconnect()`` is called on both clients.
7. Post-disconnect: ``receive_thread`` not alive, ``run_event`` cleared.

Run
---
    python tests/integration/exception_handling/clients_exception_handling.py
"""

import sys
import threading
import time

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message
from tests.integration._interface import get_args

TIMEOUT = 3.0
INTENTIONAL_ERROR = "intentional test exception"


class SenderClient(BaseClient):
    """Sends a single test message and the server-exit signal."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="exc_sender",
            provides=["test_message", "server-exit"],
        )


class ReceiverClient(BaseClient):
    """Raises on every received message to demonstrate exception propagation."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="exc_receiver",
            requires=["test_message"],
            provides=["dummy"],
        )

    def on_receive(self, data: message_pb2.Message):
        """Intentionally raise to test exception propagation from spin()."""
        raise ValueError(INTENTIONAL_ERROR)


if __name__ == "__main__":
    args = get_args("Exception-handling test: no freeze, no ghost threads")

    sender = SenderClient(args.port)
    receiver = ReceiverClient(args.port)

    sender.send_data(generate_message("test_message", byte_payload=b"hello"))

    # spin() must propagate the ValueError raised inside on_receive
    try:
        receiver.spin(timeout=TIMEOUT)
        # on_receive raised --- should not reach here
        sender.logger.error("ERROR: expected ValueError from spin(), got no exception")
        sys.exit(1)
    except ValueError as exc:
        assert str(exc) == INTENTIONAL_ERROR, (
            f"Unexpected error message: '{exc}'"
        )
        sender.logger.info("OK: spin() propagated ValueError as expected: '%s'", exc)

    # --- assert clean state before disconnect ---
    # receive_thread is the gRPC receive loop --- still alive (it runs independently)
    assert receiver.receive_thread.is_alive(), (
        "receive_thread should still be alive before disconnect"
    )
    assert receiver.run_event.is_set(), "run_event should still be set before disconnect"

    # --- disconnect must complete without hanging ---
    t0 = time.monotonic()
    receiver.disconnect()
    elapsed = time.monotonic() - t0
    assert elapsed < 5.0, f"disconnect() hung for {elapsed:.1f}s"

    # --- post-disconnect invariants ---
    assert not receiver.run_event.is_set(), "run_event still set after disconnect"
    assert not receiver.receive_thread.is_alive(), "receive_thread still alive after disconnect"

    sender.logger.info("OK: disconnect() completed in %.2fs, no ghost threads", elapsed)

    # thread-count sanity: only main thread + sender's receive thread remain
    live_threads = [t for t in threading.enumerate() if not t.daemon and t.is_alive()]
    thread_names = [t.name for t in live_threads]
    sender.logger.info(
        "INFO: %d non-daemon threads remaining: %s", len(live_threads), thread_names
    )

    # shut down server and sender
    sender.send_data(
        message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="server-exit")
        )
    )
    sender.wait_done()
    sender.disconnect()

    sender.logger.info("exception_handling: all assertions passed")
    sys.exit(0)
