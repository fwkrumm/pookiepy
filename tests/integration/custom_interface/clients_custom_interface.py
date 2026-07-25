"""Custom-interface integration test --- clients using a runtime-compiled proto."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))  # enable: import _proto_setup
# pylint: disable=wrong-import-position
import _proto_setup  # side-effect: compiles + registers custom proto
from grpchook.tools import generate_message
from grpchook.baseclient import BaseClient
from tests.integration._interface import get_args
# pylint: enable=wrong-import-position

_proto_setup.ensure_loaded()


TIMEOUT = 1.0 # seconds



class GrpcTestClientCustom(BaseClient):
    def __init__(self, name: str, port: int):
        super().__init__(port,
                         name=name,
                         provides=["test_message", "server-exit"],
                         requires=["test_message"])
        self.logger.info("initialized GrpcTestClientCustom")


if __name__ == "__main__":
    args = get_args("Custom interface test")
    # start two clients and exchange a test message
    client1 = GrpcTestClientCustom("client1", args.port)
    client2 = GrpcTestClientCustom("client2", args.port)

    message1 = generate_message(message_name="test_message")
    message2 = generate_message(message_name="test_message")

    # the generate message currently would only set the message name; for custom interfaces
    # that function needs to be implemented by the dev. here we simpply set the values manually.
    # NOTE that intellisense might not work properly (yet).
    message1.payload.onlyAFloatPayload=1.1
    message2.payload.onlyAFloatPayload=2.2

    client1.send_data(message1)
    client2.send_data(message2)

    try:
        data_client1 = client1.get_data(timeout=TIMEOUT)
        data_client2 = client2.get_data(timeout=TIMEOUT)

        # check if data correctly received; self notification is not permitted by default,
        # so the clients will only receive data from the other client
        # NOTE that we have to account for float precision offsets.
        assert abs(data_client1.payload.onlyAFloatPayload - 2.2) < 1e-6,\
            f"Client 1 did not receive the correct message because it received "\
            f"{data_client1.payload.onlyAFloatPayload} instead of 2.2"
        assert abs(data_client2.payload.onlyAFloatPayload - 1.1) < 1e-6,\
            f"Client 2 did not receive the correct message because it received "\
            f"{data_client2.payload.onlyAFloatPayload} instead of 1.1"

    finally:

        client1.send_data(generate_message("server-exit"))
        client1.wait_done()  # wait until data yielded

        client1.disconnect()
        client2.disconnect()
