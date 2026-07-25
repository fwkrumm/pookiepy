"""Integration test clients for the static_data scenario."""
import sys
import time
from pathlib import Path


# make sure imports can be resolved
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))  # pylint: disable=wrong-import-position
# pylint: disable=wrong-import-position  # project path must be set before importing project modules
from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message, set_metadata
from tests.integration._interface import get_args

TIMEOUT = 1.0 # seconds
REQUEST_RUNS = 5
class GrpcTestClientProvider(BaseClient):
    """
    gRPC client implementation
    """

    def __init__(self, name: str, port: int):
        super().__init__(port,
                         name=name,
                         provides=["test_message", "server-exit"])
        self.logger.info("initialized GrpcTestClientProvider")

class GrpcTestClientRequester(BaseClient):
    """
    gRPC client implementation
    """

    def __init__(self, name: str, port: int):
        super().__init__(port,
                         name=name,
                         requires=["test_message"],
                         provides=["test_message_request"])
        self.logger.info("initialized GrpcTestClientRequester")

if __name__ == "__main__":
    args = get_args("Static data test")
    client1 = GrpcTestClientProvider("client1", args.port)

    # client 1 sends the data (for client2) which is not connected yet.
    data_to_send = generate_message(message_name="test_message",
                                    byte_payload=b"test_data")

    # set_metadata is also called internally, but since we want to log the uuid here
    # we set it manually. The data will not be overwritten.
    set_metadata(data_to_send)
    client1.logger.info("Client 1 sending data with uuid %s", data_to_send.metaInfo.messageId)
    client1.send_data(data_to_send)
    client1.wait_done() # wait until data yielded

    client1.logger.info("static data stored, waiting 1 second before connecting "\
                        "client 2 and requesting data...")
    time.sleep(1)

    client2 = GrpcTestClientRequester("client2", args.port)
    try:
        # now we request the stored data and check if the message matches the one we sent before
        for _ in range(REQUEST_RUNS):
            # we can request the data multiple times since we did not implement a clean up
            # mechanism on server side. so each time we request the data it is put to the
            # notification queue from the server and we can request it again.
            client2.send_data(generate_message(message_name="test_message_request"))
            received_data = client2.get_data(timeout=TIMEOUT)
            client1.logger.info("Client 2 received data with uuid %s",
                                received_data.metaInfo.messageId)
            assert received_data == data_to_send, "Received data does not match sent data"

    finally:

        client1.send_data(message_pb2.Message(\
            metaInfo=message_pb2.MetaInformation(messageName="server-exit"))
        )
        client1.wait_done()

        client1.disconnect()
        client2.disconnect()
