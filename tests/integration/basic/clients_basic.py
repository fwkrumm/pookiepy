"""Basic connectivity integration test --- two clients exchange messages."""
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message
from grpchook import message_pb2
from tests.integration._interface import get_args

TIMEOUT = 2 # seconds

class BasicClient(BaseClient):
    """
    gRPC client implementation
    """

    def __init__(self, name: str, port: int):
        super().__init__(port,
                         name=name,
                         provides=["test_message", "server-exit"],
                         requires=["test_message"])
        self.logger.info("initialized BasicClient")

    def on_receive(self, data: message_pb2.Message):

        if self.name == "client1":
            # client1 should receive client2's message
            assert data.payload.bytePayload == b"client2",\
                f"Expected payload 'client2', got {data.payload.bytePayload}"
        elif self.name == "client2":
            # client2 should receive client1's message
            assert data.payload.bytePayload == b"client1",\
                f"Expected payload 'client1', got {data.payload.bytePayload}"


if __name__ == "__main__":
    args = get_args("Basic connectivity test")

    client1 = BasicClient("client1", args.port)
    client2 = BasicClient("client2", args.port)

    data1 = generate_message(message_name="test_message",
                             byte_payload=b"client1")
    data2 = generate_message(message_name="test_message",
                             byte_payload=b"client2")

    # disconnect manually via disconnect()
    client1.send_data(data1)
    client2.send_data(data2)
    try:
        client1.spin(timeout=TIMEOUT)
        client2.spin(timeout=TIMEOUT)
    finally:
        client1.disconnect()
        client2.disconnect()

    # test with context manager (automatic disconnect) and
    # multiple connects
    with client1, client2:
        client1.send_data(data1)
        client2.send_data(data2)
        client1.spin(timeout=TIMEOUT)
        client2.spin(timeout=TIMEOUT)

    with client1, client2:
        client1.send_data(data1)
        client2.send_data(data2)
        client1.spin(timeout=TIMEOUT)
        client2.spin(timeout=TIMEOUT)

        # we are done, let server exit (patched via hook! this is no general feature)
        client1.send_data(generate_message(message_name="server-exit"))
        client1.wait_done()
