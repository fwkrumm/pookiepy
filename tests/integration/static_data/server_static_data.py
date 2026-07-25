"""Integration test server for the static_data scenario."""
import sys
import threading
from pathlib import Path

# make sure imports can be resolved
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))  # pylint: disable=wrong-import-position
# pylint: disable=wrong-import-position  # project path must be set before importing project modules

from tests.integration._server_base import IntegrationServer
from tests.integration._interface import get_args
from grpchook.baseserver import Peer
from grpchook.exceptions import GrpcValueError
from grpchook import message_pb2

class StaticDataServer(IntegrationServer):
    """
    gRPC server implementation
    """

    def _add_static_data(self, message_name: str, data: message_pb2.Message):
        if not isinstance(message_name, str):
            raise GrpcValueError(f"{self}: message_name must be a string")
        if not isinstance(data, message_pb2.Message):
            raise GrpcValueError(f"{self}: data must be of type message_pb2.Message")
        with self._static_data_lock:
            self._static_data[message_name] = data

    def _get_static_data(self, message_name: str) -> message_pb2.Message | None:
        with self._static_data_lock:
            return self._static_data.get(message_name, None)


    def on_init(self):
        """
        Override to perform additional setup after server initialization.
        """
        # will probalby inbuilt only contain the "watchdog" data for specialized watchdof clients
        # however the user can basically use it within the overwrite function.
        self._static_data = {} # dict for static data which can be requested by clients;s
        self._static_data_lock = threading.Lock() # lock for static data dict

        self.logger.info("server on_init called")

    def on_receive(self,
                   peer: Peer,
                   request: message_pb2.Message
                   ) -> bool:
        """
        Override to handle incoming messages. By default, all messages are added to the
        notification queue.

        Parameters
        ----------
        peer : Peer
            The peer that sent the message
        request : message_pb2.Message
            The message sent by the client

        Returns
        -------
        bool : True if the message should be added to the notification queue, False otherwise.
        """
        message_name = request.metaInfo.messageName

        if message_name.endswith("_request"):
            self.logger.info("Received message %s which if stored as static data, "\
                             "overwriting static data with new message.", message_name)
            message_name = message_name.removesuffix("_request")
            data = self._get_static_data(message_name)
            assert data is not None, f"Received request for message_name {message_name} but no "\
                "static data found for this message_name."

            # add stored data to notification queue
            self.logger.info("adding message with message_name %s to notification queue of peer %s",
                             message_name, peer)

            # now add the data to the notification queue of the client itself. NOTE that we
            # cannot answer "directly" because of the async nature of this project.
            self._data_register.add_data_for_message_name(peer.client_id, message_name,
                                                         data, target_client_id=peer.client_id)

        else:
            self.logger.info("adding message with message_name %s to static data, which can "\
                             "be requested by clients at any time.", message_name)

            # example of how to use static data dict; in this case every message is stored
            # under its message_name and can be requested by clients at any time;
            # of course the user can also implement a more complex logic for storing static data
            self._add_static_data(message_name, request)

        super().on_receive(peer, request)  # handles server-exit feature for this integration tests
        return False  # routing handled manually above

if __name__ == "__main__":
    args = get_args("Static data test")
    s = StaticDataServer(args.port)
    s.serve_forever()
