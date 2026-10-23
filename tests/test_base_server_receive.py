"""Exercise the server receive worker without sockets or background threads."""

import queue
import threading
import unittest
from unittest.mock import MagicMock, patch

import grpc

from pookiepy import message_pb2
from pookiepy.baseserver import BaseServer, Peer


class TestClientReceive(unittest.TestCase):
    """Verify handshake ordering, hook decisions, routing, and worker cleanup."""

    def setUp(self):
        self.server = BaseServer(port=50099, name="ReceiveTest")
        self.server.logger = MagicMock()
        self.peer = Peer("fake-peer", "session-1")
        self.context = MagicMock()
        self.notifications = queue.Queue()
        self.exit_event = threading.Event()
        self.handshake = message_pb2.PookieMessage(
            metaInfo=message_pb2.MetaInformation(
                clientInfo=message_pb2.ClientProvides(
                    uuid="client-1", name="worker", requires=["topic"], provides=["output"]
                )
            )
        )

    def receive(self, requests):
        """Run the receive worker synchronously with the supplied request iterator."""
        self.server._handle_client_receive(  # pylint: disable=protected-access
            iter(requests), self.context, self.peer, self.notifications, self.exit_event
        )

    def test_handshake_enqueues_welcome_before_subscribing(self):
        """Subscribers cannot race ahead of the welcome message during registration."""
        self.server.on_client_accepted = MagicMock()
        register = self.server._data_register  # pylint: disable=protected-access

        def broadcast_on_subscribe(client_id, topic, notifications):
            self.assertEqual(client_id, "client-1")
            self.assertEqual(topic, "topic")
            self.assertIs(notifications, self.notifications)
            self.assertEqual(notifications.qsize(), 1)
            notifications.put(self.server.generate_message(topic, None, b"broadcast"))

        with patch.object(
            register,
            "add_notification_queue_for_message_name",
            side_effect=broadcast_on_subscribe,
        ) as subscribe:
            self.receive([self.handshake])

        subscribe.assert_called_once()
        self.assertEqual(self.peer.client_id, "client-1")
        self.assertEqual(self.peer.name, "worker")
        self.server.on_client_accepted.assert_called_once_with(self.peer, self.handshake)
        welcome = self.notifications.get_nowait()
        self.assertEqual(welcome.metaInfo.serverInfo.name, self.server.name)
        self.assertEqual(welcome.metaInfo.serverInfo.uuid, self.peer.session_id)
        self.assertTrue(welcome.metaInfo.serverInfo.serverId)
        self.assertTrue(welcome.metaInfo.messageId)
        self.assertTrue(welcome.metaInfo.HasField("timestamp"))
        self.assertEqual(
            self.notifications.get_nowait().payload.bytePayload, b"broadcast"
        )
        self.assertTrue(self.exit_event.is_set())

    def test_rejected_handshake_sets_status_even_when_abort_raises(self):
        """Rejected peers never receive a welcome, subscribe, or invoke acceptance hooks."""
        self.server.on_client_connect = MagicMock(return_value=False)
        self.server.on_client_accepted = MagicMock()
        self.context.abort.side_effect = Exception("gRPC background abort")
        with patch.object(self.server, "_data_register") as register:
            self.receive([self.handshake])

        self.server.on_client_connect.assert_called_once_with(self.handshake, self.context)
        self.context.set_code.assert_called_once_with(grpc.StatusCode.PERMISSION_DENIED)
        self.context.set_details.assert_called_once_with("connection rejected by server")
        self.context.abort.assert_called_once_with(
            grpc.StatusCode.PERMISSION_DENIED, "connection rejected by server"
        )
        self.server.on_client_accepted.assert_not_called()
        register.add_notification_queue_for_message_name.assert_not_called()
        self.assertTrue(self.notifications.empty())
        self.assertTrue(self.exit_event.is_set())

    def test_receive_hook_controls_routing_and_history(self):
        """Dropped data stays local; accepted data gains a server history hop and routes."""
        self.peer.client_id = "client-1"
        dropped = self.server.generate_message("output", None, b"drop")
        forwarded = self.server.generate_message("output", None, b"forward")
        forwarded.history.add(name="client", perfCounter=1.0)
        self.server.on_receive = MagicMock(side_effect=[False, True])
        with patch.object(self.server, "_data_register") as register, patch(
            "pookiepy.baseserver.time.perf_counter", return_value=12.5
        ):
            self.receive([dropped, forwarded])

        self.assertEqual(self.server.on_receive.call_count, 2)
        register.add_data_for_message_name.assert_called_once_with(
            "client-1", "output", forwarded
        )
        self.assertEqual(len(dropped.history), 0)
        self.assertEqual(len(forwarded.history), 2)
        self.assertEqual(forwarded.history[-1].name, "server")
        self.assertEqual(forwarded.history[-1].perfCounter, 12.5)
        self.assertTrue(forwarded.history[-1].HasField("receiveTimestamp"))
        self.assertTrue(self.exit_event.is_set())

    def test_rpc_error_signals_worker_exit(self):
        """Transport disconnects are swallowed but always signal stream completion."""
        def disconnected_requests():
            yield from ()
            raise grpc.RpcError("disconnected")

        self.receive(disconnected_requests())
        self.assertTrue(self.exit_event.is_set())

    def test_hook_failure_propagates_and_signals_worker_exit(self):
        """Application hook failures still release the stream's waiting sender."""
        self.peer.client_id = "client-1"
        self.server.on_receive = MagicMock(side_effect=RuntimeError("hook failed"))
        with self.assertRaisesRegex(RuntimeError, "hook failed"):
            self.receive([self.server.generate_message("output", None, b"payload")])
        self.assertTrue(self.exit_event.is_set())