"""Unit checks for the interactive streaming LM proxy."""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from examples.interactive_streaming import _lm_http
from examples.interactive_streaming.LMProxyClient import LMProxyClient
from examples.interactive_streaming.TextClient import TextClient
from pookiepy import message_pb2
from pookiepy.baseclient import BaseClient
from pookiepy.tools import json_to_struct


class TestLMProxyClient(unittest.TestCase):
    def test_missing_requests_fails_before_connecting(self):
        with patch.object(_lm_http, "requests", None), patch.object(BaseClient, "__init__") as init:
            with self.assertRaisesRegex(RuntimeError, "uv run --with requests"):
                LMProxyClient("lm-proxy", 49999)
        init.assert_not_called()


class TestTextClient(unittest.TestCase):
    def test_receive_hook_queues_chunk_without_printing(self):
        client = object.__new__(TextClient)
        message = message_pb2.PookieMessage(
            payload=message_pb2.Payload(structPayload=json_to_struct({"chunk": "Hallo", "done": False}))
        )
        output = io.StringIO()

        with redirect_stdout(output):
            should_queue = client.on_receive(message)

        self.assertTrue(should_queue)
        self.assertEqual(output.getvalue(), "")