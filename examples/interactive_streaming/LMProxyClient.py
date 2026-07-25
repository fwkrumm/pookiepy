"""LM Studio proxy client: receives lm_request, streams lm_response_stream chunks."""
import threading
import time

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import json_to_struct, struct_to_json
from examples.interactive_streaming import _lm_http
from examples.interactive_streaming._lm_http import (
    _iter_stream, _fetch_sync, _offline_stream, make_http_session,
)


try:
    import requests
except ImportError:
    requests = None


class LMProxyClient(BaseClient):
    """Proxy: receives lm_request, queries LM Studio, streams lm_response_stream chunks."""

    def __init__(self, name: str, port: int, lmstudio_base: str | None = None,
                 model: str = "gemma-4e2b"):
        super().__init__(port, name=name, provides=["lm_response_stream"], requires=["lm_request"])
        self.lmstudio_base = lmstudio_base or "http://127.0.0.1:1234/v1"
        self.model = model

        if requests:
            sess = make_http_session()
            if sess:
                _lm_http._session = sess
                self.logger.debug("persistent HTTP session created")
        else:
            self.logger.warning("requests not installed --- HTTP calls will fail")

        threading.Thread(target=self.spin_forever, daemon=True).start()

    def _send_chunk(self, request: message_pb2.Message, text: str,
                    done: bool = False):
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="lm_response_stream"),
            payload=message_pb2.Payload(
                structPayload=json_to_struct({"chunk": text, "done": done})
            ),
        )
        msg.metaInfo.messageId = request.metaInfo.messageId
        self.send_data(msg)

    def _handle_request(self, request: message_pb2.Message):
        """Forward an lm_request to LM Studio and stream back the response."""
        try:
            prompt = struct_to_json(request.payload.structPayload).get("text", "")
        except (ValueError, TypeError, AttributeError):
            prompt = ""

        if not prompt:
            self._send_chunk(request, "", done=True)
            return

        mid = request.metaInfo.messageId
        self.logger.info("Forwarding to LM Studio %s  messageId=%s", self.lmstudio_base, mid)

        had_chunks = False
        try:
            for chunk in _iter_stream(prompt, self.lmstudio_base, self.model):
                if not self.run_event.is_set():
                    self.logger.debug("proxy shutting down, aborting stream messageId=%s", mid)
                    return
                self._send_chunk(request, chunk)
                had_chunks = True
        except OSError:
            self.logger.exception("stream failed messageId=%s", mid)

        if not had_chunks:
            try:
                self._send_chunk(request, _fetch_sync(prompt, self.lmstudio_base, self.model))
                had_chunks = True
            except OSError:
                self.logger.exception("sync fetch failed messageId=%s", mid)

        if not had_chunks:
            self.logger.warning("LM Studio unreachable, using offline stream  messageId=%s", mid)
            for chunk in _offline_stream(prompt):
                self._send_chunk(request, chunk)

        self._send_chunk(request, "", done=True)

    def on_receive(self, data: message_pb2.Message) -> bool:
        threading.Thread(target=self._handle_request, args=(data,), daemon=True).start()
        return True


if __name__ == "__main__":
    proxy = LMProxyClient("lm-proxy", 49999)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        proxy.disconnect()
