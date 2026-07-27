"""Interactive text client for the LM Studio streaming example."""
import uuid
import queue

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import json_to_struct, struct_to_json


class TextClient(BaseClient):
    """
    Interactive text client. Sends `lm_request` and receives streaming
    `lm_response_stream` messages (partial chunks with `done` flag).
    """

    def __init__(self, identifier: str, port: int):
        super().__init__(port, name=identifier,
                         provides=["lm_request"],
                         requires=["lm_response_stream"])
        self.logger.setLevel("WARNING")

    def on_receive(self, data: message_pb2.Message) -> bool:
        try:
            payload = (
                struct_to_json(data.payload.structPayload)
                if data.payload and data.payload.structPayload
                else {}
            )
        except (ValueError, TypeError, AttributeError):
            payload = {}

        chunk = payload.get("chunk", "")
        done = payload.get("done", False)

        if chunk:
            print(chunk, end="", flush=True)
        if done:
            print()
        return True

    def interactive_loop(self):
        """
        Prompt user, send request, stream responses until `done` True.
        """
        try:
            while True:
                text = input("You: ").strip()
                if not text:
                    continue
                if text.lower() in ("exit", "quit"):
                    break

                msg_id = str(uuid.uuid4())
                msg = message_pb2.Message(
                    metaInfo=message_pb2.MetaInformation(
                        messageId=msg_id,
                        messageName="lm_request",
                    ),
                    payload=message_pb2.Payload(structPayload=json_to_struct({"text": text}))
                )

                self.send_data(msg)

                # collect streaming chunks for this request
                while True:
                    try:
                        resp = self.get_data()
                    except queue.Empty:
                        print("\nTimeout waiting for response")
                        break
                    except (RuntimeError, OSError) as e:
                        print("\nClient stopped:", e)
                        return

                    if resp.metaInfo.responseToId != msg_id:
                        # not for this request; ignore
                        continue

                    try:
                        payload = (
                            struct_to_json(resp.payload.structPayload)
                            if resp.payload and resp.payload.structPayload
                            else {}
                        )
                    except (ValueError, TypeError, AttributeError):
                        payload = {}

                    chunk = payload.get("chunk", "")
                    done = payload.get("done", False)
                    if chunk:
                        print(chunk, end="", flush=True)
                    if done:
                        print()
                        break

        finally:
            self.disconnect()


if __name__ == "__main__":
    client = TextClient("text-ui", 49999)
    client.interactive_loop()
