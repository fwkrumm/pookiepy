"""gRPC client that executes sandboxed file operations (create / edit / delete)."""
import os
import tempfile
import threading
from pathlib import Path

from pookiepy.baseclient import BaseClient
from pookiepy import message_pb2
from pookiepy.tools import struct_to_json, json_to_struct

# ── Constants ─────────────────────────────────────────────────────────────────

MCP_OPERATIONS = ["mcp.file.create", "mcp.file.edit", "mcp.file.delete"]
MCP_RESPONSE    = "mcp.file.response"

# Use the OS temp directory so no project files are ever touched.
# Windows: %TMP% / %TEMP%   Unix: $TMPDIR / /tmp
BASE_DIR      = Path(tempfile.gettempdir()) / "mcp_server"
MAX_FILE_SIZE = 1024 * 1024  # 1 MB


# ── FileOperationClient ──────────────────────────────────────────────────────

class FileOperationClient(BaseClient):
    """
    gRPC client that executes sandboxed file operations (create / edit / delete).

    Receives mcp.file.{create,edit,delete} messages from the gRPC server,
    executes the requested file operation (restricted to BASE_DIR), and
    sends back an mcp.file.response message to the original requester.

    Security
    --------
    - All paths are resolved with Path.resolve() + relative_to() so that
      any attempt to escape BASE_DIR (e.g. via "..") is rejected.
    - Content size is capped at MAX_FILE_SIZE bytes.
    - Writes are atomic: temp file → os.replace().
    """

    def __init__(self, port: int):
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        super().__init__(
            name="FileOperationClient",
            port=port,
            requires=MCP_OPERATIONS,
            provides=[MCP_RESPONSE],
        )

    def on_init(self):
        self.logger.info(
            "FileOperationClient ready.  ALL file operations are restricted to: %s",
            BASE_DIR.resolve(),
        )
        # Drive on_receive() from a background thread so the client processes
        # incoming messages without the caller needing to call spin_forever().
        threading.Thread(target=self.spin_forever, daemon=True).start()

    # ── Path safety ───────────────────────────────────────────────────────────

    def _safe_path(self, rel_path: str) -> Path | None:
        """
        Resolve *rel_path* relative to BASE_DIR.
        Returns None when the resolved path escapes BASE_DIR.
        """
        if not rel_path:
            return None
        try:
            target = (BASE_DIR / rel_path).resolve()
            target.relative_to(BASE_DIR.resolve())  # raises ValueError if outside
            return target
        except (ValueError, OSError):
            return None

    # ── Response helper ───────────────────────────────────────────────────────

    def _make_response(self, ok: bool, operation: str, path: str,
                       error: str = "") -> message_pb2.Message:
        payload: dict = {"ok": ok, "operation": operation, "path": path}
        if error:
            payload["error"] = error
        return message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName=MCP_RESPONSE),
            payload=message_pb2.Payload(structPayload=json_to_struct(payload)),
        )

    # ── File operations ───────────────────────────────────────────────────────

    def _handle_create(self, data: dict) -> tuple[bool, str]:
        """Atomically create a new file at *data['path']* with *data['content']*."""
        path    = data.get("path", "")
        content = data.get("content", "")
        enc     = data.get("encoding", "utf-8")

        target = self._safe_path(path)
        if target is None:
            return False, f"path '{path}' is outside the allowed directory"
        if target.exists():
            return False, f"file already exists: {path}"
        if len(content.encode(enc, errors="replace")) > MAX_FILE_SIZE:
            return False, f"content exceeds {MAX_FILE_SIZE} bytes"

        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            self.logger.info("create: writing to '%s'", target)
            tmp.write_text(content, encoding=enc)
            os.replace(tmp, target)
            self.logger.info("create: file written at '%s'", target)
            return True, ""
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            return False, str(exc)

    def _handle_edit(self, data: dict) -> tuple[bool, str]:
        """Atomically overwrite an existing file at *data['path']* with *data['content']*."""
        path    = data.get("path", "")
        content = data.get("content", "")
        enc     = data.get("encoding", "utf-8")

        target = self._safe_path(path)
        if target is None:
            return False, f"path '{path}' is outside the allowed directory"
        if not target.exists():
            return False, f"file does not exist: {path}"
        if len(content.encode(enc, errors="replace")) > MAX_FILE_SIZE:
            return False, f"content exceeds {MAX_FILE_SIZE} bytes"

        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            self.logger.info("edit: overwriting '%s'", target)
            tmp.write_text(content, encoding=enc)
            os.replace(tmp, target)
            self.logger.info("edit: file updated at '%s'", target)
            return True, ""
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            return False, str(exc)

    def _handle_delete(self, data: dict) -> tuple[bool, str]:
        """Remove the file at *data['path']* if it exists inside BASE_DIR."""
        path = data.get("path", "")

        target = self._safe_path(path)
        if target is None:
            return False, f"path '{path}' is outside the allowed directory"
        if not target.exists():
            return False, f"file does not exist: {path}"

        try:
            self.logger.info("delete: removing '%s'", target)
            target.unlink()
            self.logger.info("delete: file removed at '%s'", target)
            return True, ""
        except OSError as exc:
            return False, str(exc)

    # ── Hook ────────────────────────────────────────────────────────────────────────────

    def on_receive(self, data: message_pb2.Message) -> bool:
        """Dispatch incoming file operation requests to the appropriate handler."""
        operation = data.metaInfo.messageName
        payload   = struct_to_json(data.payload.structPayload)
        path      = payload.get("path", "")

        self.logger.info("op=%s  path='%s'", operation, path)

        if operation == "mcp.file.create":
            ok, error = self._handle_create(payload)
        elif operation == "mcp.file.edit":
            ok, error = self._handle_edit(payload)
        elif operation == "mcp.file.delete":
            ok, error = self._handle_delete(payload)
        else:
            self.logger.warning("Unknown operation: %s", operation)
            return True

        abs_path = self._safe_path(path)

        if ok:
            self.logger.info("op=%s  rel='%s'  abs='%s'  -> OK",
                             operation, path, abs_path)
        else:
            self.logger.warning("op=%s  rel='%s'  -> FAIL: %s", operation, path, error)

        response = self._make_response(ok, operation, path, error)
        response.metaInfo.responseToId = data.metaInfo.messageId
        self.send_data(response)
        return True


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = FileOperationClient(49998)
    try:
        client.spin_forever()
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()
