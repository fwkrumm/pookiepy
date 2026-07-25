# mcp_server example

Autonomous LLM agent demo. An LLM (via LM Studio) drives file operations and
code execution entirely through gRPC --- it never touches the filesystem directly.
The agent's task is to generate and launch a Flask "aero glass" dashboard app.

---

## Architecture

```
LlmBridgeClient ──mcp.file.*──────────────► McpGrpcServer (BaseServer, port 49998)
(LLM agent)     ──mcp.run.execute──────────►        │
                ◄─mcp.file.response─────────         │ fan-out
                ◄─mcp.run.response──────────         │
                                            ┌────────┴────────┐
                                            ▼                 ▼
                                  FileOperationClient   RunnerClient
                                  (create/edit/delete   (venv + uv +
                                   files in BASE_DIR)    run main.py)
```

The server is a plain `BaseServer`. All domain logic lives in the three clients.

---

## Clients

| File | Role |
|---|---|
| `GrpcServer.py` | Plain `BaseServer` --- routes messages, no custom logic |
| `FileOperationClient.py` | Executes `mcp.file.*` operations sandboxed to `BASE_DIR` |
| `RunnerClient.py` | Creates a venv, installs deps, runs `main.py`; reports output |
| `LlmBridgeClient.py` | Autonomous agent: queries LLM → emits tool calls → feeds results back |

## Message names

| messageName         | Direction               | Payload fields                            |
|---------------------|-------------------------|-------------------------------------------|
| `mcp.file.create`   | LLM → FileOperationClient | `path`, `content`, `encoding` (opt.)    |
| `mcp.file.edit`     | LLM → FileOperationClient | `path`, `content`, `encoding` (opt.)    |
| `mcp.file.delete`   | LLM → FileOperationClient | `path`                                  |
| `mcp.file.response` | FileOperationClient → LLM | `ok`, `operation`, `path`, `error`      |
| `mcp.run.execute`   | LLM → RunnerClient        | _(empty --- runs `main.py` in BASE_DIR)_  |
| `mcp.run.response`  | RunnerClient → LLM        | `ok`, `timed_out`, `output`             |

All messages use `payload.structPayload`.

---

## Security

- All paths are resolved with `Path.resolve()` + `relative_to(BASE_DIR)` --- `..` traversal is rejected.
- File content is capped at **1 MB**.
- Writes are atomic: temp file → `os.replace()`.
- `BASE_DIR` is `<system_tmp>/mcp_server/` --- no project files are ever touched.

---

## Requirements

- **LM Studio** running at `http://127.0.0.1:1234` with a model loaded.
  Recommended: `qwen/qwen3-coder-30b` with context ≥ 8192 tokens.
- `pip install requests` (included in `requirements_examples.txt`).

---

## How to run

```
python examples/mcp_server/run_example.py
```

`run_example.py` starts all four components in-process (server + 3 clients).
The agent loop runs autonomously and exits when it outputs `<done/>` or hits
the iteration cap (40 turns). Output files land in `<system_tmp>/mcp_server/`.

---

Alternatively, you can start each component in a separate terminal for easier observation and debugging:

Start LM Studio, load a coder model, then run `run_example.py`. The LLM writes
a Flask app via gRPC tool calls, `RunnerClient` launches it, and the agent
confirms it works.
