# pookiepy

**pookiepy** is a Python framework for building asynchronous gRPC bidirectional-streaming services. Subclass `BaseServer` and `BaseClient`, override the hooks you need --- the framework handles all gRPC plumbing.

[![PyPI](https://img.shields.io/pypi/v/pookiepy)](https://pypi.org/project/pookiepy/)
[![Python](https://img.shields.io/pypi/pyversions/pookiepy)](https://pypi.org/project/pookiepy/)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue)](https://github.com/fwkrumm/pookiepy/blob/master/LICENSE.txt)

> **Status: Work in Progress.**
> The project is open source and will remain open source.
> Feedback is highly appreciated. Questions? Please create a [GitHub issue](https://github.com/fwkrumm/pookiepy/issues).
> Treat with caution. If you depend on it, **pin your version**.
> Semantic versioning will only begin with the first official release, starting at version **1.0.0**.
> Note that until and including version 0.0.11 the project was named **grpchook**.

---

## Table of Contents

- [Disclaimer](#disclaimer)
- [When to Use and When Not to Use pookiepy](#when-to-use-and-when-not-to-use-pookiepy)
- [Requirements](#requirements)
- [Installation](#installation)
  - [From PyPI](#from-pypi)
  - [From Source](#from-source)
- [Quick Start](#quick-start)
- [Parameters](#parameters)
- [Minimal Examples](#minimal-examples)
- [Examples](#examples)
- [Testing](#testing)
- [Extend default Configuration](#extend-default-configuration)
- [Regenerating the gRPC Interface](#regenerating-the-grpc-interface)
- [ToDos and Roadmap](#todos-and-roadmap)
- [Known Issues and Troubleshooting](#known-issues-and-troubleshooting)
- [Contributing](#contributing)
- [Compatibility Adjustments](#compatibility-adjustments)
- [License](#license)
- [Release History](#release-history)

---
<a name="disclaimer"></a>
<a id="disclaimer"></a>

## Disclaimer

Core architecture and design were created by a human developer. AI was used extensively for unit and integration test creation, examples, documentation, refinements, and selected code sections. Core logic was human-reviewed, but the full test suite has not been fully audited --- AI-introduced oversights may still exist. Please report any issues you find.

This software is provided **"as is"**, without warranty of any kind. The developer is not responsible for any damage, data loss, security vulnerabilities, or other issues that may arise from using this software. **You use it at your own risk.** See [LICENSE.txt](https://github.com/fwkrumm/pookiepy/blob/master/LICENSE.txt) for the full BSD 3-Clause terms.


---
<a name="when-to-use-and-when-not-to-use-pookiepy"></a>
<a id="when-to-use-and-when-not-to-use-pookiepy"></a>
## When to Use and When Not to Use pookiepy

### When to Use pookiepy
- You need a simple, Python-based gRPC bidirectional streaming server and client.
- You want a data exchange blueprint for developers or AI agents to build on top of.
- You want a framework that can be extended with custom hooks for specific events.
- You want to distribute clients to many different machines (e.g. voice recorder, voice to text, text to LLM, and vice versa until the final response is replayed)

  **Example --- four clients on four machines, all routed through one pookiepy server:**

  > 💡 Diagram requires the [Markdown Preview Mermaid Support](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) extension to render in VS Code.

  ```mermaid
  flowchart LR
      subgraph M1["📦 Machine 1"]
          VR["🎤 Voice Recorder"]
      end
      subgraph M2["📦 Machine 2"]
          STT["📝 Speech-to-Text"]
      end
      subgraph M3["📦 Machine 3"]
          LLM["🤖 LLM Processor"]
      end
      subgraph M4["📦 Machine 4"]
          RP["🔊 Voice Replay"]
      end

      SRV(["⚙️ pookiepy Server"])

      VR  -->|"① audio"| SRV
      SRV -->|"① audio"| STT
      STT -->|"② transcript"| SRV
      SRV -->|"② transcript"| LLM
      LLM -->|"③ llm_response"| SRV
      SRV -->|"③ llm_response"| RP
  ```

### When Not to Use pookiepy
- When you need a very large number of clients; the threading model may introduce overhead.
- When you need direct peer-to-peer communication without a server intermediary; pookiepy routes all messages through a central server.
- You want a framework that supports multiple programming languages out of the box; pookiepy is (currently) Python-only.

---
<a name="requirements"></a>
<a id="requirements"></a>

## Requirements

- Python 3.10 or later
- A dedicated virtual environment is **strongly recommended** --- gRPC version conflicts with other packages are common when using pookiepy.

---
<a name="installation"></a>
<a id="installation"></a>

## Installation

<a name="from-pypi"></a>
<a id="from-pypi"></a>
### From PyPI

```bash
pip install pookiepy
```

<a name="from-source"></a>
<a id="from-source"></a>
### From Source

```bash
git clone https://github.com/fwkrumm/pookiepy.git
cd pookiepy
pip install -e .
```

---
<a name="quick-start"></a>
<a id="quick-start"></a>

## Quick Start

With pookiepy installed, save these two files in the same directory. Each client
sends a greeting when it joins, then keeps listening. The server forwards each
greeting to all other subscribed clients, never back to its sender.

### 1. Start a plain server

**`server.py`**

```python
from pookiepy.baseserver import BaseServer


if __name__ == "__main__":
    BaseServer(port=50051).serve_forever()
```

In terminal 1, run:

```bash
python server.py
```

Leave it running. No subclass or hook is needed for default message forwarding.

### 2. Connect clients

**`client.py`**

```python
from pookiepy.baseclient import BaseClient
from pookiepy.exceptions import ClientExit
from pookiepy.tools import generate_message


client = BaseClient(
    port=50051,
    name=input("Your name: "),
    provides=["greeting"],
    requires=["greeting"],
)
try:
    client.send_data(
        generate_message("greeting", byte_payload=f"Hello from {client.name}!".encode())
    )
    print("Listening for other clients. Press Ctrl+C to stop.")
    while True:
        message = client.get_data()
        print(message.payload.bytePayload.decode())
except (KeyboardInterrupt, ClientExit):
    print("Client stopped.")
finally:
    client.disconnect()
```

In terminal 2, run this and enter `Alice` when prompted:

```bash
python client.py
```

In terminal 3, run the same command and enter `Bob`:

```bash
python client.py
```

Alice's terminal now shows this greeting, alongside connection logs:

```text
Hello from Bob!
```

Open another terminal, run `python client.py`, and enter `Charlie`. Both Alice
and Bob now see `Hello from Charlie!`. Repeat with more clients as needed
(up to the server's worker limit).

Each client sends **one** greeting on startup. Messages are not replayed, so Bob
does not see Alice's earlier greeting, and nobody sees their own. The first client
will simply wait until another joins.

`provides=["greeting"]` allows sending that message name;
`requires=["greeting"]` subscribes to it. `get_data()` has no timeout here, so all
clients keep listening until stopped. Press **Ctrl+C** in a client terminal to
disconnect it cleanly; stop the server with Ctrl+C when finished.

### Generate starter files

To scaffold your own project instead, run this from its root:

```bash
python -m pookiepy --generate-skeletons
```

This creates `server_skeleton.py` and `client_skeleton.py`. They are starting
points to adapt, not the ready-to-run greeting demo above. For a custom protobuf schema, see
the [custom interface guide](pookiepy/assets/skills/pookiepy/references/API_REFERENCE.md#custom-interface).

For more examples and the development workflow, see the
[pookiepy skill](pookiepy/assets/skills/pookiepy/SKILL.md) and
[API reference](pookiepy/assets/skills/pookiepy/references/API_REFERENCE.md).

---

<a name="parameters"></a>
<a id="parameters"></a>

## Parameters

Run `python -m pookiepy --help` for all options. Generation writes under the current
working directory; run these commands from the intended project root.

```text
python -m pookiepy --generate                           # skeletons + all skills
python -m pookiepy --generate-skeletons                 # server + client only
python -m pookiepy --generate-server                    # server only
python -m pookiepy --generate-client                    # client only
python -m pookiepy --install-skills                     # all four skill targets
python -m pookiepy --install-skills-github              # GitHub Copilot only
python -m pookiepy --install-skills-claude              # Claude Code only
python -m pookiepy --install-skills-cursor              # Cursor only
python -m pookiepy --install-skills-codex               # Codex only
python -m pookiepy --generate-interface                 # message.proto + instructions
python -m pookiepy --generate-interface-with-skeletons  # proto + matching skeletons
```

### Agent skill installation

| Command | Project-relative destination |
|---|---|
| `--install-skills-github` | `.github/skills/pookiepy/` |
| `--install-skills-claude` | `.claude/skills/pookiepy/` |
| `--install-skills-cursor` | `.cursor/skills/pookiepy/` |
| `--install-skills-codex` | `.agents/skills/pookiepy/` |

`--install-skills` installs all four targets. Each contains
`SKILL.md` and `references/API_REFERENCE.md`. Per-tool flags may be combined, for
example `python -m pookiepy --install-skills-github --install-skills-claude`.

The CLI creates required parent directories, without IDE detection, network
access, user-global writes, Git-root discovery, or IDE settings changes. No empty
optional skill directories are created. Existing identical assets are left
untouched; conflicting files, non-directory parents, or destination symlinks
produce a nonzero exit before any skill writes. Review and move conflicts before
retrying. Runtime write failures may leave a partial install; fix the filesystem
problem and rerun. Scaffold files retain their existing skip-without-overwrite
behavior. With `--generate`, skill preflight runs before skeleton generation;
`--generate` still takes precedence over other scaffold-generation flags.

**Compatibility:** the former guide-export flag remains a hidden deprecated
alias that warns and installs all skills; it no longer exports a root-level guide.
See the [migration notes](docs/required_adjustments/0.0.22.md#agent-skill-cli-migration).

**Discovery assumptions:** these are project-scoped locations documented by
[GitHub Copilot](https://docs.github.com/en/copilot/concepts/customizations/agent-skills),
[Claude Code](https://code.claude.com/docs/en/skills),
[Cursor](https://cursor.com/docs/skills), and
[Codex](https://learn.chatgpt.com/docs/build-skills).
Some tools recognize several roots and may list duplicates; prefer a per-tool flag
when using one environment. Tool version, active harness, and organizational
policy govern discovery. Start a fresh session and verify the skill in the tool's
skill picker, then try a pookiepy-specific prompt. Installation alone does not
guarantee activation. Commit project skill files for remote/cloud agents that need
them; local user files are not automatically available remotely.

---
<a name="minimal-examples"></a>
<a id="minimal-examples"></a>

## Minimal Examples

### Ultra-minimal --- no subclassing required

The simplest possible working setup: start a server, connect two clients, exchange a message.
Everything runs in a single script --- no subclassing or hook overrides needed.

```python
# example_minimal.py
import threading
from pookiepy.baseserver import BaseServer
from pookiepy.baseclient import BaseClient
from pookiepy.tools import generate_message

# start the server in a background thread
server = BaseServer(port=50051, name="server")
threading.Thread(target=server.serve_forever, daemon=True).start()

# both clients declare the same channel name
# fan-out skips the sender, so client_b receives what client_a sends
client_a = BaseClient(port=50051, name="A", provides=["ping"], requires=["ping"])
client_b = BaseClient(port=50051, name="B", provides=["ping"], requires=["ping"])

client_a.send_data(generate_message("ping", byte_payload=b"hello client B"))
client_b.send_data(generate_message("ping", byte_payload=b"hello client A"))

msg_b = client_b.get_data(timeout=5.0)
msg_a = client_a.get_data(timeout=5.0)

# b"hello client A"
client_a.logger.info(msg_a.payload.bytePayload)

# b"hello client B"
client_b.logger.info(msg_b.payload.bytePayload)

client_a.disconnect()
client_b.disconnect()
server.shutdown()
```

### Request / response --- subclass with hooks

For real workloads, subclass `BaseServer` to control routing and `BaseClient` to react to messages
via the `on_receive` hook.

Design note (important): request/response in pookiepy is intentionally minimalistic.
There is no dedicated `request()` helper in the core API by default; correlation is done via
`messageId` and `responseToId` in normal hook/polling flow. This keeps the framework lean,
transparent, and robust for mixed traffic patterns.

Need full reference flow?
- `tests/integration/request_response/server_request_response.py`
- `tests/integration/request_response/clients_request_response.py`

**`server.py`**

```python
from pookiepy.baseserver import BaseServer, Peer
from pookiepy.tools import generate_message
import pookiepy.message_pb2 as pb2


class EchoServer(BaseServer):
    def __init__(self):
        super().__init__(port=50051, name="echo-server")

    def on_receive(self, peer: Peer, request: pb2.PookieMessage) -> bool:
        if request.metaInfo.messageName == "request":
            reply = generate_message("response", byte_payload=request.payload.bytePayload)
            self._data_register.add_data_for_message_name(
                peer.client_id, "response", reply,
                target_client_id=peer.client_id,   # unicast back to sender
            )
            return False   # skip default fan-out; routing handled above
        return True


EchoServer().serve_forever()
```

**`client.py`**

```python
from pookiepy.baseclient import BaseClient
from pookiepy.tools import generate_message
from pookiepy.exceptions import ClientExit, GrpcEmpty
import pookiepy.message_pb2 as pb2


class EchoClient(BaseClient):
    def __init__(self):
        super().__init__(port=50051, name="echo-client",
                         provides=["request"], requires=["response"])

    def on_receive(self, data: pb2.PookieMessage):
        print(f"Server replied: {data.payload.bytePayload.decode()}")


client = EchoClient()
client.send_data(generate_message("request", byte_payload=b"hello, pookiepy!"))
try:
    client.spin(timeout=5.0)   # calls on_receive() per message
except (ClientExit, GrpcEmpty):
    pass   # timeout/disconnect
client.disconnect()
```

Run the server first, then the client:

```bash
# terminal 1
python server.py

# terminal 2
python client.py
```

---
<a name="examples"></a>
<a id="examples"></a>

## Examples

Runnable examples are available in two locations:

- `examples/` --- self-contained, scenario-focused examples
- `tests/integration/` --- integration test scenarios covering a broad range of use cases

Run them on a machine with adequate resources; some scenarios are resource-intensive.

---
<a name="testing"></a>
<a id="testing"></a>

## Testing

Install dev dependencies and run the unit tests:

```bash
pip install -r requirements_dev.txt
python -m unittest discover -s tests
```

Integration tests are in `tests/integration/` and can be run via:

```bash
python tests/integration/run_integration_tests.py
```

---
<a name="extend-default-configuration"></a>
<a id="extend-default-configuration"></a>

## Extend default Configuration

Example for a client to use the default configuration but disable proxy forwarding:


```python
from pookiepy.baseclient import BaseClient, ClientConfig

class TestClient(BaseClient):
    def __init__(self):
        config = ClientConfig()
        config.grpc_options += [("grpc.enable_http_proxy", 0)]
        super().__init__(port=50051, name="test-client", config=config)

```

---
<a name="regenerating-the-grpc-interface"></a>
<a id="regenerating-the-grpc-interface"></a>


## Regenerating the gRPC Interface

If you modify `pookiepy/message.proto` after cloning the repository, regenerate the Python bindings with:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --pyi_out=. pookiepy/message.proto
```

Note that all clients which connect to a server should use the same proto layout. For runtime checking, set matching manual schema labels on both sides, for example `ClientConfig(schema_version="chat-v1")` and `ServerConfig(schema_version="chat-v1")`. If both values stay empty, the connection is allowed and the server logs a warning that schema validation could not run. The different signals for the clients must be used in substructures:

```proto
message Payload {
    // For client A
    SomeTypeA payloadClientA = 1;

	// For client B
    SomeTypeB payloadClientB = 2;

    ...
}

```

---
<a name="todos-and-roadmap"></a>
<a id="todos-and-roadmap"></a>

## ToDos and Roadmap

### General
- can we increase performance/speed by using multiplexing?
- more tests for examples
- reduce code repetition in workflow files

### Performance & Stability
- Evaluate replacing the threading model with `asyncio` if the performance gain justifies the API tradeoff.
- Verify behavior when connections are interrupted mid-stream; ensure no ghost threads or queue deadlocks occur.

### Planned Features
- Multi-language client example (e.g., C++ or Rust).

---
<a name="known-issues-and-troubleshooting"></a>
<a id="known-issues-and-troubleshooting"></a>

## Known Issues and Troubleshooting

TBD

---
<a name="contributing"></a>
<a id="contributing"></a>

## Contributing

Contributions are welcome. Please open an issue first for major changes so the approach can be discussed. For bug fixes and small improvements, a pull request is sufficient.

---
<a name="compatibility-adjustments"></a>
<a id="compatibility-adjustments"></a>

## Compatibility Adjustments

Version-specific migration notes for breaking API and behavior changes are maintained in [docs/required_adjustments/](docs/required_adjustments/).

---
<a name="license"></a>
<a id="license"></a>

## License

BSD 3-Clause --- see [LICENSE.txt](https://github.com/fwkrumm/pookiepy/blob/master/LICENSE.txt).

---
<a name="release-history"></a>
<a id="release-history"></a>

## Release History


| Version / Git Tag on Master | Description |
|----------------------------|-------------|
| 0.0.1                      | Unpublished. |
| 0.0.2                      | Initial public release. |
| 0.0.3                      | Add ms timestamp resolution to log output and minor adjustments to readme. |
| 0.0.4                      | Add executor for server and wait for shutdown. |
| 0.0.5                      | Fix readme on pypi page. |
| 0.0.6                      | Update how-to markdown to include custom interface description. |
| 0.0.7                      | Fix race condition which allowed clients to put data before welcome message. |
| 0.0.8                      | Fix link in readme for pypi page. |
| 0.0.9                      | Minor performance adjustments, adding compression parameter, changing logging parameters. |
| 0.0.10                     | Add on_data_yield hook, added responseToId field, more explicit logging for startup. |
| 0.0.11                     | Add deprecation warning because of project rename. |
| 0.0.12                     | Project renamed to pookiepy. |
| 0.0.13                     | Revert publish via token and add note concerning old project name. |
| 0.0.14                     | Change schema version to manual setting because of incompatibilities between different languages. |
| 0.0.15                     | Add exception for custom interface mismatch, add `__version__` to `__all__`. |
| 0.0.16                     | Change client spin control flow and server delivery-result APIs; add queue-growth configuration. |
| 0.0.17                     | Rename non-ambiguous message name. Simplify custom interface usage. Minor improvements to logs and comments. added voice client to interactive streaming example. added workflow for skeletons. added threading backend for timer. |
| 0.0.18                     | Correct on_init position, hopefully fix publish pipeline, correct on_receive hook position. |
| 0.0.19                     | Minor bug fixes and improvements to the publish pipeline. |
| 0.0.20                     | Harden stream lifecycle and shutdown handling; reject excess connections promptly and improve queue-drop reporting. |
| 0.0.21                     | Replace bespoke logging/timing with jacl4py and SimPyTick; remove local logger/timer modules and duplicate tests. Direct package imports replace old paths; logger handler levels, file paths, timer compensation and worker-error handling follow package APIs. See [migration notes](docs/required_adjustments/0.0.21.md). |
| 0.0.22                     | Replace guide export with an agent skill and API reference; add `--install-skills` and per-tool installers for GitHub Copilot, Claude Code, Cursor, and Codex. Validate packaged skills and runnable examples in CLI smoke checks. See [migration notes](docs/required_adjustments/0.0.22.md). |
| 0.0.23                     | Add a runnable echo walkthrough to the README Quick Start, including run instructions and expected output. |
| 0.0.24                     | Simplify Quick Start to a plain server and multiple persistent clients using default fan-out, without self-notification; document Ctrl+C cleanup and expected output. No upgrade adjustments required; see [upgrade notes](docs/required_adjustments/0.0.24.md). |
