# pookiepy

**pookiepy** is a Python framework for building asynchronous gRPC bidirectional-streaming services. Subclass `BaseServer` and `BaseClient`, override the hooks you need --- the framework handles all gRPC plumbing.

[![PyPI](https://img.shields.io/pypi/v/pookiepy)](https://pypi.org/project/pookiepy/)
[![Python](https://img.shields.io/pypi/pyversions/pookiepy)](https://pypi.org/project/pookiepy/)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue)](https://github.com/fwkrumm/pookiepy/blob/master/LICENSE.txt)

> **Status: Work in Progress.**
> The project is open source and will remain open source.
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

Refer to [HOW_TO.md](pookiepy/assets/HOW_TO.md) for the full API reference and code examples.
Alternatively run

```bash
python -m pookiepy --generate-skeletons
```

to generate a very basic server and client skeleton in the current directory.
Use

```bash
python -m pookiepy --generate-interface-with-skeletons
```

to generate the skeletons along with a copy of the `message.proto` interface file in the current directory to modify which is then used by the skeletons.

---

<a name="parameters"></a>
<a id="parameters"></a>

## Parameters

You can print the following text via `python -m pookiepy --help`:

```bash
usage: python -m pookiepy [-h] [--generate] [--generate-skeletons] [--generate-server] [--generate-client] [--generate-how-to] [--generate-interface] [--generate-interface-with-skeletons]

pookiepy scaffolding tool.

Generates skeleton server/client files and the HOW_TO reference
document into the current working directory.

options:
  -h, --help            show this help message and exit
  --generate            Generate server_skeleton.py, client_skeleton.py, and HOW_TO.md
  --generate-skeletons  Generate server_skeleton.py and client_skeleton.py
  --generate-server     Generate server_skeleton.py only
  --generate-client     Generate client_skeleton.py only
  --generate-how-to     Copy HOW_TO.md into the current directory
  --generate-interface  Copy message.proto into the current directory and print customisation instructions
  --generate-interface-with-skeletons
                        Copy message.proto and write server_skeleton.py + client_skeleton.py that load the custom interface at startup via compile_and_register()

examples:
  python -m pookiepy --generate                          # skeleton + HOW_TO
  python -m pookiepy --generate-skeletons                  # server + client only
  python -m pookiepy --generate-server                   # server only
  python -m pookiepy --generate-client                   # client only
  python -m pookiepy --generate-how-to                   # HOW_TO.md only
  python -m pookiepy --generate-interface                # message.proto + instructions
  python -m pookiepy --generate-interface-with-skeletons  # proto + matching skeletons
```




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

client_a.send_data(generate_message("ping", byte_payload=b"hello"))

msg = client_b.get_data(timeout=5.0)
client_a.logger.info(msg.payload.bytePayload)   # b"hello"
client_b.logger.info(msg.payload.bytePayload)   # b"hello"

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

    def on_receive(self, peer: Peer, request: pb2.Message) -> bool:
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
import pookiepy.message_pb2 as pb2


class EchoClient(BaseClient):
    def __init__(self):
        super().__init__(port=50051, name="echo-client",
                         provides=["request"], requires=["response"])

    def on_receive(self, data: pb2.Message):
        print(f"Server replied: {data.payload.bytePayload.decode()}")


client = EchoClient()
client.send_data(generate_message("request", byte_payload=b"hello, pookiepy!"))
client.spin(timeout=5.0)   # calls on_receive() per message; returns on timeout/disconnect
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

Note that all clients which connect to a server have to use the same proto schema version i.e. the same proto file. The different signals for the clients must be used in substructures:

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
| 0.0.10                     | Add on_data_yield hook, added responseToId field, more explicit logging for startup |
| 0.0.11                     | Add deprecation warning because of project rename. |
| 0.0.12                     | Project renamed to pookiepy |
| 0.0.13                     | Revert publish via token and add note concerning old project name |
| 0.0.14                     | Minor bug fixes |
