# interactive_streaming

Bidirectional streaming chat demo using **LM Studio** as the LLM backend.
A proxy client forwards user prompts to LM Studio and streams the response back token-by-token.

## Architecture

```
TextClient  ──lm_request──►  GrpcServer  ──lm_request──►  LMProxyClient
            ◄─lm_response_stream──────────────────────────  (HTTP → LM Studio)
```

- **GrpcServer** --- plain `BaseServer`, no custom logic.
- **LMProxyClient** --- receives `lm_request`, calls LM Studio `/v1/chat/completions` (streaming), sends back `lm_response_stream` chunks with a `done` flag.
- **TextClient** --- interactive CLI; reads user input, sends `lm_request`, prints streaming chunks as they arrive.

## Requirements

- LM Studio running locally at `http://127.0.0.1:1234` with a model loaded (default: `gemma-4e2b`).
- `pip install requests` (included in `requirements_examples.txt`).

> If LM Studio is not running, `LMProxyClient` falls back to an offline stub that echoes the prompt.

## How to run

**Option A --- two separate terminals:**

```
# Terminal 1: server + proxy together
python examples/interactive_streaming/run_server_proxy.py

# Terminal 2: interactive text UI
python examples/interactive_streaming/run_text_client.py
```

**Option B --- three separate processes:**

```
python examples/interactive_streaming/GrpcServerExample.py
python examples/interactive_streaming/LMProxyClient.py
python examples/interactive_streaming/TextClient.py
```

Type a prompt in the `TextClient` terminal and the LLM response streams back live. `exit` or `quit` to stop.

## TL;DR

Start `run_server_proxy.py`, then `run_text_client.py`. Type prompts, get streamed LLM responses. Needs LM Studio running locally.
