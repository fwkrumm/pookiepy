"""
LlmBridgeClient --- autonomous LLM agent that drives file operations via gRPC.

The LLM (OpenAI-compatible endpoint, e.g. LM Studio) receives a task and a
system prompt describing available tools.  It emits <tool>JSON</tool> blocks;
this client parses them, translates each into a gRPC Message, sends it through
the server to FileOperationClient / RunnerClient, and feeds the result back to
the LLM.  The loop continues until the LLM outputs <done/> or MAX_ITERATIONS
is reached.

Every action passes through the gRPC server --- the LLM never touches the
filesystem directly.  The BaseServer's on_receive() is therefore a natural
monitoring / gating choke point.
"""

import json
import time
import uuid
import threading

from grpchook.baseclient import BaseClient
from grpchook import message_pb2
from grpchook.tools import json_to_struct, struct_to_json
from grpchook.exceptions import ClientExit
from examples.mcp_server.FileOperationClient import MCP_RESPONSE
from examples.mcp_server.RunnerClient import RUN_REQUEST, RUN_RESPONSE
from examples.mcp_server._task import TASK, _SYSTEM_PROMPT
from examples.mcp_server._llm_utils import _TOOL_RE, _sanitize, _fix_json_strings

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    requests = None
    HTTPAdapter = None

# ── Configuration ─────────────────────────────────────────────────────────────

# LM Studio (or any OpenAI-compatible) endpoint and model.
# The active model is auto-detected from /v1/models at startup.
# Suggested model: qwen/qwen3-coder-30b  (set context length ≥ 8192 tokens)
LMSTUDIO_BASE = 'http://127.0.0.1:1234/v1'

MAX_ITERATIONS = 40   # absolute safety cap
MAX_DEAD_TURNS = 3    # consecutive turns with no tool executed → abort
LLM_TIMEOUT    = 120  # seconds --- max wait for a single LLM response
GRPC_TIMEOUT   = 180  # max wait for a gRPC tool response (uv install on first run can be slow)
MAX_CTX_TURNS  = 12   # keep at most this many assistant+user pairs in context

# Map tool names the LLM uses → gRPC message names
_TOOL_TO_MSG = {
    'create_file': 'mcp.file.create',
    'edit_file':   'mcp.file.edit',
    'delete_file': 'mcp.file.delete',
    'run_program': RUN_REQUEST,
}

# ── LlmBridgeClient ───────────────────────────────────────────────────────────

class LlmBridgeClient(BaseClient):
    """Autonomous LLM agent that drives file operations and program runs via gRPC."""

    def __init__(self, port: int,
                 lmstudio_base: str = LMSTUDIO_BASE,
                 model: str | None = None):
        self.lmstudio_base = lmstudio_base.rstrip('/')

        # Build a persistent HTTP session for LM Studio calls
        if requests is None or HTTPAdapter is None:
            raise RuntimeError('requests library is required: pip install requests')
        sess = requests.Session()
        adapter = HTTPAdapter(pool_connections=2, pool_maxsize=4)
        sess.mount('http://', adapter)
        sess.mount('https://', adapter)
        self._http = sess

        self.model = model or self._detect_model()
        self.run_succeeded = False  # set to True only when LLM emits <done/>

        # All file-op message names plus run
        provides = list(_TOOL_TO_MSG.values())
        requires = [MCP_RESPONSE, RUN_RESPONSE]

        super().__init__(
            name='LlmBridgeClient',
            port=port,
            provides=provides,
            requires=requires,
        )

    def _detect_model(self) -> str:
        """Query /v1/models and return the id of the first loaded model."""
        url = f'{self.lmstudio_base}/models'
        try:
            resp = self._http.get(url, timeout=10)
            resp.raise_for_status()
            models = resp.json().get('data', [])
            if not models:
                raise RuntimeError('No models loaded in LM Studio')
            model_id = models[0]['id']
            return model_id
        except (OSError, RuntimeError, ValueError) as exc:
            raise RuntimeError(f'Could not detect LM Studio model: {exc}') from exc

    def on_init(self):
        self.logger.info(
            'LlmBridgeClient connected --- model=%s  base=%s',
            self.model, self.lmstudio_base,
        )
        self.logger.info('Task:\n%s', TASK.strip())
        # Start agentic loop in a daemon thread so __init__ returns immediately
        threading.Thread(target=self._run_agentic_loop, daemon=True).start()

    # ── LLM call ──────────────────────────────────────────────────────────────

    def _call_llm(self, messages: list[dict]) -> str:
        """Blocking, non-streaming call to the OpenAI-compatible endpoint."""
        url = f'{self.lmstudio_base}/chat/completions'
        headers = {'Content-Type': 'application/json'}
        payload = {
            'model':       self.model,
            'messages':    messages,
            'temperature': 0.2,
            'stream':      False,
        }
        self.logger.debug('LLM call --- %d messages in context', len(messages))
        resp = self._http.post(url, json=payload, headers=headers, timeout=LLM_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if 'choices' in data and data['choices']:
            content = (data['choices'][0].get('message') or {}).get('content', '')
            return _sanitize(content)
        raise RuntimeError(f'Unexpected LLM response structure: {list(data.keys())}')

    # ── Tool parsing ──────────────────────────────────────────────────────────

    def _parse_tools(self, text: str) -> list[dict]:
        """Extract and parse all <tool>...</tool> blocks from LLM output.

        Robustness measures:
        - _fix_json_strings() only escapes control chars *inside* JSON strings,
          leaving structural whitespace between keys intact.
        - json.JSONDecoder().raw_decode() accepts a valid JSON object even when
          the LLM appended explanation text after the closing '}'.
        - We scan for the first '{' in case the LLM prefixes the JSON with prose.
        """
        tools = []
        for m in _TOOL_RE.finditer(text):
            raw = m.group(1).strip()
            sanitized = _fix_json_strings(raw)

            # Skip any leading prose and start at the first JSON object
            start = sanitized.find('{')
            if start == -1:
                self.logger.warning('No JSON object found in <tool> block:\n%r', raw[:400])
                continue

            try:
                obj, end = json.JSONDecoder().raw_decode(sanitized, start)
                if end < len(sanitized.rstrip()):
                    self.logger.debug(
                        'Trailing content after JSON ignored (%d chars): %r',
                        len(sanitized) - end,
                        sanitized[end:end + 120],
                    )
                tools.append(obj)
            except json.JSONDecodeError as exc:
                self.logger.warning(
                    'Could not parse tool JSON: %s\n  sanitized (first 500):\n%r',
                    exc, sanitized[:500],
                )
        return tools

    # ── gRPC tool execution ───────────────────────────────────────────────────

    def _execute_tool(self, tool: dict) -> str:
        """
        Translate one tool call dict into a gRPC Message, send it through the
        server, wait for the response, and return a human-readable result string
        for the LLM's next turn.
        """
        name = tool.get('name', '')
        msg_name = _TOOL_TO_MSG.get(name)
        if not msg_name:
            return f"ERROR: unknown tool '{name}'. Valid tools: {list(_TOOL_TO_MSG)}"

        # Build payload --- run_program needs no payload fields
        payload_dict: dict = {}
        if name in ('create_file', 'edit_file'):
            payload_dict = {
                'path':    tool.get('path', ''),
                'content': tool.get('content', ''),
            }
        elif name == 'delete_file':
            payload_dict = {'path': tool.get('path', '')}

        # Assign messageId now so we can log it (set_metadata won't overwrite)
        msg_id = str(uuid.uuid4())
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(
                messageName=msg_name,
                messageId=msg_id,
            ),
            payload=message_pb2.Payload(structPayload=json_to_struct(payload_dict)),
        )

        self.logger.info('tool=%s  msg=%s  id=%s', name, msg_name, msg_id[:8])
        self.send_data(msg)

        # Block until the matching response arrives
        try:
            response = self.get_data(timeout=GRPC_TIMEOUT)
        except ClientExit as exc:
            return f'ERROR waiting for tool response: {exc}'

        result = struct_to_json(response.payload.structPayload)
        return self._format_result(name, result)

    def _format_result(self, tool_name: str, result: dict) -> str:
        """Convert a raw result dict into a concise string for the LLM."""
        ok = result.get('ok', False)

        if tool_name == 'run_program':
            if ok:
                stdout = result.get('stdout', '').strip()
                stderr = result.get('stderr', '').strip()
                out = 'run_program: SUCCESS (server started, timed_out=true)\n'
                if stdout:
                    out += f'stdout:\n{stdout}\n'
                if stderr:
                    out += f'stderr (startup messages):\n{stderr}\n'
                return out
            stdout = result.get('stdout', '').strip()
            stderr = result.get('stderr', '').strip()
            out = (
                f"run_program: FAILED (exit_code={result.get('exit_code')})\n"
                f'stdout:\n{stdout}\n'
                f'stderr:\n{stderr}'
            )
            out += (
                '\n\nDo NOT call run_program again. '
                'Analyze the error above, fix the root cause using '
                'create_file or edit_file, then call run_program.'
            )
            return out
        # file operation
        path = result.get('path', '?')
        if ok:
            return f"{tool_name}: OK --- path='{path}'"
        return f"{tool_name}: FAILED --- path='{path}'  error={result.get('error','?')}"

    # ── Agentic loop ──────────────────────────────────────────────────────────

    def _trim_context(self, messages: list[dict]) -> list[dict]:
        """Keep system prompt + last MAX_CTX_TURNS assistant/user pairs."""
        system = [m for m in messages if m['role'] == 'system']
        rest   = [m for m in messages if m['role'] != 'system']
        return system + rest[-(MAX_CTX_TURNS * 2):]

    def _run_agentic_loop(self):
        """
        Main loop: LLM → parse tools → execute via gRPC → feed result back.
        Runs in its own thread.  Calls self.disconnect() when finished.

        Terminates when:
          - LLM emits <done/>
          - MAX_DEAD_TURNS consecutive turns produce no tool call (stuck)
          - MAX_ITERATIONS turns reached (absolute safety cap)
          - An unrecoverable error occurs
        """
        messages = [
            {'role': 'system', 'content': _SYSTEM_PROMPT},
            {'role': 'user',   'content': 'Please start working on the task.'},
        ]

        self.logger.info('Agentic loop starting')

        dead_turns = 0
        iteration  = 0

        while self.run_event.is_set():
            iteration += 1
            if iteration > MAX_ITERATIONS:
                self.logger.warning(
                    'Safety cap of %d iterations reached without <done/>.', MAX_ITERATIONS
                )
                break

            self.logger.info('── Iteration %d (dead_streak=%d) ──', iteration, dead_turns)

            # ── LLM call (trim context first to avoid overflow) ──
            try:
                reply = self._call_llm(self._trim_context(messages))
            except (OSError, RuntimeError, ValueError) as exc:
                self.logger.error('LLM call failed: %s', exc)
                break

            self.logger.debug('Full LLM reply:\n%s', reply)
            self.logger.info('LLM reply (%d chars):\n%s', len(reply), reply[:2000])
            messages.append({'role': 'assistant', 'content': reply})

            # ── Check for completion ──
            if '<done/>' in reply:
                self.logger.info('LLM signalled <done/> --- task complete.')
                self.run_succeeded = True
                break

            # ── Execute tool calls ──
            tools = self._parse_tools(reply)
            if not tools:
                dead_turns += 1
                self.logger.warning(
                    'No tool call found (dead streak %d / %d).',
                    dead_turns, MAX_DEAD_TURNS,
                )
                if dead_turns >= MAX_DEAD_TURNS:
                    self.logger.error(
                        'Stuck: %d consecutive turns with no tool call --- aborting.',
                        dead_turns,
                    )
                    break
                messages.append({
                    'role': 'user',
                    'content': (
                        'Your reply contained no <tool> block. '
                        'You MUST call exactly one tool or output <done/>. '
                        'Do not explain --- just act now.'
                    ),
                })
                continue

            dead_turns = 0  # reset on successful tool parse

            results: list[str] = []
            for tool in tools:
                if not self.run_event.is_set():
                    break
                result_str = self._execute_tool(tool)
                self.logger.info('Tool result: %s', result_str[:500])
                results.append(result_str)

            user_content = 'Tool results:\n' + '\n---\n'.join(results)
            messages.append({'role': 'user', 'content': user_content})

        self.logger.info('Agentic loop finished after %d iterations.', iteration)
        self.disconnect()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    client = LlmBridgeClient(49998)
    # Block until the agentic loop finishes (disconnect() clears run_event)
    try:
        while client.run_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        client.disconnect()
