"""Payload-limit sweep integration test -- client-only orchestrator.

Meaningful loopback benchmark rules used here:
1) warmup phase before timing,
2) multiple repeats per parameter pair,
3) randomized case order per repeat,
4) aggregated metrics (median and IQR),
5) explicit near-limit and over-limit checks.
"""

from __future__ import annotations

import os
import random
import socket
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from grpchook.baseclient import BaseClient, ClientConfig
from grpchook.exceptions import (
    ClientExit,
    GrpcConnectionError,
    GrpcEmpty,
    GrpcResourceExhaustedError,
)
from grpchook.tools import generate_message
from grpchook import message_pb2
from tests.integration._interface import get_args

MIB = 1024 * 1024
KIB = 1024

STREAM_NAME = "bulk_payload"
EXIT_NAME = "server-exit"
SERVER_STARTUP_WAIT_S = 1.0
SERVER_STOP_WAIT_S = 10.0
SEND_DRAIN_TIMEOUT_S = 20.0

# Keep runtime bounded for CI while still collecting useful statistics.
WARMUP_MESSAGES = 4
MEASURE_MESSAGES = 16
REPEATS = 4
RANDOM_SEED = 20260723
FAIL_CASE_TIMEOUT_S = 4.0


@dataclass(frozen=True)
class SweepCase:
    """One benchmark case in the send/receive-limit sweep."""

    max_send_bytes: int
    max_receive_bytes: int
    payload_bytes: int
    messages: int = MEASURE_MESSAGES


CASES: list[SweepCase] = [
    SweepCase(max_send_bytes=8 * MIB, max_receive_bytes=8 * MIB, payload_bytes=2 * MIB),
    SweepCase(max_send_bytes=8 * MIB, max_receive_bytes=32 * MIB, payload_bytes=2 * MIB),
    SweepCase(max_send_bytes=32 * MIB, max_receive_bytes=8 * MIB, payload_bytes=2 * MIB),
    SweepCase(max_send_bytes=32 * MIB, max_receive_bytes=32 * MIB, payload_bytes=2 * MIB),
]


@dataclass(frozen=True)
class LimitCheckCase:
    """Case that validates message-size limit behavior."""

    label: str
    max_send_bytes: int
    max_receive_bytes: int
    payload_bytes: int
    expect_success: bool


LIMIT_CHECK_CASES: list[LimitCheckCase] = [
    LimitCheckCase(
        label="near_limit_should_pass",
        max_send_bytes=8 * MIB,
        max_receive_bytes=8 * MIB,
        payload_bytes=7 * MIB,
        expect_success=True,
    ),
    LimitCheckCase(
        label="over_limit_should_fail",
        max_send_bytes=8 * MIB,
        max_receive_bytes=8 * MIB,
        payload_bytes=9 * MIB,
        expect_success=False,
    ),
]


def _build_client_options(
    max_send_bytes: int,
    max_receive_bytes: int,
) -> list[tuple[str, int | bool]]:
    """Return default client options extended with explicit message-size caps."""
    options = list(ClientConfig().grpc_options)
    options.extend([
        ("grpc.max_send_message_length", max_send_bytes),
        ("grpc.max_receive_message_length", max_receive_bytes),
    ])
    return options


def _history_e2e_ms(msg: message_pb2.Message) -> float | None:
    """Compute end-to-end wall-clock latency from history timestamps."""
    if not msg.history:
        return None

    first = msg.history[0]
    last = msg.history[-1]
    has_first = first.receiveTimestamp.seconds or first.receiveTimestamp.nanos
    has_last = last.receiveTimestamp.seconds or last.receiveTimestamp.nanos
    if not (has_first and has_last):
        return None

    delta = last.receiveTimestamp.ToDatetime() - first.receiveTimestamp.ToDatetime()
    return delta.total_seconds() * 1000


def _p95(values: list[float]) -> float:
    """Compute stable p95 for short lists."""
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=20, method="inclusive")[18]


def _iqr(values: list[float]) -> float:
    """Compute interquartile range (Q3-Q1) for at least two values."""
    if len(values) < 2:
        return 0.0
    q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return q3 - q1


def _pick_unused_port() -> int:
    """Reserve an ephemeral localhost port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _project_env() -> dict[str, str]:
    """Prepare env so subprocesses can import project packages reliably."""
    env = os.environ.copy()
    project_root = Path(__file__).resolve().parents[3]
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(project_root) + (os.pathsep + existing if existing else "")
    return env


def _start_server(max_send_bytes: int, max_receive_bytes: int, port: int) -> subprocess.Popen:
    """Start per-case server subprocess and fail fast on startup errors."""
    script_path = Path(__file__).resolve().parent / "server_payload_limits_sweep.py"
    integration_root = script_path.parent.parent
    env = _project_env()

    cmd = [
        sys.executable,
        str(script_path),
        "--port",
        str(port),
        "--max-send-message-length",
        str(max_send_bytes),
        "--max-receive-message-length",
        str(max_receive_bytes),
    ]

    proc = subprocess.Popen(  # pylint: disable=consider-using-with
        cmd,
        cwd=str(integration_root),
        env=env,
    )

    time.sleep(SERVER_STARTUP_WAIT_S)
    if proc.poll() is not None:
        raise RuntimeError(
            "Server failed to start for case "
            f"send={max_send_bytes} recv={max_receive_bytes}. "
            f"exit={proc.returncode}"
        )

    return proc


def _stop_server(proc: subprocess.Popen):
    """Terminate server process if still alive."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=SERVER_STOP_WAIT_S)
    except subprocess.TimeoutExpired:
        proc.kill()


class SenderClient(BaseClient):
    """Benchmark sender client."""

    def __init__(self, port: int, options: list[tuple[str, int | bool]], name: str):
        cfg = ClientConfig(grpc_options=options)
        super().__init__(
            port,
            name=name,
            provides=[STREAM_NAME, EXIT_NAME],
            requires=[],
            config=cfg,
        )


class ReceiverClient(BaseClient):  # pylint: disable=too-many-instance-attributes
    """Benchmark receiver client that validates payload and records timings."""

    def __init__(
        self,
        port: int,
        options: list[tuple[str, int | bool]],
        name: str,
        expected_payload: bytes,
        expected_messages: int,
    ):
        self.expected_payload = expected_payload
        self.expected_messages = expected_messages
        self.received = 0
        self.history_e2e_ms: list[float] = []
        self.done = threading.Event()

        cfg = ClientConfig(grpc_options=options)
        super().__init__(
            port,
            name=name,
            provides=[],
            requires=[STREAM_NAME],
            config=cfg,
        )

    def on_receive(self, data: message_pb2.Message) -> bool:
        if data.payload.bytePayload != self.expected_payload:
            raise AssertionError("Payload mismatch at receiver")

        self.received += 1
        e2e_ms = _history_e2e_ms(data)
        if e2e_ms is not None:
            self.history_e2e_ms.append(e2e_ms)

        if self.received >= self.expected_messages:
            self.done.set()
        return True


@dataclass
class RunResult:
    max_send_mb: int
    max_receive_mb: int
    payload_kb: int
    messages: int
    throughput_mbps: float
    history_p50_ms: float
    history_p95_ms: float


@dataclass
class AggregateResult:  # pylint: disable=too-many-instance-attributes
    max_send_mb: int
    max_receive_mb: int
    payload_kb: int
    messages: int
    repeats: int
    throughput_median_mbps: float
    throughput_iqr_mbps: float
    history_p50_median_ms: float
    history_p95_median_ms: float


@dataclass
class LimitCheckResult:
    label: str
    expected_behavior: str
    observed_behavior: str
    assertion_passed: bool
    detail: str


def _wait_for_count(receiver: ReceiverClient, count: int, timeout_s: float) -> bool:
    """Wait until receiver got at least count messages."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if receiver.received >= count:
            return True
        time.sleep(0.01)
    return False


def _wait_send_queue_drain(client: BaseClient, timeout_s: float) -> bool:
    """Wait until send_queue unfinished_tasks reaches zero within timeout.

    Uses non-blocking polling to avoid indefinite hangs in CI when the stream is
    already broken and a blocking wait_done() could stall.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if getattr(client.send_queue, "unfinished_tasks", 0) == 0:
            return True
        if not client.run_event.is_set() and getattr(client.send_queue, "unfinished_tasks", 0) > 0:
            # stream closed while tasks remain; do not spin forever
            return False
        time.sleep(0.01)
    return False


# pylint: disable=too-many-locals,too-many-branches
def _run_case_once(
    sweep_case: SweepCase,
    timeout_s: float,
    warmup_messages: int,
    run_suffix: str,
) -> RunResult:
    """Run one benchmark case once with warmup and timed section."""
    port = _pick_unused_port()
    server_proc = _start_server(sweep_case.max_send_bytes, sweep_case.max_receive_bytes, port)

    options = _build_client_options(sweep_case.max_send_bytes, sweep_case.max_receive_bytes)
    payload = b"X" * sweep_case.payload_bytes

    sender = None
    receiver = None
    spin_thread = None

    try:
        case_name = f"s{sweep_case.max_send_bytes // MIB}_r{sweep_case.max_receive_bytes // MIB}"
        total_expected = warmup_messages + sweep_case.messages

        receiver = ReceiverClient(
            port=port,
            options=options,
            name=f"payload_receiver_{case_name}_{run_suffix}",
            expected_payload=payload,
            expected_messages=total_expected,
        )
        sender = SenderClient(
            port=port,
            options=options,
            name=f"payload_sender_{case_name}_{run_suffix}",
        )

        spin_thread = threading.Thread(target=receiver.spin_forever, daemon=True)
        spin_thread.start()

        for _ in range(warmup_messages):
            sender.send_data(generate_message(STREAM_NAME, byte_payload=payload), add_history=True)
        if not _wait_send_queue_drain(sender, SEND_DRAIN_TIMEOUT_S):
            raise TimeoutError("Sender queue did not drain during warmup phase")

        if warmup_messages > 0 and not _wait_for_count(
            receiver,
            warmup_messages,
            timeout_s=max(timeout_s, 20.0),
        ):
            raise TimeoutError(
                "Warmup did not complete: "
                f"got {receiver.received}/{warmup_messages}"
            )

        warmup_history_count = len(receiver.history_e2e_ms)

        t0 = time.perf_counter()
        for _ in range(sweep_case.messages):
            sender.send_data(generate_message(STREAM_NAME, byte_payload=payload), add_history=True)
        if not _wait_send_queue_drain(sender, SEND_DRAIN_TIMEOUT_S):
            raise TimeoutError("Sender queue did not drain during timed phase")

        if not receiver.done.wait(timeout=max(timeout_s, 20.0)):
            raise TimeoutError(
                f"Receiver got {receiver.received}/{total_expected} messages "
                f"for send={sweep_case.max_send_bytes} recv={sweep_case.max_receive_bytes}"
            )
        t1 = time.perf_counter()

        elapsed_s = t1 - t0
        total_bytes = sweep_case.messages * sweep_case.payload_bytes
        throughput_mbps = total_bytes / (elapsed_s * MIB)

        timed_history = receiver.history_e2e_ms[
            warmup_history_count : warmup_history_count + sweep_case.messages
        ]
        if len(timed_history) != sweep_case.messages:
            raise RuntimeError(
                f"Expected {sweep_case.messages} timed history points, got {len(timed_history)}"
            )

        history_p50_ms = statistics.median(timed_history)
        history_p95_ms = _p95(timed_history)

        return RunResult(
            max_send_mb=sweep_case.max_send_bytes // MIB,
            max_receive_mb=sweep_case.max_receive_bytes // MIB,
            payload_kb=sweep_case.payload_bytes // KIB,
            messages=sweep_case.messages,
            throughput_mbps=throughput_mbps,
            history_p50_ms=history_p50_ms,
            history_p95_ms=history_p95_ms,
        )
    finally:
        if sender is not None:
            try:
                sender.disconnect()
            except GrpcConnectionError:
                pass
        if receiver is not None:
            try:
                receiver.disconnect()
            except GrpcConnectionError:
                pass
        if spin_thread is not None:
            spin_thread.join(timeout=5.0)
        _stop_server(server_proc)


# pylint: enable=too-many-locals,too-many-branches


def _run_limit_check(limit_case: LimitCheckCase) -> LimitCheckResult:  # pylint: disable=too-many-branches,too-many-statements
    """Validate one explicit message-size limit behavior case."""
    port = _pick_unused_port()
    server_proc = _start_server(limit_case.max_send_bytes, limit_case.max_receive_bytes, port)

    options = _build_client_options(limit_case.max_send_bytes, limit_case.max_receive_bytes)
    payload = b"Y" * limit_case.payload_bytes

    sender = None
    receiver = None

    try:
        sender = SenderClient(
            port=port,
            options=options,
            name=f"limit_sender_{limit_case.label}",
        )
        receiver = ReceiverClient(
            port=port,
            options=options,
            name=f"limit_receiver_{limit_case.label}",
            expected_payload=payload,
            expected_messages=1,
        )

        sender.send_data(generate_message(STREAM_NAME, byte_payload=payload), add_history=True)
        queue_drained = _wait_send_queue_drain(sender, SEND_DRAIN_TIMEOUT_S)

        if limit_case.expect_success:
            if not queue_drained:
                return LimitCheckResult(
                    label=limit_case.label,
                    expected_behavior="accept",
                    observed_behavior="blocked",
                    assertion_passed=False,
                    detail="sender queue did not drain before timeout",
                )

            got = False
            try:
                data = receiver.get_data(timeout=max(FAIL_CASE_TIMEOUT_S, 8.0))
                got = data.payload.bytePayload == payload
            except (GrpcConnectionError, GrpcEmpty, ClientExit):
                got = False
            if got:
                return LimitCheckResult(
                    label=limit_case.label,
                    expected_behavior="accept",
                    observed_behavior="accept",
                    assertion_passed=True,
                    detail="message delivered under configured limit",
                )
            return LimitCheckResult(
                label=limit_case.label,
                expected_behavior="accept",
                observed_behavior="blocked",
                assertion_passed=False,
                detail="message not delivered before timeout",
            )
        failure_type = "send-queue-not-drained"
        if queue_drained:
            try:
                sender.get_data(timeout=FAIL_CASE_TIMEOUT_S)
                failure_type = "none"
            except GrpcResourceExhaustedError:
                failure_type = "GrpcResourceExhaustedError"
            except GrpcConnectionError:
                failure_type = "GrpcConnectionError"
            except (GrpcEmpty, ClientExit):
                failure_type = "no-explicit-limit-error"

        delivered = False
        try:
            receiver.get_data(timeout=1.5)
            delivered = True
        except (GrpcResourceExhaustedError, GrpcConnectionError, GrpcEmpty, ClientExit):
            delivered = False

        passed = (not delivered) and failure_type != "none"
        observed_behavior = "blocked" if not delivered else "accept"
        return LimitCheckResult(
            label=limit_case.label,
            expected_behavior="blocked",
            observed_behavior=observed_behavior,
            assertion_passed=passed,
            detail=(
                f"failure_type={failure_type}, delivered={delivered}, "
                f"payload_MB={limit_case.payload_bytes / MIB:.1f}"
            ),
        )
    finally:
        if sender is not None:
            try:
                sender.disconnect()
            except GrpcConnectionError:
                pass
        if receiver is not None:
            try:
                receiver.disconnect()
            except GrpcConnectionError:
                pass
        _stop_server(server_proc)


def _aggregate_results(
    cases: list[SweepCase],
    run_results: dict[tuple[int, int, int, int], list[RunResult]],
) -> list[AggregateResult]:
    """Aggregate repeated runs per case using robust statistics."""
    aggregated: list[AggregateResult] = []

    for case in cases:
        key = (
            case.max_send_bytes,
            case.max_receive_bytes,
            case.payload_bytes,
            case.messages,
        )
        runs = run_results.get(key, [])
        if not runs:
            continue

        throughput = [r.throughput_mbps for r in runs]
        hist_p50 = [r.history_p50_ms for r in runs]
        hist_p95 = [r.history_p95_ms for r in runs]

        aggregated.append(
            AggregateResult(
                max_send_mb=case.max_send_bytes // MIB,
                max_receive_mb=case.max_receive_bytes // MIB,
                payload_kb=case.payload_bytes // KIB,
                messages=case.messages,
                repeats=len(runs),
                throughput_median_mbps=statistics.median(throughput),
                throughput_iqr_mbps=_iqr(throughput),
                history_p50_median_ms=statistics.median(hist_p50),
                history_p95_median_ms=statistics.median(hist_p95),
            )
        )

    return aggregated


def _format_aggregate_table(results: list[AggregateResult]) -> str:
    """Render aggregate results as fixed-width text table."""
    headers = [
        "send_MB",
        "recv_MB",
        "payload_KB",
        "msgs",
        "repeats",
        "thr_median_MBps",
        "thr_iqr_MBps",
        "hist_e2e_p50_med_ms",
        "hist_e2e_p95_med_ms",
    ]

    rows = [
        [
            str(r.max_send_mb),
            str(r.max_receive_mb),
            str(r.payload_kb),
            str(r.messages),
            str(r.repeats),
            f"{r.throughput_median_mbps:.2f}",
            f"{r.throughput_iqr_mbps:.2f}",
            f"{r.history_p50_median_ms:.3f}",
            f"{r.history_p95_median_ms:.3f}",
        ]
        for r in results
    ]

    widths = [len(h) for h in headers]
    for row in rows:
        widths = [max(w, len(cell)) for w, cell in zip(widths, row)]

    def _line(cells: list[str]) -> str:
        return " | ".join(cell.ljust(width) for cell, width in zip(cells, widths))

    separator = "-+-".join("-" * width for width in widths)
    lines = [_line(headers), separator]
    lines.extend(_line(row) for row in rows)
    return "\n".join(lines)


def _format_limit_check_table(results: list[LimitCheckResult]) -> str:
    """Render explicit limit-behavior checks as fixed-width text table."""
    headers = [
        "case",
        "expected_behavior",
        "observed_behavior",
        "assertion_passed",
        "detail",
    ]
    rows = [
        [
            r.label,
            r.expected_behavior,
            r.observed_behavior,
            "yes" if r.assertion_passed else "no",
            r.detail,
        ]
        for r in results
    ]

    widths = [len(h) for h in headers]
    for row in rows:
        widths = [max(w, len(cell)) for w, cell in zip(widths, row)]

    def _line(cells: list[str]) -> str:
        return " | ".join(cell.ljust(width) for cell, width in zip(cells, widths))

    separator = "-+-".join("-" * width for width in widths)
    lines = [_line(headers), separator]
    lines.extend(_line(row) for row in rows)
    return "\n".join(lines)


def _print_metric_legend() -> None:
    """Print metric definitions and benchmark methodology."""
    print("Benchmark method:")
    print(f"- warmup messages per run: {WARMUP_MESSAGES}")
    print(f"- timed messages per run: {MEASURE_MESSAGES}")
    print(f"- repeats per case: {REPEATS}")
    print("- case order randomized each repeat (seeded) to reduce order bias")
    print("")
    print("Metric definitions:")
    print("- thr_median_MBps: median payload throughput over repeated runs")
    print("- thr_iqr_MBps: interquartile range of throughput (Q3-Q1)")
    print("- hist_e2e_p50_med_ms: median of per-run p50 end-to-end latency")
    print("- hist_e2e_p95_med_ms: median of per-run p95 end-to-end latency")
    print("- End-to-end latency uses first->last receiveTimestamp in message history")


if __name__ == "__main__":
    args = get_args("Payload-limit sweep benchmark on loopback")

    print("Payload-limit sweep: meaningful loopback comparison mode")
    print(f"Randomization seed: {RANDOM_SEED}")

    rng = random.Random(RANDOM_SEED)
    collected_results: dict[tuple[int, int, int, int], list[RunResult]] = {}

    for repeat in range(1, REPEATS + 1):
        run_order = CASES.copy()
        rng.shuffle(run_order)

        print(f"\n[repeat {repeat}/{REPEATS}] randomized run order:")
        for idx, run_case in enumerate(run_order, start=1):
            print(
                f"  ({idx}/{len(run_order)}) "
                f"send={run_case.max_send_bytes // MIB}MB "
                f"recv={run_case.max_receive_bytes // MIB}MB "
                f"payload={run_case.payload_bytes // KIB}KB "
                f"timed_messages={run_case.messages}"
            )

            case_run_suffix = f"rep{repeat}_ord{idx}"
            result = _run_case_once(
                run_case,
                timeout_s=args.timeout,
                warmup_messages=WARMUP_MESSAGES,
                run_suffix=case_run_suffix,
            )

            case_key = (
                run_case.max_send_bytes,
                run_case.max_receive_bytes,
                run_case.payload_bytes,
                run_case.messages,
            )
            collected_results.setdefault(case_key, []).append(result)

    aggregate_results = _aggregate_results(CASES, collected_results)

    limit_results: list[LimitCheckResult] = []
    print("\nRunning explicit limit-enforcement checks:")
    for limit_check_case in LIMIT_CHECK_CASES:
        print(
            f"- {limit_check_case.label}: send={limit_check_case.max_send_bytes // MIB}MB "
            f"recv={limit_check_case.max_receive_bytes // MIB}MB "
            f"payload={limit_check_case.payload_bytes / MIB:.1f}MB"
        )
        limit_results.append(_run_limit_check(limit_check_case))

    print("\nPayload-limit sweep summary (loopback, relative metrics):")
    _print_metric_legend()
    print(_format_aggregate_table(aggregate_results))

    print("\nLimit enforcement checks:")
    print(_format_limit_check_table(limit_results))

    failed_checks = [r for r in limit_results if not r.assertion_passed]
    if failed_checks:
        raise SystemExit(
            "Limit enforcement check failed: "
            + ", ".join(r.label for r in failed_checks)
        )
