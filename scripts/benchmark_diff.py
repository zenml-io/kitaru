"""Benchmark the current cohort diff implementation for issue #525.

Run deterministic scenarios with::

    uv run scripts/benchmark_diff.py deterministic --output design/diff-benchmark.json

Run end-to-end scenarios on the active local/default stack with::

    uv run scripts/benchmark_diff.py local --output design/diff-benchmark-local.json
"""

import argparse
import json
import tracemalloc
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from kitaru import checkpoint, flow
from kitaru._client._models import (
    ArtifactRef,
    CheckpointCall,
    Execution,
    ExecutionStatus,
)
from kitaru.client import KitaruClient
from kitaru.diff import diff, diff_cohort


@dataclass
class Counters:
    """Operations performed while producing one benchmark result."""

    execution_list_calls: int = 0
    execution_get_calls: int = 0
    backend_pages_read: int = 0
    candidate_rows_read: int = 0
    artifact_hydrations: int = 0
    artifact_loads: int = 0


@dataclass
class Measurement:
    """Serializable result for one benchmark scenario."""

    scenario: str
    mode: str
    originals: int
    replays: int
    unrelated: int
    artifact_bytes: int
    explicit_ids: bool
    elapsed_seconds: float
    peak_memory_bytes: int
    counters: Counters
    warning_count: int


class _FakeArtifact:
    def __init__(self, value: str, counters: Counters) -> None:
        self._value = value
        self._counters = counters

    def load(self) -> str:
        self._counters.artifact_loads += 1
        return self._value


class _FakeExecutionsAPI:
    def __init__(
        self,
        executions: dict[str, Execution],
        candidates: list[Execution],
        counters: Counters,
    ) -> None:
        self._executions = executions
        self._candidates = candidates
        self._counters = counters

    def get(self, exec_id: str) -> Execution:
        self._counters.execution_get_calls += 1
        return self._executions[exec_id]

    def _list_replays_for_originals(
        self,
        *,
        original_exec_ids: Sequence[str],
        expected_flow_name: str | None,
        limit: int,
    ) -> tuple[list[Execution], bool]:
        self._counters.execution_list_calls += 1
        original_ids = set(original_exec_ids)
        scanned = self._candidates[: limit + 1]
        matching = [
            item
            for item in scanned[:limit]
            if item.original_exec_id in original_ids
            and item.flow_name == expected_flow_name
        ]
        page_size = min(100, limit)
        self._counters.backend_pages_read += (
            ceil(len(scanned) / page_size) if scanned else 1
        )
        self._counters.candidate_rows_read += len(scanned)
        return matching, len(scanned) > limit


class _FakeClient:
    def __init__(
        self,
        executions: dict[str, Execution],
        candidates: list[Execution],
        artifacts: dict[str, str],
        counters: Counters,
    ) -> None:
        self.executions = _FakeExecutionsAPI(executions, candidates, counters)
        self._artifacts = artifacts
        self._counters = counters

    def _get_artifact_version(
        self, artifact_id: str, *, hydrate: bool
    ) -> _FakeArtifact:
        assert hydrate
        self._counters.artifact_hydrations += 1
        return _FakeArtifact(self._artifacts[artifact_id], self._counters)


def _artifact_ref(artifact_id: str, client: Any) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        name="output",
        kind="str",
        save_type="str",
        producing_call=None,
        metadata={},
        _client=client,
    )


def _execution(
    exec_id: str,
    *,
    original_exec_id: str | None,
    artifact_id: str,
    original_call_id: str | None,
    client: Any,
) -> Execution:
    now = datetime(2026, 7, 10, tzinfo=UTC)
    checkpoint_call = CheckpointCall(
        call_id=f"call-{exec_id}",
        name="payload",
        status=ExecutionStatus.COMPLETED,
        started_at=now,
        ended_at=now,
        metadata={},
        original_call_id=original_call_id,
        parent_call_ids=[],
        failure=None,
        attempts=[],
        artifacts=[_artifact_ref(artifact_id, client)],
        checkpoint_type="checkpoint",
    )
    return Execution(
        exec_id=exec_id,
        flow_id="flow-benchmark",
        flow_name="issue_525_benchmark_flow",
        status=ExecutionStatus.COMPLETED,
        started_at=now,
        ended_at=now,
        stack_name="default",
        metadata={},
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=original_exec_id,
        checkpoints=[checkpoint_call],
        artifacts=[],
        _client=client,
        project_name="default",
    )


def _fake_fixture(
    *, originals: int, replays_per_original: int, unrelated: int, artifact_bytes: int
) -> tuple[_FakeClient, list[str], dict[str, list[str]], Counters]:
    counters = Counters()
    artifacts: dict[str, str] = {}
    executions: dict[str, Execution] = {}
    candidates: list[Execution] = []
    replay_ids: dict[str, list[str]] = {}
    placeholder = SimpleNamespace()

    for original_index in range(originals):
        original_id = f"original-{original_index}"
        artifact_id = f"artifact-{original_id}"
        artifacts[artifact_id] = "o" * artifact_bytes
        original = _execution(
            original_id,
            original_exec_id=None,
            artifact_id=artifact_id,
            original_call_id=None,
            client=placeholder,
        )
        executions[original_id] = original
        replay_ids[original_id] = []
        for replay_index in range(replays_per_original):
            replay_id = f"replay-{original_index}-{replay_index}"
            replay_artifact_id = f"artifact-{replay_id}"
            artifacts[replay_artifact_id] = "r" * artifact_bytes
            replay = _execution(
                replay_id,
                original_exec_id=original_id,
                artifact_id=replay_artifact_id,
                original_call_id=f"call-{original_id}",
                client=placeholder,
            )
            executions[replay_id] = replay
            candidates.append(replay)
            replay_ids[original_id].append(replay_id)

    for index in range(unrelated):
        exec_id = f"unrelated-{index}"
        artifact_id = f"artifact-{exec_id}"
        artifacts[artifact_id] = "u"
        unrelated_execution = _execution(
            exec_id,
            original_exec_id="not-in-cohort",
            artifact_id=artifact_id,
            original_call_id=None,
            client=placeholder,
        )
        executions[exec_id] = unrelated_execution
        candidates.append(unrelated_execution)

    client = _FakeClient(executions, candidates, artifacts, counters)
    return client, list(replay_ids), replay_ids, counters


def _measure(
    *,
    scenario: str,
    mode: str,
    originals: int,
    replays: int,
    unrelated: int,
    artifact_bytes: int,
    explicit_ids: bool,
    counters: Counters,
    operation: Callable[[], Any],
) -> Measurement:
    tracemalloc.start()
    started = perf_counter()
    result = operation()
    elapsed = perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rows = getattr(result, "rows", [result])
    warning_count = sum(len(row.warnings) for row in rows)
    return Measurement(
        scenario=scenario,
        mode=mode,
        originals=originals,
        replays=replays,
        unrelated=unrelated,
        artifact_bytes=artifact_bytes,
        explicit_ids=explicit_ids,
        elapsed_seconds=elapsed,
        peak_memory_bytes=peak,
        counters=counters,
        warning_count=warning_count,
    )


def _run_fake_scenario(
    scenario: str,
    *,
    originals: int,
    replays_per_original: int,
    unrelated: int = 0,
    artifact_bytes: int = 32,
    explicit_ids: bool = False,
) -> Measurement:
    client, original_ids, replay_ids, counters = _fake_fixture(
        originals=originals,
        replays_per_original=replays_per_original,
        unrelated=unrelated,
        artifact_bytes=artifact_bytes,
    )

    def operation() -> Any:
        with patch("kitaru.diff.KitaruClient", return_value=client):
            if explicit_ids:
                original_id = original_ids[0]
                return diff(original_id, *replay_ids[original_id])
            return diff_cohort(original_ids)

    return _measure(
        scenario=scenario,
        mode="deterministic",
        originals=originals,
        replays=originals * replays_per_original,
        unrelated=unrelated,
        artifact_bytes=artifact_bytes,
        explicit_ids=explicit_ids,
        counters=counters,
        operation=operation,
    )


def deterministic_benchmarks() -> list[Measurement]:
    """Run every issue scenario with deterministic generated records."""
    # Import analytics and resolve UI helpers before measuring. Otherwise the
    # first scenario includes one-time Python import cost that later scenarios
    # do not pay.
    _run_fake_scenario("warm-up", originals=1, replays_per_original=1)
    return [
        _run_fake_scenario(
            "50 originals, one replay", originals=50, replays_per_original=1
        ),
        _run_fake_scenario(
            "60 originals, three replays", originals=60, replays_per_original=3
        ),
        _run_fake_scenario(
            "50 originals plus unrelated executions",
            originals=50,
            replays_per_original=1,
            unrelated=1_000,
        ),
        _run_fake_scenario(
            "explicit replay IDs",
            originals=1,
            replays_per_original=3,
            explicit_ids=True,
        ),
        _run_fake_scenario(
            "small artifacts", originals=10, replays_per_original=1, artifact_bytes=32
        ),
        _run_fake_scenario(
            "large artifacts",
            originals=10,
            replays_per_original=1,
            artifact_bytes=1_000_000,
        ),
        _run_fake_scenario(
            "exactly 10,000 scanned executions",
            originals=50,
            replays_per_original=1,
            unrelated=9_950,
        ),
        _run_fake_scenario(
            "10,000 execution scan cap plus older row",
            originals=50,
            replays_per_original=1,
            unrelated=9_951,
        ),
    ]


@checkpoint
def create_benchmark_payload(payload_bytes: int) -> str:
    """Create a deterministic artifact of the requested size."""
    return "x" * payload_bytes


@checkpoint
def summarize_benchmark_payload(payload: str) -> int:
    """Create a second checkpoint so replay can start after payload creation."""
    return len(payload)


@flow
def issue_525_benchmark_flow(payload_bytes: int) -> int:
    """Small local flow used only by this benchmark."""
    return summarize_benchmark_payload(create_benchmark_payload(payload_bytes))


def _create_local_pairs(
    *, originals: int, replays_per_original: int, artifact_bytes: int, tag: str
) -> tuple[list[str], dict[str, list[str]]]:
    client = KitaruClient()
    original_ids: list[str] = []
    replay_ids: dict[str, list[str]] = {}
    for _original_index in range(originals):
        handle = issue_525_benchmark_flow.run(artifact_bytes)
        handle.wait()
        original_id = str(handle.exec_id)
        original_ids.append(original_id)
        replay_ids[original_id] = []
        for replay_index in range(replays_per_original):
            replacement = chr(97 + replay_index % 26) * artifact_bytes
            checkpoint_overrides = (
                {"create_benchmark_payload": {"output": replacement}}
                if artifact_bytes < 100_000
                else None
            )
            submission = client.executions.replay(
                original_id,
                at="summarize_benchmark_payload",
                checkpoint_overrides=checkpoint_overrides,
                tag=tag,
                wait=True,
            )
            replay_ids[original_id].append(submission.results[0].replay_exec_id)
    return original_ids, replay_ids


class _CountingArtifact:
    def __init__(self, artifact: Any, counters: Counters) -> None:
        self._artifact = artifact
        self._counters = counters

    def load(self) -> Any:
        self._counters.artifact_loads += 1
        return self._artifact.load()


class _CountingClient:
    def __init__(self, client: KitaruClient, counters: Counters) -> None:
        self._client_ref = client
        self._counters = counters
        self.executions = _CountingExecutions(client, counters)

    def _get_artifact_version(
        self, artifact_id: str, *, hydrate: bool
    ) -> _CountingArtifact:
        self._counters.artifact_hydrations += 1
        return _CountingArtifact(
            self._client_ref._get_artifact_version(artifact_id, hydrate=hydrate),
            self._counters,
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client_ref, name)


class _CountingExecutions:
    def __init__(self, client: KitaruClient, counters: Counters) -> None:
        self._api = client.executions
        self._counters = counters

    def get(self, exec_id: str) -> Execution:
        self._counters.execution_get_calls += 1
        return self._api.get(exec_id)

    def _list_replays_for_originals(
        self,
        *,
        original_exec_ids: Sequence[str],
        expected_flow_name: str | None,
        limit: int,
    ) -> tuple[list[Any], bool]:
        self._counters.execution_list_calls += 1
        return self._api._list_replays_for_originals(
            original_exec_ids=original_exec_ids,
            expected_flow_name=expected_flow_name,
            limit=limit,
        )


@contextmanager
def _count_backend_pages(
    client: KitaruClient,
    counters: Counters,
    *,
    relevant_exec_ids: set[str],
    unrelated_candidate_rows: list[int],
) -> Any:
    zenml_client = client._client()
    original = zenml_client.list_pipeline_runs

    def counted(*args: Any, **kwargs: Any) -> Any:
        counters.backend_pages_read += 1
        result = original(*args, **kwargs)
        counters.candidate_rows_read += len(result.items)
        unrelated_candidate_rows[0] += sum(
            str(item.id) not in relevant_exec_ids for item in result.items
        )
        return result

    with patch.object(zenml_client, "list_pipeline_runs", side_effect=counted):
        yield


def _measure_local(
    scenario: str,
    *,
    original_ids: list[str],
    replay_ids: dict[str, list[str]],
    artifact_bytes: int,
    explicit_ids: bool = False,
) -> Measurement:
    counters = Counters()
    client = KitaruClient()
    counting_client = _CountingClient(client, counters)
    unrelated_candidate_rows = [0]
    relevant_exec_ids = set(original_ids)
    relevant_exec_ids.update(
        replay_exec_id
        for original_replay_ids in replay_ids.values()
        for replay_exec_id in original_replay_ids
    )

    def operation() -> Any:
        with (
            _count_backend_pages(
                client,
                counters,
                relevant_exec_ids=relevant_exec_ids,
                unrelated_candidate_rows=unrelated_candidate_rows,
            ),
            patch("kitaru.diff.KitaruClient", return_value=counting_client),
        ):
            if explicit_ids:
                original_id = original_ids[0]
                return diff(original_id, *replay_ids[original_id])
            return diff_cohort(original_ids)

    measured_replays = (
        len(replay_ids[original_ids[0]])
        if explicit_ids
        else sum(len(ids) for ids in replay_ids.values())
    )
    measurement = _measure(
        scenario=scenario,
        mode="local",
        originals=len(original_ids),
        replays=measured_replays,
        unrelated=0,
        artifact_bytes=artifact_bytes,
        explicit_ids=explicit_ids,
        counters=counters,
        operation=operation,
    )
    measurement.unrelated = unrelated_candidate_rows[0]
    return measurement


def local_benchmarks(*, scale: str) -> list[Measurement]:
    """Create and measure real runs on the active stack."""
    sizes = [(2, 1), (2, 3)] if scale == "smoke" else [(50, 1), (60, 3)]
    results: list[Measurement] = []
    for originals, replays_per_original in sizes:
        original_ids, replay_ids = _create_local_pairs(
            originals=originals,
            replays_per_original=replays_per_original,
            artifact_bytes=32,
            tag=f"issue-525-benchmark-{originals}x{replays_per_original}",
        )
        results.append(
            _measure_local(
                f"{originals} local originals, {replays_per_original} replay(s) each",
                original_ids=original_ids,
                replay_ids=replay_ids,
                artifact_bytes=32,
            )
        )
        if originals == sizes[0][0]:
            results.append(
                _measure_local(
                    "local explicit replay IDs",
                    original_ids=original_ids[:1],
                    replay_ids=replay_ids,
                    artifact_bytes=32,
                    explicit_ids=True,
                )
            )
    for artifact_bytes in (32, 1_000_000):
        original_ids, replay_ids = _create_local_pairs(
            originals=1,
            replays_per_original=1,
            artifact_bytes=artifact_bytes,
            tag=f"issue-525-benchmark-artifact-{artifact_bytes}",
        )
        results.append(
            _measure_local(
                f"local artifact size {artifact_bytes}",
                original_ids=original_ids,
                replay_ids=replay_ids,
                artifact_bytes=artifact_bytes,
            )
        )
    return results


def _write_results(measurements: Sequence[Measurement], output: Path | None) -> None:
    payload = [asdict(measurement) for measurement in measurements]
    rendered = json.dumps(payload, indent=2)
    print(rendered)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("deterministic", "local"))
    parser.add_argument("--scale", choices=("smoke", "full"), default="smoke")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    measurements = (
        deterministic_benchmarks()
        if args.mode == "deterministic"
        else local_benchmarks(scale=args.scale)
    )
    _write_results(measurements, args.output)


if __name__ == "__main__":
    main()
