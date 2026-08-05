#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Worker CLI configuration, lifecycle, and inspection behavior."""

import asyncio
import builtins
import inspect
import io
import json
import signal
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    LabelSelector,
    WorkerListParams,
    WorkerResponse,
    WorkerRuntime,
)
from kitaru.cli import app as app_module
from kitaru.cli import workers
from kitaru.cli.config import ResolvedTarget
from kitaru.cli.output import (
    CLIError,
    CommandResult,
    OutputContext,
    reset_output_context,
    set_output_context,
)


class DrainWorker:
    """Worker fake that exposes whether graceful drain waits for held work."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.stop: asyncio.Event | None = None

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Wait for stop, then model a held task that drains later."""
        assert stop is not None
        self.stop = stop
        self.started.set()
        await stop.wait()
        await self.release.wait()


@dataclass
class StubWorkers:
    """Worker resource fake for list and exact-name lookup."""

    items: list[WorkerResponse]
    list_calls: list[Any] = field(default_factory=list)

    async def list(self, params: Any = None) -> Any:
        self.list_calls.append(params)
        return SimpleNamespace(items=self.items, next_cursor=None)

    async def iter(self):
        for item in self.items:
            yield item

    async def get(self, worker_id: uuid.UUID) -> WorkerResponse:
        return next(item for item in self.items if item.id == worker_id)


def _response(name: str, *, live: bool) -> WorkerResponse:
    """Build a worker response with only behavior-relevant fields."""
    now = datetime.now(UTC)
    return WorkerResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        name=name,
        scope={},
        runtime=WorkerRuntime(platform="bare"),
        last_seen_at=datetime.now(UTC),
        live=live,
        metadata={},
    )


def _output_context(stdout: io.StringIO, stderr: io.StringIO) -> OutputContext:
    return OutputContext(
        command="worker.start",
        mode="jsonl",
        debug=False,
        traceback=False,
        stdout=stdout,
        stderr=stderr,
        rich=False,
    )


def test_config_merge_overrides_only_explicit_scope_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI kinds refine env scope while preserving selectors and job id."""
    job_id = uuid.uuid4()
    monkeypatch.setenv("KITARU_WORKER_SCOPE__KINDS", '["importer"]')
    monkeypatch.setenv(
        "KITARU_WORKER_SCOPE__SELECTORS",
        '[{"key":"pool","values":["cpu"],"required":true}]',
    )
    monkeypatch.setenv("KITARU_WORKER_SCOPE__JOB_ID", str(job_id))
    monkeypatch.setenv("KITARU_WORKER_CONCURRENCY", "2")
    monkeypatch.setenv("KITARU_WORKER_METADATA", '{"source":"env"}')

    config = workers.build_worker_config(
        kinds=[TaskKind.AGENT, TaskKind.EVALUATOR],
        concurrency=4,
        metadata=["source=cli", "pool=local"],
    )

    assert config.scope.kinds == [TaskKind.AGENT, TaskKind.EVALUATOR]
    assert config.scope.selectors == [
        LabelSelector(key="pool", values=["cpu"], required=True)
    ]
    assert config.scope.job_id == job_id
    assert config.concurrency == 4
    assert config.metadata == {"source": "cli", "pool": "local"}
    assert "request_timeout" not in type(config).model_fields


def test_selector_shorthand_and_json_cover_required_behavior() -> None:
    """Compact selectors stay convenient while JSON exposes `required`."""
    config = workers.build_worker_config(
        selectors=[
            "pool=cpu,gpu",
            '{"key":"tenant","values":["prod"],"required":true}',
        ]
    )
    assert config.scope.selectors == [
        LabelSelector(key="pool", values=["cpu", "gpu"]),
        LabelSelector(key="tenant", values=["prod"], required=True),
    ]
    with pytest.raises(CLIError, match="Invalid --selector"):
        workers.build_worker_config(selectors=["missing-values="])
    with pytest.raises(ValueError, match="greater than or equal to 1"):
        workers.build_worker_config(concurrency=0)


def test_contract_environment_forwards_only_url_and_environment_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker startup never creates or forwards a credential-store path."""
    selected_server = "https://selected.example.com"
    monkeypatch.setenv("KITARU_API_URL", "https://previous.example.com")
    monkeypatch.setenv("KITARU_CREDENTIALS_PATH", "/previous/credentials.json")
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_environment")

    with (
        pytest.raises(RuntimeError, match="startup failed"),
        workers.worker_contract_environment(
            ResolvedTarget(selected_server, "explicit")
        ),
    ):
        assert workers.os.environ["KITARU_API_URL"] == selected_server
        assert workers.os.environ["KITARU_API_KEY"] == "KITKEY_environment"
        assert "KITARU_CREDENTIALS_PATH" not in workers.os.environ
        raise RuntimeError("startup failed")

    assert workers.os.environ["KITARU_API_URL"] == "https://previous.example.com"
    assert workers.os.environ["KITARU_CREDENTIALS_PATH"] == "/previous/credentials.json"
    assert workers.os.environ["KITARU_API_KEY"] == "KITKEY_environment"


def test_contract_environment_rejects_stored_or_device_only_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A credential-store path cannot substitute for the worker API-key contract."""
    monkeypatch.delenv("KITARU_API_KEY", raising=False)
    monkeypatch.setenv("KITARU_CREDENTIALS_PATH", "/stored/credentials.json")

    with (
        pytest.raises(CLIError) as error,
        workers.worker_contract_environment(
            ResolvedTarget("https://selected.example.com", "explicit")
        ),
    ):
        raise AssertionError("unreachable")
    assert error.value.kind == "invalid_configuration"
    assert error.value.message == (
        "worker start requires KITARU_API_KEY in the environment."
    )


async def test_natural_completion_reports_completed_and_restores_signals() -> None:
    """Natural completion reports its reason and restores process handlers."""

    class CompletedWorker:
        async def run(self, stop: asyncio.Event | None = None) -> None:
            assert stop is not None

    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(_output_context(stdout, stderr))
    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = (
        signal.getsignal(signal.SIGTERM) if hasattr(signal, "SIGTERM") else None
    )
    try:
        result = await workers.ForegroundWorkerProcess(
            CompletedWorker(), {"name": "local"}
        ).run()
    finally:
        reset_output_context(token)

    assert result.exit_code == 0
    assert result.item == {
        "name": "local",
        "status": "stopped",
        "server_record": "retained_until_stale",
        "stop_reason": "completed",
    }
    assert signal.getsignal(signal.SIGINT) is previous_sigint
    if hasattr(signal, "SIGTERM"):
        assert signal.getsignal(signal.SIGTERM) is previous_sigterm


async def test_repeated_sigint_preserves_safe_drain() -> None:
    """Repeated interrupts do not bypass held-task cleanup with a hard exit."""
    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(_output_context(stdout, stderr))
    try:
        worker = DrainWorker()
        process = workers.ForegroundWorkerProcess(worker, {"name": "local"})
        run = asyncio.create_task(process.run())
        await worker.started.wait()

        process.handle_sigint()
        await asyncio.sleep(0)
        assert process.stop.is_set()
        assert not run.done()
        process.handle_sigint()
        assert not run.done()

        worker.release.set()
        result = await run
        assert result.exit_code == 130
        assert result.event == "stopped"
        assert result.item is not None
        assert result.item["stop_reason"] == "sigint"
        assert result.item["server_record"] == "retained_until_stale"
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == ["starting", "draining"]
    assert events[1]["item"] == {"reason": "sigint"}
    assert stderr.getvalue() == ""


async def test_sigterm_then_sigint_preserves_first_stop_reason() -> None:
    """A later SIGINT does not bypass or relabel an active SIGTERM drain."""
    if not hasattr(signal, "SIGTERM"):
        pytest.skip("SIGTERM is not available")

    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(_output_context(stdout, stderr))
    try:
        worker = DrainWorker()
        process = workers.ForegroundWorkerProcess(worker, {"name": "local"})
        run = asyncio.create_task(process.run())
        await worker.started.wait()

        process.handle_sigterm()
        process.handle_sigterm()
        await asyncio.sleep(0)
        assert process.stop.is_set()
        assert not run.done()
        process.handle_sigint()
        assert not run.done()

        worker.release.set()
        result = await run
        assert result.exit_code == 143
        assert result.item is not None
        assert result.item["stop_reason"] == "sigterm"
        assert result.item["server_record"] == "retained_until_stale"
    finally:
        reset_output_context(token)

    events = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [event["event"] for event in events] == ["starting", "draining"]
    assert events[1]["item"] == {"reason": "sigterm"}
    assert stderr.getvalue() == ""


class _FailedFlushWriter(io.StringIO):
    """Structured writer whose consumer closes during flush."""

    def __init__(self, error_type: type[Exception]) -> None:
        super().__init__()
        self._error_type = error_type

    def flush(self) -> None:
        """Simulate a closed downstream consumer."""
        raise self._error_type("output stream is closed")


def test_signal_sets_stop_before_emitting_drain(monkeypatch) -> None:
    """Signal handling requests the drain before writing its lifecycle event."""
    process = workers.ForegroundWorkerProcess(DrainWorker(), {"name": "local"})
    stop_states: list[bool] = []
    monkeypatch.setattr(
        workers,
        "emit_event",
        lambda *args: stop_states.append(process.stop.is_set()),
    )

    process.handle_sigint()

    assert stop_states == [True]


@pytest.mark.parametrize("error_type", [BrokenPipeError, ValueError])
def test_closed_output_cannot_prevent_signal_drain(
    error_type: type[Exception],
) -> None:
    """A failed drain event write still records the signal and sets stop."""
    stdout = _FailedFlushWriter(error_type)
    token = set_output_context(_output_context(stdout, io.StringIO()))
    try:
        process = workers.ForegroundWorkerProcess(DrainWorker(), {"name": "local"})
        process.handle_sigint()
    finally:
        reset_output_context(token)

    assert process.stop.is_set()


def test_worker_start_schema_describes_safe_graceful_signals() -> None:
    """Offline discovery promises only the safe first-signal drain."""
    description = app_module._FUNCTION_SPECS[app_module.worker_start].description
    assert "first SIGINT or SIGTERM requests a safe drain" in description
    assert "exits immediately" not in description


async def test_worker_failure_propagates_without_a_false_stopped_event() -> None:
    """Startup/runtime failures are left to the shared CLI error boundary."""

    class FailedWorker:
        async def run(self, stop: asyncio.Event | None = None) -> None:
            del stop
            raise RuntimeError("worker failed")

    stdout = io.StringIO()
    stderr = io.StringIO()
    token = set_output_context(_output_context(stdout, stderr))
    try:
        process = workers.ForegroundWorkerProcess(FailedWorker(), {"name": "broken"})
        with pytest.raises(RuntimeError, match="worker failed"):
            await process.run()
    finally:
        reset_output_context(token)
    assert [json.loads(line)["event"] for line in stdout.getvalue().splitlines()] == [
        "starting"
    ]


async def test_list_and_get_report_live_or_stale() -> None:
    """Inspection never describes stale server records as stopped workers."""
    live = _response("shared", live=True)
    stale = _response("old", live=False)
    resource = StubWorkers([live, stale])
    client = SimpleNamespace(workers=resource)

    listed = await workers.list_workers(
        client, size=20, cursor=None, sort="created:desc", filter=None
    )
    assert [item["status"] for item in listed.items or []] == ["live", "stale"]

    fetched = await workers.get_worker(client, "old")
    assert fetched.item["id"] == str(stale.id)
    assert fetched.item["status"] == "stale"


async def test_worker_list_rejects_unsupported_sort_before_network_call() -> None:
    """Worker list names its valid sort values without relying on server rejection."""
    resource = StubWorkers([])
    client = SimpleNamespace(workers=resource)

    with pytest.raises(CLIError) as error:
        await workers.list_workers(
            client,
            size=20,
            cursor=None,
            sort="last_seen_at:desc",
            filter=None,
        )

    assert error.value.kind == "invalid_arguments"
    assert error.value.message == "--sort must be created:asc or created:desc."
    assert resource.list_calls == []


def test_worker_list_defaults_to_newest_first() -> None:
    """The CLI requests workers from newest to oldest by default."""
    default_sort = inspect.signature(app_module.worker_list).parameters["sort"].default
    assert WorkerListParams(sort=default_sort).sort == "created:desc"


async def test_exact_worker_name_conflicts_before_returning_a_record() -> None:
    """Duplicate exact names remain a stable conflict instead of guessing."""
    client = SimpleNamespace(
        workers=StubWorkers(
            [_response("duplicate", live=True), _response("duplicate", live=False)]
        )
    )
    with pytest.raises(CLIError) as error:
        await workers.get_worker(client, "duplicate")
    assert error.value.kind == "conflict"


def test_missing_worker_extra_has_actionable_hint(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Only foreground start requires and reports the worker extra."""
    real_import = builtins.__import__

    def missing_worker(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "kitaru.worker":
            raise ModuleNotFoundError(
                "No module named 'pydantic_settings'", name="pydantic_settings"
            )
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_worker)
    assert app_module.main(["worker", "--help"]) == 0
    assert "Run and inspect generic local workers" in capsys.readouterr().out

    with pytest.raises(CLIError) as error:
        workers.load_worker_runtime()
    assert error.value.kind == "invalid_configuration"
    assert error.value.hint == "Install kitaru[cli,worker]."


def test_cli_start_passes_kind_scope_and_emits_stream_result(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The app maps CLI kinds into the generic worker without provider state."""
    calls: list[tuple[ResolvedTarget, dict[str, Any]]] = []

    async def fake_start(target: ResolvedTarget, **options: Any) -> CommandResult:
        calls.append((target, options))
        return CommandResult(item={"status": "stopped"}, event="stopped")

    monkeypatch.setattr(workers, "start_worker", fake_start)

    assert (
        app_module.main(
            [
                "worker",
                "start",
                "--server",
                "https://api.example.com",
                "--machine",
                "--kinds",
                "agent",
                "--kinds",
                "importer",
                "--job-id",
                str(job_id := uuid.uuid4()),
                "--concurrency",
                "3",
                "--request-timeout",
                "7.5",
            ]
        )
        == 0
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["command"] == "worker.start"
    assert payload["event"] == "stopped"
    target, options = calls[0]
    assert target.server_url == "https://api.example.com"
    assert options["kinds"] == [TaskKind.AGENT, TaskKind.IMPORTER]
    assert options["job_id"] == job_id
    assert options["concurrency"] == 3
    assert "request_timeout" not in options
    assert captured.err == ""
