from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest

pytest.importorskip("pydantic_ai")

from kitaru.adapters.pydantic_ai import _sandbox
from kitaru.config import SandboxCommandResult
from kitaru.errors import KitaruUsageError


def _fake_core_result(
    *,
    command: str = "python --version",
    cwd: str | None = "/workspace",
    stdout: str = "Python 3.12.0\n",
    stderr: str = "",
    exit_code: int = 0,
    stdout_truncated: bool = False,
    stderr_truncated: bool = False,
    cleanup_succeeded: bool = True,
    cleanup_error: str | None = None,
) -> SandboxCommandResult:
    return SandboxCommandResult(
        command=command,
        cwd=cwd,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
        stack_id="stack-1",
        stack_name="dev",
        sandbox_id="sandbox-1",
        sandbox_name="sandbox-dev",
        session_id="session-1",
        cleanup="destroy",
        cleanup_succeeded=cleanup_succeeded,
        cleanup_error=cleanup_error,
    )


def _tool_context() -> Any:
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    ctx.tool_call_id = "call_sandbox"
    return ctx


async def _get_tool(toolset: Any, ctx: Any) -> Any:
    return (await toolset.get_tools(ctx))[_sandbox.SANDBOX_COMMAND_TOOL_NAME]


def _fake_agent_for_message_parts(parts: list[Any], *, output: str) -> Any:
    from pydantic_ai.messages import ModelResponse

    class FakeResult:
        def __init__(self) -> None:
            self.output = output

        def all_messages(self) -> list[ModelResponse]:
            return [ModelResponse(parts=parts)]

    class FakeAgent:
        def run_sync(self, _prompt: str) -> FakeResult:
            return FakeResult()

    return FakeAgent()


def _install_checkpoint_recorder(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    from kitaru.runtime import _checkpoint_scope

    checkpoint_steps: list[str] = []

    async def fake_checkpoint(
        *, step_name: str, body: Callable[[], Awaitable[Any]], **kwargs: Any
    ) -> Any:
        checkpoint_steps.append(step_name)
        with _checkpoint_scope(
            name=step_name,
            checkpoint_type=kwargs["config"].get("type", "tool_call"),
        ):
            return await body()

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fake_checkpoint,
    )
    return checkpoint_steps


def _install_fake_tracker(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    recorded_events: list[dict[str, Any]] = []

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, Any]:
            assert tool_call_id == "call_sandbox"
            return "event-1", object()

        def record_tool_event(self, *_args: Any, **kwargs: Any) -> None:
            recorded_events.append(kwargs)

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.get_current_tracker",
        lambda: FakeTracker(),
    )
    return recorded_events


def _assert_recorded_tool_event(
    event: dict[str, Any],
    *,
    status: str,
    error_type: type[BaseException] | None = None,
) -> None:
    assert event["status"] == status
    assert event["name"] == _sandbox.SANDBOX_COMMAND_TOOL_NAME
    assert event["toolset_kind"] == "function"
    assert event["capture_mode"] == "metadata"
    assert isinstance(event["duration_ms"], float)
    assert event["hitl"] is False
    assert event["artifacts"] == {}
    if error_type is None:
        assert "error" not in event
    else:
        assert isinstance(event["error"], error_type)


@pytest.mark.anyio
async def test_sandbox_command_toolset_exposes_safe_function_tool_schema() -> None:
    from pydantic_ai import FunctionToolset

    toolset = _sandbox.sandbox_command_toolset(max_chars=20_000)

    assert isinstance(toolset, FunctionToolset)
    tool = await _get_tool(toolset, _tool_context())
    schema = tool.tool_def.parameters_json_schema
    properties = schema["properties"]

    assert set(properties) == {"command", "cwd"}
    assert schema["required"] == ["command"]
    assert "env" not in properties


@pytest.mark.parametrize("invalid_max_chars", [-1, True, 1.5, "100"])
def test_sandbox_command_toolset_rejects_invalid_max_chars(
    invalid_max_chars: Any,
) -> None:
    with pytest.raises(KitaruUsageError):
        _sandbox.sandbox_command_toolset(max_chars=invalid_max_chars)


@pytest.mark.parametrize("invalid_cleanup", ["keep", "", None, 1])
def test_sandbox_command_toolset_rejects_invalid_cleanup(
    invalid_cleanup: Any,
) -> None:
    with pytest.raises(KitaruUsageError):
        _sandbox.sandbox_command_toolset(cleanup=invalid_cleanup)


@pytest.mark.anyio
async def test_sandbox_command_tool_calls_shared_helper_with_factory_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import SandboxCommandToolResult

    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(*args: Any, **kwargs: Any) -> SandboxCommandResult:
        calls.append({"args": args, "kwargs": kwargs})
        return _fake_core_result(command=cast(str, args[0]), cwd=kwargs.get("cwd"))

    monkeypatch.setattr(
        _sandbox.kitaru, "run_sandbox_command", fake_run_sandbox_command
    )
    toolset = _sandbox.sandbox_command_toolset(max_chars=123, cleanup="close")
    ctx = _tool_context()
    tool = await _get_tool(toolset, ctx)

    result = await toolset.call_tool(
        _sandbox.SANDBOX_COMMAND_TOOL_NAME,
        {"command": "python --version", "cwd": "/workspace"},
        ctx,
        tool,
    )

    assert isinstance(result, SandboxCommandToolResult)
    assert result.stdout == "Python 3.12.0\n"
    assert calls == [
        {
            "args": ("python --version",),
            "kwargs": {"cwd": "/workspace", "max_chars": 123, "cleanup": "close"},
        }
    ]


@pytest.mark.anyio
async def test_sandbox_command_tool_uses_llm_facing_default_max_chars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(*_args: Any, **kwargs: Any) -> SandboxCommandResult:
        calls.append(kwargs)
        return _fake_core_result()

    monkeypatch.setattr(
        _sandbox.kitaru, "run_sandbox_command", fake_run_sandbox_command
    )
    toolset = _sandbox.sandbox_command_toolset()
    ctx = _tool_context()
    tool = await _get_tool(toolset, ctx)

    await toolset.call_tool(
        _sandbox.SANDBOX_COMMAND_TOOL_NAME,
        {"command": "python --version"},
        ctx,
        tool,
    )

    assert calls == [
        {
            "cwd": None,
            "max_chars": _sandbox.DEFAULT_SANDBOX_TOOL_MAX_CHARS,
            "cleanup": "destroy",
        }
    ]


@pytest.mark.anyio
async def test_sandbox_command_tool_result_excludes_infrastructure_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _sandbox.kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: _fake_core_result(),
    )
    toolset = _sandbox.sandbox_command_toolset()
    ctx = _tool_context()
    tool = await _get_tool(toolset, ctx)

    result = await toolset.call_tool(
        _sandbox.SANDBOX_COMMAND_TOOL_NAME,
        {"command": "python --version"},
        ctx,
        tool,
    )

    assert result.model_dump(mode="json") == {
        "command": "python --version",
        "cwd": "/workspace",
        "stdout": "Python 3.12.0\n",
        "stderr": "",
        "exit_code": 0,
        "stdout_truncated": False,
        "stderr_truncated": False,
        "cleanup_succeeded": True,
        "cleanup_error": None,
    }
    assert "stack_id" not in result.model_dump(mode="json")
    assert "sandbox_id" not in result.model_dump(mode="json")
    assert "session_id" not in result.model_dump(mode="json")


@pytest.mark.anyio
async def test_sandbox_command_tool_returns_non_zero_exit_as_normal_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _sandbox.kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: _fake_core_result(
            stdout="", stderr="nope", exit_code=7
        ),
    )
    toolset = _sandbox.sandbox_command_toolset()
    ctx = _tool_context()
    tool = await _get_tool(toolset, ctx)

    result = await toolset.call_tool(
        _sandbox.SANDBOX_COMMAND_TOOL_NAME,
        {"command": "false"},
        ctx,
        tool,
    )

    assert result.exit_code == 7
    assert result.stderr == "nope"


@pytest.mark.anyio
async def test_sandbox_command_tool_propagates_helper_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.errors import KitaruStateError

    def raise_missing_sandbox(*_args: Any, **_kwargs: Any) -> SandboxCommandResult:
        raise KitaruStateError("Active stack has no sandbox component.")

    monkeypatch.setattr(_sandbox.kitaru, "run_sandbox_command", raise_missing_sandbox)
    toolset = _sandbox.sandbox_command_toolset()
    ctx = _tool_context()
    tool = await _get_tool(toolset, ctx)

    with pytest.raises(KitaruStateError, match="no sandbox"):
        await toolset.call_tool(
            _sandbox.SANDBOX_COMMAND_TOOL_NAME,
            {"command": "python --version"},
            ctx,
            tool,
        )


@pytest.mark.anyio
async def test_sandbox_command_toolset_uses_existing_function_toolset_wrapper() -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruFunctionToolset
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset

    wrapped = kitaruify_toolset(
        _sandbox.sandbox_command_toolset(),
        capture=CapturePolicy(correlate_otel_spans=False),
    )

    assert isinstance(wrapped, KitaruFunctionToolset)
    assert wrapped.toolset_kind == "function"


@pytest.mark.anyio
async def test_sandbox_command_tool_opens_calls_strategy_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    monkeypatch.setattr(
        _sandbox.kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: _fake_core_result(),
    )
    checkpoint_steps = _install_checkpoint_recorder(monkeypatch)
    wrapped = kitaruify_toolset(
        _sandbox.sandbox_command_toolset(),
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = _tool_context()
    tool = await _get_tool(wrapped, ctx)

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool(
            _sandbox.SANDBOX_COMMAND_TOOL_NAME,
            {"command": "python --version"},
            ctx,
            tool,
        )

    assert result.exit_code == 0
    assert checkpoint_steps == [f"{_sandbox.SANDBOX_COMMAND_TOOL_NAME}_tool"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "core_result",
    [
        _fake_core_result(exit_code=0),
        _fake_core_result(stdout="", stderr="nope", exit_code=5),
    ],
)
async def test_sandbox_command_tool_tracking_records_completed_events(
    monkeypatch: pytest.MonkeyPatch,
    core_result: SandboxCommandResult,
) -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset

    monkeypatch.setattr(
        _sandbox.kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: core_result,
    )
    recorded_events = _install_fake_tracker(monkeypatch)
    wrapped = kitaruify_toolset(
        _sandbox.sandbox_command_toolset(),
        capture=CapturePolicy(tool_capture="metadata", correlate_otel_spans=False),
        tool_checkpoint_config_by_name={_sandbox.SANDBOX_COMMAND_TOOL_NAME: False},
    )
    ctx = _tool_context()
    tool = await _get_tool(wrapped, ctx)

    result = await wrapped.call_tool(
        _sandbox.SANDBOX_COMMAND_TOOL_NAME,
        {"command": "python --version"},
        ctx,
        tool,
    )

    assert result.exit_code == core_result.exit_code
    assert len(recorded_events) == 1
    _assert_recorded_tool_event(recorded_events[0], status="completed")


def test_pydantic_ai_sandbox_example_fails_without_sandbox_tool_call() -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset
    from pydantic_ai.messages import TextPart

    agent = _fake_agent_for_message_parts(
        [TextPart(content="Python 3.12.0")], output="Python 3.12.0"
    )

    with pytest.raises(
        RuntimeError,
        match=f"without calling {_sandbox.SANDBOX_COMMAND_TOOL_NAME}",
    ):
        pydantic_ai_sandbox_toolset.run_sandbox_agent_turn(agent)


def test_pydantic_ai_sandbox_example_accepts_recorded_sandbox_tool_call() -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset
    from pydantic_ai.messages import ToolCallPart

    agent = _fake_agent_for_message_parts(
        [
            ToolCallPart(
                tool_name=_sandbox.SANDBOX_COMMAND_TOOL_NAME,
                args={"command": "python --version"},
                tool_call_id="call_sandbox",
            )
        ],
        output="exit code 0; Python 3.12.0",
    )

    assert (
        pydantic_ai_sandbox_toolset.run_sandbox_agent_turn(agent)
        == "exit code 0; Python 3.12.0"
    )


def test_pydantic_ai_sandbox_example_keeps_per_tool_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset
    from pydantic_ai.models.test import TestModel

    from kitaru.runtime import _checkpoint_scope, _flow_scope

    calls: list[dict[str, Any]] = []
    checkpoint_steps: list[str] = []
    checkpoint_configs: dict[str, dict[str, Any]] = {}

    def fake_run_sandbox_command(*args: Any, **kwargs: Any) -> SandboxCommandResult:
        calls.append({"args": args, "kwargs": kwargs})
        return _fake_core_result(
            command=cast(str, args[0]),
            cwd=kwargs.get("cwd"),
            stdout="sandbox output\n",
        )

    async def fake_checkpoint(
        *, step_name: str, body: Callable[[], Awaitable[Any]], **kwargs: Any
    ) -> Any:
        checkpoint_steps.append(step_name)
        config = dict(kwargs["config"])
        checkpoint_configs[step_name] = config
        with _checkpoint_scope(
            name=step_name,
            checkpoint_type=config.get("type", "checkpoint"),
        ):
            return await body()

    monkeypatch.setattr(
        _sandbox.kitaru, "run_sandbox_command", fake_run_sandbox_command
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
        fake_checkpoint,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fake_checkpoint,
    )

    durable_agent = pydantic_ai_sandbox_toolset.build_agent(
        model=TestModel(call_tools=[_sandbox.SANDBOX_COMMAND_TOOL_NAME]),
        max_chars=321,
    )

    with _flow_scope(name="sandbox_agent_flow"):
        answer = pydantic_ai_sandbox_toolset.run_sandbox_agent_turn(durable_agent)
        final_answer = pydantic_ai_sandbox_toolset.publish_sandbox_answer._func(answer)

    assert "sandbox output" in final_answer
    assert calls == [
        {
            "args": ("a",),
            "kwargs": {"cwd": None, "max_chars": 321, "cleanup": "destroy"},
        }
    ]
    assert "sandboxed_pydantic_ai_agent_model_request" in checkpoint_steps
    assert f"{_sandbox.SANDBOX_COMMAND_TOOL_NAME}_tool" in checkpoint_steps
    assert checkpoint_configs[f"{_sandbox.SANDBOX_COMMAND_TOOL_NAME}_tool"] == {
        "cache": False,
        "type": "tool_call",
    }


def test_pydantic_ai_sandbox_example_submit_disables_flow_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset

    run_calls: list[dict[str, Any]] = []

    class FakeFlow:
        def run(self, **kwargs: Any) -> object:
            run_calls.append(kwargs)
            return object()

    monkeypatch.setattr(pydantic_ai_sandbox_toolset, "sandbox_toolset_flow", FakeFlow())

    handle = pydantic_ai_sandbox_toolset.submit_sandbox_toolset_flow(
        model="test",
        max_chars=123,
    )

    assert handle is not None
    assert run_calls == [{"model": "test", "max_chars": 123, "cache": False}]


def test_pydantic_ai_sandbox_example_wait_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset

    from kitaru.client import ExecutionStatus

    times = iter([0.0, 0.5, 1.5])
    slept: list[float] = []

    class FakeHandle:
        exec_id = "exec-timeout"

        @property
        def status(self) -> ExecutionStatus:
            return ExecutionStatus.RUNNING

    monkeypatch.setattr(
        pydantic_ai_sandbox_toolset.time, "monotonic", lambda: next(times)
    )
    monkeypatch.setattr(pydantic_ai_sandbox_toolset.time, "sleep", slept.append)

    with pytest.raises(TimeoutError, match=r"exec-timeout.*running"):
        pydantic_ai_sandbox_toolset.wait_for_completion(
            FakeHandle(), poll_seconds=0.01, timeout_seconds=1.0
        )

    assert slept == [0.01]


def test_pydantic_ai_sandbox_example_waits_without_result_extraction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset

    from kitaru.client import ExecutionStatus

    statuses = [ExecutionStatus.RUNNING, ExecutionStatus.COMPLETED]
    slept: list[float] = []

    class FakeHandle:
        @property
        def status(self) -> ExecutionStatus:
            return statuses.pop(0)

        def wait(self) -> object:
            raise AssertionError("wait() should not be used for this demo shape")

    monkeypatch.setattr(pydantic_ai_sandbox_toolset.time, "sleep", slept.append)

    status = pydantic_ai_sandbox_toolset.wait_for_completion(
        FakeHandle(), poll_seconds=0.01
    )

    assert status is ExecutionStatus.COMPLETED
    assert slept == [0.01]


def test_pydantic_ai_sandbox_example_failed_status_raises_execution_details() -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset

    from kitaru.client import ExecutionStatus
    from kitaru.errors import KitaruExecutionError

    class FakeHandle:
        exec_id = "exec-failed"
        get_called = False

        def get(self) -> object:
            self.get_called = True
            raise KitaruExecutionError(
                "specific sandbox failure",
                exec_id=self.exec_id,
                status=ExecutionStatus.FAILED,
            )

    handle = FakeHandle()

    with pytest.raises(KitaruExecutionError, match="specific sandbox failure"):
        pydantic_ai_sandbox_toolset._raise_failed_execution_details(
            handle, ExecutionStatus.FAILED
        )

    assert handle.get_called is True


@pytest.mark.anyio
async def test_sandbox_command_tool_tracking_records_failed_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.errors import KitaruStateError

    def raise_missing_sandbox(*_args: Any, **_kwargs: Any) -> SandboxCommandResult:
        raise KitaruStateError("Active stack has no sandbox component.")

    monkeypatch.setattr(_sandbox.kitaru, "run_sandbox_command", raise_missing_sandbox)
    recorded_events = _install_fake_tracker(monkeypatch)
    wrapped = kitaruify_toolset(
        _sandbox.sandbox_command_toolset(),
        capture=CapturePolicy(tool_capture="metadata", correlate_otel_spans=False),
        tool_checkpoint_config_by_name={_sandbox.SANDBOX_COMMAND_TOOL_NAME: False},
    )
    ctx = _tool_context()
    tool = await _get_tool(wrapped, ctx)

    with pytest.raises(KitaruStateError, match="no sandbox"):
        await wrapped.call_tool(
            _sandbox.SANDBOX_COMMAND_TOOL_NAME,
            {"command": "python --version"},
            ctx,
            tool,
        )

    assert len(recorded_events) == 1
    _assert_recorded_tool_event(
        recorded_events[0], status="failed", error_type=KitaruStateError
    )


def test_pydantic_ai_sandbox_toolset_example_imports_and_wires_agent() -> None:
    from examples.integrations.pydantic_ai_agent import pydantic_ai_sandbox_toolset

    from kitaru.adapters.pydantic_ai import KitaruAgent

    agent = pydantic_ai_sandbox_toolset.build_agent(model="test")

    assert isinstance(agent, KitaruAgent)
    assert agent.name == "sandboxed_pydantic_ai_agent"
    assert agent.checkpoint_strategy == "calls"
