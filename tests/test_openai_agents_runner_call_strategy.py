"""Focused tests for OpenAI runner-call checkpointing and RunState bridging."""

import asyncio
import json
from dataclasses import dataclass
from functools import partial
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

pytest.importorskip("agents")

from agents import Agent, ModelSettings, RunConfig, Runner, handoff
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import ResponseOutputMessage, ResponseOutputText
from pydantic import BaseModel
from zenml.client import Client

import kitaru
import kitaru.adapters.openai_agents._agent as openai_agent_module
import kitaru.adapters.openai_agents._runner as openai_runner
from kitaru import SandboxCommandResult, flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIApprovalDecision,
    OpenAIRunRequest,
    OpenAIRunStateEnvelope,
    sandbox_command_tool,
)
from kitaru.errors import KitaruStateError


class StructuredSupportAnswer(BaseModel):
    verdict: str
    confidence: float


class StaticTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(self.text, response_id=f"resp_static_{self.call_count}")

    def _kitaru_cache_identity(self) -> dict[str, str]:
        return {"text": self.text}

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class PublicStateTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        return _text_response(self.text, response_id="resp_public_state")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


def _text_response(text: str, *, response_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseOutputMessage(
                id=f"msg_{response_id}",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text=text,
                        type="output_text",
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            )
        ],
        usage=Usage(requests=1, input_tokens=2, output_tokens=3, total_tokens=5),
        response_id=response_id,
    )


def _wait_for_hydrated_run(exec_id: str) -> Any:
    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    if not run.status.is_finished:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    assert run.status.is_successful
    return run.get_hydrated_version()


def _step_names(hydrated_run: Any) -> set[str]:
    return set(hydrated_run.steps)


def _fake_sandbox_result() -> SandboxCommandResult:
    return SandboxCommandResult(
        command="python --version",
        cwd=None,
        stdout="Python 3.12.0\n",
        stderr="",
        exit_code=0,
        stdout_truncated=False,
        stderr_truncated=False,
        stack_id="stack-id",
        stack_name="dev",
        sandbox_id="sandbox-id",
        sandbox_name="dev",
        session_id="session-id",
        cleanup="destroy",
        cleanup_succeeded=True,
        cleanup_error=None,
    )


def _active_sandbox_identity(
    *,
    stack_id: str = "stack-id",
    stack_name: str = "dev-stack",
    sandbox_id: str | None = "sandbox-id",
    sandbox_name: str | None = "dev-sandbox",
) -> dict[str, str | None]:
    return {
        "kind": "active_sandbox",
        "stack_id": stack_id,
        "stack_name": stack_name,
        "sandbox_id": sandbox_id,
        "sandbox_name": sandbox_name,
    }


def _patch_active_sandbox_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stack_id: str = "stack-id",
    stack_name: str = "dev-stack",
    sandbox_id: str | None = "sandbox-id",
    sandbox_name: str | None = "dev-sandbox",
) -> dict[str, str | None]:
    import kitaru.config as kitaru_config

    identity = _active_sandbox_identity(
        stack_id=stack_id,
        stack_name=stack_name,
        sandbox_id=sandbox_id,
        sandbox_name=sandbox_name,
    )
    monkeypatch.setattr(
        kitaru_config,
        "_active_sandbox_cache_identity",
        lambda: identity,
    )
    return identity


@dataclass(frozen=True)
class WorkerContext:
    team_id: str
    user_id: str
    thread_id: str
    worker_name: str = "support-worker"
    is_background: bool = False
    group_chat: bool = False
    chat_mode: str | None = None
    scope_user_id: str | None = None
    scope_group_id: str | None = None
    plugin: str | None = None
    project_id: str | None = None
    tool_settings: dict[str, Any] | None = None
    message_id: str | None = None
    doc_id: str | None = None


@pytest.mark.anyio
async def test_async_bridge_forwards_exact_context_to_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    async def fake_run(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    monkeypatch.setattr(Runner, "run", fake_run)

    result = await openai_runner.run_openai_agent(
        agent=SimpleNamespace(name="agent"),
        input="hello",
        max_turns=3,
        run_config=RunConfig(tracing_disabled=True),
        context=ctx,
    )

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_sync_bridge_forwards_exact_context_to_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    def fake_run_sync(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    monkeypatch.setattr(Runner, "run_sync", fake_run_sync)

    result = openai_runner.run_openai_agent_sync(
        agent=SimpleNamespace(name="agent"),
        input="hello",
        max_turns=3,
        run_config=RunConfig(tracing_disabled=True),
        context=ctx,
    )

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_runner_sync_threads_interruption_payload_capture_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.openai_agents import OpenAICapturePolicy, OpenAIRunResult

    seen_save_payloads: list[bool] = []

    def fake_run_openai_agent_sync(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(final_output="ok")

    def fake_build_run_result(_sdk_result: Any, **kwargs: Any) -> OpenAIRunResult:
        seen_save_payloads.append(kwargs["save_interruption_payloads"])
        return OpenAIRunResult(status="completed", final_output="ok")

    runner = KitaruRunner(
        SimpleNamespace(name="capture-agent"),
        checkpoint_strategy="runner_call",
        capture=OpenAICapturePolicy(save_interruption_payloads=False),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "build_run_result",
        fake_build_run_result,
    )

    prompt = f"hello-{uuid4().hex}"
    result = runner.run_sync(OpenAIRunRequest.start(prompt))

    assert result.status == "completed"
    assert result.final_output == "ok"
    assert seen_save_payloads == [False]


def test_runner_call_run_sync_forwards_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    def fake_run_openai_agent_sync(**kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    runner = KitaruRunner(
        SimpleNamespace(name="context-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )

    result = runner.run_sync(OpenAIRunRequest.start("hello"), context=ctx)

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_runner_call_cache_key_omits_context_when_context_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_payloads: list[dict[str, Any]] = []

    def fake_checkpoint_cache_key(payload: dict[str, Any]) -> str:
        seen_payloads.append(payload)
        return "cache-key"

    monkeypatch.setitem(
        KitaruRunner._runner_call_cache_key.__globals__,
        "checkpoint_cache_key",
        fake_checkpoint_cache_key,
    )
    runner = KitaruRunner(
        SimpleNamespace(name="no-context-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    cache_key = runner._runner_call_cache_key(
        OpenAIRunRequest.start("hello"),
        agent=runner.agent,
        run_config=RunConfig(tracing_disabled=True),
        context_cache_identity=None,
        surface="run",
    )

    assert cache_key == "cache-key"
    assert "context" not in seen_payloads[0]


def test_runner_call_cache_identity_varies_by_agent_instructions() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    permissive_runner = KitaruRunner(
        Agent(
            name="behavior-cache-agent",
            model=StaticTextModel("ok"),
            instructions="You may run sandbox commands when useful.",
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    restrictive_runner = KitaruRunner(
        Agent(
            name="behavior-cache-agent",
            model=StaticTextModel("ok"),
            instructions="Never run shell commands; explain only.",
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    permissive_key = permissive_runner._runner_call_cache_key(
        request,
        agent=permissive_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    restrictive_key = restrictive_runner._runner_call_cache_key(
        request,
        agent=restrictive_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = permissive_runner._agent_cache_identity(
        permissive_runner.agent
    )["behavior"]

    assert permissive_key != restrictive_key
    assert (
        behavior_identity["instructions"] == "You may run sandbox commands when useful."
    )


def test_runner_call_cache_identity_uses_stable_callable_instructions() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_instructions(message: str) -> Any:
        def instructions(_context: Any, _agent: Any) -> str:
            return message

        return instructions

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="callable-instructions-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    same_runner_a = make_runner(make_instructions("Use the sandbox when helpful."))
    same_runner_b = make_runner(make_instructions("Use the sandbox when helpful."))
    different_runner = make_runner(make_instructions("Never run shell commands."))

    same_key_a = same_runner_a._runner_call_cache_key(
        request,
        agent=same_runner_a.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    same_key_b = same_runner_b._runner_call_cache_key(
        request,
        agent=same_runner_b.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    different_key = different_runner._runner_call_cache_key(
        request,
        agent=different_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = same_runner_a._agent_cache_identity(same_runner_a.agent)[
        "behavior"
    ]

    assert same_key_a == same_key_b
    assert different_key != same_key_a
    assert "object_id" not in json.dumps(behavior_identity)


def test_runner_call_cache_identity_includes_callable_private_instance_state() -> None:
    class Instructions:
        def __init__(self, message: str) -> None:
            self._message = message

        def __call__(self, _context: Any, _agent: Any) -> str:
            return self._message

    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="callable-instance-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    sandbox_runner = make_runner(Instructions("Use the sandbox."))
    no_shell_runner = make_runner(Instructions("Never run shell commands."))

    sandbox_key = sandbox_runner._runner_call_cache_key(
        request,
        agent=sandbox_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    no_shell_key = no_shell_runner._runner_call_cache_key(
        request,
        agent=no_shell_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = sandbox_runner._agent_cache_identity(sandbox_runner.agent)[
        "behavior"
    ]

    assert sandbox_key != no_shell_key
    assert behavior_identity["instructions"]["instance_state"]["_message"] == (
        "Use the sandbox."
    )


def test_runner_call_cache_identity_includes_slotted_callable_state() -> None:
    class Instructions:
        __slots__ = ("_message",)

        def __init__(self, message: str) -> None:
            self._message = message

        def __call__(self, _context: Any, _agent: Any) -> str:
            return self._message

    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="slotted-callable-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    sandbox_runner = make_runner(Instructions("Use the sandbox."))
    no_shell_runner = make_runner(Instructions("Never run shell commands."))

    sandbox_key = sandbox_runner._runner_call_cache_key(
        request,
        agent=sandbox_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    no_shell_key = no_shell_runner._runner_call_cache_key(
        request,
        agent=no_shell_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = sandbox_runner._agent_cache_identity(sandbox_runner.agent)[
        "behavior"
    ]

    assert sandbox_key != no_shell_key
    assert behavior_identity["instructions"]["instance_state"]["_message"] == (
        "Use the sandbox."
    )


def test_runner_call_cache_identity_includes_bound_method_private_state() -> None:
    class InstructionSource:
        def __init__(self, message: str) -> None:
            self._message = message

        def instructions(self, _context: Any, _agent: Any) -> str:
            return self._message

    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="bound-method-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    sandbox_runner = make_runner(InstructionSource("Use the sandbox.").instructions)
    no_shell_runner = make_runner(
        InstructionSource("Never run shell commands.").instructions
    )

    sandbox_key = sandbox_runner._runner_call_cache_key(
        request,
        agent=sandbox_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    no_shell_key = no_shell_runner._runner_call_cache_key(
        request,
        agent=no_shell_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = sandbox_runner._agent_cache_identity(sandbox_runner.agent)[
        "behavior"
    ]

    assert sandbox_key != no_shell_key
    assert (
        behavior_identity["instructions"]["bound_to"]["instance_state"]["_message"]
        == "Use the sandbox."
    )


def test_runner_call_cache_identity_handles_builtin_callables() -> None:
    identity = openai_agent_module._behavior_value_cache_identity(len)

    assert identity["module"] == "builtins"
    assert "object_id" not in json.dumps(identity)


def test_runner_call_cache_identity_includes_referenced_globals() -> None:
    source = "def instructions(_context, _agent):\n    return SYSTEM_PROMPT"
    sandbox_namespace = {"SYSTEM_PROMPT": "Use the sandbox."}
    no_shell_namespace = {"SYSTEM_PROMPT": "Never run shell commands."}
    exec(source, sandbox_namespace)
    exec(source, no_shell_namespace)
    sandbox_instructions = sandbox_namespace["instructions"]
    no_shell_instructions = no_shell_namespace["instructions"]
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="global-callable-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    sandbox_runner = make_runner(sandbox_instructions)
    no_shell_runner = make_runner(no_shell_instructions)

    sandbox_key = sandbox_runner._runner_call_cache_key(
        request,
        agent=sandbox_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    no_shell_key = no_shell_runner._runner_call_cache_key(
        request,
        agent=no_shell_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = sandbox_runner._agent_cache_identity(sandbox_runner.agent)[
        "behavior"
    ]

    assert sandbox_key != no_shell_key
    assert behavior_identity["instructions"]["globals"]["SYSTEM_PROMPT"] == (
        "Use the sandbox."
    )


def test_runner_call_cache_identity_includes_callable_bytecode() -> None:
    namespace: dict[str, Any] = {}
    exec(
        "def build():\n    return (lambda value: value + 1, lambda value: value - 1)",
        namespace,
    )
    add_instruction, subtract_instruction = namespace["build"]()
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="bytecode-callable-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    add_runner = make_runner(add_instruction)
    subtract_runner = make_runner(subtract_instruction)

    add_key = add_runner._runner_call_cache_key(
        request,
        agent=add_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    subtract_key = subtract_runner._runner_call_cache_key(
        request,
        agent=subtract_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    add_code = add_runner._agent_cache_identity(add_runner.agent)["behavior"][
        "instructions"
    ]["code"]
    subtract_code = subtract_runner._agent_cache_identity(subtract_runner.agent)[
        "behavior"
    ]["instructions"]["code"]

    assert add_key != subtract_key
    assert add_code["bytecode"] != subtract_code["bytecode"]
    assert "filename" not in add_code
    assert "firstlineno" not in add_code


def test_runner_call_cache_identity_includes_callable_defaults_and_partials() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_default_instructions(message: str) -> Any:
        def instructions(_context: Any, _agent: Any, text: str = message) -> str:
            return text

        return instructions

    def parameterized_instructions(
        _context: Any,
        _agent: Any,
        *,
        message: str,
    ) -> str:
        return message

    def make_runner(instructions: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="callable-defaults-cache-agent",
                model=StaticTextModel("ok"),
                instructions=instructions,
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    default_runner = make_runner(make_default_instructions("Use the sandbox."))
    other_default_runner = make_runner(
        make_default_instructions("Never run shell commands.")
    )
    partial_runner = make_runner(
        partial(parameterized_instructions, message="Use the sandbox.")
    )
    other_partial_runner = make_runner(
        partial(parameterized_instructions, message="Never run shell commands.")
    )

    default_key = default_runner._runner_call_cache_key(
        request,
        agent=default_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    other_default_key = other_default_runner._runner_call_cache_key(
        request,
        agent=other_default_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    partial_key = partial_runner._runner_call_cache_key(
        request,
        agent=partial_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    other_partial_key = other_partial_runner._runner_call_cache_key(
        request,
        agent=other_partial_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )

    assert default_key != other_default_key
    assert partial_key != other_partial_key


def test_runner_call_cache_identity_varies_by_string_agent_model_value() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    nano_runner = KitaruRunner(
        Agent(name="string-model-cache-agent", model="gpt-5-nano"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    mini_runner = KitaruRunner(
        Agent(name="string-model-cache-agent", model="gpt-4.1-mini"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    nano_key = nano_runner._runner_call_cache_key(
        request,
        agent=nano_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    mini_key = mini_runner._runner_call_cache_key(
        request,
        agent=mini_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    model_identity = nano_runner._agent_cache_identity(nano_runner.agent)["model"]

    assert nano_key != mini_key
    assert model_identity["python_type"] == "builtins.str"
    assert model_identity["value"] == "gpt-5-nano"


def test_runner_call_cache_identity_varies_by_custom_model_public_state() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    concise_model = StaticTextModel("short answer")
    detailed_model = StaticTextModel("detailed answer")
    concise_runner = KitaruRunner(
        Agent(name="custom-model-cache-agent", model=concise_model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    detailed_runner = KitaruRunner(
        Agent(name="custom-model-cache-agent", model=detailed_model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    concise_key = concise_runner._runner_call_cache_key(
        request,
        agent=concise_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    detailed_key = detailed_runner._runner_call_cache_key(
        request,
        agent=detailed_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    model_identity = concise_runner._agent_cache_identity(concise_runner.agent)["model"]

    concise_model.text = "changed answer"
    after_text_mutation_key = concise_runner._runner_call_cache_key(
        request,
        agent=concise_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    concise_model.text = "short answer"
    concise_model.call_count = 99
    after_call_count_mutation_key = concise_runner._runner_call_cache_key(
        request,
        agent=concise_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )

    assert concise_key != detailed_key
    assert after_text_mutation_key != concise_key
    assert after_call_count_mutation_key == concise_key
    assert model_identity["python_type"].endswith(".StaticTextModel")
    assert model_identity["cache_identity"] == {"text": "short answer"}
    assert "value" not in model_identity


def test_model_cache_identity_uses_public_state_without_hook() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    concise_model = PublicStateTextModel("short answer")
    detailed_model = PublicStateTextModel("detailed answer")
    concise_runner = KitaruRunner(
        Agent(name="custom-model-public-state-cache-agent", model=concise_model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    detailed_runner = KitaruRunner(
        Agent(name="custom-model-public-state-cache-agent", model=detailed_model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    concise_key = concise_runner._runner_call_cache_key(
        request,
        agent=concise_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    detailed_key = detailed_runner._runner_call_cache_key(
        request,
        agent=detailed_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    model_identity = concise_runner._agent_cache_identity(concise_runner.agent)["model"]

    assert concise_key != detailed_key
    assert model_identity["python_type"].endswith(".PublicStateTextModel")
    assert model_identity["value"]["python_type"].endswith(".PublicStateTextModel")
    assert model_identity["public_state"] == {"text": "short answer"}


def test_runner_call_cache_identity_varies_by_agent_model_settings() -> None:
    request = OpenAIRunRequest.start("hello")
    cold_settings = ModelSettings(temperature=0.0)
    warm_settings = ModelSettings(temperature=0.8)
    run_config = RunConfig(tracing_disabled=True)
    cold_runner = KitaruRunner(
        Agent(
            name="model-settings-cache-agent",
            model=StaticTextModel("ok"),
            model_settings=cold_settings,
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    warm_runner = KitaruRunner(
        Agent(
            name="model-settings-cache-agent",
            model=StaticTextModel("ok"),
            model_settings=warm_settings,
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    cold_key = cold_runner._runner_call_cache_key(
        request,
        agent=cold_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    warm_key = warm_runner._runner_call_cache_key(
        request,
        agent=warm_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    behavior_identity = cold_runner._agent_cache_identity(cold_runner.agent)["behavior"]

    assert cold_key != warm_key
    assert behavior_identity["model_settings"]["fields"]["temperature"] == 0.0


def test_runner_call_cache_identity_varies_by_sandbox_tool_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_active_sandbox_identity(monkeypatch)
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    runner_small_output = KitaruRunner(
        Agent(
            name="sandbox-cache-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=100, cleanup="destroy")],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    runner_large_output = KitaruRunner(
        Agent(
            name="sandbox-cache-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=20_000, cleanup="destroy")],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    runner_close_cleanup = KitaruRunner(
        Agent(
            name="sandbox-cache-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=100, cleanup="close")],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    runner_short_timeout = KitaruRunner(
        Agent(
            name="sandbox-cache-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=100, timeout_seconds=1)],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    small_key = runner_small_output._runner_call_cache_key(
        request,
        agent=runner_small_output.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    large_key = runner_large_output._runner_call_cache_key(
        request,
        agent=runner_large_output.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    close_key = runner_close_cleanup._runner_call_cache_key(
        request,
        agent=runner_close_cleanup.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    timeout_key = runner_short_timeout._runner_call_cache_key(
        request,
        agent=runner_short_timeout.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )

    assert len({small_key, large_key, close_key, timeout_key}) == 4


def test_runner_call_cache_key_allows_unused_sandbox_tool_without_active_sandbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.config as kitaru_config

    def fail_active_sandbox_identity() -> dict[str, str | None]:
        raise KitaruStateError("The active stack has no sandbox component.")

    monkeypatch.setattr(
        kitaru_config,
        "_active_sandbox_cache_identity",
        fail_active_sandbox_identity,
    )
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    runner = KitaruRunner(
        Agent(
            name="unused-sandbox-tool-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=100)],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    cache_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    tool_identity = runner._agent_cache_identity(runner.agent)["tools"][0]

    assert isinstance(cache_key, str)
    assert tool_identity["tool_cache_identity"]["active_sandbox"] == {
        "kind": "active_sandbox_unavailable",
        "error_type": "KitaruStateError",
        "message": "The active stack has no sandbox component.",
    }


def test_runner_call_cache_identity_varies_by_active_sandbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.config as kitaru_config

    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    active_identity = _active_sandbox_identity(stack_id="stack-a")
    monkeypatch.setattr(
        kitaru_config,
        "_active_sandbox_cache_identity",
        lambda: active_identity,
    )
    runner = KitaruRunner(
        Agent(
            name="sandbox-cache-agent",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=100, cleanup="destroy")],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    stack_a_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    active_identity = _active_sandbox_identity(stack_id="stack-b")
    stack_b_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    tool_identity = runner._agent_cache_identity(runner.agent)["tools"][0]

    assert stack_a_key != stack_b_key
    assert tool_identity["tool_cache_identity"]["active_sandbox"]["stack_id"] == (
        "stack-b"
    )


def test_runner_call_cache_identity_varies_by_sandbox_tool_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_active_sandbox_identity(monkeypatch)
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    read_only_runner = KitaruRunner(
        Agent(
            name="sandbox-description-agent",
            model=StaticTextModel("ok"),
            tools=[
                sandbox_command_tool(
                    description="Run read-only inspection commands only."
                )
            ],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )
    write_allowed_runner = KitaruRunner(
        Agent(
            name="sandbox-description-agent",
            model=StaticTextModel("ok"),
            tools=[
                sandbox_command_tool(
                    description="Run inspection commands and safe file writes."
                )
            ],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: run_config,
    )

    read_only_key = read_only_runner._runner_call_cache_key(
        request,
        agent=read_only_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    write_allowed_key = write_allowed_runner._runner_call_cache_key(
        request,
        agent=write_allowed_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    tool_identity = read_only_runner._agent_cache_identity(read_only_runner.agent)[
        "tools"
    ][0]

    assert read_only_key != write_allowed_key
    assert tool_identity["description"] == "Run read-only inspection commands only."
    assert (
        tool_identity["params_json_schema"]["properties"]["command"]["description"]
        == "Non-empty shell command to run inside the active stack sandbox."
    )


def test_runner_call_cache_identity_varies_by_handoff_sandbox_tool_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_active_sandbox_identity(monkeypatch)
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    def make_runner(max_chars: int) -> KitaruRunner:
        handoff_agent = Agent(
            name="sandbox-handoff-child",
            model=StaticTextModel("handoff ok"),
            tools=[sandbox_command_tool(max_chars=max_chars)],
        )
        return KitaruRunner(
            Agent(
                name="sandbox-handoff-parent",
                model=StaticTextModel("parent ok"),
                handoffs=cast(Any, [handoff(handoff_agent)]),
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    small_handoff_runner = make_runner(100)
    large_handoff_runner = make_runner(20_000)

    small_handoff_key = small_handoff_runner._runner_call_cache_key(
        request,
        agent=small_handoff_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    large_handoff_key = large_handoff_runner._runner_call_cache_key(
        request,
        agent=large_handoff_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )

    assert small_handoff_key != large_handoff_key


def test_runner_call_cache_identity_varies_by_handoff_input_filter() -> None:
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)
    child_agent = Agent(
        name="filtered-handoff-child",
        model=StaticTextModel("handoff ok"),
    )

    def support_only(input_data: Any) -> Any:
        return input_data

    def billing_only(input_data: Any) -> Any:
        return input_data

    def make_runner(input_filter: Any) -> KitaruRunner:
        return KitaruRunner(
            Agent(
                name="filtered-handoff-parent",
                model=StaticTextModel("parent ok"),
                handoffs=cast(
                    Any,
                    [handoff(child_agent, input_filter=input_filter)],
                ),
            ),
            checkpoint_strategy="runner_call",
            run_config_factory=lambda: run_config,
        )

    support_runner = make_runner(support_only)
    billing_runner = make_runner(billing_only)

    support_key = support_runner._runner_call_cache_key(
        request,
        agent=support_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    billing_key = billing_runner._runner_call_cache_key(
        request,
        agent=billing_runner.agent,
        run_config=run_config,
        context_cache_identity=None,
        surface="run",
    )
    handoff_identity = support_runner._agent_cache_identity(support_runner.agent)[
        "handoffs"
    ][0]

    assert support_key != billing_key
    assert handoff_identity["input_filter"]["qualname"].endswith("support_only")
    assert "on_invoke_handoff" in handoff_identity


def test_runner_call_agent_cache_identity_handles_handoff_cycles() -> None:
    agent = Agent(name="cyclic-agent", model=StaticTextModel("ok"))
    object.__setattr__(agent, "handoffs", [agent])
    runner = KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    identity = runner._agent_cache_identity(agent)

    assert identity["handoffs"] == [
        {
            "kind": "agent",
            "agent": {
                "name": "cyclic-agent",
                "python_type": "agents.agent.Agent",
                "recursive_reference": True,
            },
        }
    ]


def test_runner_call_agent_cache_identity_bounds_deep_handoff_chains() -> None:
    max_depth = openai_agent_module._MAX_HANDOFF_CACHE_IDENTITY_DEPTH
    agents = [
        Agent(name=f"deep-agent-{index}", model=StaticTextModel("ok"))
        for index in range(max_depth + 3)
    ]
    for index, agent in enumerate(agents[:-1]):
        object.__setattr__(agent, "handoffs", cast(Any, [handoff(agents[index + 1])]))
    runner = KitaruRunner(
        agents[0],
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    first_identity = runner._agent_cache_identity(agents[0])
    second_identity = runner._agent_cache_identity(agents[0])
    current_agent_identity = first_identity
    for _ in range(max_depth + 1):
        current_agent_identity = current_agent_identity["handoffs"][0]["agent"]

    assert first_identity == second_identity
    assert current_agent_identity == {
        "name": f"deep-agent-{max_depth + 1}",
        "python_type": "agents.agent.Agent",
        "max_handoff_depth_exceeded": True,
    }


def test_runner_call_cache_identity_varies_by_structural_context() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="cache-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    team_a_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=runner._context_cache_identity(
            WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
        ),
        surface="run",
    )
    team_b_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=runner._context_cache_identity(
            WorkerContext(team_id="team-b", user_id="user-1", thread_id="thread-1")
        ),
        surface="run",
    )

    assert team_a_key != team_b_key


def test_custom_context_cache_identity_can_ignore_per_run_fields() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="projected-agent"),
        checkpoint_strategy="runner_call",
        context_cache_identity=lambda ctx: {
            "team_id": ctx.team_id,
            "user_id": ctx.user_id,
            "thread_id": ctx.thread_id,
            "worker_name": ctx.worker_name,
            "is_background": ctx.is_background,
            "group_chat": ctx.group_chat,
            "chat_mode": ctx.chat_mode,
            "scope_user_id": ctx.scope_user_id,
            "scope_group_id": ctx.scope_group_id,
            "plugin": ctx.plugin,
            "project_id": ctx.project_id,
            "tool_settings": ctx.tool_settings,
        },
    )

    first = runner._context_cache_identity(
        WorkerContext(
            team_id="team-a",
            user_id="user-1",
            thread_id="thread-1",
            tool_settings={"enabled": True, "limit": 3},
            message_id="msg-1",
            doc_id="doc-1",
        )
    )
    second = runner._context_cache_identity(
        WorkerContext(
            team_id="team-a",
            user_id="user-1",
            thread_id="thread-1",
            tool_settings={"enabled": True, "limit": 3},
            message_id="msg-2",
            doc_id="doc-2",
        )
    )

    assert first == second
    assert "message_id" not in first
    assert "doc_id" not in first
    assert first["tool_settings"] == {"enabled": True, "limit": 3}


def test_default_context_cache_identity_uses_pure_data_structure() -> None:
    runner = KitaruRunner(SimpleNamespace(name="data-agent"))

    first = runner._context_cache_identity(
        WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    )
    second = runner._context_cache_identity(
        WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    )

    assert first == second
    assert first["fields"]["team_id"] == "team-a"


def test_context_cache_identity_sorts_sets_deterministically() -> None:
    runner = KitaruRunner(SimpleNamespace(name="set-agent"))

    first = runner._context_cache_identity({"enabled_tools": {"search", "crm"}})
    second = runner._context_cache_identity({"enabled_tools": {"crm", "search"}})

    assert first == second
    assert first["enabled_tools"] == {
        "collection_type": "set",
        "items": ["crm", "search"],
    }


def test_context_cache_identity_is_cycle_safe() -> None:
    runner = KitaruRunner(SimpleNamespace(name="cycle-agent"))
    cycle: list[Any] = []
    cycle.append(cycle)

    identity = runner._context_cache_identity({"cycle": cycle})

    cycle_item = identity["cycle"]["items"][0]
    assert cycle_item["serialization_error"] == "cycle_detected"
    assert "opaque_cache_token" in cycle_item


def test_context_cache_identity_stops_before_oversized_collections() -> None:
    from kitaru.adapters.openai_agents._serialization import stable_cache_identity

    identity = stable_cache_identity(
        ["first", "second", "third"],
        opaque_objects_unique=True,
        max_items=2,
    )

    assert identity["serialization_error"] == "max_items_exceeded"
    assert "items" not in identity
    assert "first" not in repr(identity)


def test_behavior_cache_identity_stops_before_oversized_collections() -> None:
    identity = openai_agent_module._behavior_value_cache_identity(
        list(range(openai_agent_module._MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS + 1))
    )

    assert identity == {
        "collection_type": "list",
        "item_count": openai_agent_module._MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS + 1,
        "serialization_error": "max_items_exceeded",
    }


def test_behavior_cache_identity_bounds_depth_but_keeps_callables_distinct() -> None:
    def first_callback() -> None:
        return None

    def second_callback() -> None:
        return None

    def nested(value: Any) -> Any:
        for _ in range(openai_agent_module._MAX_BEHAVIOR_CACHE_IDENTITY_DEPTH):
            value = [value]
        return value

    first = openai_agent_module._behavior_value_cache_identity(nested(first_callback))
    second = openai_agent_module._behavior_value_cache_identity(nested(second_callback))

    assert first != second
    first_marker = first
    for _ in range(openai_agent_module._MAX_BEHAVIOR_CACHE_IDENTITY_DEPTH):
        first_marker = first_marker["items"][0]
    assert first_marker["python_type"] == (
        f"{type(first_callback).__module__}.{type(first_callback).__qualname__}"
    )
    assert first_marker["module"] == first_callback.__module__
    assert first_marker["qualname"].endswith("first_callback")
    assert "code" not in first_marker
    assert first_marker["serialization_error"] == "max_depth_exceeded"
    assert "object_id" not in first_marker


def test_opaque_context_cache_identity_is_distinct_per_object() -> None:
    class OpaqueContext:
        pass

    runner = KitaruRunner(SimpleNamespace(name="opaque-agent"))

    first = runner._context_cache_identity(OpaqueContext())
    second = runner._context_cache_identity(OpaqueContext())

    assert first["python_type"] == second["python_type"]
    assert first != second


def test_runner_call_strategy_preserves_structured_final_output() -> None:
    model = StaticTextModel('{"verdict":"safe","confidence":0.91}')
    agent = Agent(
        name="runner-structured",
        model=model,
        output_type=StructuredSupportAnswer,
    )
    runner = KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    result = runner.run_sync(OpenAIRunRequest.start("return structured result"))

    assert result.status == "completed"
    assert isinstance(result.final_output, StructuredSupportAnswer)
    assert result.final_output.verdict == "safe"
    assert result.final_output.confidence == 0.91


def test_runner_call_strategy_checkpoints_outer_runner_and_caches(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = monkeypatch
    model = StaticTextModel("outer call result")
    agent_name = f"openai_runner_call_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def runner_call_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    first = runner_call_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    names = _step_names(first_hydrated)
    assert any("openai_runner_call" in name for name in names)
    assert not any("openai_model_call" in name for name in names)
    assert model.call_count == 1

    second = runner_call_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert model.call_count == 1


def test_runner_call_strategy_does_not_invoke_calls_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as openai_agent_module
    import kitaru.adapters.openai_agents._model as openai_model_module
    import kitaru.adapters.openai_agents._tools as openai_tools_module

    sandbox_calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(command: str, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append({"command": command, **kwargs})
        return _fake_sandbox_result()

    def fail_model_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap model calls")

    def fail_tool_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap function tools")

    def fake_run_openai_agent_sync(**kwargs: Any) -> SimpleNamespace:
        tool = kwargs["agent"].tools[0]
        tool_result = asyncio.run(
            tool.on_invoke_tool(
                SimpleNamespace(),
                '{"command":"python --version"}',
            )
        )
        payload = json.loads(tool_result)
        assert payload["exit_code"] == 0
        assert payload["stdout"] == "Python 3.12.0\n"
        return SimpleNamespace(final_output="ok")

    monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
    monkeypatch.setattr(
        openai_model_module,
        "kitaruify_openai_model",
        fail_model_wrapper,
    )
    monkeypatch.setattr(
        openai_tools_module,
        "kitaruify_openai_tools",
        fail_tool_wrapper,
    )
    monkeypatch.setattr(
        openai_agent_module,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )

    runner = KitaruRunner(
        Agent(
            name="coarse",
            model=StaticTextModel("ok"),
            tools=[sandbox_command_tool(max_chars=123)],
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    prompt = f"hello-{uuid4().hex}"
    result = runner.run_sync(OpenAIRunRequest.start(prompt))

    assert result.status == "completed"
    assert result.final_output == "ok"
    assert sandbox_calls == [
        {
            "command": "python --version",
            "cwd": None,
            "max_chars": 123,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
        }
    ]


def test_interruption_summary_omits_payloads_when_capture_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    interruption = {
        "tool_name": "send_email",
        "call_id": "call_sensitive_email",
        "message": "approval required",
        "arguments": {"message": "SECRET_DO_NOT_LOG"},
    }
    sdk_result = SimpleNamespace(
        interruptions=[interruption],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    assert result.status == "interrupted"
    summary = result.interruptions[0]
    assert summary.tool_name == "send_email"
    assert summary.call_id == "call_sensitive_email"
    assert summary.message == "approval required"
    assert summary.arguments is None
    assert summary.arguments_preview is None
    assert "SECRET_DO_NOT_LOG" not in result.model_dump_json()


def test_interruption_summary_does_not_mine_nested_identity_when_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    sdk_result = SimpleNamespace(
        interruptions=[
            {
                "arguments": {
                    "name": "SECRET_DO_NOT_LOG_NAME",
                    "call_id": "SECRET_DO_NOT_LOG_CALL_ID",
                },
            }
        ],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    summary = result.interruptions[0]
    assert summary.tool_name is None
    assert summary.call_id is None
    assert summary.arguments is None
    assert summary.arguments_preview is None
    serialized = result.model_dump_json()
    assert "SECRET_DO_NOT_LOG_NAME" not in serialized
    assert "SECRET_DO_NOT_LOG_CALL_ID" not in serialized


def test_interruption_summary_does_not_promote_argument_message_when_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    sdk_result = SimpleNamespace(
        interruptions=[
            {
                "tool_name": "send_email",
                "call_id": "call_sensitive_email",
                "arguments": {"message": "SECRET_DO_NOT_LOG"},
            }
        ],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    summary = result.interruptions[0]
    assert summary.message is None
    assert summary.arguments is None
    assert summary.arguments_preview is None
    assert "SECRET_DO_NOT_LOG" not in result.model_dump_json()


def test_interruption_summary_keeps_payloads_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    interruption = {
        "tool_name": "send_email",
        "call_id": "call_sensitive_email",
        "message": "approval required",
        "arguments": {"message": "SECRET_VISIBLE"},
    }
    sdk_result = SimpleNamespace(
        interruptions=[interruption],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
    )

    summary = result.interruptions[0]
    assert summary.arguments is not None
    assert summary.arguments_preview is not None
    assert "SECRET_VISIBLE" in result.model_dump_json()


def test_run_state_envelope_serialization_and_approval_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._runner as openai_runner

    class FakeState:
        def __init__(self) -> None:
            self._current_step = SimpleNamespace(interruptions=["first", "second"])
            self.approved: list[object] = []
            self.rejected: list[tuple[object, str | None]] = []

        def to_json(self, **kwargs: object) -> dict[str, object]:
            return {
                "current_turn": 1,
                "strict_context": kwargs["strict_context"],
                "has_context_serializer": callable(kwargs["context_serializer"]),
            }

        def approve(self, item: object) -> None:
            self.approved.append(item)

        def reject(
            self,
            item: object,
            *,
            rejection_message: str | None = None,
        ) -> None:
            self.rejected.append((item, rejection_message))

    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")
    state = FakeState()

    envelope = openai_runner.serialize_run_state(
        state,
        strict_sdk_version=True,
        context_serializer=lambda value: value,
        strict_context=True,
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(interruption_index=1, approve=True),
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(
            interruption_index=0,
            approve=False,
            rejection_message="nope",
        ),
    )

    assert envelope.agents_sdk_version == "0.15.0"
    assert envelope.state_json["current_turn"] == 1
    assert state.approved == ["second"]
    assert state.rejected == [("first", "nope")]


def test_runner_resume_applies_decision_before_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_state = SimpleNamespace(
        _current_step=SimpleNamespace(interruptions=["approval-item"]),
        approved=[],
    )

    def approve(item: object) -> None:
        fake_state.approved.append(item)

    fake_state.approve = approve
    seen_input: list[object] = []

    runner = KitaruRunner(
        SimpleNamespace(name="resume-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._sdk_input_sync.__globals__,
        "deserialize_run_state_sync",
        lambda *_args, **_kwargs: fake_state,
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        lambda **kwargs: (
            seen_input.append(kwargs["input"])
            or SimpleNamespace(final_output="resumed")
        ),
    )
    request = OpenAIRunRequest.resume(
        OpenAIRunStateEnvelope(
            agents_sdk_version="0.15.0",
            state_json={"current_turn": 1},
        ),
        OpenAIApprovalDecision(approve=True),
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert result.final_output == "resumed"
    assert fake_state.approved == ["approval-item"]
    assert seen_input == [fake_state]
