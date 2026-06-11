"""Helpers for caller-owned Gemini function calls run in Kitaru sandboxes."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias, cast

from pydantic import BaseModel, ConfigDict

import kitaru
from kitaru.config import DEFAULT_SANDBOX_COMMAND_MAX_CHARS, SandboxCommandResult
from kitaru.errors import KitaruUsageError

from ._types import (
    GeminiInteractionFunctionCall,
    GeminiInteractionRequest,
    GeminiInteractionResult,
)

SandboxCommand: TypeAlias = str | Sequence[str]
SandboxCommandBuilder: TypeAlias = Callable[
    [GeminiInteractionFunctionCall], SandboxCommand
]
SandboxPayloadBuilder: TypeAlias = Callable[
    [GeminiInteractionFunctionCall, SandboxCommandResult], Any
]
SandboxFunctionRegistry: TypeAlias = (
    Mapping[str, "GeminiSandboxFunctionSpec | SandboxCommand | SandboxCommandBuilder"]
    | Sequence["GeminiSandboxFunctionSpec"]
)
DEFAULT_GEMINI_FUNCTION_RESULT_OUTPUT_MAX_CHARS = 4_000


@dataclass(frozen=True)
class GeminiSandboxFunctionSpec:
    """Registered Gemini function name and sandbox command construction.

    The command can be static, such as ``"python --version"``, or a callable
    that receives only safe call metadata: step index, step type, call ID, and
    function name. This helper intentionally does not expose model-supplied
    function arguments in v1.
    """

    function_name: str
    command: SandboxCommand | SandboxCommandBuilder
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    max_chars: int = DEFAULT_SANDBOX_COMMAND_MAX_CHARS
    cleanup: Literal["destroy", "close"] = "destroy"
    result_payload_builder: SandboxPayloadBuilder | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.function_name, str) or not self.function_name.strip():
            raise KitaruUsageError(
                "Gemini sandbox function specs require a non-empty function_name."
            )

    def build_command(self, call: GeminiInteractionFunctionCall) -> SandboxCommand:
        """Return the sandbox command for a matched Gemini function call."""
        command = self.command
        if isinstance(command, str):
            return command
        if isinstance(command, Sequence):
            return cast(Sequence[str], command)
        builder = cast(SandboxCommandBuilder, command)
        return builder(call)

    def build_payload(
        self,
        call: GeminiInteractionFunctionCall,
        sandbox_result: SandboxCommandResult,
    ) -> Any:
        """Return the payload that will be sent back to Gemini."""
        if self.result_payload_builder is not None:
            return self.result_payload_builder(call, sandbox_result)
        return _default_function_result_payload(sandbox_result)


class GeminiSandboxFunctionExecution(BaseModel):
    """Result of one caller-owned Gemini function executed in a Kitaru sandbox."""

    model_config = ConfigDict(extra="forbid")

    call: GeminiInteractionFunctionCall
    sandbox_result: SandboxCommandResult
    function_result_payload: Any
    function_result_request: GeminiInteractionRequest


def execute_gemini_sandbox_function_call(
    result: GeminiInteractionResult,
    functions: SandboxFunctionRegistry,
    *,
    call_id: str | None = None,
    model: str | None = None,
    agent: str | None = None,
    environment: str | dict[str, Any] | None = None,
    tools: list[dict[str, Any]] | None = None,
    system_instruction: str | None = None,
    generation_config: dict[str, Any] | None = None,
    agent_config: dict[str, Any] | None = None,
    response_format: dict[str, Any] | None = None,
    response_mime_type: str | None = None,
    background: bool = False,
    store: bool = True,
    timeout_s: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> GeminiSandboxFunctionExecution:
    """Run one registered Gemini custom function in Kitaru's active sandbox.

    The helper does not call Gemini. It turns one ``requires_action`` result into
    one sandbox command execution and returns a matching
    ``GeminiInteractionRequest.function_result(...)`` for the caller to send with
    the existing Gemini runner.
    """
    if result.status != "requires_action":
        raise KitaruUsageError(
            "Gemini sandbox function execution requires a result with "
            "status='requires_action'."
        )
    if result.agent is not None:
        raise KitaruUsageError(
            "Gemini sandbox function execution only supports caller-owned custom "
            "functions from model interactions. Provider-agent and Antigravity "
            "results remain Google-owned and cannot be redirected into Kitaru's "
            "sandbox helper."
        )
    if result.interaction_id is None:
        raise KitaruUsageError(
            "Gemini sandbox function execution requires result.interaction_id so "
            "the function_result request can continue the same interaction."
        )

    registry = _normalize_function_registry(functions)
    call, spec = _select_function_call(result, registry, call_id=call_id)
    target_model, target_agent = _resolve_continuation_target(
        result,
        model_override=model,
        agent_override=agent,
    )
    sandbox_result = kitaru.run_sandbox_command(
        spec.build_command(call),
        cwd=spec.cwd,
        env=spec.env,
        max_chars=spec.max_chars,
        cleanup=spec.cleanup,
    )
    payload = spec.build_payload(call, sandbox_result)
    request = GeminiInteractionRequest.function_result(
        previous_interaction_id=result.interaction_id,
        function_call_id=call.call_id,
        function_name=call.function_name,
        function_result=payload,
        model=target_model,
        agent=target_agent,
        environment=environment,
        tools=tools,
        system_instruction=system_instruction,
        generation_config=generation_config,
        agent_config=agent_config,
        response_format=response_format,
        response_mime_type=response_mime_type,
        background=background,
        store=store,
        timeout_s=timeout_s,
        metadata=metadata,
    )
    return GeminiSandboxFunctionExecution(
        call=call,
        sandbox_result=sandbox_result,
        function_result_payload=payload,
        function_result_request=request,
    )


def _resolve_continuation_target(
    result: GeminiInteractionResult,
    *,
    model_override: str | None,
    agent_override: str | None,
) -> tuple[str | None, str | None]:
    target_model = result.model if model_override is None else model_override
    target_agent = result.agent if agent_override is None else agent_override
    if (target_model is None) == (target_agent is None):
        raise KitaruUsageError(
            "Gemini sandbox function execution needs exactly one continuation "
            "target. Pass model=... for model interactions or agent=... for a "
            "future caller-owned agent path."
        )
    return target_model, target_agent


def _default_function_result_payload(
    sandbox_result: SandboxCommandResult,
) -> dict[str, Any]:
    stdout, stdout_payload_truncated = _clip_payload_output(sandbox_result.stdout)
    stderr, stderr_payload_truncated = _clip_payload_output(sandbox_result.stderr)
    return {
        "ok": sandbox_result.exit_code == 0,
        "exit_code": sandbox_result.exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": (
            sandbox_result.stdout_truncated or stdout_payload_truncated
        ),
        "stderr_truncated": (
            sandbox_result.stderr_truncated or stderr_payload_truncated
        ),
        "stdout_payload_truncated": stdout_payload_truncated,
        "stderr_payload_truncated": stderr_payload_truncated,
        "payload_output_max_chars": DEFAULT_GEMINI_FUNCTION_RESULT_OUTPUT_MAX_CHARS,
        "cleanup": {
            "policy": sandbox_result.cleanup,
            "succeeded": sandbox_result.cleanup_succeeded,
            "error": sandbox_result.cleanup_error,
        },
    }


def _clip_payload_output(value: str) -> tuple[str, bool]:
    if len(value) <= DEFAULT_GEMINI_FUNCTION_RESULT_OUTPUT_MAX_CHARS:
        return value, False
    return value[:DEFAULT_GEMINI_FUNCTION_RESULT_OUTPUT_MAX_CHARS], True


def _normalize_function_registry(
    functions: SandboxFunctionRegistry,
) -> dict[str, GeminiSandboxFunctionSpec]:
    if isinstance(functions, Mapping):
        specs: dict[str, GeminiSandboxFunctionSpec] = {}
        for name, value in functions.items():
            if not isinstance(name, str) or not name.strip():
                raise KitaruUsageError(
                    "Gemini sandbox function registry names must be non-empty strings."
                )
            if isinstance(value, GeminiSandboxFunctionSpec):
                if value.function_name != name:
                    raise KitaruUsageError(
                        "Gemini sandbox function registry key "
                        f"{name!r} does not match spec.function_name "
                        f"{value.function_name!r}."
                    )
                specs[name] = value
            else:
                specs[name] = GeminiSandboxFunctionSpec(
                    function_name=name,
                    command=cast(SandboxCommand | SandboxCommandBuilder, value),
                )
        if specs:
            return specs
    elif isinstance(functions, Sequence):
        specs = {}
        for spec in functions:
            if not isinstance(spec, GeminiSandboxFunctionSpec):
                raise KitaruUsageError(
                    "Gemini sandbox function registry sequences must contain "
                    "GeminiSandboxFunctionSpec objects."
                )
            if spec.function_name in specs:
                raise KitaruUsageError(
                    f"Gemini sandbox function {spec.function_name!r} is registered "
                    "more than once."
                )
            specs[spec.function_name] = spec
        if specs:
            return specs

    raise KitaruUsageError(
        "Gemini sandbox function execution requires at least one registered function."
    )


def _select_function_call(
    result: GeminiInteractionResult,
    registry: Mapping[str, GeminiSandboxFunctionSpec],
    *,
    call_id: str | None,
) -> tuple[GeminiInteractionFunctionCall, GeminiSandboxFunctionSpec]:
    candidates = result.function_call_steps
    if not candidates:
        raise KitaruUsageError(
            "Gemini result has status='requires_action' but no function-call steps "
            "were reported."
        )

    selected_steps = [
        step for step in candidates if call_id is None or step.call_id == call_id
    ]
    if call_id is not None and not selected_steps:
        raise KitaruUsageError(
            f"No Gemini function call with call_id={call_id!r} was reported."
        )

    matches: list[tuple[GeminiInteractionFunctionCall, GeminiSandboxFunctionSpec]] = []
    missing_call_id_names: list[str] = []
    missing_name_ids: list[str] = []
    unregistered_names: list[str] = []

    for step in selected_steps:
        function_name = step.tool_name
        if function_name is None:
            missing_name_ids.append(step.call_id or f"step #{step.index}")
            continue
        if function_name not in registry:
            unregistered_names.append(function_name)
            continue
        if step.call_id is None:
            missing_call_id_names.append(function_name)
            continue
        call = GeminiInteractionFunctionCall.from_step_summary(step)
        matches.append((call, registry[function_name]))

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        call_ids = ", ".join(call.call_id for call, _ in matches)
        raise KitaruUsageError(
            "Gemini reported multiple registered function calls. Pass call_id=... "
            f"to choose one of: {call_ids}."
        )
    if missing_call_id_names:
        names = ", ".join(sorted(set(missing_call_id_names)))
        raise KitaruUsageError(
            "Gemini reported registered function call(s) without call IDs: "
            f"{names}. Kitaru needs a call ID to send the function_result back."
        )
    if missing_name_ids:
        ids = ", ".join(missing_name_ids)
        raise KitaruUsageError(
            "Gemini reported function-call step(s) without function names: "
            f"{ids}. Registering sandbox commands by name requires tool_name/name."
        )
    if unregistered_names:
        names = ", ".join(sorted(set(unregistered_names)))
        registered = ", ".join(sorted(registry))
        raise KitaruUsageError(
            "Gemini requested function call(s) that are not registered for sandbox "
            f"execution: {names}. Registered functions: {registered}."
        )
    raise KitaruUsageError("No Gemini function call matched the sandbox registry.")


__all__ = [
    "GeminiSandboxFunctionExecution",
    "GeminiSandboxFunctionSpec",
    "execute_gemini_sandbox_function_call",
]
