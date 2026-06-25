"""Internal fast-agent wrapping helpers."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, Protocol, cast

from kitaru.errors import KitaruUsageError
from kitaru.runtime import _is_inside_checkpoint as is_inside_checkpoint
from kitaru.runtime import _is_inside_flow as is_inside_flow

from ._utils import (
    CheckpointConfig,
    checkpoint_cache_key,
    checkpoint_input_value,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    safe_step_name,
    with_default_type,
)

FastAgentCallKind = Literal["model", "tool"]

_AGENT_WRAPPED_ATTR = "_kitaru_fast_agent_wrapped"
_OPTIONAL_LLM_OPERATIONS = frozenset({"structured", "structured_schema"})


@dataclass(frozen=True)
class FastAgentCall:
    """Description of one intercepted fast-agent model or tool call."""

    agent_name: str
    kind: FastAgentCallKind
    operation: str
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]
    tool_name: str | None = None
    model_name: str | None = None
    provider: str | None = None
    is_async: bool = False


class FastAgentCallRecorder(Protocol):
    """Callable that can record or replace an intercepted fast-agent call."""

    def __call__(
        self,
        call: FastAgentCall,
        proceed: Callable[[], Any],
    ) -> Any: ...


def passthrough_call_recorder(
    call: FastAgentCall,
    proceed: Callable[[], Any],
) -> Any:
    """Run the original fast-agent call without recording anything."""
    del call
    return proceed()


class KitaruFastAgentCallRecorder:
    """Record fast-agent model and tool calls as Kitaru checkpoints."""

    def __init__(
        self,
        *,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
    ) -> None:
        self._model_checkpoint_config = model_checkpoint_config or {}
        self._tool_checkpoint_config = tool_checkpoint_config or {}

    def __call__(
        self,
        call: FastAgentCall,
        proceed: Callable[[], Any],
    ) -> Any:
        if not is_inside_flow() or is_inside_checkpoint():
            return proceed()

        call_input = _checkpoint_call_input(call)
        cache_key = checkpoint_cache_key(call_input)
        checkpoint_inputs = {"call_input": call_input}
        step_name = _checkpoint_step_name(call)
        config = with_default_type(
            self._checkpoint_config(call),
            "llm_call" if call.kind == "model" else "tool_call",
        )

        if call.is_async:

            async def async_body() -> Any:
                result = proceed()
                if inspect.isawaitable(result):
                    return await result
                return result

            return run_async_in_checkpoint(
                config=config,
                step_name=step_name,
                body=async_body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )

        def sync_body() -> Any:
            result = proceed()
            if inspect.isawaitable(result):
                raise TypeError(
                    "fast-agent call was marked sync but returned an awaitable."
                )
            return result

        return run_sync_in_checkpoint(
            config=config,
            step_name=step_name,
            body=sync_body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _checkpoint_config(self, call: FastAgentCall) -> CheckpointConfig:
        config = (
            self._model_checkpoint_config
            if call.kind == "model"
            else self._tool_checkpoint_config
        )
        return cast(CheckpointConfig, dict(config))


def kitaru_call_recorder(
    *,
    model_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config: CheckpointConfig | None = None,
) -> KitaruFastAgentCallRecorder:
    """Return the default Kitaru checkpoint recorder for calls-mode wrapping."""
    return KitaruFastAgentCallRecorder(
        model_checkpoint_config=model_checkpoint_config,
        tool_checkpoint_config=tool_checkpoint_config,
    )


class _FastAgentLLMWrapper:
    """Proxy for an attached fast-agent LLM object."""

    def __init__(
        self,
        llm: Any,
        *,
        agent_name: str,
        recorder: FastAgentCallRecorder,
    ) -> None:
        self._kitaru_fast_agent_original_llm = llm
        self._kitaru_fast_agent_agent_name = agent_name
        self._kitaru_fast_agent_recorder = recorder

    def __getattribute__(self, name: str) -> Any:
        if name in _OPTIONAL_LLM_OPERATIONS:
            llm = object.__getattribute__(self, "_kitaru_fast_agent_original_llm")
            if not callable(getattr(llm, name, None)):
                raise AttributeError(name)
        return object.__getattribute__(self, name)

    @property
    def original_llm(self) -> Any:
        return self._kitaru_fast_agent_original_llm

    def generate(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_model_call("generate", args, kwargs)

    def structured(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_model_call("structured", args, kwargs)

    def structured_schema(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_model_call("structured_schema", args, kwargs)

    def _record_model_call(
        self,
        operation: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        llm = self._kitaru_fast_agent_original_llm
        method = getattr(llm, operation)
        call = FastAgentCall(
            agent_name=self._kitaru_fast_agent_agent_name,
            kind="model",
            operation=operation,
            args=args,
            kwargs=dict(kwargs),
            model_name=_optional_str(getattr(llm, "model_name", None)),
            provider=_optional_str(getattr(llm, "provider", None)),
            is_async=_is_async_callable(method),
        )

        def proceed() -> Any:
            return method(*args, **kwargs)

        return self._kitaru_fast_agent_recorder(call, proceed)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._kitaru_fast_agent_original_llm, name)


def wrap_fast_agent_app(
    app: Any,
    *,
    recorder: FastAgentCallRecorder | None = None,
) -> Any:
    """Wrap every active agent discoverable from a fast-agent AgentApp."""
    resolved_recorder = cast(
        FastAgentCallRecorder, recorder or passthrough_call_recorder
    )
    agents = list(_iter_app_agents(app))
    if not agents:
        raise KitaruUsageError(
            "Could not discover fast-agent agents on the object yielded by "
            "`FastAgent.run()`. Kitaru expects an AgentApp-like object with a "
            "non-empty `_agents`, `agents`, or `active_agents` mapping."
        )
    for agent in agents:
        wrap_fast_agent_agent(agent, recorder=resolved_recorder)
    return app


def wrap_fast_agent_agent(
    agent: Any,
    *,
    recorder: FastAgentCallRecorder | None = None,
) -> Any:
    """Install model/tool/clone wrappers on one fast-agent-like agent object."""
    if getattr(agent, _AGENT_WRAPPED_ATTR, False):
        return agent

    resolved_recorder = cast(
        FastAgentCallRecorder, recorder or passthrough_call_recorder
    )
    agent_name = _agent_name(agent)
    _wrap_attached_llm(agent, agent_name=agent_name, recorder=resolved_recorder)
    _wrap_call_tool(agent, agent_name=agent_name, recorder=resolved_recorder)
    _wrap_spawn_detached_instance(agent, recorder=resolved_recorder)
    setattr(agent, _AGENT_WRAPPED_ATTR, True)
    return agent


def _wrap_attached_llm(
    agent: Any,
    *,
    agent_name: str,
    recorder: FastAgentCallRecorder,
) -> None:
    llm_attr, llm = _resolve_llm_attr(agent)
    if llm is None or isinstance(llm, _FastAgentLLMWrapper):
        return
    setattr(
        agent,
        llm_attr,
        _FastAgentLLMWrapper(llm, agent_name=agent_name, recorder=recorder),
    )


def _wrap_call_tool(
    agent: Any,
    *,
    agent_name: str,
    recorder: FastAgentCallRecorder,
) -> None:
    call_tool = getattr(agent, "call_tool", None)
    if not callable(call_tool):
        return

    def wrapped_call_tool(*args: Any, **kwargs: Any) -> Any:
        tool_name = _tool_name_from_call(args, kwargs)
        call = FastAgentCall(
            agent_name=agent_name,
            kind="tool",
            operation="call_tool",
            args=args,
            kwargs=dict(kwargs),
            tool_name=tool_name,
            is_async=_is_async_callable(call_tool),
        )
        return recorder(call, lambda: call_tool(*args, **kwargs))

    agent.call_tool = wrapped_call_tool


def _wrap_spawn_detached_instance(
    agent: Any,
    *,
    recorder: FastAgentCallRecorder,
) -> None:
    spawn = getattr(agent, "spawn_detached_instance", None)
    if not callable(spawn):
        return

    async def wrapped_spawn_detached_instance(*args: Any, **kwargs: Any) -> Any:
        clone = await spawn(*args, **kwargs)
        return wrap_fast_agent_agent(clone, recorder=recorder)

    agent.spawn_detached_instance = wrapped_spawn_detached_instance


def _iter_app_agents(app: Any) -> Iterable[Any]:
    for attr_name in ("_agents", "agents", "active_agents"):
        value = getattr(app, attr_name, None)
        if isinstance(value, Mapping):
            yield from value.values()
            return


def _resolve_llm_attr(agent: Any) -> tuple[str, Any | None]:
    if hasattr(agent, "_llm"):
        return "_llm", getattr(agent, "_llm", None)
    return "llm", getattr(agent, "llm", None)


def _agent_name(agent: Any) -> str:
    for attr_name in ("name", "_name"):
        value = getattr(agent, attr_name, None)
        if isinstance(value, str) and value.strip():
            return value
    config = getattr(agent, "config", None)
    config_name = getattr(config, "name", None)
    if isinstance(config_name, str) and config_name.strip():
        return config_name
    return type(agent).__name__


def _tool_name_from_call(
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> str | None:
    if args and isinstance(args[0], str):
        return args[0]
    name = kwargs.get("name")
    return name if isinstance(name, str) else None


def _is_async_callable(value: Any) -> bool:
    unwrapped = value
    while isinstance(unwrapped, partial):
        unwrapped = unwrapped.func
    with suppress(ValueError):
        unwrapped = inspect.unwrap(unwrapped)
    if inspect.iscoroutinefunction(unwrapped):
        return True
    if inspect.isfunction(unwrapped) or inspect.ismethod(unwrapped):
        return False
    if not callable(unwrapped):
        return False
    call = unwrapped.__call__
    with suppress(ValueError):
        call = inspect.unwrap(call)
    return inspect.iscoroutinefunction(call)


def _checkpoint_call_input(call: FastAgentCall) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "agent_name": call.agent_name,
        "kind": call.kind,
        "operation": call.operation,
        "args": checkpoint_input_value(call.args),
        "kwargs": checkpoint_input_value(call.kwargs),
    }
    if call.tool_name is not None:
        payload["tool_name"] = call.tool_name
    if call.model_name is not None:
        payload["model_name"] = call.model_name
    if call.provider is not None:
        payload["provider"] = call.provider
    return payload


def _checkpoint_step_name(call: FastAgentCall) -> str:
    if call.kind == "tool":
        operation = f"{call.tool_name or call.operation}_tool_call"
    else:
        operation = f"{call.operation}_model_call"
    return safe_step_name(f"{call.agent_name}_{operation}")


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None
