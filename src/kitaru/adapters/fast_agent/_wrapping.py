"""Internal fast-agent wrapping helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

FastAgentCallKind = Literal["model", "tool"]

_AGENT_WRAPPED_ATTR = "_kitaru_fast_agent_wrapped"


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


class FastAgentCallRecorder(Protocol):
    """Callable that can record or replace an intercepted fast-agent call."""

    def __call__(
        self,
        call: FastAgentCall,
        proceed: Callable[[], Any],
    ) -> Any: ...


def passthrough_call_recorder(
    _call: FastAgentCall,
    proceed: Callable[[], Any],
) -> Any:
    """Run the original fast-agent call without recording anything."""
    return proceed()


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

    @property
    def original_llm(self) -> Any:
        return self._kitaru_fast_agent_original_llm

    def generate(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_model_call("generate", args, kwargs)

    def structured(self, *args: Any, **kwargs: Any) -> Any:
        return self._record_model_call("structured", args, kwargs)

    def _record_model_call(
        self,
        operation: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        llm = self._kitaru_fast_agent_original_llm
        call = FastAgentCall(
            agent_name=self._kitaru_fast_agent_agent_name,
            kind="model",
            operation=operation,
            args=args,
            kwargs=dict(kwargs),
            model_name=_optional_str(getattr(llm, "model_name", None)),
            provider=_optional_str(getattr(llm, "provider", None)),
        )

        def proceed() -> Any:
            method = getattr(llm, operation)
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
    resolved_recorder = recorder or passthrough_call_recorder
    for agent in _iter_app_agents(app):
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

    resolved_recorder = recorder or passthrough_call_recorder
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


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) else None
