import asyncio
import os
import sys
import tempfile
import threading
import time
import uuid
import warnings
from dataclasses import dataclass
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any

import kitaru
from kitaru._source_aliases import build_pipeline_registration_name
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruRuntimeError, KitaruUsageError
from kitaru.flow import _is_multiple_terminal_steps_output_error

from pydantic_ai import _utils, messages as _messages, models, usage as _usage
from pydantic_ai.agent import AbstractAgent, AgentRun, WrapperAgent
from pydantic_ai.agent.abstract import (
    AgentInstructions,
    AgentMetadata,
    AgentModelSettings,
    EventStreamHandler,
)
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT, OutputSpec
from pydantic_ai.tools import AgentDepsT, AgentNativeTool, DeferredToolResults
from pydantic_ai.toolsets import AbstractToolset

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._logging import logger
from ._mcp_server import has_running_mcp_toolset
from ._model import KitaruModel, model_cache_run_context
from ._policy import CapturePolicy
from ._toolset import kitaruify_toolset
from ._threading_compat import inline_sync_tool_execution as _inline_sync_tool_execution
from ._tracking import get_current_tracker, tracker_scope
from ._utils import (
    CheckpointConfig,
    CheckpointStrategy,
    ToolCheckpointOverrides,
    checkpoint_input_value,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    turn_cache_key,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
    validate_tool_checkpoint_overrides,
)

# Pydantic AI renamed "built-in tools" to "native tools" in v1.95 while
# keeping the old ``builtin_tools=`` runtime keyword as a deprecated alias.
# Kitaru still exposes the old keyword for compatibility, so keep the local
# type name stable while importing the new upstream type.
AgentBuiltinTool = AgentNativeTool

_TRACKING_ACTIVE: ContextVar[bool] = ContextVar("kitaru_tracking_active", default=False)
_INTERNAL_ITER_ALLOWED: ContextVar[bool] = ContextVar(
    "kitaru_internal_iter_allowed", default=False
)
_INTERNAL_RUN_SYNC_DELEGATION: ContextVar[bool] = ContextVar(
    "kitaru_internal_run_sync_delegation", default=False
)


@dataclass(frozen=True)
class _TurnCheckpointCallConfig:
    cache_key: str
    checkpoint_inputs: dict[str, Any]
    checkpoint_config: CheckpointConfig
    force_turn_checkpoint: bool


# Auto-flow bodies keyed by uuid. The @kitaru.flow entrypoint must be module-
# level for ZenML dynamic-pipeline source resolution, so it can't close over
# its body — the registry bridges the gap. In-process only; remote stacks
# require an explicit @kitaru.flow.
_AUTO_FLOW_BODIES: dict[str, "_AutoFlowSlot"] = {}
# Intentionally unbounded: agent names should be stable/low-cardinality, and
# evicting generated flows can break ZenML/Kitaru source resolution.
_AUTO_FLOW_DEFINITIONS: dict[str, Any] = {}
_AUTO_FLOW_LOCK = threading.Lock()

if f"src.{__name__}" not in sys.modules:
    sys.modules[f"src.{__name__}"] = sys.modules[__name__]


def _has_tool_checkpoint_opt_out(
    overrides: ToolCheckpointOverrides | None,
) -> bool:
    """Return whether any named tool explicitly opts out of checkpointing."""
    return bool(overrides and any(override is False for override in overrides.values()))


def _strategy_from_granular_checkpoints(
    granular_checkpoints: bool,
) -> CheckpointStrategy:
    return "calls" if granular_checkpoints else "turn"


def _resolve_checkpoint_strategy(
    *,
    checkpoint_strategy: CheckpointStrategy | None,
    granular_checkpoints: bool | None,
) -> CheckpointStrategy:
    if checkpoint_strategy is None:
        if granular_checkpoints is None:
            return "calls"
        return _strategy_from_granular_checkpoints(granular_checkpoints)

    validated_strategy = validate_checkpoint_strategy(checkpoint_strategy)
    if granular_checkpoints is None:
        return validated_strategy

    mapped_from_bool = _strategy_from_granular_checkpoints(granular_checkpoints)
    if mapped_from_bool != validated_strategy:
        raise KitaruUsageError(
            "`checkpoint_strategy` and `granular_checkpoints` conflict. "
            'Use `checkpoint_strategy="calls"` with `granular_checkpoints=True`, '
            'or `checkpoint_strategy="turn"` with `granular_checkpoints=False`.'
        )
    return validated_strategy


def _builtin_tools_kwargs(
    builtin_tools: Sequence[AgentBuiltinTool[Any]] | None,
) -> dict[str, Sequence[AgentBuiltinTool[Any]]]:
    """Return deprecated upstream kwargs only when the caller supplied tools."""
    if builtin_tools is None:
        return {}
    return {"builtin_tools": builtin_tools}


class _AutoFlowSlot:
    __slots__ = ("body", "error", "has_result", "result")

    def __init__(self, body: Callable[[], Any]) -> None:
        self.body = body
        self.result: Any = None
        self.error: BaseException | None = None
        self.has_result = False


def _load_auto_flow_body(serialized_body_path: str) -> Callable[[], Any]:
    try:
        import cloudpickle
    except ImportError as error:  # pragma: no cover - depends on env packaging
        raise KitaruUsageError(
            "Auto-flow requires `cloudpickle` in the local runtime environment."
        ) from error
    with open(serialized_body_path, "rb") as stream:
        return cloudpickle.load(stream)


def _try_serialize_auto_flow_body(body: Callable[[], Any]) -> str | None:
    """Best-effort cloudpickle of ``body`` for remote-stack workers; returns ``None`` on failure."""
    try:
        import cloudpickle
    except ImportError:
        return None
    path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".kitaru-autoflow"
        ) as stream:
            path = stream.name
            cloudpickle.dump(body, stream)
        return path
    except Exception:
        logger.debug(
            "Auto-flow body could not be cloudpickled; remote stacks will need "
            "an explicit `@kitaru.flow` wrapper.",
            exc_info=True,
        )
        if path is not None:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        return None


def _run_auto_flow_body(run_id: str, serialized_body_path: str | None = None) -> Any:
    with _AUTO_FLOW_LOCK:
        slot = _AUTO_FLOW_BODIES.get(run_id)
    if slot is None and serialized_body_path is not None:
        slot = _AutoFlowSlot(_load_auto_flow_body(serialized_body_path))
    if slot is None:
        raise KitaruUsageError(
            f"Kitaru auto-flow body {run_id!r} not found in registry. Auto-flow "
            "is local-only; wrap your agent call in an explicit `@kitaru.flow` "
            "for remote stacks."
        )
    try:
        slot.result = slot.body()
        slot.has_result = True
        return slot.result
    except Exception as exc:
        slot.error = exc
        raise


# Keep the original importable auto-flow for runs recorded before per-agent
# auto-flow names existed.
@kitaru.flow
def _kitaru_pydantic_ai_auto_flow(
    run_id: str, serialized_body_path: str | None = None
) -> Any:
    return _run_auto_flow_body(run_id, serialized_body_path)


def _auto_flow_name_for_agent(agent_name: str) -> str:
    return build_pipeline_registration_name(f"{agent_name}_flow")


def _install_auto_flow_source_entrypoint(
    flow_name: str,
    entrypoint: Callable[[str, str | None], Any],
) -> None:
    # ZenML reloads dynamic pipelines by importing module attributes; install
    # generated entrypoints before decoration so Kitaru can source-alias them.
    setattr(sys.modules[__name__], flow_name, entrypoint)


def _make_auto_flow_entrypoint(flow_name: str) -> Callable[[str, str | None], Any]:
    def _auto_flow_entrypoint(
        run_id: str, serialized_body_path: str | None = None
    ) -> Any:
        return _run_auto_flow_body(run_id, serialized_body_path)

    _auto_flow_entrypoint.__name__ = flow_name
    _auto_flow_entrypoint.__qualname__ = flow_name
    _auto_flow_entrypoint.__module__ = __name__
    return _auto_flow_entrypoint


def _auto_flow_for_agent(agent_name: str) -> Any:
    flow_name = _auto_flow_name_for_agent(agent_name)
    with _AUTO_FLOW_LOCK:
        flow_definition = _AUTO_FLOW_DEFINITIONS.get(flow_name)
        if flow_definition is not None:
            return flow_definition

        entrypoint = _make_auto_flow_entrypoint(flow_name)
        _install_auto_flow_source_entrypoint(flow_name, entrypoint)
        flow_definition = kitaru.flow(entrypoint)
        _AUTO_FLOW_DEFINITIONS[flow_name] = flow_definition
        return flow_definition


def _is_wrapped_handler(handler: Any) -> bool:
    if getattr(handler, "_kitaru_wrapped", False):
        return True
    inner = getattr(handler, "func", None) or getattr(handler, "__func__", None)
    return bool(inner is not None and getattr(inner, "_kitaru_wrapped", False))


_STREAMING_HOOK_ATTRS = (
    "on_event",
    "on_run_event_stream",
    "event",
    "run_event_stream",
)
_STREAMING_HOOK_REGISTRY_KEYS = ("_on_event", "wrap_run_event_stream")


def _capabilities_imply_streaming_hooks(
    capabilities: Sequence[AbstractCapability[Any]] | None,
) -> bool:
    if capabilities is None:
        return False

    for capability in capabilities:
        if getattr(capability, "has_wrap_run_event_stream", False):
            return True

        registry = getattr(capability, "_registry", None)
        if isinstance(registry, Mapping) and any(
            registry.get(key) for key in _STREAMING_HOOK_REGISTRY_KEYS
        ):
            return True

        for attr in _STREAMING_HOOK_ATTRS:
            value = getattr(capability, attr, None)
            if value is not None and value is not _utils.UNSET:
                return True

    return False


def _upstream_run_kwargs(
    *,
    conversation_id: str | None,
    output_retries: int | None,
) -> dict[str, Any]:
    """Return PydanticAI run kwargs only when callers explicitly set them."""
    kwargs: dict[str, Any] = {}
    if conversation_id is not None:
        kwargs["conversation_id"] = conversation_id
    if output_retries is not None:
        kwargs["output_retries"] = output_retries
    return kwargs


def _track_run_completed(method: str, error: BaseException | None) -> None:
    if error is None:
        status = "completed"
    elif isinstance(error, asyncio.CancelledError):
        status = "cancelled"
    else:
        status = "failed"
    payload: dict[str, Any] = {"method": method, "status": status}
    if error is not None:
        payload["error_type"] = type(error).__name__
    track(AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED, payload)


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
        capture: CapturePolicy | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        turn_checkpoint_config: CheckpointConfig | None = None,
        checkpoint_strategy: CheckpointStrategy | None = None,
        granular_checkpoints: bool | None = None,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
        mcp_checkpoint_config: CheckpointConfig | None = None,
        persist_message_history: bool = False,
        allow_sync_tool_body_waits: bool = False,
    ) -> None:
        """Wrap an agent so its runs become durable under Kitaru.

        Outside a flow, ``run()`` / ``run_sync()`` auto-open a ``@kitaru.flow``.

        **Calls strategy (default):** no turn checkpoint; each top-level
        model/tool/MCP call per turn opens its own checkpoint, giving per-call
        replay/retry boundaries and a less crowded artifact view. Sub-calls
        nested inside an already-open granular checkpoint (for example tool
        calls inside the turn's model request) fall back to inline tracking —
        they do *not* open a second checkpoint. The ``model_/tool_/mcp_checkpoint_config``
        kwargs (and the per-tool ``tool_checkpoint_config_by_name`` map, where
        ``False`` opts a tool out entirely) are honored in this strategy.
        ``granular_checkpoints=True`` is kept as a legacy-compatible alias.
        Cross-run cache behavior for adapter-created granular checkpoints is
        still being tightened.

        **Turn strategy** (``checkpoint_strategy="turn"`` or legacy
        ``granular_checkpoints=False``): each run opens one
        ``@kitaru.checkpoint`` named after the agent; model/tool/MCP calls are
        recorded as child events under that checkpoint.

        If an ordinary synchronous Pydantic AI tool body calls
        ``kp.wait_for_input(...)`` directly, pass both
        ``tool_checkpoint_config_by_name={"tool_name": False}`` and
        ``allow_sync_tool_body_waits=True``. The ``False`` override only skips
        the adapter-created tool checkpoint. The explicit
        ``allow_sync_tool_body_waits`` flag asks Pydantic AI to keep supported
        sync tool bodies on the workflow thread for the whole run, and Kitaru
        fails before tool execution if that private compatibility hook is not
        available.

        When ``persist_message_history=True``, the adapter remembers the final
        ``result.all_messages()`` of each run on the instance and auto-injects
        it as ``message_history`` on the next call if the caller doesn't supply
        one — one instance then represents one conversation. Pass an explicit
        ``message_history=`` to override for a single call.

        Limits of ``persist_message_history``:

        - **In-memory only**: history lives on the Python instance. Adapter-owned
          cached run results can refresh it, but restarts, new processes, and
          replay paths that skip this adapter call start with no instance history.
        - **Serial use**: concurrent ``run`` / ``run_sync`` calls on the same
          instance race on the stored history. Gate concurrency externally or
          use one instance per concurrent conversation.
        - **Unbounded**: the list grows with each successful run; apply your
          own truncation or summarization for long-lived conversations.
        - **Success-only**: history is only updated after a successful run,
          so a partial failure leaves the last-successful history in place.
        """
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise UserError(
                "KitaruAgent requires the wrapped agent to define a concrete model at construction time; "
                "pass `model=` to the Agent constructor."
            )

        self._name = name or wrapped.name
        if self._name is None:
            raise UserError(
                "KitaruAgent requires a stable `name`; pass `name=` to KitaruAgent or set the wrapped agent name."
            )
        self._capture = capture or CapturePolicy()
        self._event_stream_handler = event_stream_handler
        self._turn_checkpoint_config: CheckpointConfig = (
            validate_checkpoint_config(
                turn_checkpoint_config, context="turn_checkpoint_config"
            )
            or {}
        )
        self._checkpoint_strategy = _resolve_checkpoint_strategy(
            checkpoint_strategy=checkpoint_strategy,
            granular_checkpoints=granular_checkpoints,
        )
        self._warned_streaming_fallback = False
        self._warned_checkpoint_history_limit = False
        has_granular_configs = any(
            value is not None
            for value in (
                model_checkpoint_config,
                tool_checkpoint_config,
                tool_checkpoint_config_by_name,
                mcp_checkpoint_config,
            )
        )
        if has_granular_configs and not self._uses_calls_strategy:
            raise KitaruUsageError(
                'Per-call checkpoint configs require `checkpoint_strategy="calls"` '
                "or legacy `granular_checkpoints=True`."
            )
        if allow_sync_tool_body_waits and not self._uses_calls_strategy:
            raise KitaruUsageError(
                "`allow_sync_tool_body_waits=True` requires "
                '`checkpoint_strategy="calls"` or legacy '
                "`granular_checkpoints=True`, and a matching checkpoint opt-out "
                'such as `tool_checkpoint_config_by_name={"tool_name": False}`.'
            )
        if allow_sync_tool_body_waits and not _has_tool_checkpoint_opt_out(
            tool_checkpoint_config_by_name
        ):
            raise KitaruUsageError(
                "`allow_sync_tool_body_waits=True` requires at least one per-tool "
                "checkpoint opt-out such as "
                '`tool_checkpoint_config_by_name={"tool_name": False}`. '
                "The opt-out keeps `kp.wait_for_input(...)` out of a synthetic "
                "tool checkpoint; the flag only controls Pydantic AI sync-tool "
                "threading."
            )
        self._allow_sync_tool_body_waits = allow_sync_tool_body_waits
        if self._uses_calls_strategy:
            self._model_checkpoint_config = (
                validate_checkpoint_config(
                    model_checkpoint_config or {},
                    context="model_checkpoint_config",
                )
                or {}
            )
            self._tool_checkpoint_config = (
                validate_checkpoint_config(
                    tool_checkpoint_config or {},
                    context="tool_checkpoint_config",
                )
                or {}
            )
            self._tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = (
                validate_tool_checkpoint_overrides(
                    tool_checkpoint_config_by_name,
                    context="tool_checkpoint_config_by_name",
                )
            )
            self._mcp_checkpoint_config = (
                validate_checkpoint_config(
                    mcp_checkpoint_config or {},
                    context="mcp_checkpoint_config",
                )
                or {}
            )
        else:
            self._model_checkpoint_config = None
            self._tool_checkpoint_config = None
            self._tool_checkpoint_config_by_name = None
            self._mcp_checkpoint_config = None
        self._model = KitaruModel(
            wrapped.model,
            capture=self._capture,
            agent_name=self._name,
            checkpoint_config=self._model_checkpoint_config,
        )
        self._toolsets = self._prepare_toolsets(list(wrapped.toolsets))
        self._persist_message_history = persist_message_history
        self._last_messages: list[_messages.ModelMessage] | None = None
        self._message_history_lock = threading.Lock()
        track(
            AnalyticsEvent.PYDANTIC_AI_WRAPPED,
            {
                "toolset_count": len(self._toolsets),
                "checkpoint_strategy": self._checkpoint_strategy,
                "granular_checkpoints": self._uses_calls_strategy,
                "persist_message_history": persist_message_history,
                "allow_sync_tool_body_waits": allow_sync_tool_body_waits,
            },
        )

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        raise UserError(
            "The agent name cannot be changed after creation. Create a new KitaruAgent instead."
        )

    @property
    def model(self) -> Model:
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        return self._toolsets

    @property
    def event_stream_handler(self) -> EventStreamHandler[AgentDepsT] | None:
        return self._event_stream_handler or super().event_stream_handler

    @property
    def capture(self) -> CapturePolicy:
        return self._capture

    @property
    def checkpoint_strategy(self) -> CheckpointStrategy:
        return self._checkpoint_strategy

    @property
    def _uses_calls_strategy(self) -> bool:
        return self._checkpoint_strategy == "calls"

    @contextmanager
    def _kitaru_overrides(self) -> Iterator[None]:
        with super().override(model=self._model, toolsets=self._toolsets, tools=[]):
            yield

    def _prepare_toolsets(
        self, toolsets: Sequence[AbstractToolset[AgentDepsT]]
    ) -> list[AbstractToolset[AgentDepsT]]:
        def _visit(value: AbstractToolset[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
            return kitaruify_toolset(
                value,
                capture=self._capture,
                tool_checkpoint_config=self._tool_checkpoint_config,
                tool_checkpoint_config_by_name=self._tool_checkpoint_config_by_name,
                mcp_checkpoint_config=self._mcp_checkpoint_config,
            )

        return [toolset.visit_and_replace(_visit) for toolset in toolsets]

    def _prepare_event_stream_handler(
        self,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
    ) -> EventStreamHandler[AgentDepsT] | None:
        effective_handler = event_stream_handler or self.event_stream_handler
        if effective_handler is None:
            return None
        if _is_wrapped_handler(effective_handler):
            return effective_handler

        async def _tracked_handler(ctx: Any, stream: AsyncIterable[Any]) -> None:
            started_at = time.perf_counter()
            error: Exception | None = None
            try:
                await effective_handler(ctx, stream)
            except Exception as exc:
                error = exc
                raise
            finally:
                tracker = get_current_tracker()
                if tracker is not None:
                    tracker.record_stream_event(
                        duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                        error=error,
                    )

        _tracked_handler._kitaru_wrapped = True  # ty: ignore[unresolved-attribute]
        return _tracked_handler

    def _validate_model_override(
        self, model: models.Model | models.KnownModelName | str | None
    ) -> None:
        if model is None:
            return
        raise UserError(
            "KitaruAgent does not support per-run `model=` overrides; create a new KitaruAgent "
            "wrapping a different agent instead."
        )

    @contextmanager
    def override(
        self,
        *,
        name: str | _utils.Unset = _utils.UNSET,
        deps: AgentDepsT | _utils.Unset = _utils.UNSET,
        model: models.Model | models.KnownModelName | str | _utils.Unset = _utils.UNSET,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | _utils.Unset = _utils.UNSET,
        tools: Sequence[Any] | _utils.Unset = _utils.UNSET,
        instructions: AgentInstructions[AgentDepsT] | _utils.Unset = _utils.UNSET,
        model_settings: AgentModelSettings[AgentDepsT] | _utils.Unset = _utils.UNSET,
        spec: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        unsupported: list[str] = []
        if _utils.is_set(name):
            unsupported.append("`name=`")
        if _utils.is_set(model):
            unsupported.append("`model=`")
        if _utils.is_set(toolsets):
            unsupported.append("`toolsets=`")
        if _utils.is_set(tools):
            unsupported.append("`tools=`")
        if unsupported:
            overrides = ", ".join(unsupported)
            raise UserError(
                f"KitaruAgent does not support contextual {overrides} overrides; create a new KitaruAgent instead."
            )

        with super().override(
            deps=deps,
            instructions=instructions,
            model_settings=model_settings,
            spec=spec,
        ):
            yield

    def _should_track(self) -> bool:
        if _TRACKING_ACTIVE.get():
            return False
        if is_inside_checkpoint():
            return True
        return self._uses_calls_strategy and is_inside_flow()

    @contextmanager
    def _tracking_scope(self) -> Iterator[None]:
        if not self._should_track():
            yield
            return

        token = _TRACKING_ACTIVE.set(True)
        try:
            with tracker_scope(self._name):
                yield
        finally:
            _TRACKING_ACTIVE.reset(token)

    @contextmanager
    def _allow_internal_iter(self) -> Iterator[None]:
        token = _INTERNAL_ITER_ALLOWED.set(True)
        try:
            yield
        finally:
            _INTERNAL_ITER_ALLOWED.reset(token)

    @staticmethod
    def _turn_checkpoint_inputs(
        *,
        user_prompt: str | Sequence[_messages.UserContent] | None,
        message_history: Sequence[_messages.ModelMessage] | None,
    ) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        if user_prompt is not None:
            inputs["user_prompt"] = checkpoint_input_value(user_prompt)
        if message_history is not None:
            inputs["message_history"] = checkpoint_input_value(list(message_history))
        return inputs

    def _turn_checkpoint_config_for_call(
        self,
        *,
        disable_cache: bool,
    ) -> CheckpointConfig:
        if not disable_cache:
            return self._turn_checkpoint_config
        return {**self._turn_checkpoint_config, "cache": False}

    def _prepare_turn_checkpoint_call_config(
        self,
        *,
        user_prompt: str | Sequence[_messages.UserContent] | None,
        message_history: Sequence[_messages.ModelMessage] | None,
        deferred_tool_results: DeferredToolResults | None,
        output_type: OutputSpec[Any] | None,
        conversation_id: str | None,
        output_retries: int | None,
        instructions: AgentInstructions[AgentDepsT],
        deps: AgentDepsT | None,
        model_settings: AgentModelSettings[AgentDepsT] | None,
        usage_limits: _usage.UsageLimits | None,
        usage: _usage.RunUsage | None,
        metadata: AgentMetadata[AgentDepsT] | None,
        infer_name: bool,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None,
        spec: dict[str, Any] | None,
    ) -> _TurnCheckpointCallConfig:
        force_turn_checkpoint = (
            event_stream_handler is not None
            or _capabilities_imply_streaming_hooks(capabilities)
        )
        return _TurnCheckpointCallConfig(
            cache_key=turn_cache_key(
                agent_name=self._name,
                user_prompt=user_prompt,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                output_type=output_type,
                conversation_id=conversation_id,
                output_retries=output_retries,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=toolsets,
                builtin_tools=builtin_tools,
                event_stream_handler=event_stream_handler,
                capabilities=capabilities,
                spec=spec,
            ),
            checkpoint_inputs=self._turn_checkpoint_inputs(
                user_prompt=user_prompt,
                message_history=message_history,
            ),
            checkpoint_config=self._turn_checkpoint_config_for_call(
                disable_cache=force_turn_checkpoint,
            ),
            force_turn_checkpoint=force_turn_checkpoint,
        )

    async def _auto_checkpoint_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
    ) -> Any:
        return await run_async_in_checkpoint(
            config=checkpoint_config or self._turn_checkpoint_config,
            step_name=self._name or "agent",
            body=body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _auto_checkpoint_sync(
        self,
        body: Callable[[], Any],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
    ) -> Any:
        return run_sync_in_checkpoint(
            config=checkpoint_config or self._turn_checkpoint_config,
            step_name=self._name or "agent",
            body=body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _invoke_in_auto_flow(self, body: Callable[[], Any]) -> Any:
        run_id = uuid.uuid4().hex
        slot = _AutoFlowSlot(body)
        serialized_body_path: str | None = None
        flow_result: Any = None
        with _AUTO_FLOW_LOCK:
            _AUTO_FLOW_BODIES[run_id] = slot
        try:
            serialized_body_path = _try_serialize_auto_flow_body(body)
            handle = _auto_flow_for_agent(self._name).run(run_id, serialized_body_path)
            try:
                flow_result = handle.wait()
            except KitaruRuntimeError as error:
                # Granular auto-flows can finish with multiple terminal adapter
                # checkpoints. The auto-flow body ran in this process, so prefer
                # the in-memory result that the module-level flow stored for us.
                if slot.has_result and _is_multiple_terminal_steps_output_error(error):
                    flow_result = slot.result
                else:
                    raise
        finally:
            with _AUTO_FLOW_LOCK:
                _AUTO_FLOW_BODIES.pop(run_id, None)
            if serialized_body_path is not None:
                try:
                    os.remove(serialized_body_path)
                except FileNotFoundError:
                    pass

        if slot.error is not None:
            raise slot.error
        if slot.has_result:
            return slot.result
        return flow_result

    def _effective_message_history(
        self,
        explicit: Sequence[_messages.ModelMessage] | None,
    ) -> Sequence[_messages.ModelMessage] | None:
        if explicit is not None or not self._persist_message_history:
            return explicit
        with self._message_history_lock:
            return list(self._last_messages) if self._last_messages else None

    def _remember_messages(self, result: Any) -> None:
        if not self._persist_message_history:
            return
        all_messages = getattr(result, "all_messages", None)
        if not callable(all_messages):
            raise KitaruRuntimeError(
                "KitaruAgent could not refresh persisted message history because "
                "the run result does not expose all_messages()."
            )
        with self._message_history_lock:
            self._last_messages = list(all_messages())

    def _warn_if_persist_history_inside_checkpoint(self) -> None:
        if (
            not self._persist_message_history
            or self._warned_checkpoint_history_limit
            or _INTERNAL_RUN_SYNC_DELEGATION.get()
            or not is_inside_checkpoint()
        ):
            return
        self._warned_checkpoint_history_limit = True
        message = (
            "`persist_message_history=True` is only in-memory. This agent call "
            "is running inside an existing `@kitaru.checkpoint`; if that "
            "checkpoint is served from cache during replay/resume, the adapter "
            "will not execute and cannot restore `_last_messages`. For "
            "resume-safe conversations, call the agent at flow scope in "
            "granular mode, or pass `message_history=` explicitly from your "
            "own durable storage."
        )
        warnings.warn(message, UserWarning, stacklevel=3)

    def _use_granular(self, force_turn_checkpoint: bool) -> bool:
        # The calls strategy cannot apply to streaming turns: per-call checkpointing
        # a streamed ``request_stream`` would require draining and replaying
        # the stream inside a sync ZenML step. Fall back to the turn checkpoint
        # so model/tool events still land under a tracked boundary.
        return self._uses_calls_strategy and not force_turn_checkpoint

    def _log_streaming_fallback(self) -> None:
        if self._warned_streaming_fallback:
            return
        dropped_configs = [
            name
            for name, config in (
                ("model_checkpoint_config", self._model_checkpoint_config),
                ("tool_checkpoint_config", self._tool_checkpoint_config),
                (
                    "tool_checkpoint_config_by_name",
                    self._tool_checkpoint_config_by_name,
                ),
                ("mcp_checkpoint_config", self._mcp_checkpoint_config),
            )
            if config
        ]
        if not dropped_configs:
            return
        self._warned_streaming_fallback = True
        logger.warning(
            "Falling back to turn checkpointing for a streamed PydanticAI run; "
            "granular checkpoint configs are ignored for this call.",
            extra={"dropped_configs": dropped_configs, "agent_name": self._name},
        )
        if is_inside_flow() and not is_inside_checkpoint():
            kitaru.log(
                adapter="pydantic_ai",
                streaming_fallback=True,
                dropped_checkpoint_configs=dropped_configs,
            )

    @staticmethod
    def _ensure_run_sync_safe() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            "`KitaruAgent.run_sync()` cannot be called from a running event loop. "
            "Use `await agent.run(...)` instead."
        )

    def _should_inline_sync_tools(self) -> bool:
        """Return whether the user explicitly requested inline sync tools.

        This is deliberately run-wide rather than per-tool. Pydantic AI decides
        whether to offload sync tools before Kitaru reaches an individual tool
        body, so the safe compatibility seam is the agent run boundary. The
        checkpoint opt-out remains checkpoint-only; this separate flag controls
        the private Pydantic AI threading compatibility path.
        """
        return self._allow_sync_tool_body_waits

    def _ensure_auto_flow_mcp_lifecycle_safe(
        self,
        *,
        call_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None,
    ) -> None:
        if is_inside_flow():
            return

        if not call_toolsets:
            effective_toolsets = self._toolsets
        else:
            effective_toolsets = (*self._toolsets, *call_toolsets)
        if not has_running_mcp_toolset(effective_toolsets):
            return

        raise KitaruUsageError(
            "KitaruAgent cannot auto-open a flow around an already-running "
            "PydanticAI MCP server. Auto-flow runs async agent bodies in a "
            "worker thread/event loop so it can wait for the flow without "
            "blocking your caller loop. Your MCP server is already open on the "
            "caller loop, so moving the agent run can hang after a successful "
            "MCP request. Wrap this call in an explicit `@kitaru.flow` while "
            "the MCP server lifecycle is open, or do not pre-open the MCP server "
            "and let PydanticAI/Kitaru auto-connect it."
        )

    async def _run_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        force_turn_checkpoint: bool = False,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        auto_flow_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return await body()
            return await self._auto_checkpoint_async(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
            )

        self._ensure_auto_flow_mcp_lifecycle_safe(call_toolsets=auto_flow_toolsets)

        # Outside any flow: auto-open one. FlowHandle.wait() is sync-blocking,
        # so we dispatch the flow to a worker thread (no running loop there,
        # so asyncio.run is safe for the agent coroutine).
        async def _await_body() -> Any:
            return await self._run_async(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                auto_flow_toolsets=auto_flow_toolsets,
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._invoke_in_auto_flow(lambda: asyncio.run(_await_body())),
        )

    def _run_sync(
        self,
        body: Callable[[], Any],
        *,
        force_turn_checkpoint: bool = False,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
        checkpoint_config: CheckpointConfig | None = None,
        auto_flow_toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return body()
            return self._auto_checkpoint_sync(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
            )
        self._ensure_auto_flow_mcp_lifecycle_safe(call_toolsets=auto_flow_toolsets)
        return self._invoke_in_auto_flow(
            lambda: self._run_sync(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
                checkpoint_config=checkpoint_config,
                auto_flow_toolsets=auto_flow_toolsets,
            )
        )

    def _require_explicit_checkpoint(self, method_name: str) -> None:
        if _INTERNAL_ITER_ALLOWED.get():
            return
        if is_inside_checkpoint():
            return
        raise UserError(
            f"`agent.{method_name}()` requires an explicit `@kitaru.checkpoint`. "
            "Kitaru cannot auto-open one around a streaming context manager; "
            "wrap the surrounding block in `@kitaru.flow` + `@kitaru.checkpoint`, "
            "or use `agent.run()` with an `event_stream_handler` instead."
        )

    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._validate_model_override(model)
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            output_retries=output_retries,
        )

        async def _body() -> Any:
            with (
                self._kitaru_overrides(),
                self._tracking_scope(),
                self._allow_internal_iter(),
                _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
                model_cache_run_context(
                    conversation_id=conversation_id, message_history=effective_history
                ),
            ):
                result = await super(KitaruAgent, self).run(
                    user_prompt,
                    output_type=output_type,
                    message_history=effective_history,
                    deferred_tool_results=deferred_tool_results,
                    **upstream_run_kwargs,
                    model=None,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=prepared_toolsets,
                    **_builtin_tools_kwargs(builtin_tools),
                    event_stream_handler=wrapped_handler,
                    capabilities=capabilities,
                    spec=spec,
                )
            return result

        turn_call_config = self._prepare_turn_checkpoint_call_config(
            user_prompt=user_prompt,
            message_history=effective_history,
            deferred_tool_results=deferred_tool_results,
            output_type=output_type,
            conversation_id=conversation_id,
            output_retries=output_retries,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=prepared_toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=wrapped_handler,
            capabilities=capabilities,
            spec=spec,
        )
        if turn_call_config.force_turn_checkpoint and self._uses_calls_strategy:
            self._log_streaming_fallback()

        error: BaseException | None = None
        try:
            result = await self._run_async(
                _body,
                force_turn_checkpoint=turn_call_config.force_turn_checkpoint,
                cache_key=turn_call_config.cache_key,
                checkpoint_inputs=turn_call_config.checkpoint_inputs,
                checkpoint_config=turn_call_config.checkpoint_config,
                auto_flow_toolsets=prepared_toolsets,
            )
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run", error)

    def run_sync(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._ensure_run_sync_safe()
        self._validate_model_override(model)
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            output_retries=output_retries,
        )

        def _body() -> Any:
            with (
                self._kitaru_overrides(),
                self._tracking_scope(),
                self._allow_internal_iter(),
                _inline_sync_tool_execution(enabled=self._should_inline_sync_tools()),
                model_cache_run_context(
                    conversation_id=conversation_id, message_history=effective_history
                ),
            ):
                delegation_token = _INTERNAL_RUN_SYNC_DELEGATION.set(True)
                try:
                    result = super(KitaruAgent, self).run_sync(
                        user_prompt,
                        output_type=output_type,
                        message_history=effective_history,
                        deferred_tool_results=deferred_tool_results,
                        **upstream_run_kwargs,
                        model=None,
                        instructions=instructions,
                        deps=deps,
                        model_settings=model_settings,
                        usage_limits=usage_limits,
                        usage=usage,
                        metadata=metadata,
                        infer_name=infer_name,
                        toolsets=prepared_toolsets,
                        **_builtin_tools_kwargs(builtin_tools),
                        event_stream_handler=wrapped_handler,
                        capabilities=capabilities,
                        spec=spec,
                    )
                finally:
                    _INTERNAL_RUN_SYNC_DELEGATION.reset(delegation_token)
            return result

        turn_call_config = self._prepare_turn_checkpoint_call_config(
            user_prompt=user_prompt,
            message_history=effective_history,
            deferred_tool_results=deferred_tool_results,
            output_type=output_type,
            conversation_id=conversation_id,
            output_retries=output_retries,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            infer_name=infer_name,
            toolsets=prepared_toolsets,
            builtin_tools=builtin_tools,
            event_stream_handler=wrapped_handler,
            capabilities=capabilities,
            spec=spec,
        )
        if turn_call_config.force_turn_checkpoint and self._uses_calls_strategy:
            self._log_streaming_fallback()

        error: BaseException | None = None
        try:
            result = self._run_sync(
                _body,
                force_turn_checkpoint=turn_call_config.force_turn_checkpoint,
                cache_key=turn_call_config.cache_key,
                checkpoint_inputs=turn_call_config.checkpoint_inputs,
                checkpoint_config=turn_call_config.checkpoint_config,
                auto_flow_toolsets=prepared_toolsets,
            )
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run_sync", error)

    @asynccontextmanager
    async def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        self._validate_model_override(model)
        self._require_explicit_checkpoint("run_stream")
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            output_retries=output_retries,
        )

        with (
            self._kitaru_overrides(),
            self._tracking_scope(),
            model_cache_run_context(
                conversation_id=conversation_id, message_history=message_history
            ),
        ):
            async with super(KitaruAgent, self).run_stream(
                user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                **upstream_run_kwargs,
                model=None,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=prepared_toolsets,
                **_builtin_tools_kwargs(builtin_tools),
                event_stream_handler=wrapped_handler,
                capabilities=capabilities,
                spec=spec,
            ) as streamed_result:
                yield streamed_result

    @asynccontextmanager
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        output_retries: int | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        capabilities: Sequence[AbstractCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[AgentRun[AgentDepsT, Any]]:
        # iter() yields a run handle inside an `async with` body; auto-checkpointing it
        # would require a checkpoint primitive that itself is a context manager, which
        # kitaru.checkpoint isn't. Wrap iter() in an explicit @kitaru.checkpoint instead.
        self._validate_model_override(model)
        self._require_explicit_checkpoint("iter")
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        upstream_run_kwargs = _upstream_run_kwargs(
            conversation_id=conversation_id,
            output_retries=output_retries,
        )
        with (
            self._kitaru_overrides(),
            self._tracking_scope(),
            model_cache_run_context(
                conversation_id=conversation_id, message_history=message_history
            ),
        ):
            async with self.wrapped.iter(
                user_prompt=user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                **upstream_run_kwargs,
                model=None,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=prepared_toolsets,
                **_builtin_tools_kwargs(builtin_tools),
                capabilities=capabilities,
                spec=spec,
            ) as run:
                yield run
