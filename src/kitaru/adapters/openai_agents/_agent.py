"""Public runner wrapper for the OpenAI Agents SDK adapter foundation."""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import fields, is_dataclass, replace
from typing import Any, cast

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import OpenAICapturePolicy
from ._runner import (
    agents_sdk_version,
    apply_approval_decision,
    build_run_result,
    deserialize_run_state,
    deserialize_run_state_sync,
    run_openai_agent,
    run_openai_agent_sync,
)
from ._tracking import tracker_scope
from ._types import (
    OpenAIApprovalDecision,
    OpenAIRunRequest,
    OpenAIRunResult,
    OpenAIRunStateEnvelope,
    OpenAIUsageSummary,
)
from ._utils import (
    CheckpointConfig,
    CheckpointStrategy,
    ToolCheckpointOverride,
    checkpoint_cache_key,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
    validate_tool_checkpoint_overrides,
)


def _is_openai_agent(value: Any) -> bool:
    try:
        from agents import Agent
    except ImportError:
        return False
    return isinstance(value, Agent)


class KitaruRunner:
    """Wrap an OpenAI Agents SDK agent with Kitaru durability settings.

    ``checkpoint_strategy='calls'`` runs the OpenAI SDK runner at flow scope
    while wrapping supported inner model and local ``FunctionTool`` calls in
    Kitaru synthetic checkpoints.

    ``checkpoint_strategy='runner_call'`` opens one coarse synthetic checkpoint
    around the outer OpenAI ``Runner.run(...)`` / ``run_sync(...)`` call and
    deliberately leaves the inner SDK model/tool call paths unwrapped.
    """

    def __init__(
        self,
        agent: Any,
        *,
        name: str | None = None,
        checkpoint_strategy: CheckpointStrategy = "calls",
        run_config_factory: Callable[[], Any | None] | None = None,
        capture: OpenAICapturePolicy | None = None,
        run_checkpoint_config: CheckpointConfig | None = None,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config_by_name: dict[str, ToolCheckpointOverride] | None = None,
        custom_tool_checkpoint_config: CheckpointConfig | None = None,
        mcp_checkpoint_config: CheckpointConfig | None = None,
        context_serializer: Callable[[Any], Any] | None = None,
        context_deserializer: Callable[[Any], Any] | None = None,
        strict_context: bool = True,
        strict_sdk_version: bool = True,
        cost_calculator: Callable[[OpenAIUsageSummary], float | None] | None = None,
    ) -> None:
        self._agent = agent
        resolved_name = name or getattr(agent, "name", None)
        if not isinstance(resolved_name, str) or not resolved_name.strip():
            raise KitaruUsageError(
                "KitaruRunner requires a stable `name`; pass `name=` or set "
                "the wrapped OpenAI agent's `name`."
            )
        self._name: str = resolved_name

        if (context_serializer is None) != (context_deserializer is None):
            raise KitaruUsageError(
                "`context_serializer` and `context_deserializer` must be "
                "provided together."
            )

        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._run_config_factory = run_config_factory
        self._capture = capture or OpenAICapturePolicy()
        self._run_checkpoint_config: CheckpointConfig = validate_checkpoint_config(
            run_checkpoint_config,
            context="run_checkpoint_config",
        ) or cast(CheckpointConfig, {})
        self._model_checkpoint_config: CheckpointConfig | None
        self._tool_checkpoint_config: CheckpointConfig | None
        if self._checkpoint_strategy == "calls":
            self._model_checkpoint_config = validate_checkpoint_config(
                model_checkpoint_config or cast(CheckpointConfig, {}),
                context="model_checkpoint_config",
            ) or cast(CheckpointConfig, {})
            self._tool_checkpoint_config = validate_checkpoint_config(
                tool_checkpoint_config or cast(CheckpointConfig, {}),
                context="tool_checkpoint_config",
            ) or cast(CheckpointConfig, {})
        else:
            self._model_checkpoint_config = validate_checkpoint_config(
                model_checkpoint_config,
                context="model_checkpoint_config",
            )
            self._tool_checkpoint_config = validate_checkpoint_config(
                tool_checkpoint_config,
                context="tool_checkpoint_config",
            )
        self._tool_checkpoint_config_by_name = validate_tool_checkpoint_overrides(
            tool_checkpoint_config_by_name,
            context="tool_checkpoint_config_by_name",
        )
        self._custom_tool_checkpoint_config = validate_checkpoint_config(
            custom_tool_checkpoint_config,
            context="custom_tool_checkpoint_config",
        )
        self._mcp_checkpoint_config = validate_checkpoint_config(
            mcp_checkpoint_config,
            context="mcp_checkpoint_config",
        )
        self._context_serializer = context_serializer
        self._context_deserializer = context_deserializer
        self._strict_context = strict_context
        self._strict_sdk_version = strict_sdk_version
        self._cost_calculator = cost_calculator

        track(
            AnalyticsEvent.OPENAI_AGENTS_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "has_run_config_factory": run_config_factory is not None,
                "strict_context": strict_context,
                "strict_sdk_version": strict_sdk_version,
            },
        )

    @property
    def agent(self) -> Any:
        return self._agent

    @property
    def name(self) -> str:
        return self._name

    @property
    def checkpoint_strategy(self) -> CheckpointStrategy:
        return self._checkpoint_strategy

    @property
    def capture(self) -> OpenAICapturePolicy:
        return self._capture

    async def run(self, request: OpenAIRunRequest) -> OpenAIRunResult:
        """Run or resume an OpenAI agent asynchronously."""
        if self._checkpoint_strategy == "calls":
            self._require_calls_scope()
            result = await self._run_calls_async(request)
        else:
            result = await self._run_runner_call_async(request)
        self._track_completed("run", result)
        return result

    def run_sync(self, request: OpenAIRunRequest) -> OpenAIRunResult:
        """Run or resume an OpenAI agent synchronously."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise KitaruUsageError(
                "`KitaruRunner.run_sync()` cannot be called inside an already "
                "running event loop. Use `await KitaruRunner.run(...)` instead."
            )
        if self._checkpoint_strategy == "calls":
            self._require_calls_scope()
            result = self._run_calls_sync(request)
        else:
            result = self._run_runner_call_sync(request)
        self._track_completed("run_sync", result)
        return result

    def _require_calls_scope(self) -> None:
        if self._checkpoint_strategy == "calls" and is_inside_checkpoint():
            raise KitaruUsageError(
                "`checkpoint_strategy='calls'` opens model/tool checkpoints and "
                "must run from a flow body, not from inside another checkpoint."
            )

    async def _run_calls_async(self, request: OpenAIRunRequest) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(wrap_calls=True)
        return await self._run_sdk_async(
            request,
            agent=prepared_agent,
            run_config=run_config,
        )

    def _run_calls_sync(self, request: OpenAIRunRequest) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(wrap_calls=True)
        return self._run_sdk_sync(
            request,
            agent=prepared_agent,
            run_config=run_config,
        )

    async def _run_runner_call_async(
        self, request: OpenAIRunRequest
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(wrap_calls=False)

        async def _body() -> OpenAIRunResult:
            return await self._run_sdk_async(
                request,
                agent=prepared_agent,
                run_config=run_config,
            )

        if is_inside_flow() and not is_inside_checkpoint():
            return await run_async_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_openai_runner_call",
                body=_body,
                cache_key=self._runner_call_cache_key(
                    request,
                    agent=prepared_agent,
                    run_config=run_config,
                ),
            )
        return await _body()

    def _run_runner_call_sync(self, request: OpenAIRunRequest) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(wrap_calls=False)

        def _body() -> OpenAIRunResult:
            return self._run_sdk_sync(
                request,
                agent=prepared_agent,
                run_config=run_config,
            )

        if is_inside_flow() and not is_inside_checkpoint():
            return run_sync_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_openai_runner_call",
                body=_body,
                cache_key=self._runner_call_cache_key(
                    request,
                    agent=prepared_agent,
                    run_config=run_config,
                ),
            )
        return _body()

    async def _run_sdk_async(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
    ) -> OpenAIRunResult:
        sdk_input = await self._sdk_input_async(request, agent=agent)
        with tracker_scope(self._name) as tracker:
            sdk_result = await run_openai_agent(
                agent=agent,
                input=sdk_input,
                max_turns=request.max_turns or 10,
                run_config=run_config,
            )
            return self._finalize_run_result(
                build_run_result(
                    sdk_result,
                    strict_sdk_version=self._strict_sdk_version,
                    context_serializer=self._context_serializer,
                    strict_context=self._strict_context,
                ),
                tracker=tracker,
            )

    def _run_sdk_sync(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
    ) -> OpenAIRunResult:
        sdk_input = self._sdk_input_sync(request, agent=agent)
        with tracker_scope(self._name) as tracker:
            sdk_result = run_openai_agent_sync(
                agent=agent,
                input=sdk_input,
                max_turns=request.max_turns or 10,
                run_config=run_config,
            )
            return self._finalize_run_result(
                build_run_result(
                    sdk_result,
                    strict_sdk_version=self._strict_sdk_version,
                    context_serializer=self._context_serializer,
                    strict_context=self._strict_context,
                ),
                tracker=tracker,
            )

    async def _sdk_input_async(self, request: OpenAIRunRequest, *, agent: Any) -> Any:
        if request.kind == "start":
            return request.input
        pending_state, decision = self._validated_resume_request_parts(request)
        state = await deserialize_run_state(
            pending_state,
            agent=agent,
            context_deserializer=self._context_deserializer,
            strict_context=self._strict_context,
            strict_sdk_version=self._strict_sdk_version,
        )
        return apply_approval_decision(state, decision)

    def _sdk_input_sync(self, request: OpenAIRunRequest, *, agent: Any) -> Any:
        if request.kind == "start":
            return request.input
        pending_state, decision = self._validated_resume_request_parts(request)
        state = deserialize_run_state_sync(
            pending_state,
            agent=agent,
            context_deserializer=self._context_deserializer,
            strict_context=self._strict_context,
            strict_sdk_version=self._strict_sdk_version,
        )
        return apply_approval_decision(state, decision)

    def _finalize_run_result(
        self,
        result: OpenAIRunResult,
        *,
        tracker: Any,
    ) -> OpenAIRunResult:
        updates: dict[str, Any] = {
            "event_log_artifact_name": tracker.event_log_artifact_name,
            "run_summary_artifact_name": tracker.run_summary_artifact_name,
        }
        if result.pending_state is not None and self._capture.save_run_state:
            updates["state_artifact_name"] = f"{self._name}_openai_run_state"
        if result.status == "completed" and self._capture.save_final_output:
            updates["output_artifact_name"] = f"{self._name}_openai_final_output"
        if self._cost_calculator is not None and result.usage is not None:
            updates["estimated_cost_usd"] = self._cost_calculator(
                OpenAIUsageSummary.model_validate(result.usage)
            )
        return result.model_copy(update=updates)

    def _runner_call_checkpoint_config(self) -> CheckpointConfig:
        return {
            **self._run_checkpoint_config,
            "type": self._run_checkpoint_config.get("type", "llm_call"),
        }

    def _runner_call_cache_key(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
    ) -> str:
        return checkpoint_cache_key(
            {
                "adapter": "openai_agents",
                "checkpoint_strategy": "runner_call",
                "agents_sdk_version": agents_sdk_version(),
                "agent": self._agent_cache_identity(agent),
                "run_config": self._stable_cache_identity(run_config),
                "request": request.model_dump(mode="json"),
            }
        )

    def _stable_cache_identity(self, value: Any) -> Any:
        if value is None or isinstance(value, str | int | float | bool):
            return value
        if isinstance(value, list | tuple | set | frozenset):
            return [self._stable_cache_identity(item) for item in value]
        if isinstance(value, Mapping):
            return {
                str(key): self._stable_cache_identity(nested)
                for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if is_dataclass(value) and not isinstance(value, type):
            value_type = type(value)
            return {
                "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
                "fields": {
                    field.name: self._stable_cache_identity(getattr(value, field.name))
                    for field in fields(value)
                    if not field.name.startswith("_")
                },
            }
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            try:
                return model_dump(mode="json")
            except Exception as exc:
                value_type = type(value)
                return {
                    "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
                    "name": getattr(value, "name", None),
                    "model_name": getattr(value, "model_name", None),
                    "serialization_error": type(exc).__name__,
                }
        value_type = type(value)
        return {
            "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
            "name": getattr(value, "name", None),
            "model_name": getattr(value, "model_name", None),
        }

    def _agent_cache_identity(self, agent: Any) -> dict[str, Any]:
        agent_type = type(agent)
        tools = getattr(agent, "tools", []) or []
        handoffs = getattr(agent, "handoffs", []) or []
        model = getattr(agent, "model", None)
        model_type = type(model)
        return {
            "name": self._name,
            "python_type": f"{agent_type.__module__}.{agent_type.__qualname__}",
            "model": {
                "name": getattr(model, "model_name", None),
                "python_type": f"{model_type.__module__}.{model_type.__qualname__}",
            },
            "tools": [
                {
                    "name": getattr(tool, "name", None),
                    "python_type": (
                        f"{type(tool).__module__}.{type(tool).__qualname__}"
                    ),
                }
                for tool in tools
            ],
            "handoffs": [getattr(handoff, "name", None) for handoff in handoffs],
        }

    def _prepare_execution_objects(self, *, wrap_calls: bool) -> tuple[Any, Any]:
        from agents import RunConfig

        run_config = self._run_config_factory() if self._run_config_factory else None
        if run_config is None:
            run_config = RunConfig()
        if not wrap_calls:
            return self._agent, run_config

        from agents.models.interface import Model

        from ._model import (
            kitaruify_openai_model,
            kitaruify_openai_model_provider,
        )
        from ._tools import kitaruify_openai_tools

        def _prepare_agent(agent: Any, seen: set[int]) -> Any:
            if id(agent) in seen:
                return agent
            seen.add(id(agent))
            agent_model = getattr(agent, "model", None)
            wrapped_model = (
                kitaruify_openai_model(
                    agent_model,
                    capture=self._capture,
                    agent_name=self._name,
                    checkpoint_config=self._model_checkpoint_config,
                )
                if isinstance(agent_model, Model)
                else agent_model
            )
            wrapped_tools = kitaruify_openai_tools(
                list(getattr(agent, "tools", []) or []),
                capture=self._capture,
                agent_name=self._name,
                tool_checkpoint_config=self._tool_checkpoint_config,
                tool_checkpoint_config_by_name=self._tool_checkpoint_config_by_name,
            )
            wrapped_handoffs = [
                _prepare_agent(handoff, seen) if _is_openai_agent(handoff) else handoff
                for handoff in (getattr(agent, "handoffs", []) or [])
            ]
            return replace(
                agent,
                model=wrapped_model,
                tools=wrapped_tools,
                handoffs=wrapped_handoffs,
            )

        prepared_agent = _prepare_agent(self._agent, set())

        config_model = getattr(run_config, "model", None)
        if isinstance(config_model, Model):
            run_config = replace(
                run_config,
                model=kitaruify_openai_model(
                    config_model,
                    capture=self._capture,
                    agent_name=self._name,
                    checkpoint_config=self._model_checkpoint_config,
                ),
            )
        else:
            run_config = replace(
                run_config,
                model_provider=kitaruify_openai_model_provider(
                    run_config.model_provider,
                    capture=self._capture,
                    agent_name=self._name,
                    checkpoint_config=self._model_checkpoint_config,
                ),
            )
        return prepared_agent, run_config

    def _track_completed(self, surface: str, result: OpenAIRunResult) -> None:
        track(
            AnalyticsEvent.OPENAI_AGENTS_RUN_COMPLETED,
            {
                "surface": surface,
                "checkpoint_strategy": self._checkpoint_strategy,
                "status": result.status,
                "has_usage": result.usage is not None,
                "interruption_count": len(result.interruptions),
            },
        )

    @staticmethod
    def _validated_resume_request_parts(
        request: OpenAIRunRequest,
    ) -> tuple[OpenAIRunStateEnvelope, OpenAIApprovalDecision]:
        if request.pending_state is None or request.decision is None:
            raise KitaruUsageError(
                "Resume requests require pending_state and decision."
            )
        return request.pending_state, request.decision
