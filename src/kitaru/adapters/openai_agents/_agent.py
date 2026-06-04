"""Public runner wrapper for the OpenAI Agents SDK adapter foundation."""

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import replace
from typing import Any, cast

from kitaru.adapters._result_identity import canonicalize_result_model
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
    run_openai_agent_streamed,
    run_openai_agent_streamed_sync,
    run_openai_agent_sync,
)
from ._serialization import stable_cache_identity
from ._streaming import OpenAIStreamPublisher
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
        context_cache_identity: Callable[[Any], Any] | None = None,
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
        self._context_cache_identity_projection = context_cache_identity
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

    async def run(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None = None,
    ) -> OpenAIRunResult:
        """Run or resume an OpenAI agent asynchronously."""
        self._validate_fresh_context(request, context)
        context_cache_identity = self._context_cache_identity(context)
        context_cache_key = self._context_cache_key(context_cache_identity)
        if self._checkpoint_strategy == "calls":
            self._require_calls_scope()
            result = await self._run_calls_async(
                request,
                context=context,
                context_cache_identity=context_cache_identity,
                context_cache_key=context_cache_key,
            )
        else:
            result = await self._run_runner_call_async(
                request,
                context=context,
                context_cache_identity=context_cache_identity,
                context_cache_key=context_cache_key,
            )
        result = canonicalize_result_model(result, OpenAIRunResult)
        self._track_completed("run", result)
        return result

    async def run_stream(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None = None,
    ) -> OpenAIRunResult:
        """Run or resume an OpenAI agent asynchronously, forwarding live events."""
        self._require_streaming_runner_call()
        self._validate_fresh_context(request, context)
        context_cache_identity = self._context_cache_identity(context)
        context_cache_key = self._context_cache_key(context_cache_identity)
        result = await self._run_runner_call_stream_async(
            request,
            context=context,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
        result = canonicalize_result_model(result, OpenAIRunResult)
        self._track_completed("run_stream", result)
        return result

    def run_sync(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None = None,
    ) -> OpenAIRunResult:
        """Run or resume an OpenAI agent synchronously."""
        self._reject_running_event_loop(sync_method="run_sync", async_method="run")
        self._validate_fresh_context(request, context)
        context_cache_identity = self._context_cache_identity(context)
        context_cache_key = self._context_cache_key(context_cache_identity)
        if self._checkpoint_strategy == "calls":
            self._require_calls_scope()
            result = self._run_calls_sync(
                request,
                context=context,
                context_cache_identity=context_cache_identity,
                context_cache_key=context_cache_key,
            )
        else:
            result = self._run_runner_call_sync(
                request,
                context=context,
                context_cache_identity=context_cache_identity,
                context_cache_key=context_cache_key,
            )
        result = canonicalize_result_model(result, OpenAIRunResult)
        self._track_completed("run_sync", result)
        return result

    def run_stream_sync(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None = None,
    ) -> OpenAIRunResult:
        """Run or resume an OpenAI agent synchronously, forwarding live events."""
        self._reject_running_event_loop(
            sync_method="run_stream_sync",
            async_method="run_stream",
        )
        self._require_streaming_runner_call()
        self._validate_fresh_context(request, context)
        context_cache_identity = self._context_cache_identity(context)
        context_cache_key = self._context_cache_key(context_cache_identity)
        result = self._run_runner_call_stream_sync(
            request,
            context=context,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
        result = canonicalize_result_model(result, OpenAIRunResult)
        self._track_completed("run_stream_sync", result)
        return result

    @staticmethod
    def _reject_running_event_loop(*, sync_method: str, async_method: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            f"`KitaruRunner.{sync_method}()` cannot be called inside an "
            "already running event loop. Use `await "
            f"KitaruRunner.{async_method}(...)` instead."
        )

    def _require_calls_scope(self) -> None:
        if self._checkpoint_strategy == "calls" and is_inside_checkpoint():
            raise KitaruUsageError(
                "`checkpoint_strategy='calls'` opens model/tool checkpoints and "
                "must run from a flow body, not from inside another checkpoint."
            )

    def _require_streaming_runner_call(self) -> None:
        if self._checkpoint_strategy == "runner_call":
            return
        raise KitaruUsageError(
            "`KitaruRunner.run_stream(...)` and `run_stream_sync(...)` support "
            "only `checkpoint_strategy='runner_call'` today. Streaming with "
            "`checkpoint_strategy='calls'` needs replay-safe per-call buffering "
            "and is intentionally deferred."
        )

    async def _run_calls_async(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=True,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
        return await self._run_sdk_async(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context=context,
        )

    def _run_calls_sync(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=True,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )
        return self._run_sdk_sync(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context=context,
        )

    async def _run_runner_call_async(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=False,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )

        async def _body() -> OpenAIRunResult:
            return await self._run_sdk_async(
                request,
                agent=prepared_agent,
                run_config=run_config,
                context=context,
            )

        return await self._run_runner_call_checkpoint_async(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context_cache_identity=context_cache_identity,
            surface="run",
            body=_body,
        )

    def _run_runner_call_sync(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=False,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )

        def _body() -> OpenAIRunResult:
            return self._run_sdk_sync(
                request,
                agent=prepared_agent,
                run_config=run_config,
                context=context,
            )

        return self._run_runner_call_checkpoint_sync(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context_cache_identity=context_cache_identity,
            surface="run",
            body=_body,
        )

    async def _run_runner_call_stream_async(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=False,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )

        async def _body() -> OpenAIRunResult:
            return await self._run_sdk_stream_async(
                request,
                agent=prepared_agent,
                run_config=run_config,
                context=context,
            )

        return await self._run_runner_call_checkpoint_async(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context_cache_identity=context_cache_identity,
            surface="stream",
            stream_identity=self._stream_cache_identity(),
            body=_body,
        )

    def _run_runner_call_stream_sync(
        self,
        request: OpenAIRunRequest,
        *,
        context: Any | None,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> OpenAIRunResult:
        prepared_agent, run_config = self._prepare_execution_objects(
            wrap_calls=False,
            context_cache_identity=context_cache_identity,
            context_cache_key=context_cache_key,
        )

        def _body() -> OpenAIRunResult:
            return self._run_sdk_stream_sync(
                request,
                agent=prepared_agent,
                run_config=run_config,
                context=context,
            )

        return self._run_runner_call_checkpoint_sync(
            request,
            agent=prepared_agent,
            run_config=run_config,
            context_cache_identity=context_cache_identity,
            surface="stream",
            stream_identity=self._stream_cache_identity(),
            body=_body,
        )

    async def _run_runner_call_checkpoint_async(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context_cache_identity: Any,
        surface: str,
        body: Callable[[], Coroutine[Any, Any, OpenAIRunResult]],
        stream_identity: dict[str, Any] | None = None,
    ) -> OpenAIRunResult:
        if is_inside_flow() and not is_inside_checkpoint():
            return await run_async_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_openai_runner_call",
                body=body,
                cache_key=self._runner_call_cache_key(
                    request,
                    agent=agent,
                    run_config=run_config,
                    context_cache_identity=context_cache_identity,
                    surface=surface,
                    stream_identity=stream_identity,
                ),
            )
        return await body()

    def _run_runner_call_checkpoint_sync(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context_cache_identity: Any,
        surface: str,
        body: Callable[[], OpenAIRunResult],
        stream_identity: dict[str, Any] | None = None,
    ) -> OpenAIRunResult:
        if is_inside_flow() and not is_inside_checkpoint():
            return run_sync_in_checkpoint(
                config=self._runner_call_checkpoint_config(),
                step_name=f"{self._name}_openai_runner_call",
                body=body,
                cache_key=self._runner_call_cache_key(
                    request,
                    agent=agent,
                    run_config=run_config,
                    context_cache_identity=context_cache_identity,
                    surface=surface,
                    stream_identity=stream_identity,
                ),
            )
        return body()

    async def _run_sdk_async(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context: Any | None,
    ) -> OpenAIRunResult:
        sdk_input = await self._sdk_input_async(request, agent=agent)
        with tracker_scope(self._name) as tracker:
            sdk_result = await run_openai_agent(
                agent=agent,
                input=sdk_input,
                max_turns=request.max_turns or 10,
                run_config=run_config,
                context=context,
            )
            return self._build_and_finalize_run_result(sdk_result, tracker=tracker)

    def _run_sdk_sync(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context: Any | None,
    ) -> OpenAIRunResult:
        sdk_input = self._sdk_input_sync(request, agent=agent)
        with tracker_scope(self._name) as tracker:
            sdk_result = run_openai_agent_sync(
                agent=agent,
                input=sdk_input,
                max_turns=request.max_turns or 10,
                run_config=run_config,
                context=context,
            )
            return self._build_and_finalize_run_result(sdk_result, tracker=tracker)

    async def _run_sdk_stream_async(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context: Any | None,
    ) -> OpenAIRunResult:
        sdk_input = await self._sdk_input_async(request, agent=agent)
        publisher = OpenAIStreamPublisher(
            agent_name=self._name,
            include_text_deltas=self._capture.include_stream_text_deltas,
        )
        with tracker_scope(self._name) as tracker:
            publisher.started()
            try:
                sdk_result = await run_openai_agent_streamed(
                    agent=agent,
                    input=sdk_input,
                    max_turns=request.max_turns or 10,
                    run_config=run_config,
                    context=context,
                    on_event=publisher.event,
                )
                result = self._build_and_finalize_run_result(
                    sdk_result,
                    tracker=tracker,
                )
            except Exception as exc:
                publisher.failed(exc)
                raise
        publisher.completed(status=result.status)
        return result

    def _run_sdk_stream_sync(
        self,
        request: OpenAIRunRequest,
        *,
        agent: Any,
        run_config: Any,
        context: Any | None,
    ) -> OpenAIRunResult:
        sdk_input = self._sdk_input_sync(request, agent=agent)
        publisher = OpenAIStreamPublisher(
            agent_name=self._name,
            include_text_deltas=self._capture.include_stream_text_deltas,
        )
        with tracker_scope(self._name) as tracker:
            publisher.started()
            try:
                sdk_result = run_openai_agent_streamed_sync(
                    agent=agent,
                    input=sdk_input,
                    max_turns=request.max_turns or 10,
                    run_config=run_config,
                    context=context,
                    on_event=publisher.event,
                )
                result = self._build_and_finalize_run_result(
                    sdk_result,
                    tracker=tracker,
                )
            except Exception as exc:
                publisher.failed(exc)
                raise
        publisher.completed(status=result.status)
        return result

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

    def _build_and_finalize_run_result(
        self,
        sdk_result: Any,
        *,
        tracker: Any,
    ) -> OpenAIRunResult:
        return self._finalize_run_result(
            build_run_result(
                sdk_result,
                strict_sdk_version=self._strict_sdk_version,
                context_serializer=self._context_serializer,
                strict_context=self._strict_context,
                save_interruption_payloads=self._capture.save_interruption_payloads,
            ),
            tracker=tracker,
        )

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
        context_cache_identity: Any,
        surface: str,
        stream_identity: dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "adapter": "openai_agents",
            "checkpoint_strategy": "runner_call",
            "surface": surface,
            "agents_sdk_version": agents_sdk_version(),
            "agent": self._agent_cache_identity(agent),
            "run_config": stable_cache_identity(run_config),
            "request": request.model_dump(mode="json"),
            "capture": {
                "save_interruption_payloads": self._capture.save_interruption_payloads,
                "save_run_state": self._capture.save_run_state,
                "save_final_output": self._capture.save_final_output,
            },
        }
        if stream_identity is not None:
            payload["stream"] = stream_identity
        if context_cache_identity is not None:
            payload["context"] = context_cache_identity
        return checkpoint_cache_key(payload)

    def _stream_cache_identity(self) -> dict[str, Any]:
        return {
            "sdk_surface": "Runner.run_streamed",
            "include_stream_text_deltas": self._capture.include_stream_text_deltas,
        }

    def _context_cache_identity(self, context: Any | None) -> Any:
        if context is None:
            return None
        projected = (
            self._context_cache_identity_projection(context)
            if self._context_cache_identity_projection is not None
            else context
        )
        return stable_cache_identity(projected, opaque_objects_unique=True)

    def _context_cache_key_for_context(self, context: Any | None) -> str | None:
        return self._context_cache_key(self._context_cache_identity(context))

    @staticmethod
    def _context_cache_key(context_cache_identity: Any) -> str | None:
        if context_cache_identity is None:
            return None
        return checkpoint_cache_key({"context": context_cache_identity})

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

    def _prepare_execution_objects(
        self,
        *,
        wrap_calls: bool,
        context_cache_identity: Any,
        context_cache_key: str | None,
    ) -> tuple[Any, Any]:
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
                context_cache_identity=context_cache_identity,
                context_cache_key=context_cache_key,
                context_cache_key_factory=self._context_cache_key_for_context,
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
    def _validate_fresh_context(
        request: OpenAIRunRequest,
        context: Any | None,
    ) -> None:
        if context is not None and request.kind == "resume":
            raise KitaruUsageError(
                "Fresh `context=` can only be supplied for kind='start' requests. "
                "Resume requests rebuild SDK state from the saved RunState envelope."
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
