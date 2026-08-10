#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Transparent public runner for compiled LangGraph runnables."""

import asyncio
import os
import threading
import uuid
from collections.abc import Callable, Coroutine, Mapping, Sequence
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import suppress
from typing import Any, Generic, TypeVar, cast

from langchain_core.runnables import Runnable
from langchain_core.runnables.config import RunnableConfig, merge_configs
from langgraph.types import Command

from .capability import (
    CapabilityOperation,
    LangGraphCapabilityView,
    LocalSubagentFactorySpec,
    ToolPolicyError,
    UnsupportedCapabilityError,
    UnsupportedInvocationError,
    UnsupportedWorkerInterruptError,
    _make_capability_manifest,
    _require_operation,
    _validate_manifest,
)
from .capture import CapturePolicy
from .recording import _ACTIVE_INVOCATION, InvocationRecorder, _has_interrupt

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ResultT = TypeVar("ResultT")

_FINALIZATION_TIMEOUT_SECONDS = 5.0
_FINALIZATION_CLEANUP_TIMEOUT_SECONDS = 0.5


def _import_supported_factories() -> tuple[
    Callable[..., Runnable[Any, Any]],
    Callable[..., Runnable[Any, Any]] | None,
]:
    """Return create_agent and, when deepagents is installed, create_deep_agent."""
    from langchain.agents import create_agent

    try:
        from deepagents import create_deep_agent
    except ImportError:
        return create_agent, None
    return create_agent, create_deep_agent


class _LoopThreadBridge:
    """Run one invocation's async Kitaru client on a dedicated loop thread."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._started = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="kitaru-langgraph-recording",
            daemon=True,
        )
        self._thread.start()
        self._started.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._started.set()
        self._loop.run_forever()
        self._loop.close()

    def run(
        self,
        coroutine: Coroutine[Any, Any, ResultT],
        *,
        timeout: float | None = None,
    ) -> ResultT:
        """Submit one coroutine and wait for its result."""
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def close(self) -> None:
        """Stop and join the invocation loop thread."""
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


class KitaruGraphRunner(Generic[InputT, OutputT]):
    """Record supported invocations of one existing compiled graph."""

    def __init__(
        self,
        graph: Runnable[InputT, OutputT],
        *,
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        session_name: str | None = None,
        batch_size: int = 20,
        capture_policy: CapturePolicy | None = None,
    ) -> None:
        """Initialize a wrapper without changing graph persistence.

        Args:
            graph: Existing compiled LangGraph or compatible public runnable.
            agent_id: Optional Kitaru agent identifier.
            agent_version_id: Optional Kitaru agent-version identifier.
            session_name: Recorded name, falling back to KITARU_SESSION_NAME.
            batch_size: Number of completed child nodes per ingest batch.
            capture_policy: Redaction and resource limits for stored copies.

        Raises:
            TypeError: If graph is not a public Runnable.
            ValueError: If batch_size is invalid.
        """
        if not isinstance(graph, Runnable):
            raise TypeError("graph must implement langchain_core.runnables.Runnable")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._graph = graph
        self._agent_id = agent_id
        self._agent_version_id = agent_version_id
        self._session_name = session_name or os.environ.get("KITARU_SESSION_NAME")
        self._batch_size = batch_size
        self._capture_policy = capture_policy or CapturePolicy()
        self._middleware: object | None = None
        self._manifest = None

    @classmethod
    def from_agent_factory(
        cls,
        factory: Callable[..., Runnable[InputT, OutputT]],
        *,
        factory_kwargs: Mapping[str, Any],
        local_subagents: Sequence[LocalSubagentFactorySpec] = (),
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        session_name: str | None = None,
        batch_size: int = 20,
        capture_policy: CapturePolicy | None = None,
    ) -> "KitaruGraphRunner[InputT, OutputT]":
        """Construct a supported agent with Kitaru middleware outermost."""
        create_agent, create_deep_agent = _import_supported_factories()

        if factory is not create_agent and factory is not create_deep_agent:
            message = (
                "factory must be langchain.agents.create_agent or "
                "deepagents.create_deep_agent"
            )
            if create_deep_agent is None:
                message += "; Deep Agents support requires kitaru-langgraph[deepagents]"
            raise ValueError(message)
        if local_subagents and factory is not create_deep_agent:
            raise ValueError("local_subagents require deepagents.create_deep_agent")

        built_subagents: list[dict[str, Any]] = []
        for spec in local_subagents:
            subgraph, _ = cls._call_supported_factory(
                spec.factory, spec.copied_kwargs()
            )
            built_subagents.append(
                {
                    "name": spec.name,
                    "description": spec.description,
                    "runnable": subgraph,
                }
            )

        kwargs = dict(factory_kwargs)
        existing_subagents = list(kwargs.get("subagents") or ())
        if built_subagents:
            kwargs["subagents"] = [*built_subagents, *existing_subagents]
        graph, middleware = cls._call_supported_factory(factory, kwargs)
        runner = cls(
            graph,
            agent_id=agent_id,
            agent_version_id=agent_version_id,
            session_name=session_name,
            batch_size=batch_size,
            capture_policy=capture_policy,
        )
        runner._middleware = middleware
        runner._manifest = _make_capability_manifest(
            middleware,
            local_subagents=tuple(spec.name for spec in local_subagents),
            opaque_targets=("opaque_subagents",) if existing_subagents else (),
        )
        return runner

    @staticmethod
    def _call_supported_factory(
        factory: Callable[..., Runnable[Any, Any]], kwargs: dict[str, Any]
    ) -> tuple[Runnable[Any, Any], object]:
        """Inject one middleware and call one known factory exactly once."""
        create_agent, create_deep_agent = _import_supported_factories()

        from .langchain import KitaruLangGraphMiddleware

        if factory is not create_agent and factory is not create_deep_agent:
            raise ValueError("local subagent factory is unsupported")
        caller_middleware = list(kwargs.get("middleware") or ())
        if any(
            isinstance(item, KitaruLangGraphMiddleware) for item in caller_middleware
        ):
            raise ValueError("factory middleware already contains Kitaru middleware")
        requested_model = kwargs.get("model")
        middleware = KitaruLangGraphMiddleware(
            requested_model=requested_model
            if isinstance(requested_model, str)
            else None
        )
        kwargs["middleware"] = [middleware, *caller_middleware]
        return factory(**kwargs), middleware

    @property
    def graph(self) -> Runnable[InputT, OutputT]:
        """Return the exact wrapped runnable."""
        return self._graph

    @property
    def capabilities(self) -> LangGraphCapabilityView:
        """Return immutable operations available through this construction."""
        return _validate_manifest(self._manifest, self._middleware)

    def invoke(
        self,
        input: InputT,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> OutputT:
        """Record one synchronous graph invocation."""
        bridge = _LoopThreadBridge()
        recorder: InvocationRecorder | None = None
        try:
            recorder = bridge.run(
                InvocationRecorder.setup(
                    input,
                    cast(dict[str, Any] | None, config),
                    agent_id=self._agent_id,
                    agent_version_id=self._agent_version_id,
                    session_name=self._session_name,
                    batch_size=self._batch_size,
                    policy=self._capture_policy,
                )
            )
            recorder.sync_bridge = bridge
            try:
                self._preflight(recorder)
                effective_input = self._effective_input(input, recorder)
                merged = self._merge_config(config, recorder, bridge=bridge)
                token = _ACTIVE_INVOCATION.set(recorder)
                try:
                    result = self._graph.invoke(effective_input, merged, **kwargs)
                finally:
                    _ACTIVE_INVOCATION.reset(token)
            except BaseException as graph_error:
                self._finalize_sync(
                    bridge, recorder, error=graph_error, graph_succeeded=False
                )
                raise
            self._finalize_sync(bridge, recorder, result=result, graph_succeeded=True)
            if recorder.task_id is not None and _has_interrupt(result):
                raise UnsupportedWorkerInterruptError(
                    "LangGraph worker interrupt and resume are not supported"
                )
            return result
        finally:
            bridge.close()

    async def ainvoke(
        self,
        input: InputT,
        config: RunnableConfig | None = None,
        **kwargs: Any,
    ) -> OutputT:
        """Record one asynchronous graph invocation."""
        recorder = await InvocationRecorder.setup(
            input,
            cast(dict[str, Any] | None, config),
            agent_id=self._agent_id,
            agent_version_id=self._agent_version_id,
            session_name=self._session_name,
            batch_size=self._batch_size,
            policy=self._capture_policy,
        )
        try:
            self._preflight(recorder)
            effective_input = self._effective_input(input, recorder)
            merged = self._merge_config(config, recorder, bridge=None)
            token = _ACTIVE_INVOCATION.set(recorder)
            try:
                result = await self._graph.ainvoke(effective_input, merged, **kwargs)
            finally:
                _ACTIVE_INVOCATION.reset(token)
        except BaseException as graph_error:
            await self._finalize_async(
                recorder, error=graph_error, graph_succeeded=False
            )
            raise
        await self._finalize_async(recorder, result=result, graph_succeeded=True)
        if recorder.task_id is not None and _has_interrupt(result):
            raise UnsupportedWorkerInterruptError(
                "LangGraph worker interrupt and resume are not supported"
            )
        return result

    def _preflight(self, recorder: InvocationRecorder) -> None:
        """Reject unavailable replay work before the graph executes."""
        override = recorder.override
        if override is None and recorder.replay is None:
            return
        view = self.capabilities
        if override is not None:
            if isinstance(override.model, dict):
                from .langchain import KitaruLangGraphMiddleware

                if (
                    not isinstance(self._middleware, KitaruLangGraphMiddleware)
                    or self._middleware.requested_model is None
                ):
                    raise UnsupportedCapabilityError(
                        "Mapped model override requires a string model at construction"
                    )
            operations = (
                (override.model, CapabilityOperation.OVERRIDE_MODEL),
                (override.prompt, CapabilityOperation.OVERRIDE_PROMPT),
                (override.system_prompt, CapabilityOperation.OVERRIDE_SYSTEM_PROMPT),
                (override.model_params, CapabilityOperation.OVERRIDE_MODEL_PARAMS),
            )
            for value, operation in operations:
                if value is not None:
                    _require_operation(view, operation)
        if recorder.replay is None:
            return
        policies = [
            recorder.replay.tool_policy.default,
            *recorder.replay.tool_policy.tools.values(),
        ]
        from kitaru.api_models.v1.replay_config import LLMConfig, PassthroughConfig

        if any(isinstance(policy, LLMConfig) for policy in policies):
            raise ToolPolicyError("Tool policy 'llm' is not supported")
        if any(not isinstance(policy, PassthroughConfig) for policy in policies):
            for target in view.targets:
                _require_operation(
                    view,
                    CapabilityOperation.SUBSTITUTE_TOOL_RESULT,
                    target=target.name,
                )

    @staticmethod
    async def _finalize_async(
        recorder: InvocationRecorder,
        *,
        result: Any = None,
        error: BaseException | None = None,
        graph_succeeded: bool,
    ) -> None:
        """Finish recording within the adapter's post-delegation deadline."""
        task = asyncio.create_task(recorder.finalize(result=result, error=error))
        try:
            done, _ = await asyncio.wait({task}, timeout=_FINALIZATION_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            task.cancel()
            stopped = await KitaruGraphRunner._drain_finalization_task(task)
            if stopped:
                await KitaruGraphRunner._close_after_finalization_abort(recorder)
            raise
        if task not in done:
            task.cancel()
            stopped = await KitaruGraphRunner._drain_finalization_task(task)
            recorder.latch("finalize_timeout", TimeoutError())
            if stopped:
                await KitaruGraphRunner._close_after_finalization_abort(recorder)
            recorder.warn(graph_succeeded=graph_succeeded)
            return
        try:
            task.result()
        except asyncio.CancelledError:
            raise
        except BaseException as finalization_error:
            recorder.latch("finalize", finalization_error)
            await KitaruGraphRunner._close_after_finalization_abort(recorder)
            recorder.warn(graph_succeeded=graph_succeeded)

    @staticmethod
    def _finalize_sync(
        bridge: _LoopThreadBridge,
        recorder: InvocationRecorder,
        *,
        result: Any = None,
        error: BaseException | None = None,
        graph_succeeded: bool,
    ) -> None:
        """Finish synchronous recording without blocking the caller forever."""
        hard_timeout = (
            _FINALIZATION_TIMEOUT_SECONDS + 3 * _FINALIZATION_CLEANUP_TIMEOUT_SECONDS
        )
        try:
            bridge.run(
                KitaruGraphRunner._finalize_async(
                    recorder,
                    result=result,
                    error=error,
                    graph_succeeded=graph_succeeded,
                ),
                timeout=hard_timeout,
            )
        except BaseException as finalization_error:
            stage = (
                "finalize_timeout"
                if isinstance(finalization_error, TimeoutError)
                else "finalize"
            )
            recorder.latch(stage, finalization_error)
            with suppress(BaseException):
                bridge.run(
                    KitaruGraphRunner._close_after_finalization_abort(recorder),
                    timeout=_FINALIZATION_CLEANUP_TIMEOUT_SECONDS,
                )
            recorder.warn(graph_succeeded=graph_succeeded)

    @staticmethod
    async def _drain_finalization_task(task: asyncio.Task[Any]) -> bool:
        """Give cancellation one bounded event-loop turn to finish."""
        done, _ = await asyncio.wait(
            {task}, timeout=_FINALIZATION_CLEANUP_TIMEOUT_SECONDS
        )
        if task not in done:
            return False
        with suppress(BaseException):
            task.result()
        return True

    @staticmethod
    async def _close_after_finalization_abort(
        recorder: InvocationRecorder,
    ) -> None:
        """Close the client after an aborted finalizer within a short deadline."""
        close_task = asyncio.create_task(recorder._close())
        done, _ = await asyncio.wait(
            {close_task}, timeout=_FINALIZATION_CLEANUP_TIMEOUT_SECONDS
        )
        if close_task in done:
            with suppress(BaseException):
                close_task.result()
            return
        close_task.cancel()
        await KitaruGraphRunner._drain_finalization_task(close_task)

    def _effective_input(
        self, caller_input: InputT, recorder: InvocationRecorder
    ) -> InputT:
        """Apply whole-input replacement while preserving any Command."""
        if isinstance(caller_input, Command):
            return caller_input
        return cast(InputT, recorder.effective_input)

    def _merge_config(
        self,
        config: RunnableConfig | None,
        recorder: InvocationRecorder,
        *,
        bridge: _LoopThreadBridge | None,
    ) -> RunnableConfig:
        """Copy caller config and append the invocation callback."""
        from .callbacks import AsyncKitaruCallback, SyncKitaruCallback

        callback = (
            SyncKitaruCallback(recorder, bridge)
            if bridge is not None
            else AsyncKitaruCallback(recorder)
        )
        return merge_configs(config, {"callbacks": [callback]})

    def stream(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded streaming."""
        self._unsupported("stream")

    async def astream(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded asynchronous streaming."""
        self._unsupported("astream")
        if False:  # pragma: no cover - preserves async-generator shape
            yield None

    async def astream_events(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded event streaming."""
        self._unsupported("astream_events")
        if False:  # pragma: no cover
            yield None

    async def astream_log(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded log streaming."""
        self._unsupported("astream_log")
        if False:  # pragma: no cover
            yield None

    def batch(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded batching."""
        self._unsupported("batch")

    async def abatch(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded asynchronous batching."""
        self._unsupported("abatch")

    def batch_as_completed(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded completed-order batching."""
        self._unsupported("batch_as_completed")

    async def abatch_as_completed(self, *_: Any, **__: Any) -> Any:
        """Reject unrecorded asynchronous completed-order batching."""
        self._unsupported("abatch_as_completed")

    @staticmethod
    def _unsupported(name: str) -> None:
        raise UnsupportedInvocationError(
            f"KitaruGraphRunner does not support {name}(); use invoke() or ainvoke()"
        )
