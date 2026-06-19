"""Public runner wrapper for the OpenAI Agents SDK adapter foundation."""

import asyncio
import json
import weakref
from collections.abc import Callable, Coroutine
from contextlib import suppress
from dataclasses import replace
from functools import partial
from hashlib import sha256
from types import ModuleType
from typing import Any, cast

from kitaru._llm_usage import (
    build_usage_record,
    estimate_calculated_cost_usd,
    log_usage_record,
)
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
from ._tools import _resolve_tool_cache_identity
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

_MAX_HANDOFF_CACHE_IDENTITY_DEPTH = 32
_MAX_BEHAVIOR_CACHE_IDENTITY_DEPTH = 16
_MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS = 64
_MAX_BEHAVIOR_CACHE_IDENTITY_CODE_BYTES = 8192
_HANDOFF_BEHAVIOR_CACHE_ATTRIBUTES = (
    "input_filter",
    "on_invoke_handoff",
    "is_enabled",
    "nest_handoff_history",
    "strict_json_schema",
)
_AGENT_BEHAVIOR_CACHE_ATTRIBUTES = (
    "handoff_description",
    "instructions",
    "prompt",
    "model_settings",
    "input_guardrails",
    "output_guardrails",
    "output_type",
    "hooks",
    "tool_use_behavior",
    "reset_tool_choice",
    "mcp_servers",
    "mcp_config",
)


def _is_openai_agent(value: Any) -> bool:
    try:
        from agents import Agent
    except ImportError:
        return False
    return isinstance(value, Agent)


def _referenced_agent_from_handoff(handoff: Any) -> Any | None:
    """Return the original agent held by an explicit SDK handoff wrapper."""
    # OpenAI Agents SDK `handoff(agent)` wrappers keep the original agent behind
    # this private weakref today. Including it makes Kitaru's runner-call cache
    # identity change when that referenced agent's tools/model change.
    agent_ref = getattr(handoff, "_agent_ref", None)
    return agent_ref() if callable(agent_ref) else None


def _explicit_model_cache_identity(model: Any) -> Any | None:
    for attr_name in ("_kitaru_cache_identity", "kitaru_cache_identity"):
        hook = getattr(model, attr_name, None)
        if hook is not None:
            return hook() if callable(hook) else hook
    return None


def _behavior_value_cache_identity(
    value: Any,
    *,
    seen_ids: set[int] | None = None,
    depth: int = 0,
) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if depth >= _MAX_BEHAVIOR_CACHE_IDENTITY_DEPTH:
        return _bounded_behavior_cache_identity(value, reason="max_depth_exceeded")
    if seen_ids is None:
        seen_ids = set()

    value_id = id(value)
    if value_id in seen_ids:
        return {
            "python_type": KitaruRunner._python_type(type(value)),
            "recursive_reference": True,
        }

    seen_ids.add(value_id)
    try:
        if callable(value):
            return _callable_cache_identity(value, seen_ids=seen_ids, depth=depth)
        if isinstance(value, list | tuple):
            if len(value) > _MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS:
                return _bounded_collection_cache_identity(
                    value,
                    reason="max_items_exceeded",
                )
            return {
                "collection_type": type(value).__name__,
                "items": [
                    _behavior_value_cache_identity(
                        item,
                        seen_ids=seen_ids,
                        depth=depth + 1,
                    )
                    for item in value
                ],
            }
        if isinstance(value, dict):
            if len(value) > _MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS:
                return _bounded_collection_cache_identity(
                    value,
                    reason="max_items_exceeded",
                )
            return {
                str(key): _behavior_value_cache_identity(
                    nested,
                    seen_ids=seen_ids,
                    depth=depth + 1,
                )
                for key, nested in sorted(value.items(), key=lambda item: str(item[0]))
            }
        return stable_cache_identity(value, opaque_objects_unique=False)
    finally:
        seen_ids.remove(value_id)


def _bounded_behavior_cache_identity(value: Any, *, reason: str) -> dict[str, Any]:
    identity = (
        _callable_static_cache_identity(value, include_code=False)
        if callable(value)
        else {"python_type": KitaruRunner._python_type(type(value))}
    )
    return {**identity, "serialization_error": reason}


def _bounded_collection_cache_identity(
    value: list[Any] | tuple[Any, ...] | dict[Any, Any],
    *,
    reason: str,
) -> dict[str, Any]:
    return {
        "collection_type": type(value).__name__,
        "item_count": len(value),
        "serialization_error": reason,
    }


def _callable_static_cache_identity(
    value: Any,
    *,
    include_code: bool = True,
) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "python_type": KitaruRunner._python_type(type(value)),
        "module": getattr(value, "__module__", None),
        "qualname": getattr(value, "__qualname__", None),
    }
    code = getattr(value, "__code__", None)
    if include_code and code is not None:
        identity["code"] = {
            "name": getattr(code, "co_name", None),
            "argcount": getattr(code, "co_argcount", None),
            "posonlyargcount": getattr(code, "co_posonlyargcount", None),
            "kwonlyargcount": getattr(code, "co_kwonlyargcount", None),
            "flags": getattr(code, "co_flags", None),
            "bytecode": _code_bytes_cache_identity(getattr(code, "co_code", b"")),
            "exceptiontable": _code_bytes_cache_identity(
                getattr(code, "co_exceptiontable", b""),
            ),
            "constants": stable_cache_identity(
                getattr(code, "co_consts", ()),
                opaque_objects_unique=False,
                max_depth=3,
                max_items=_MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS,
            ),
            "names": _code_sequence_cache_identity(getattr(code, "co_names", ())),
            "varnames": _code_sequence_cache_identity(getattr(code, "co_varnames", ())),
            "freevars": _code_sequence_cache_identity(getattr(code, "co_freevars", ())),
            "cellvars": _code_sequence_cache_identity(getattr(code, "co_cellvars", ())),
        }
    return identity


def _code_bytes_cache_identity(code_bytes: bytes) -> str | dict[str, Any]:
    if len(code_bytes) <= _MAX_BEHAVIOR_CACHE_IDENTITY_CODE_BYTES:
        return code_bytes.hex()
    return {
        "byte_count": len(code_bytes),
        "sha256": sha256(code_bytes).hexdigest(),
        "serialization_error": "max_bytes_exceeded",
    }


def _code_sequence_cache_identity(
    values: tuple[str, ...],
) -> list[str] | dict[str, Any]:
    items = list(values)
    if len(items) <= _MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS:
        return items
    return {
        "collection_type": "tuple",
        "item_count": len(items),
        "included_items": items[:_MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS],
        "sha256": _cache_identity_digest(items),
        "serialization_error": "max_items_exceeded",
    }


def _cache_identity_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return sha256(payload).hexdigest()


def _callable_cache_identity(
    value: Any,
    *,
    seen_ids: set[int],
    depth: int,
) -> dict[str, Any]:
    identity = _callable_static_cache_identity(value)
    if isinstance(value, partial):
        identity["partial"] = {
            "function": _behavior_value_cache_identity(
                value.func,
                seen_ids=seen_ids,
                depth=depth + 1,
            ),
            "args": _behavior_value_cache_identity(
                value.args,
                seen_ids=seen_ids,
                depth=depth + 1,
            ),
            "keywords": _behavior_value_cache_identity(
                value.keywords or {},
                seen_ids=seen_ids,
                depth=depth + 1,
            ),
        }
    defaults = getattr(value, "__defaults__", None)
    if defaults:
        identity["defaults"] = _behavior_value_cache_identity(
            defaults,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    kwdefaults = getattr(value, "__kwdefaults__", None)
    if kwdefaults:
        identity["kwdefaults"] = _behavior_value_cache_identity(
            kwdefaults,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    globals_identity = _callable_referenced_globals_cache_identity(
        value,
        seen_ids=seen_ids,
        depth=depth + 1,
    )
    if globals_identity:
        identity["globals"] = globals_identity
    if not hasattr(value, "__code__") and not isinstance(value, partial):
        identity["call_method"] = _behavior_value_cache_identity(
            type(value).__call__,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    func = getattr(value, "__func__", None)
    if func is not None:
        identity["function"] = _behavior_value_cache_identity(
            func,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    self_obj = getattr(value, "__self__", None)
    if self_obj is not None:
        identity["bound_to"] = _bound_self_cache_identity(
            self_obj,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    closure = getattr(value, "__closure__", None)
    if closure:
        if len(closure) > _MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS:
            identity["closure"] = {
                "collection_type": "tuple",
                "item_count": len(closure),
                "serialization_error": "max_items_exceeded",
            }
        else:
            identity["closure"] = [
                _closure_cell_cache_identity(
                    cell,
                    seen_ids=seen_ids,
                    depth=depth + 1,
                )
                for cell in closure
            ]
    instance_attrs = _callable_instance_attributes(value)
    if instance_attrs:
        identity["instance_state"] = _behavior_value_cache_identity(
            instance_attrs,
            seen_ids=seen_ids,
            depth=depth + 1,
        )
    return identity


def _callable_referenced_globals_cache_identity(
    value: Any,
    *,
    seen_ids: set[int],
    depth: int,
) -> dict[str, Any] | None:
    code = getattr(value, "__code__", None)
    globals_dict = getattr(value, "__globals__", None)
    if code is None or not isinstance(globals_dict, dict):
        return None

    names = sorted(
        {name for name in getattr(code, "co_names", ()) if name in globals_dict}
    )
    if not names:
        return None
    globals_identity = {
        name: _global_value_cache_identity(
            globals_dict[name],
            seen_ids=seen_ids,
            depth=depth + 1,
        )
        for name in names
    }
    if len(names) > _MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS:
        return {
            "collection_type": "dict",
            "item_count": len(names),
            "included_keys": names[:_MAX_BEHAVIOR_CACHE_IDENTITY_ITEMS],
            "sha256": _cache_identity_digest(globals_identity),
            "serialization_error": "max_items_exceeded",
        }
    return globals_identity


def _global_value_cache_identity(
    value: Any,
    *,
    seen_ids: set[int],
    depth: int,
) -> Any:
    if isinstance(value, ModuleType):
        return {
            "python_type": KitaruRunner._python_type(type(value)),
            "module": value.__name__,
        }
    if isinstance(value, type):
        return {
            "python_type": KitaruRunner._python_type(value),
            "module": getattr(value, "__module__", None),
            "qualname": getattr(value, "__qualname__", None),
        }
    return _behavior_value_cache_identity(
        value,
        seen_ids=seen_ids,
        depth=depth,
    )


def _bound_self_cache_identity(
    value: Any,
    *,
    seen_ids: set[int],
    depth: int,
) -> Any:
    instance_attrs = _callable_instance_attributes(value)
    if not instance_attrs:
        return stable_cache_identity(value, opaque_objects_unique=False)
    return {
        "python_type": KitaruRunner._python_type(type(value)),
        "instance_state": _behavior_value_cache_identity(
            instance_attrs,
            seen_ids=seen_ids,
            depth=depth + 1,
        ),
    }


def _callable_instance_attributes(value: Any) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    with suppress(TypeError):
        attrs.update(vars(value))
    for cls in type(value).__mro__:
        slots = getattr(cls, "__slots__", ())
        if isinstance(slots, str):
            slots = (slots,)
        for slot in slots:
            if slot in {"__dict__", "__weakref__"} or slot in attrs:
                continue
            try:
                attrs[slot] = getattr(value, slot)
            except AttributeError:
                continue
    return attrs


def _closure_cell_cache_identity(
    cell: Any,
    *,
    seen_ids: set[int],
    depth: int,
) -> Any:
    try:
        return _behavior_value_cache_identity(
            cell.cell_contents,
            seen_ids=seen_ids,
            depth=depth,
        )
    except ValueError:
        return {"empty_closure_cell": True}


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
        warnings = list(result.warnings)
        usage_summary = (
            OpenAIUsageSummary.model_validate(result.usage)
            if result.usage is not None
            else None
        )
        estimated_cost = estimate_calculated_cost_usd(
            calculator=self._cost_calculator,
            usage=usage_summary,
            warnings=warnings,
            adapter_name="OpenAI Agents",
        )
        updates["warnings"] = warnings
        updates["estimated_cost_usd"] = estimated_cost
        finalized = result.model_copy(update=updates)
        if self._capture.save_usage:
            usage_record = build_usage_record(
                adapter="openai_agents",
                surface="runner_call",
                call_name=self._name,
                event_id=tracker.run_label,
                record_id=tracker.run_label,
                usage=finalized.usage,
                model=usage_summary.model_name if usage_summary is not None else None,
                estimated_cost_usd=finalized.estimated_cost_usd,
                cost_source=(
                    "calculator" if finalized.estimated_cost_usd is not None else "none"
                ),
                cost_source_label="openai_agents.cost_calculator",
                status=finalized.status,
                billing_effect="incurred"
                if finalized.status == "completed"
                else "unknown",
                cache_status="executed",
                warnings=finalized.warnings,
            )
            log_usage_record(usage_record)
        return finalized

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

    def _agent_cache_identity(
        self,
        agent: Any,
        seen_agent_ids: set[int] | None = None,
        handoff_depth: int = 0,
    ) -> dict[str, Any]:
        if seen_agent_ids is None:
            seen_agent_ids = set()
        agent_type = type(agent)
        agent_name = (
            self._name if agent is self._agent else getattr(agent, "name", None)
        )
        identity: dict[str, Any] = {
            "name": agent_name,
            "python_type": self._python_type(agent_type),
        }
        agent_id = id(agent)
        if agent_id in seen_agent_ids:
            return {**identity, "recursive_reference": True}
        if handoff_depth > _MAX_HANDOFF_CACHE_IDENTITY_DEPTH:
            return {**identity, "max_handoff_depth_exceeded": True}

        seen_agent_ids.add(agent_id)
        try:
            tools = getattr(agent, "tools", []) or []
            handoffs = getattr(agent, "handoffs", []) or []
            model = getattr(agent, "model", None)
            behavior_identity = {
                attr_name: _behavior_value_cache_identity(attr_value)
                for attr_name in _AGENT_BEHAVIOR_CACHE_ATTRIBUTES
                if (attr_value := getattr(agent, attr_name, None)) is not None
            }
            return {
                **identity,
                "model": self._model_value_cache_identity(model),
                "behavior": behavior_identity,
                "tools": [self._tool_cache_identity(tool) for tool in tools],
                "handoffs": [
                    self._handoff_cache_identity(
                        handoff,
                        seen_agent_ids,
                        handoff_depth=handoff_depth,
                    )
                    for handoff in handoffs
                ],
            }
        finally:
            seen_agent_ids.remove(agent_id)

    def _handoff_cache_identity(
        self,
        handoff: Any,
        seen_agent_ids: set[int],
        *,
        handoff_depth: int,
    ) -> dict[str, Any]:
        next_handoff_depth = handoff_depth + 1
        if _is_openai_agent(handoff):
            return {
                "kind": "agent",
                "agent": self._agent_cache_identity(
                    handoff,
                    seen_agent_ids,
                    handoff_depth=next_handoff_depth,
                ),
            }
        identity: dict[str, Any] = {
            "kind": "handoff",
            "name": getattr(handoff, "name", None),
            "python_type": self._python_type(type(handoff)),
        }
        for attr_name in (
            "agent_name",
            "tool_name",
            "tool_description",
            "input_json_schema",
        ):
            attr_value = getattr(handoff, attr_name, None)
            if attr_value is not None:
                identity[attr_name] = stable_cache_identity(attr_value)
        for attr_name in _HANDOFF_BEHAVIOR_CACHE_ATTRIBUTES:
            attr_value = getattr(handoff, attr_name, None)
            if attr_value is not None:
                identity[attr_name] = _behavior_value_cache_identity(attr_value)
        referenced_agent = _referenced_agent_from_handoff(handoff)
        if _is_openai_agent(referenced_agent):
            identity["agent"] = self._agent_cache_identity(
                referenced_agent,
                seen_agent_ids,
                handoff_depth=next_handoff_depth,
            )
        return identity

    def _model_value_cache_identity(self, model: Any) -> dict[str, Any]:
        return self._build_model_value_cache_identity(model)

    def _build_model_value_cache_identity(self, model: Any) -> dict[str, Any]:
        model_type = type(model)
        identity: dict[str, Any] = {
            "name": getattr(model, "model_name", None),
            "python_type": self._python_type(model_type),
            "value": stable_cache_identity(model),
        }
        explicit_identity = _explicit_model_cache_identity(model)
        if explicit_identity is not None:
            identity["cache_identity"] = stable_cache_identity(explicit_identity)
        else:
            public_state = self._public_model_state_cache_identity(model)
            if public_state is not None:
                identity["public_state"] = public_state
        return identity

    @staticmethod
    def _public_model_state_cache_identity(model: Any) -> Any | None:
        attrs = getattr(model, "__dict__", None)
        if not isinstance(attrs, dict):
            return None
        public_attrs = {
            key: value
            for key, value in attrs.items()
            if not key.startswith("_") and not callable(value)
        }
        if not public_attrs:
            return None
        return stable_cache_identity(public_attrs)

    @staticmethod
    def _tool_cache_identity(tool: Any) -> dict[str, Any]:
        identity: dict[str, Any] = {
            "name": getattr(tool, "name", None),
            "python_type": KitaruRunner._python_type(type(tool)),
        }
        description = getattr(tool, "description", None)
        if description is not None:
            identity["description"] = stable_cache_identity(description)
        params_json_schema = getattr(tool, "params_json_schema", None)
        if params_json_schema is not None:
            identity["params_json_schema"] = stable_cache_identity(params_json_schema)
        tool_cache_identity = _resolve_tool_cache_identity(tool)
        if tool_cache_identity is not None:
            identity["tool_cache_identity"] = stable_cache_identity(tool_cache_identity)
        return identity

    @staticmethod
    def _python_type(value_type: type[Any]) -> str:
        return f"{value_type.__module__}.{value_type.__qualname__}"

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

        prepared_by_id: dict[int, Any] = {}

        def _prepare_agent(agent: Any) -> Any:
            agent_id = id(agent)
            if agent_id in prepared_by_id:
                return prepared_by_id[agent_id]

            try:
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
                prepared_agent = replace(
                    agent,
                    model=wrapped_model,
                    tools=wrapped_tools,
                    handoffs=[],
                )
                prepared_by_id[agent_id] = prepared_agent
                wrapped_handoffs = [
                    _prepare_handoff(handoff)
                    for handoff in (getattr(agent, "handoffs", []) or [])
                ]
                object.__setattr__(prepared_agent, "handoffs", wrapped_handoffs)
                return prepared_agent
            except Exception:
                prepared_by_id.pop(agent_id, None)
                raise

        def _prepare_handoff(handoff: Any) -> Any:
            if _is_openai_agent(handoff):
                return _prepare_agent(handoff)

            referenced_agent = _referenced_agent_from_handoff(handoff)
            if not _is_openai_agent(referenced_agent):
                return handoff

            prepared_handoff_agent = _prepare_agent(referenced_agent)
            original_on_invoke = getattr(handoff, "on_invoke_handoff", None)
            if not callable(original_on_invoke):
                return handoff

            async def _wrapped_on_invoke_handoff(
                context: Any,
                input_json: str | None = None,
            ) -> Any:
                invoked_agent = await original_on_invoke(context, input_json)
                if invoked_agent is referenced_agent:
                    return prepared_handoff_agent
                if _is_openai_agent(invoked_agent):
                    return _prepare_agent(invoked_agent)
                return invoked_agent

            wrapped_handoff = replace(
                handoff,
                on_invoke_handoff=_wrapped_on_invoke_handoff,
            )
            object.__setattr__(
                wrapped_handoff,
                "_agent_ref",
                weakref.ref(prepared_handoff_agent),
            )
            return wrapped_handoff

        prepared_agent = _prepare_agent(self._agent)

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
