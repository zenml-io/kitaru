"""Small bridge around OpenAI Agents SDK runner calls."""

import asyncio
from collections.abc import Callable
from functools import lru_cache
from importlib import metadata
from inspect import isawaitable
from typing import Any

from kitaru.errors import KitaruUsageError

from ._serialization import to_json_safe
from ._types import (
    OpenAIApprovalDecision,
    OpenAIInterruptionSummary,
    OpenAIRunResult,
    OpenAIRunStateEnvelope,
)
from ._usage import normalize_usage


@lru_cache(maxsize=1)
def agents_sdk_version() -> str:
    try:
        return metadata.version("openai-agents")
    except metadata.PackageNotFoundError:
        return "unknown"


async def run_openai_agent(
    *,
    agent: Any,
    input: Any,
    max_turns: int,
    run_config: Any,
    context: Any | None = None,
) -> Any:
    from agents import Runner

    return await Runner.run(
        agent,
        input,
        max_turns=max_turns,
        run_config=run_config,
        context=context,
    )


def run_openai_agent_sync(
    *,
    agent: Any,
    input: Any,
    max_turns: int,
    run_config: Any,
    context: Any | None = None,
) -> Any:
    from agents import Runner

    return Runner.run_sync(
        agent,
        input,
        max_turns=max_turns,
        run_config=run_config,
        context=context,
    )


async def run_openai_agent_streamed(
    *,
    agent: Any,
    input: Any,
    max_turns: int,
    run_config: Any,
    context: Any | None = None,
    on_event: Callable[[Any], Any] | None = None,
) -> Any:
    """Run an OpenAI agent through ``Runner.run_streamed(...)`` and drain it."""
    from agents import Runner

    result: Any = Runner.run_streamed(
        agent,
        input,
        max_turns=max_turns,
        run_config=run_config,
        context=context,
    )
    if isawaitable(result):
        result = await result

    stream_events = result.stream_events
    if on_event is None:
        async for _event in stream_events():
            pass
        return result

    async for event in stream_events():
        callback_result = on_event(event)
        if isawaitable(callback_result):
            await callback_result
    return result


def run_openai_agent_streamed_sync(
    *,
    agent: Any,
    input: Any,
    max_turns: int,
    run_config: Any,
    context: Any | None = None,
    on_event: Callable[[Any], Any] | None = None,
) -> Any:
    """Synchronous wrapper for ``run_openai_agent_streamed(...)``."""
    return asyncio.run(
        run_openai_agent_streamed(
            agent=agent,
            input=input,
            max_turns=max_turns,
            run_config=run_config,
            context=context,
            on_event=on_event,
        )
    )


def serialize_run_state(
    state: Any,
    *,
    strict_sdk_version: bool,
    context_serializer: Any | None = None,
    strict_context: bool = False,
    warnings: list[str] | None = None,
) -> OpenAIRunStateEnvelope:
    """Serialize an OpenAI ``RunState`` into Kitaru's stable envelope."""
    to_json = getattr(state, "to_json", None)
    if not callable(to_json):
        raise KitaruUsageError(
            "OpenAI interrupted run did not expose `RunState.to_json()`. "
            "Kitaru cannot persist resume state for this SDK result."
        )

    state_json = to_json(
        context_serializer=context_serializer,
        strict_context=strict_context,
    )
    if not isinstance(state_json, dict):
        raise KitaruUsageError("OpenAI `RunState.to_json()` did not return a dict.")
    return OpenAIRunStateEnvelope(
        agents_sdk_version=agents_sdk_version(),
        state_json=state_json,
        strict_sdk_version=strict_sdk_version,
        context_codec=(
            _callable_name(context_serializer)
            if context_serializer is not None
            else None
        ),
        warnings=warnings or [],
    )


async def deserialize_run_state(
    envelope: OpenAIRunStateEnvelope,
    *,
    agent: Any,
    context_deserializer: Any | None = None,
    strict_context: bool = False,
    strict_sdk_version: bool = True,
) -> Any:
    """Deserialize a Kitaru state envelope back into an OpenAI ``RunState``."""
    validate_state_envelope_sdk_version(
        envelope,
        strict_sdk_version=strict_sdk_version,
    )
    from agents import RunState

    state = RunState.from_json(
        agent,
        envelope.state_json,
        context_deserializer=context_deserializer,
        strict_context=strict_context,
    )
    if asyncio.iscoroutine(state):
        return await state
    return state


def deserialize_run_state_sync(
    envelope: OpenAIRunStateEnvelope,
    *,
    agent: Any,
    context_deserializer: Any | None = None,
    strict_context: bool = False,
    strict_sdk_version: bool = True,
) -> Any:
    """Synchronous wrapper for ``deserialize_run_state(...)``."""
    return asyncio.run(
        deserialize_run_state(
            envelope,
            agent=agent,
            context_deserializer=context_deserializer,
            strict_context=strict_context,
            strict_sdk_version=strict_sdk_version,
        )
    )


def validate_state_envelope_sdk_version(
    envelope: OpenAIRunStateEnvelope,
    *,
    strict_sdk_version: bool,
) -> None:
    """Raise on SDK version drift when strict resume is enabled."""
    if not (strict_sdk_version and envelope.strict_sdk_version):
        return
    current_version = agents_sdk_version()
    if current_version != envelope.agents_sdk_version:
        raise KitaruUsageError(
            "OpenAI RunState was captured with openai-agents "
            f"{envelope.agents_sdk_version!r}, but this process has "
            f"{current_version!r}. Pass `strict_sdk_version=False` to "
            "KitaruRunner only if you have verified the SDK state schema is "
            "compatible."
        )


def apply_approval_decision(state: Any, decision: OpenAIApprovalDecision) -> Any:
    """Apply one Kitaru approval/rejection decision to an OpenAI ``RunState``."""
    approval_item = _approval_item_from_state(state, decision.interruption_index)
    if decision.approve:
        approve = getattr(state, "approve", None)
        if not callable(approve):
            raise KitaruUsageError("OpenAI RunState does not expose `approve(...)`.")
        approve(approval_item)
    else:
        reject = getattr(state, "reject", None)
        if not callable(reject):
            raise KitaruUsageError("OpenAI RunState does not expose `reject(...)`.")
        reject(approval_item, rejection_message=decision.rejection_message)
    return state


def build_run_result(
    sdk_result: Any,
    *,
    strict_sdk_version: bool,
    agent: Any | None = None,
    run_config: Any | None = None,
    context_serializer: Any | None = None,
    strict_context: bool = False,
    warnings: list[str] | None = None,
    save_interruption_payloads: bool = True,
) -> OpenAIRunResult:
    """Convert an OpenAI SDK ``RunResult`` into Kitaru's serializable result."""
    interruptions = list(getattr(sdk_result, "interruptions", []) or [])
    usage = _usage_from_result(
        sdk_result,
        model_name=_known_single_runner_model_name(agent=agent, run_config=run_config),
    )
    if interruptions:
        to_state = getattr(sdk_result, "to_state", None)
        if not callable(to_state):
            raise KitaruUsageError(
                "OpenAI run was interrupted but did not expose `to_state()`."
            )
        envelope = serialize_run_state(
            to_state(),
            strict_sdk_version=strict_sdk_version,
            context_serializer=context_serializer,
            strict_context=strict_context,
            warnings=warnings,
        )
        return OpenAIRunResult(
            status="interrupted",
            pending_state=envelope,
            interruptions=[
                _summarize_interruption(
                    index,
                    interruption,
                    save_payloads=save_interruption_payloads,
                )
                for index, interruption in enumerate(interruptions)
            ],
            last_response_id=getattr(sdk_result, "last_response_id", None),
            usage=usage,
            warnings=warnings or [],
        )

    return OpenAIRunResult(
        status="completed",
        final_output=getattr(sdk_result, "final_output", None),
        last_response_id=getattr(sdk_result, "last_response_id", None),
        usage=usage,
        warnings=warnings or [],
    )


def _approval_item_from_state(state: Any, interruption_index: int) -> Any:
    interruptions = _interruptions_from_state(state)
    for interruption in interruptions:
        index = getattr(interruption, "index", None)
        if index == interruption_index:
            return interruption
    try:
        return interruptions[interruption_index]
    except IndexError as exc:
        raise KitaruUsageError(
            "OpenAI RunState does not contain interruption index "
            f"{interruption_index}. Available interruptions: "
            f"{len(interruptions)}."
        ) from exc


def _interruptions_from_state(state: Any) -> list[Any]:
    current_step = getattr(state, "_current_step", None)
    interruptions = list(getattr(current_step, "interruptions", []) or [])
    if interruptions:
        return interruptions
    public_interruptions = list(getattr(state, "interruptions", []) or [])
    if public_interruptions:
        return public_interruptions
    raise KitaruUsageError(
        "OpenAI RunState has no pending interruptions to approve or reject."
    )


def _usage_from_result(
    sdk_result: Any,
    *,
    model_name: str | None = None,
) -> dict[str, Any] | None:
    raw_responses = list(getattr(sdk_result, "raw_responses", None) or [])
    response_model_name, has_multiple_response_models = _single_raw_response_model_name(
        raw_responses
    )
    if has_multiple_response_models:
        model_name = None
    elif response_model_name is not None:
        model_name = (
            response_model_name if model_name in {None, response_model_name} else None
        )

    raw_usage = getattr(getattr(sdk_result, "context_wrapper", None), "usage", None)
    if raw_usage is None:
        raw_usage = _raw_response_usage(raw_responses)
    if raw_usage is None:
        return None
    summary = normalize_usage(to_json_safe(raw_usage), model_name=model_name)
    usage_payload = summary.model_dump(mode="json")
    if model_name is not None:
        raw_payload = usage_payload.get("raw")
        if isinstance(raw_payload, dict):
            raw_payload.setdefault("model", model_name)
    return usage_payload


def _raw_response_usage(raw_responses: list[Any]) -> Any | None:
    raw_usages = [
        usage
        for response in raw_responses
        if (usage := getattr(response, "usage", None)) is not None
    ]
    if not raw_usages:
        return None
    if len(raw_usages) == 1:
        return raw_usages[0]

    summaries = [normalize_usage(to_json_safe(usage)) for usage in raw_usages]
    aggregate: dict[str, Any] = {"raw_response_count": len(summaries)}
    for payload_key, field_name in (
        ("input_tokens", "input_tokens"),
        ("output_tokens", "output_tokens"),
        ("total_tokens", "total_tokens"),
        ("cached_input_tokens", "cached_input_tokens"),
        ("reasoning_tokens", "reasoning_tokens"),
    ):
        values = [
            value
            for summary in summaries
            if (value := getattr(summary, field_name)) is not None
        ]
        if values:
            aggregate[payload_key] = sum(values)
    return aggregate


def _single_raw_response_model_name(
    raw_responses: list[Any],
) -> tuple[str | None, bool]:
    model_names = {
        model.strip()
        for response in raw_responses
        if isinstance(model := getattr(response, "model", None), str) and model.strip()
    }
    if len(model_names) == 1:
        return next(iter(model_names)), False
    return None, len(model_names) > 1


def _known_single_runner_model_name(
    *,
    agent: Any | None,
    run_config: Any | None,
) -> str | None:
    agent_models = _runner_agent_model_names(agent, seen_agent_ids=set())
    if agent_models:
        return next(iter(agent_models)) if len(agent_models) == 1 else None
    if agent_models is None:
        return None

    config_model = _model_name_from_metadata(getattr(run_config, "model", None))
    return config_model


def _runner_agent_model_names(
    agent: Any | None,
    *,
    seen_agent_ids: set[int],
) -> set[str] | None:
    if agent is None:
        return set()
    agent_id = id(agent)
    if agent_id in seen_agent_ids:
        return set()
    seen_agent_ids.add(agent_id)

    raw_model = getattr(agent, "model", None)
    model_name = _model_name_from_metadata(raw_model)
    if model_name is None:
        if raw_model is not None:
            return None
        model_names: set[str] = set()
    else:
        model_names = {model_name}

    for handoff in getattr(agent, "handoffs", []) or []:
        handoff_agent = _agent_from_handoff_metadata(handoff)
        if handoff_agent is None:
            return None
        handoff_models = _runner_agent_model_names(
            handoff_agent,
            seen_agent_ids=seen_agent_ids,
        )
        if handoff_models is None:
            return None
        model_names.update(handoff_models)
    return model_names


def _model_name_from_metadata(model: Any) -> str | None:
    if isinstance(model, str) and model.strip():
        return model
    for attr_name in ("_requested_model_name", "model_name"):
        value = getattr(model, attr_name, None)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _agent_from_handoff_metadata(handoff: Any) -> Any | None:
    if getattr(handoff, "model", None) is not None:
        return handoff
    agent_ref = getattr(handoff, "_agent_ref", None)
    if not callable(agent_ref):
        return None
    return agent_ref()


def _summarize_interruption(
    index: int,
    interruption: Any,
    *,
    save_payloads: bool,
) -> OpenAIInterruptionSummary:
    raw = to_json_safe(interruption)
    tool_name = getattr(interruption, "tool_name", None)
    call_id = getattr(interruption, "call_id", None)
    message = getattr(interruption, "message", None)
    raw_item = getattr(interruption, "raw_item", None)
    raw_arguments = getattr(raw_item, "arguments", None)
    if tool_name is None:
        if save_payloads:
            tool_name = _find_nested(raw, "tool_name") or _find_nested(raw, "name")
        elif isinstance(raw, dict):
            tool_name = raw.get("tool_name") or raw.get("name")
    if call_id is None:
        if save_payloads:
            call_id = _find_nested(raw, "call_id")
        elif isinstance(raw, dict):
            call_id = raw.get("call_id")
    if message is None:
        if save_payloads:
            message = _find_nested(raw, "message") or _find_nested(raw, "reason")
        elif isinstance(raw, dict):
            message = raw.get("message") or raw.get("reason")
    return OpenAIInterruptionSummary(
        index=index,
        kind=type(interruption).__name__,
        tool_name=tool_name if isinstance(tool_name, str) else None,
        call_id=call_id if isinstance(call_id, str) else None,
        message=message if isinstance(message, str) else None,
        arguments=raw if save_payloads and isinstance(raw, dict) else None,
        arguments_preview=(
            (raw_arguments[:500] if isinstance(raw_arguments, str) else repr(raw)[:500])
            if save_payloads
            else None
        ),
    )


def _find_nested(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for nested in value.values():
            found = _find_nested(nested, key)
            if found is not None:
                return found
    if isinstance(value, list):
        for item in value:
            found = _find_nested(item, key)
            if found is not None:
                return found
    return None


def _callable_name(value: Any) -> str:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if isinstance(module, str) and isinstance(qualname, str):
        return f"{module}.{qualname}"
    return type(value).__name__
