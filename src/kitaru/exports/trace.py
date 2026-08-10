"""Convert supported exported traces into evaluator-facing Kitaru sessions."""

import json
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import ValidationError

from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.task.evaluator import SessionView

from .models import ExportError

TraceFormat = Literal["kitaru", "atif", "verifiers-v1"]
_REDACTED = "[REDACTED]"


def redact_secret_values(value: Any, secret_values: Iterable[str]) -> Any:
    """Return nested JSON-like data with declared secret values replaced.

    Empty values are ignored because replacing the empty string would corrupt every
    persisted string.
    """
    secrets = tuple(secret for secret in secret_values if secret)
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, _REDACTED)
        return value
    if isinstance(value, Mapping):
        return {key: redact_secret_values(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_secret_values(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secret_values(item, secrets) for item in value)
    return value


def convert_trace(
    trace: Mapping[str, Any],
    *,
    format: TraceFormat,
    context: SessionWithNodesResponse,
    secret_values: Iterable[str] = (),
) -> SessionView:
    """Convert one declared trace format using the frozen cohort session as context.

    Args:
        trace: Serialized emitted or target-intercepted trace.
        format: Exact trace format declared by the generated target adapter.
        context: Frozen cohort session supplying identity, inputs, and metadata.
        secret_values: Runtime secret values that must not be persisted.

    Raises:
        ExportError: The trace is incomplete, malformed, or ambiguous.

    Returns:
        A real multi-node session view accepted by Kitaru evaluators.
    """
    redacted = redact_secret_values(dict(trace), secret_values)
    if format == "kitaru":
        return _convert_kitaru_trace(redacted, context)
    if format == "atif":
        return _convert_atif_trace(redacted, context)
    if format == "verifiers-v1":
        return _convert_verifiers_trace(redacted, context)
    raise ExportError("unsupported_trace_format", f"Unsupported trace format: {format}")


def _require_dict(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExportError("invalid_trace", f"{path} must be an object.")
    return value


def _require_list(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise ExportError("invalid_trace", f"{path} must be an array.")
    return value


def _parse_datetime(value: Any, path: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ExportError("invalid_trace", f"{path} must be an ISO 8601 timestamp.")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ExportError(
            "invalid_trace", f"{path} is not a valid timestamp."
        ) from error


def _parse_epoch(value: Any, path: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExportError("invalid_trace", f"{path} must be epoch seconds.")
    try:
        return datetime.fromtimestamp(float(value), tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ExportError(
            "invalid_trace", f"{path} is not a valid timestamp."
        ) from error


def _node_id(session_id: uuid.UUID, index: int) -> uuid.UUID:
    return uuid.uuid5(session_id, f"export-trace-node:{index}")


def _make_node(
    context: SessionWithNodesResponse,
    *,
    index: int,
    parent_index: int | None,
    node_type: NodeType,
    name: str,
    inputs: Any,
    outputs: Any,
    status: NodeStatus = NodeStatus.COMPLETED,
    error: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    external_id: str | None = None,
    reasoning: str | None = None,
    requested_model: str | None = None,
    model: str | None = None,
    tokens: TokenUsage | None = None,
    cost: Decimal | None = None,
    tool_name: str | None = None,
    attributes: Any = None,
    metadata: dict[str, Any] | None = None,
) -> SessionNodeResponse:
    session_id = context.session.id
    return SessionNodeResponse(
        id=_node_id(session_id, index),
        session_id=session_id,
        index=index,
        parent_index=parent_index,
        secondary_parent_indexes=[],
        parent_id=(
            _node_id(session_id, parent_index) if parent_index is not None else None
        ),
        secondary_parent_ids=[],
        external_id=external_id,
        node_type=node_type,
        name=name,
        status=status,
        error=error,
        started_at=started_at,
        ended_at=ended_at,
        reasoning=reasoning,
        inputs=inputs,
        outputs=outputs,
        requested_model=requested_model,
        model=model,
        tokens=tokens,
        cost=cost,
        tool_name=tool_name,
        attributes=attributes,
        metadata=metadata or {},
    )


def _build_view(
    context: SessionWithNodesResponse,
    nodes: list[SessionNodeResponse],
    *,
    outputs: Any,
    error: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    tokens: TokenUsage | None = None,
    cost: Decimal | None = None,
) -> SessionView:
    if len(nodes) < 2:
        raise ExportError(
            "incomplete_trace", "A usable evaluator trace must contain multiple events."
        )
    status = SessionStatus.FAILED if error else SessionStatus.COMPLETED
    session = context.session.model_copy(
        update={
            "status": status,
            "outputs": outputs,
            "error": error,
            "started_at": started_at,
            "ended_at": ended_at,
            "tokens": tokens,
            "cost": cost,
            "llm_call_count": sum(
                node.node_type == NodeType.LLM_CALL for node in nodes
            ),
            "tool_call_count": sum(
                node.node_type == NodeType.TOOL_CALL for node in nodes
            ),
        }
    )
    return SessionView(session=session, nodes=nodes)


def _convert_kitaru_trace(
    trace: Mapping[str, Any], context: SessionWithNodesResponse
) -> SessionView:
    try:
        emitted = SessionWithNodesResponse.model_validate(trace)
    except ValidationError as error:
        raise ExportError(
            "invalid_trace", f"Invalid Kitaru full-session trace: {error}"
        ) from error
    if len(emitted.nodes) < 2:
        raise ExportError(
            "incomplete_trace",
            "A Kitaru full-session trace must contain multiple nodes.",
        )
    source_id = emitted.session.id
    nodes: list[SessionNodeResponse] = []
    for expected_index, node in enumerate(emitted.nodes):
        if node.index != expected_index:
            raise ExportError(
                "invalid_trace_order",
                "Kitaru node indexes must be consecutive from zero.",
            )
        if node.session_id != source_id:
            raise ExportError(
                "invalid_trace", "Every Kitaru node must belong to the emitted session."
            )
        if node.parent_index is not None and node.parent_index >= node.index:
            raise ExportError(
                "invalid_trace_order",
                "Kitaru node parents must precede their children.",
            )
        parent_id = (
            _node_id(context.session.id, node.parent_index)
            if node.parent_index is not None
            else None
        )
        nodes.append(
            node.model_copy(
                update={
                    "id": _node_id(context.session.id, node.index),
                    "session_id": context.session.id,
                    "parent_id": parent_id,
                    "secondary_parent_ids": [
                        _node_id(context.session.id, parent)
                        for parent in node.secondary_parent_indexes
                    ],
                }
            )
        )
    session = emitted.session
    return _build_view(
        context,
        nodes,
        outputs=session.outputs,
        error=session.error,
        started_at=session.started_at,
        ended_at=session.ended_at,
        tokens=session.tokens,
        cost=session.cost,
    )


def _content(value: Any, path: str) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(part, dict) for part in value):
        return value
    raise ExportError("invalid_trace", f"{path} must be text or content parts.")


def _atif_usage(value: Any, path: str) -> tuple[TokenUsage | None, Decimal | None]:
    if value is None:
        return None, None
    metrics = _require_dict(value, path)
    fields = {
        "input_tokens": metrics.get("prompt_tokens"),
        "output_tokens": metrics.get("completion_tokens"),
        "cached_input_tokens": metrics.get("cached_tokens"),
    }
    try:
        tokens = TokenUsage.model_validate(fields)
    except ValidationError as error:
        raise ExportError(
            "invalid_trace", f"{path} has invalid token usage."
        ) from error
    cost_value = metrics.get("cost_usd")
    if cost_value is None:
        cost = None
    elif isinstance(cost_value, bool) or not isinstance(cost_value, (int, float)):
        raise ExportError("invalid_trace", f"{path}.cost_usd must be numeric.")
    else:
        cost = Decimal(str(cost_value))
    return tokens, cost


def _add_token_usage(values: Sequence[TokenUsage]) -> TokenUsage | None:
    if not values:
        return None

    def total(field: str) -> int | None:
        found = [
            getattr(value, field)
            for value in values
            if getattr(value, field) is not None
        ]
        return sum(found) if found else None

    return TokenUsage(
        input_tokens=total("input_tokens"),
        output_tokens=total("output_tokens"),
        cached_input_tokens=total("cached_input_tokens"),
        reasoning_tokens=total("reasoning_tokens"),
    )


def _convert_atif_trace(
    trace: Mapping[str, Any], context: SessionWithNodesResponse
) -> SessionView:
    if trace.get("schema_version") != "ATIF-v1.7":
        raise ExportError("invalid_trace", "Harbor traces must use ATIF-v1.7.")
    agent = _require_dict(trace.get("agent"), "agent")
    if not isinstance(agent.get("name"), str) or not isinstance(
        agent.get("version"), str
    ):
        raise ExportError("invalid_trace", "ATIF agent name and version are required.")
    steps = _require_list(trace.get("steps"), "steps")
    if not steps:
        raise ExportError("incomplete_trace", "ATIF trace contains no steps.")

    nodes: list[SessionNodeResponse] = []
    usages: list[TokenUsage] = []
    costs: list[Decimal] = []
    final_output: Any = None
    last_parent: int | None = None
    for position, raw_step in enumerate(steps, start=1):
        step = _require_dict(raw_step, f"steps[{position - 1}]")
        if step.get("step_id") != position:
            raise ExportError(
                "invalid_trace_order", "ATIF step IDs must be consecutive from one."
            )
        source = step.get("source")
        if source not in {"system", "user", "agent"}:
            raise ExportError(
                "invalid_trace", f"steps[{position - 1}].source is invalid."
            )
        message = _content(step.get("message"), f"steps[{position - 1}].message")
        timestamp = _parse_datetime(
            step.get("timestamp"), f"steps[{position - 1}].timestamp"
        )
        index = len(nodes)
        if source != "agent":
            nodes.append(
                _make_node(
                    context,
                    index=index,
                    parent_index=last_parent,
                    node_type=NodeType.SPAN,
                    name=f"{source}_message",
                    inputs={"role": source, "content": message},
                    outputs=None,
                    started_at=timestamp,
                    ended_at=timestamp,
                    attributes={"message": message},
                )
            )
            last_parent = index
            continue

        tool_calls_raw = step.get("tool_calls")
        tool_calls = (
            []
            if tool_calls_raw is None
            else _require_list(tool_calls_raw, f"steps[{position - 1}].tool_calls")
        )
        serialized_calls: list[dict[str, Any]] = []
        call_ids: set[str] = set()
        for call_position, raw_call in enumerate(tool_calls):
            call = _require_dict(
                raw_call, f"steps[{position - 1}].tool_calls[{call_position}]"
            )
            call_id = call.get("tool_call_id")
            name = call.get("function_name")
            arguments = call.get("arguments")
            if (
                not isinstance(call_id, str)
                or not call_id
                or call_id in call_ids
                or not isinstance(name, str)
                or not name
                or not isinstance(arguments, dict)
            ):
                raise ExportError(
                    "invalid_trace", "ATIF tool call is incomplete or duplicated."
                )
            call_ids.add(call_id)
            serialized_calls.append(
                {"id": call_id, "name": name, "arguments": arguments}
            )
        tokens, cost = _atif_usage(
            step.get("metrics"), f"steps[{position - 1}].metrics"
        )
        if tokens is not None:
            usages.append(tokens)
        if cost is not None:
            costs.append(cost)
        llm_index = len(nodes)
        nodes.append(
            _make_node(
                context,
                index=llm_index,
                parent_index=last_parent,
                node_type=NodeType.LLM_CALL,
                name="model_call",
                inputs={"message": message},
                outputs={
                    "message": message,
                    **({"tool_calls": serialized_calls} if serialized_calls else {}),
                },
                started_at=timestamp,
                ended_at=timestamp,
                reasoning=step.get("reasoning_content"),
                requested_model=step.get("model_name") or agent.get("model_name"),
                model=step.get("model_name") or agent.get("model_name"),
                tokens=tokens,
                cost=cost,
            )
        )
        final_output = message
        observations: dict[str, Any] = {}
        observation = step.get("observation")
        if observation is not None:
            results = _require_list(
                _require_dict(observation, f"steps[{position - 1}].observation").get(
                    "results"
                ),
                f"steps[{position - 1}].observation.results",
            )
            for result_position, raw_result in enumerate(results):
                result = _require_dict(
                    raw_result,
                    f"steps[{position - 1}].observation.results[{result_position}]",
                )
                call_id = result.get("source_call_id")
                if (
                    not isinstance(call_id, str)
                    or call_id not in call_ids
                    or call_id in observations
                ):
                    raise ExportError(
                        "invalid_trace",
                        "ATIF observation does not match one tool call.",
                    )
                observations[call_id] = result.get("content")
        for serialized_call in serialized_calls:
            call_id = serialized_call["id"]
            if call_id not in observations:
                raise ExportError(
                    "incomplete_trace", f"ATIF tool call {call_id!r} has no result."
                )
            tool_index = len(nodes)
            nodes.append(
                _make_node(
                    context,
                    index=tool_index,
                    parent_index=llm_index,
                    node_type=NodeType.TOOL_CALL,
                    name=serialized_call["name"],
                    inputs=serialized_call["arguments"],
                    outputs=observations[call_id],
                    started_at=timestamp,
                    ended_at=timestamp,
                    external_id=call_id,
                    tool_name=serialized_call["name"],
                )
            )
            last_parent = tool_index
        if not serialized_calls:
            last_parent = llm_index

    return _build_view(
        context,
        nodes,
        outputs=final_output,
        started_at=nodes[0].started_at,
        ended_at=nodes[-1].ended_at,
        tokens=_add_token_usage(usages),
        cost=sum(costs, Decimal(0)) if costs else None,
    )


def _verifiers_usage(value: Any, path: str) -> tuple[TokenUsage | None, Decimal | None]:
    if value is None:
        return None, None
    usage = _require_dict(value, path)
    try:
        tokens = TokenUsage(
            input_tokens=usage.get("prompt_tokens"),
            output_tokens=usage.get("completion_tokens"),
            cached_input_tokens=usage.get("cached_input_tokens"),
            reasoning_tokens=usage.get("reasoning_tokens"),
        )
    except ValidationError as error:
        raise ExportError(
            "invalid_trace", f"{path} has invalid token usage."
        ) from error
    cost_value = usage.get("cost")
    if cost_value is None:
        return tokens, None
    if isinstance(cost_value, bool) or not isinstance(cost_value, (int, float)):
        raise ExportError("invalid_trace", f"{path}.cost must be numeric.")
    return tokens, Decimal(str(cost_value))


def _error_message(value: Any, path: str) -> str | None:
    if value is None:
        return None
    error = _require_dict(value, path)
    message = error.get("message")
    if not isinstance(message, str) or not message:
        raise ExportError("invalid_trace", f"{path}.message is required.")
    return message


def _convert_verifiers_trace(
    trace: Mapping[str, Any], context: SessionWithNodesResponse
) -> SessionView:
    if trace.get("version") != 1:
        raise ExportError(
            "invalid_trace", "Verifiers traces must use schema version 1."
        )
    raw_nodes = _require_list(trace.get("nodes"), "nodes")
    if len(raw_nodes) < 2:
        raise ExportError(
            "incomplete_trace", "Verifiers trace contains too few messages."
        )
    calls_raw = _require_list(trace.get("calls", []), "calls")
    calls: dict[int, dict[str, Any]] = {}
    for position, raw_call in enumerate(calls_raw):
        call = _require_dict(raw_call, f"calls[{position}]")
        node = call.get("node")
        if isinstance(node, bool) or not isinstance(node, int) or node in calls:
            raise ExportError(
                "invalid_trace", "Verifiers model calls need unique node indexes."
            )
        calls[node] = call

    nodes: list[SessionNodeResponse] = []
    usages: list[TokenUsage] = []
    costs: list[Decimal] = []
    known_calls: dict[str, tuple[str, dict[str, Any]]] = {}
    final_output: Any = None
    for position, raw_node in enumerate(raw_nodes):
        source = _require_dict(raw_node, f"nodes[{position}]")
        expected_parent = None if position == 0 else position - 1
        if source.get("parent") != expected_parent:
            raise ExportError(
                "invalid_trace_order",
                "Verifiers export requires one complete linear message branch.",
            )
        message = _require_dict(source.get("message"), f"nodes[{position}].message")
        role = message.get("role")
        timestamp = _parse_epoch(
            source.get("timestamp"), f"nodes[{position}].timestamp"
        )
        if role in {"system", "user"}:
            content = _content(
                message.get("content"), f"nodes[{position}].message.content"
            )
            nodes.append(
                _make_node(
                    context,
                    index=position,
                    parent_index=expected_parent,
                    node_type=NodeType.SPAN,
                    name=f"{role}_message",
                    inputs={"role": role, "content": content},
                    outputs=None,
                    started_at=timestamp,
                    ended_at=timestamp,
                    attributes={"message": content},
                )
            )
            continue
        if role == "assistant":
            if source.get("sampled") is not True:
                raise ExportError(
                    "invalid_trace", "Assistant output must be target-sampled."
                )
            content = message.get("content")
            if content is not None:
                content = _content(content, f"nodes[{position}].message.content")
                final_output = content
            tool_calls_raw = message.get("tool_calls")
            tool_calls = (
                []
                if tool_calls_raw is None
                else _require_list(
                    tool_calls_raw, f"nodes[{position}].message.tool_calls"
                )
            )
            serialized_calls: list[dict[str, Any]] = []
            for call_position, raw_tool_call in enumerate(tool_calls):
                tool_call = _require_dict(
                    raw_tool_call,
                    f"nodes[{position}].message.tool_calls[{call_position}]",
                )
                call_id = tool_call.get("id")
                name = tool_call.get("name")
                arguments = tool_call.get("arguments")
                if (
                    not isinstance(call_id, str)
                    or not call_id
                    or call_id in known_calls
                    or not isinstance(name, str)
                    or not name
                    or not isinstance(arguments, str)
                ):
                    raise ExportError(
                        "invalid_trace",
                        "Verifiers tool call is incomplete or duplicated.",
                    )
                try:
                    parsed_arguments = json.loads(arguments)
                except json.JSONDecodeError as error:
                    raise ExportError(
                        "invalid_trace", "Verifiers tool arguments are not JSON."
                    ) from error
                if not isinstance(parsed_arguments, dict):
                    raise ExportError(
                        "invalid_trace",
                        "Verifiers tool arguments must be a JSON object.",
                    )
                known_calls[call_id] = (name, parsed_arguments)
                serialized_calls.append(
                    {"id": call_id, "name": name, "arguments": arguments}
                )
            call = calls.get(position, {})
            tokens, cost = _verifiers_usage(
                call.get("usage"), f"calls[node={position}].usage"
            )
            if tokens is not None:
                usages.append(tokens)
            if cost is not None:
                costs.append(cost)
            error = _error_message(call.get("error"), f"calls[node={position}].error")
            time = _require_dict(call.get("time", {}), f"calls[node={position}].time")
            start = (
                _parse_epoch(time.get("start"), f"calls[node={position}].time.start")
                or timestamp
            )
            end = (
                _parse_epoch(time.get("end"), f"calls[node={position}].time.end")
                or timestamp
            )
            nodes.append(
                _make_node(
                    context,
                    index=position,
                    parent_index=expected_parent,
                    node_type=NodeType.LLM_CALL,
                    name="model_call",
                    inputs={"message": content},
                    outputs={
                        "message": content,
                        **(
                            {"tool_calls": serialized_calls} if serialized_calls else {}
                        ),
                    },
                    status=NodeStatus.FAILED if error else NodeStatus.COMPLETED,
                    error=error,
                    started_at=start,
                    ended_at=end,
                    reasoning=message.get("reasoning_content"),
                    requested_model=call.get("model"),
                    model=call.get("model"),
                    tokens=tokens,
                    cost=cost,
                )
            )
            continue
        if role == "tool":
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or call_id not in known_calls:
                raise ExportError(
                    "invalid_trace", "Verifiers tool result has no prior tool call."
                )
            name, arguments = known_calls.pop(call_id)
            declared_name = message.get("name")
            if declared_name is not None and declared_name != name:
                raise ExportError(
                    "invalid_trace",
                    "Verifiers tool result name does not match its call.",
                )
            content = _content(
                message.get("content"), f"nodes[{position}].message.content"
            )
            nodes.append(
                _make_node(
                    context,
                    index=position,
                    parent_index=expected_parent,
                    node_type=NodeType.TOOL_CALL,
                    name=name,
                    inputs=arguments,
                    outputs=content,
                    started_at=timestamp,
                    ended_at=timestamp,
                    external_id=call_id,
                    tool_name=name,
                )
            )
            continue
        raise ExportError(
            "invalid_trace", f"nodes[{position}].message.role is invalid."
        )
    if known_calls:
        raise ExportError(
            "incomplete_trace", "Verifiers trace has tool calls without results."
        )

    errors_raw = _require_list(trace.get("errors", []), "errors")
    messages = [
        _error_message(error, f"errors[{position}]")
        for position, error in enumerate(errors_raw)
    ]
    error = "; ".join(message for message in messages if message) or None
    if trace.get("is_completed") is not True:
        raise ExportError("incomplete_trace", "Verifiers trace did not complete.")
    if trace.get("ok") is not True and error is None:
        raise ExportError(
            "invalid_trace", "Failed Verifiers trace has no recorded error."
        )
    return _build_view(
        context,
        nodes,
        outputs=final_output,
        error=error,
        started_at=nodes[0].started_at,
        ended_at=nodes[-1].ended_at,
        tokens=_add_token_usage(usages),
        cost=sum(costs, Decimal(0)) if costs else None,
    )
