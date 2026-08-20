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


def _require_parent(value: Any, path: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExportError(
            "malformed_trace_parent", f"{path} must be an integer or null."
        )
    return value


def _validate_parent_references(
    *,
    node_index: int,
    parents: Sequence[int | None],
    indexes: set[int],
    maximum_index: int,
    path: str,
) -> None:
    declared = [parent for parent in parents if parent is not None]
    if len(declared) != len(set(declared)):
        raise ExportError("duplicate_trace_parent", f"{path} repeats the same parent.")
    for parent in declared:
        if parent < 0 or parent > maximum_index:
            raise ExportError(
                "trace_parent_out_of_range", f"{path} is outside the node index range."
            )
        if parent not in indexes:
            raise ExportError(
                "missing_trace_parent", f"{path} references a missing node."
            )
        if parent >= node_index:
            raise ExportError("forward_trace_parent", f"{path} must precede its child.")


def _validate_kitaru_graph(raw_nodes: list[Any]) -> list[dict[str, Any]]:
    nodes = [
        _require_dict(raw_node, f"nodes[{position}]")
        for position, raw_node in enumerate(raw_nodes)
    ]
    indexes: set[int] = set()
    for position, node in enumerate(nodes):
        index = node.get("index")
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            raise ExportError(
                "invalid_trace", f"nodes[{position}].index must be non-negative."
            )
        if index in indexes:
            raise ExportError(
                "duplicate_trace_node", "Kitaru node indexes must be unique."
            )
        indexes.add(index)
    maximum_index = max(indexes, default=-1)
    for position, node in enumerate(nodes):
        secondary_raw = node.get("secondary_parent_indexes", [])
        if not isinstance(secondary_raw, list):
            raise ExportError(
                "malformed_trace_parent",
                f"nodes[{position}].secondary_parent_indexes must be an array.",
            )
        parent = _require_parent(
            node.get("parent_index"), f"nodes[{position}].parent_index"
        )
        secondary = [
            _require_parent(value, f"nodes[{position}].secondary_parent_indexes")
            for value in secondary_raw
        ]
        if any(value is None for value in secondary):
            raise ExportError(
                "malformed_trace_parent",
                f"nodes[{position}].secondary_parent_indexes cannot contain null.",
            )
        _validate_parent_references(
            node_index=node["index"],
            parents=[parent, *secondary],
            indexes=indexes,
            maximum_index=maximum_index,
            path=f"nodes[{position}] parents",
        )
    if [node["index"] for node in nodes] != sorted(indexes):
        raise ExportError(
            "invalid_trace_order", "Kitaru nodes must be ordered by ascending index."
        )
    return nodes


def _make_node(
    context: SessionWithNodesResponse,
    *,
    index: int,
    parent_index: int | None,
    secondary_parent_indexes: Sequence[int] = (),
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
        secondary_parent_indexes=list(secondary_parent_indexes),
        parent_id=(
            _node_id(session_id, parent_index) if parent_index is not None else None
        ),
        secondary_parent_ids=[
            _node_id(session_id, parent) for parent in secondary_parent_indexes
        ],
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
    status: SessionStatus = SessionStatus.COMPLETED,
    error: str | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    tokens: TokenUsage | None = None,
    cost: Decimal | None = None,
    llm_call_count: int | None = None,
    tool_call_count: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> SessionView:
    if len(nodes) < 2:
        raise ExportError(
            "incomplete_trace", "A usable evaluator trace must contain multiple events."
        )
    session = context.session.model_copy(
        update={
            "status": status,
            "outputs": outputs,
            "error": error,
            "started_at": started_at,
            "ended_at": ended_at,
            "tokens": tokens,
            "cost": cost,
            "llm_call_count": llm_call_count
            if llm_call_count is not None
            else sum(node.node_type == NodeType.LLM_CALL for node in nodes),
            "tool_call_count": tool_call_count
            if tool_call_count is not None
            else sum(node.node_type == NodeType.TOOL_CALL for node in nodes),
            "metadata": {**context.session.metadata, **(metadata or {})},
        }
    )
    return SessionView(session=session, nodes=nodes)


def _convert_kitaru_trace(
    trace: Mapping[str, Any], context: SessionWithNodesResponse
) -> SessionView:
    raw_nodes = _require_list(trace.get("nodes"), "nodes")
    _validate_kitaru_graph(raw_nodes)
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
    session = emitted.session
    if session.status == SessionStatus.IN_PROGRESS or any(
        node.status == NodeStatus.IN_PROGRESS for node in emitted.nodes
    ):
        raise ExportError(
            "incomplete_trace", "Kitaru sessions and nodes must be terminal."
        )
    if (session.status == SessionStatus.COMPLETED and session.error is not None) or (
        session.status == SessionStatus.FAILED and not session.error
    ):
        raise ExportError(
            "invalid_trace", "Kitaru terminal status and error do not agree."
        )
    source_id = emitted.session.id
    source_nodes = {node.index: node for node in emitted.nodes}
    nodes: list[SessionNodeResponse] = []
    for node in emitted.nodes:
        if node.session_id != source_id:
            raise ExportError(
                "invalid_trace", "Every Kitaru node must belong to the emitted session."
            )
        expected_parent_id = (
            source_nodes[node.parent_index].id
            if node.parent_index is not None
            else None
        )
        expected_secondary_ids = [
            source_nodes[parent].id for parent in node.secondary_parent_indexes
        ]
        if node.parent_id != expected_parent_id or (
            node.secondary_parent_ids != expected_secondary_ids
        ):
            raise ExportError(
                "malformed_trace_parent",
                "Kitaru parent IDs must match their declared parent indexes.",
            )
        if (node.status == NodeStatus.COMPLETED and node.error is not None) or (
            node.status == NodeStatus.FAILED and not node.error
        ):
            raise ExportError(
                "invalid_trace", "Kitaru node status and error do not agree."
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
    return _build_view(
        context,
        nodes,
        outputs=session.outputs,
        status=session.status,
        error=session.error,
        started_at=session.started_at,
        ended_at=session.ended_at,
        tokens=session.tokens,
        cost=session.cost,
        llm_call_count=session.llm_call_count,
        tool_call_count=session.tool_call_count,
        metadata=session.metadata,
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


def _atif_final_usage(
    value: Any,
    fallback_tokens: TokenUsage | None,
    fallback_cost: Decimal | None,
) -> tuple[TokenUsage | None, Decimal | None]:
    if value is None:
        return fallback_tokens, fallback_cost
    metrics = _require_dict(value, "final_metrics")
    try:
        tokens = TokenUsage(
            input_tokens=metrics.get("total_prompt_tokens"),
            output_tokens=metrics.get("total_completion_tokens"),
            cached_input_tokens=metrics.get("total_cached_tokens"),
        )
    except ValidationError as error:
        raise ExportError(
            "invalid_trace", "final_metrics has invalid token usage."
        ) from error
    cost_value = metrics.get("total_cost_usd")
    if cost_value is None:
        cost = fallback_cost
    elif isinstance(cost_value, bool) or not isinstance(cost_value, (int, float)):
        raise ExportError(
            "invalid_trace", "final_metrics.total_cost_usd must be numeric."
        )
    else:
        cost = Decimal(str(cost_value))
    if all(
        value is None
        for value in (
            tokens.input_tokens,
            tokens.output_tokens,
            tokens.cached_input_tokens,
        )
    ):
        tokens = fallback_tokens
    return tokens, cost


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

    if trace.get("continued_trajectory_ref") is not None:
        raise ExportError(
            "incomplete_trace", "ATIF continuation traces are not terminal."
        )

    nodes: list[SessionNodeResponse] = []
    usages: list[TokenUsage] = []
    costs: list[Decimal] = []
    conversation: list[dict[str, Any]] = []
    final_output: Any = None
    pending_parents: list[int] = []
    declared_llm_calls = 0
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
        parent_index = pending_parents[0] if pending_parents else None
        secondary_parent_indexes = pending_parents[1:]
        if source != "agent":
            if step.get("llm_call_count") not in (None, 0):
                raise ExportError(
                    "unsupported_trace_shape",
                    "Only ATIF agent steps can declare model calls.",
                )
            nodes.append(
                _make_node(
                    context,
                    index=index,
                    parent_index=parent_index,
                    secondary_parent_indexes=secondary_parent_indexes,
                    node_type=NodeType.SPAN,
                    name=f"{source}_message",
                    inputs={"role": source, "content": message},
                    outputs=step.get("observation"),
                    started_at=timestamp,
                    ended_at=timestamp,
                    attributes={
                        "message": message,
                        **(
                            {"extra": step["extra"]}
                            if step.get("extra") is not None
                            else {}
                        ),
                    },
                )
            )
            conversation.append({"role": source, "content": message})
            pending_parents = [index]
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
        declared_count_raw = step.get("llm_call_count")
        if declared_count_raw is None:
            declared_count = 1
        elif isinstance(declared_count_raw, bool) or not isinstance(
            declared_count_raw, int
        ):
            raise ExportError(
                "invalid_trace", "ATIF llm_call_count must be a non-negative integer."
            )
        else:
            declared_count = declared_count_raw
        if declared_count < 0:
            raise ExportError(
                "invalid_trace", "ATIF llm_call_count must be a non-negative integer."
            )
        if declared_count == 0 and (
            step.get("metrics") is not None or step.get("reasoning_content") is not None
        ):
            raise ExportError(
                "invalid_trace",
                "ATIF deterministic steps cannot carry model metrics or reasoning.",
            )
        declared_llm_calls += declared_count
        tokens, cost = _atif_usage(
            step.get("metrics"), f"steps[{position - 1}].metrics"
        )
        if tokens is not None:
            usages.append(tokens)
        if cost is not None:
            costs.append(cost)
        call_index = len(nodes)
        call_metadata: dict[str, Any] = {"declared_llm_call_count": declared_count}
        if declared_count > 1:
            call_metadata["conversion_warnings"] = [
                "ATIF aggregates multiple model calls in one step."
            ]
        node_type = NodeType.LLM_CALL if declared_count else NodeType.SPAN
        nodes.append(
            _make_node(
                context,
                index=call_index,
                parent_index=parent_index,
                secondary_parent_indexes=secondary_parent_indexes,
                node_type=node_type,
                name="model_call" if declared_count else "deterministic_agent_step",
                inputs={"messages": list(conversation)},
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
                attributes=step.get("extra"),
                metadata=call_metadata,
            )
        )
        final_output = message
        assistant_message: dict[str, Any] = {
            "role": "assistant",
            "content": message,
        }
        if step.get("reasoning_content") is not None:
            assistant_message["reasoning_content"] = step["reasoning_content"]
        if serialized_calls:
            assistant_message["tool_calls"] = serialized_calls
        conversation.append(assistant_message)
        observations: dict[str, Any] = {}
        unassociated_results: list[dict[str, Any]] = []
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
                if call_id is None:
                    unassociated_results.append(result)
                    continue
                if not isinstance(call_id, str) or call_id not in call_ids:
                    raise ExportError(
                        "invalid_trace",
                        "ATIF observation does not match one tool call.",
                    )
                if call_id in observations:
                    raise ExportError(
                        "invalid_trace", "ATIF tool call has duplicate results."
                    )
                observations[call_id] = result.get("content")
        elif serialized_calls:
            raise ExportError(
                "missing_tool_result", "ATIF tool calls require observations."
            )
        tool_indexes: list[int] = []
        for serialized_call in serialized_calls:
            call_id = serialized_call["id"]
            if call_id not in observations:
                raise ExportError(
                    "missing_tool_result", f"ATIF tool call {call_id!r} has no result."
                )
            tool_index = len(nodes)
            nodes.append(
                _make_node(
                    context,
                    index=tool_index,
                    parent_index=call_index,
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
            tool_indexes.append(tool_index)
            conversation.append(
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "name": serialized_call["name"],
                    "content": observations[call_id],
                }
            )
        if unassociated_results:
            nodes[call_index] = nodes[call_index].model_copy(
                update={
                    "attributes": {
                        **(nodes[call_index].attributes or {}),
                        "observation_results": unassociated_results,
                    }
                }
            )
        pending_parents = tool_indexes or [call_index]

    tokens, cost = _atif_final_usage(
        trace.get("final_metrics"),
        _add_token_usage(usages),
        sum(costs, Decimal(0)) if costs else None,
    )
    return _build_view(
        context,
        nodes,
        outputs=final_output,
        started_at=nodes[0].started_at,
        ended_at=nodes[-1].ended_at,
        tokens=tokens,
        cost=cost,
        llm_call_count=declared_llm_calls,
    )


def _verifiers_usage(value: Any, path: str) -> tuple[TokenUsage | None, Decimal | None]:
    if value is None:
        return None, None
    usage = _require_dict(value, path)
    try:
        prompt_tokens = usage.get("prompt_tokens")
        cached_input_tokens = usage.get("cached_input_tokens")
        if prompt_tokens is not None and cached_input_tokens is not None:
            input_tokens = prompt_tokens + cached_input_tokens
        else:
            input_tokens = prompt_tokens
        tokens = TokenUsage(
            input_tokens=input_tokens,
            output_tokens=usage.get("completion_tokens"),
            cached_input_tokens=cached_input_tokens,
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


def _error_record(value: Any, path: str) -> dict[str, Any]:
    error = _require_dict(value, path)
    message = _error_message(error, path)
    error_type = error.get("type")
    if not isinstance(error_type, str) or not error_type:
        raise ExportError("invalid_trace", f"{path}.type is required.")
    record: dict[str, Any] = {"type": error_type, "message": message}
    for field in ("status_code", "traceback"):
        if error.get(field) is not None:
            record[field] = error[field]
    return record


def _validate_verifiers_graph(raw_nodes: list[Any]) -> list[dict[str, Any]]:
    nodes = [
        _require_dict(raw_node, f"nodes[{position}]")
        for position, raw_node in enumerate(raw_nodes)
    ]
    indexes = set(range(len(nodes)))
    maximum_index = len(nodes) - 1
    roots = 0
    for position, node in enumerate(nodes):
        if "parent" not in node:
            raise ExportError(
                "missing_trace_parent", f"nodes[{position}].parent is required."
            )
        parent = _require_parent(node["parent"], f"nodes[{position}].parent")
        if parent is None:
            roots += 1
            if position != 0:
                raise ExportError(
                    "unsupported_trace_shape",
                    "Verifiers traces must contain one connected message graph.",
                )
            continue
        _validate_parent_references(
            node_index=position,
            parents=[parent],
            indexes=indexes,
            maximum_index=maximum_index,
            path=f"nodes[{position}].parent",
        )
    if roots != 1:
        raise ExportError(
            "unsupported_trace_shape",
            "Verifiers traces must contain one connected message graph.",
        )
    return nodes


def _verifiers_message_path(
    raw_nodes: Sequence[dict[str, Any]], parent: int | None
) -> list[dict[str, Any]]:
    indexes = _ancestor_indexes(raw_nodes, parent)
    return [dict(raw_nodes[index]["message"]) for index in indexes]


def _ancestor_indexes(
    raw_nodes: Sequence[dict[str, Any]], parent: int | None
) -> list[int]:
    indexes: list[int] = []
    while parent is not None:
        indexes.append(parent)
        parent = raw_nodes[parent]["parent"]
    indexes.reverse()
    return indexes


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
    if trace.get("is_completed") is not True:
        raise ExportError("incomplete_trace", "Verifiers trace did not complete.")
    if not isinstance(trace.get("ok"), bool):
        raise ExportError("invalid_trace", "Verifiers trace ok must be boolean.")
    source_nodes = _validate_verifiers_graph(raw_nodes)

    calls_raw = _require_list(trace.get("calls", []), "calls")
    calls: dict[int, dict[str, Any]] = {}
    uncommitted_calls: list[dict[str, Any]] = []
    usages: list[TokenUsage] = []
    costs: list[Decimal] = []
    for position, raw_call in enumerate(calls_raw):
        call = _require_dict(raw_call, f"calls[{position}]")
        node = call.get("node")
        tokens, cost = _verifiers_usage(call.get("usage"), f"calls[{position}].usage")
        if tokens is not None:
            usages.append(tokens)
        if cost is not None:
            costs.append(cost)
        call_error = _error_message(call.get("error"), f"calls[{position}].error")
        if node is None:
            if call_error is None:
                raise ExportError(
                    "unsupported_trace_shape",
                    "Uncommitted Verifiers model calls must record an error.",
                )
            uncommitted_calls.append(dict(call))
            continue
        if isinstance(node, bool) or not isinstance(node, int):
            raise ExportError(
                "invalid_trace", "Verifiers model call node must be an integer or null."
            )
        if node < 0 or node >= len(source_nodes):
            raise ExportError(
                "trace_parent_out_of_range",
                "Verifiers model call references a node outside the trace.",
            )
        if node in calls:
            raise ExportError(
                "invalid_trace", "Verifiers committed model calls must be unique."
            )
        target_message = _require_dict(
            source_nodes[node].get("message"), f"nodes[{node}].message"
        )
        if (
            target_message.get("role") != "assistant"
            or source_nodes[node].get("sampled") is not True
        ):
            raise ExportError(
                "unsupported_trace_shape",
                "Verifiers model calls must reference sampled assistant messages.",
            )
        if call_error is not None:
            raise ExportError(
                "unsupported_trace_shape",
                "Failed Verifiers model calls cannot reference committed messages.",
            )
        calls[node] = call

    nodes: list[SessionNodeResponse] = []
    known_tool_calls: dict[str, tuple[int, str, dict[str, Any]]] = {}
    resolved_tool_calls: set[str] = set()
    final_output: Any = None
    for position, source in enumerate(source_nodes):
        parent = source["parent"]
        message = _require_dict(source.get("message"), f"nodes[{position}].message")
        role = message.get("role")
        timestamp = _parse_epoch(
            source.get("timestamp"), f"nodes[{position}].timestamp"
        )
        if role in {"system", "user"}:
            if source.get("sampled") is True:
                raise ExportError(
                    "invalid_trace", "Verifiers prompt messages cannot be sampled."
                )
            content = _content(
                message.get("content"), f"nodes[{position}].message.content"
            )
            nodes.append(
                _make_node(
                    context,
                    index=position,
                    parent_index=parent,
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
            structured_output: dict[str, Any] | None = None
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
                    or call_id in known_tool_calls
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
                is_terminal_output = (
                    len(tool_calls) == 1
                    and position == len(source_nodes) - 1
                    and name == "final_result"
                    and content is None
                )
                if is_terminal_output:
                    structured_output = parsed_arguments
                    final_output = parsed_arguments
                    continue
                known_tool_calls[call_id] = (position, name, parsed_arguments)
                serialized_calls.append(
                    {"id": call_id, "name": name, "arguments": arguments}
                )
            call = calls.get(position)
            if call is None:
                raise ExportError(
                    "missing_model_call",
                    "Every sampled Verifiers assistant message needs a model call.",
                )
            tokens, cost = _verifiers_usage(
                call.get("usage"), f"calls[node={position}].usage"
            )
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
                    parent_index=parent,
                    node_type=NodeType.LLM_CALL,
                    name="model_call",
                    inputs={"messages": _verifiers_message_path(source_nodes, parent)},
                    outputs=(
                        structured_output
                        if structured_output is not None
                        else {
                            "message": content,
                            **(
                                {"tool_calls": serialized_calls}
                                if serialized_calls
                                else {}
                            ),
                        }
                    ),
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
            if source.get("sampled") is True:
                raise ExportError(
                    "invalid_trace", "Verifiers tool results cannot be sampled."
                )
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or call_id not in known_tool_calls:
                raise ExportError(
                    "invalid_trace", "Verifiers tool result has no prior tool call."
                )
            if call_id in resolved_tool_calls:
                raise ExportError(
                    "invalid_trace", "Verifiers tool call has duplicate results."
                )
            call_node, name, arguments = known_tool_calls[call_id]
            ancestors = set(_ancestor_indexes(source_nodes, parent))
            if call_node not in ancestors:
                raise ExportError(
                    "unsupported_trace_shape",
                    "Verifiers tool results must descend from their tool call.",
                )
            resolved_tool_calls.add(call_id)
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
                    parent_index=parent,
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
    if set(known_tool_calls) != resolved_tool_calls:
        raise ExportError(
            "missing_tool_result", "Verifiers trace has tool calls without results."
        )

    errors_raw = _require_list(trace.get("errors", []), "errors")
    errors = [
        _error_record(error, f"errors[{position}]")
        for position, error in enumerate(errors_raw)
    ]
    if trace["ok"] is False and not errors:
        raise ExportError(
            "invalid_trace", "Failed Verifiers trace has no recorded error."
        )
    status = SessionStatus.COMPLETED if trace["ok"] else SessionStatus.FAILED
    error = errors[-1]["message"] if errors and not trace["ok"] else None
    return _build_view(
        context,
        nodes,
        outputs=final_output,
        status=status,
        error=error,
        started_at=nodes[0].started_at,
        ended_at=nodes[-1].ended_at,
        tokens=_add_token_usage(usages),
        cost=sum(costs, Decimal(0)) if costs else None,
        llm_call_count=len(calls_raw),
        metadata={
            "trace_conversion": {
                "errors": errors,
                "uncommitted_model_calls": uncommitted_calls,
            }
        },
    )
