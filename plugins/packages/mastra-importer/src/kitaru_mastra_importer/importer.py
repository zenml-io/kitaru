"""Normalize selected Mastra getTrace responses without fetching live memory."""

import hashlib
import json
from collections.abc import Iterator
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictBool, StrictStr
from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession, flatten_nodes

MAX_UPLOAD_BYTES = 64 * 1024 * 1024
_MAX_TOKEN_COUNT = 2**63 - 1


class InvalidImport(ValueError):
    """The selected export or importer parameters are invalid."""


class _Span(BaseModel):
    """Fields of the Mastra 1.51 getTrace storage record used in normalization."""

    model_config = ConfigDict(extra="allow")
    traceId: StrictStr = Field(min_length=1)
    spanId: StrictStr = Field(min_length=1)
    parentSpanId: StrictStr | None = None
    name: StrictStr
    spanType: StrictStr
    isEvent: StrictBool = False
    startedAt: AwareDatetime
    endedAt: AwareDatetime | None = None
    input: Any = None
    output: Any = None
    error: Any = None
    attributes: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


def _get_text_selector(value: Any, path: str = "") -> str | None:
    if isinstance(value, str):
        return path if value.strip() else None
    if isinstance(value, list):
        for index in range(len(value) - 1, -1, -1):
            result = _get_text_selector(value[index], f"{path}/{index}")
            if result is not None:
                return result
    if isinstance(value, dict):
        for key in ("text", "content", "messages"):
            if key in value:
                result = _get_text_selector(value[key], f"{path}/{key}")
                if result is not None:
                    return result
    return None


def _get_tokens(attributes: dict[str, Any]) -> TokenUsage | None:
    usage = attributes.get("usage")
    if usage is None:
        return None
    if not isinstance(usage, dict):
        raise ValueError("usage must be an object")
    details = {}
    for name in ("inputDetails", "outputDetails"):
        detail = usage.get(name)
        if detail is None:
            detail = {}
        if not isinstance(detail, dict):
            raise ValueError(f"{name} must be an object")
        details[name] = detail
    counts = {
        "input_tokens": usage.get("inputTokens"),
        "output_tokens": usage.get("outputTokens"),
        "cached_input_tokens": details["inputDetails"].get("cacheRead"),
        "reasoning_tokens": details["outputDetails"].get("reasoning"),
    }
    for count in counts.values():
        if count is not None and (
            type(count) is not int or not 0 <= count <= _MAX_TOKEN_COUNT
        ):
            raise ValueError(
                "token counts must fit a nonnegative signed 64-bit integer"
            )
    return TokenUsage(**counts) if any(v is not None for v in counts.values()) else None


def _get_system_selector(value: Any) -> str | None:
    messages = value.get("messages") if isinstance(value, dict) else value
    prefix = "/messages" if isinstance(value, dict) else ""
    if isinstance(messages, list):
        for index, message in enumerate(messages):
            if isinstance(message, dict) and message.get("role") == "system":
                return _get_text_selector(message, f"{prefix}/{index}")
    return None


def _get_reasoning_selectors(output: Any) -> list[str]:
    if not isinstance(output, dict):
        return []
    reasoning = output.get("reasoning")
    if isinstance(reasoning, str):
        return ["/reasoning"]
    if (
        isinstance(reasoning, list)
        and reasoning
        and all(
            isinstance(part, dict) and isinstance(part.get("text"), str)
            for part in reasoning
        )
    ):
        return [f"/reasoning/{index}/text" for index in range(len(reasoning))]
    return []


def _get_cost(attributes: dict[str, Any]) -> Decimal | None:
    context = attributes.get("costContext")
    if context is None:
        return None
    if not isinstance(context, dict):
        raise ValueError("costContext must be an object")
    value = context.get("estimatedCost")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("estimatedCost must be a number")
    cost = Decimal(str(value))
    if not cost.is_finite() or cost < 0:
        raise ValueError("estimatedCost must be finite and nonnegative")
    # Kitaru's cost field is USD. An absent or different unit is not USD evidence.
    return cost if context.get("costUnit") == "USD" else None


def _get_sequence(span: _Span) -> int:
    key = {"model_step": "stepIndex", "model_chunk": "sequenceNumber"}.get(
        span.spanType
    )
    sequence = (span.attributes or {}).get(key, 0) if key else 0
    if type(sequence) is not int or sequence < 0:
        raise ValueError("span sequence must be a nonnegative integer")
    return sequence


def _get_ordered_spans(trace: dict[str, Any]) -> list[_Span]:
    raw = trace.get("spans")
    if not isinstance(raw, list) or not raw:
        raise ValueError("getTrace response must contain a nonempty spans array")
    spans = [_Span.model_validate(item) for item in raw]
    by_id = {span.spanId: span for span in spans}
    if len(by_id) != len(spans):
        raise ValueError("duplicate spanId in trace")
    if any(span.traceId != trace["traceId"] for span in spans):
        raise ValueError("span traceId differs from getTrace traceId")
    roots = [span for span in spans if span.parentSpanId is None]
    if len(roots) != 1:
        raise ValueError("trace must have exactly one root span")
    children: dict[str, list[_Span]] = {}
    for span in spans:
        if span.endedAt is not None and span.endedAt < span.startedAt:
            raise ValueError("span endedAt precedes startedAt")
        if span.parentSpanId is not None:
            if span.parentSpanId not in by_id:
                raise ValueError("trace is incomplete: parent span is missing")
            children.setdefault(span.parentSpanId, []).append(span)
    ordered = []
    stack = [(roots[0], 1)]
    while stack:
        span, depth = stack.pop()
        if depth > 64:
            raise ValueError("span hierarchy exceeds 64 levels")
        ordered.append(span)
        stack.extend(
            (child, depth + 1)
            for child in sorted(
                children.get(span.spanId, []),
                key=lambda item: (item.startedAt, _get_sequence(item), item.spanId),
                reverse=True,
            )
        )
    if len(ordered) != len(spans):
        raise ValueError("trace contains a disconnected cycle")
    return ordered


def _get_error(span: _Span) -> str | None:
    if span.error is None:
        return None
    return span.error if isinstance(span.error, str) else json.dumps(span.error)


def _get_accounted_usage(
    span: _Span,
    by_id: dict[str, _Span],
    usage: dict[str, tuple[TokenUsage | None, Decimal | None]],
) -> tuple[TokenUsage | None, Decimal | None]:
    """Exclude step/inference counters already reported by their model loop.

    A nested generation is independent, including when called from a tool.
    Missing aggregate counters fall back individually to the source breakdown.
    """
    tokens, cost = usage[span.spanId]
    if span.spanType not in {"model_step", "model_inference"}:
        return tokens, cost
    counts = tokens.model_dump() if tokens is not None else {}
    parent_id = span.parentSpanId
    while parent_id is not None:
        parent = by_id[parent_id]
        if parent.spanType not in {"model_generation", "model_step"}:
            break
        parent_tokens, parent_cost = usage[parent_id]
        if parent_tokens is not None:
            for key, value in parent_tokens.model_dump().items():
                if value is not None:
                    counts.pop(key, None)
        if parent_cost is not None:
            cost = None
        if parent.spanType == "model_generation":
            break
        parent_id = parent.parentSpanId
    return (
        TokenUsage(**counts) if any(v is not None for v in counts.values()) else None,
        cost,
    )


def _normalize(
    trace: dict[str, Any], namespace: str | None, history_only: bool
) -> ImportedSession:
    spans = _get_ordered_spans(trace)
    root = spans[0]
    indexes = {span.spanId: index for index, span in enumerate(spans)}
    by_id = {span.spanId: span for span in spans}
    usage = {
        span.spanId: (
            _get_tokens(span.attributes or {}),
            _get_cost(span.attributes or {}),
        )
        for span in spans
    }
    nodes = []
    for index, span in enumerate(spans):
        attributes = span.attributes or {}
        tokens, cost = _get_accounted_usage(span, by_id, usage)
        node_type = {
            "model_inference": NodeType.LLM_CALL,
            "tool_call": NodeType.TOOL_CALL,
            "mcp_tool_call": NodeType.TOOL_CALL,
        }.get(span.spanType, NodeType.SPAN)
        nodes.append(
            ImportedNode(
                index=index,
                parent_index=indexes.get(span.parentSpanId),
                external_id=span.spanId,
                trace_id=span.traceId,
                node_type=node_type,
                name=span.name,
                status=(
                    NodeStatus.FAILED
                    if span.error is not None
                    else NodeStatus.COMPLETED
                    if span.endedAt or span.isEvent
                    else NodeStatus.IN_PROGRESS
                ),
                error=_get_error(span),
                started_at=span.startedAt,
                ended_at=span.endedAt,
                inputs=span.input,
                outputs=span.output,
                input_text_selector=_get_text_selector(span.input),
                output_text_selector=_get_text_selector(span.output),
                system_prompt_selector=_get_system_selector(span.input),
                reasoning_selectors=_get_reasoning_selectors(span.output),
                requested_model=attributes.get("model"),
                model=attributes.get("responseModel") or attributes.get("model"),
                model_provider=attributes.get("provider"),
                model_params=attributes.get("parameters"),
                tokens=tokens,
                cost=cost,
                tool_name=(span.model_extra or {}).get("entityId")
                if node_type is NodeType.TOOL_CALL
                else None,
                attributes=attributes,
                metadata={
                    "mastra": span.model_dump(
                        mode="json", exclude={"input", "output", "attributes", "error"}
                    )
                },
            )
        )
    attributes = root.attributes or {}
    # Session totals are persisted as signed 64-bit integers even though a
    # Python integer and the source JSON can represent larger values.
    for field in TokenUsage.model_fields:
        if (
            sum(node.tokens.model_dump()[field] or 0 for node in nodes if node.tokens)
            > _MAX_TOKEN_COUNT
        ):
            raise ValueError("session token total exceeds signed 64-bit range")
    source_metadata = root.metadata or {}
    context_id = root.model_extra.get("threadId") if root.model_extra else None
    context_id = (
        context_id
        or source_metadata.get("threadId")
        or attributes.get("conversationId")
    )
    reasons = []
    if not history_only:
        reasons.append(
            "Replay requires a history-only declaration and the "
            "context-capable adapter (#1050)"
        )
    if root.spanType != "agent_run":
        reasons.append("Root is not one agent invocation")
    if root.input is None:
        reasons.append("Export omits invocation input")
    context_spans = [
        span
        for span in spans
        if span.spanType == "model_generation"
        and span.parentSpanId == root.spanId
        and isinstance(span.input, dict)
        and isinstance(span.input.get("messages"), list)
        and span.input["messages"]
    ]
    context_span_id = context_spans[0].spanId if len(context_spans) == 1 else None
    if context_span_id is None:
        reasons.append(
            "Export does not identify one initial full model-message context"
        )
    if history_only and not isinstance(context_id, str):
        reasons.append("Export does not identify a memory-dependent invocation")
    messages = by_id[context_span_id].input["messages"] if context_span_id else []
    if any(
        not isinstance(message, dict)
        or message.get("role") not in ("system", "user", "assistant", "tool")
        or not isinstance(message.get("content"), (str, list))
        for message in messages
    ):
        reasons.append("Initial model context contains unsupported message records")
    if any(s.endedAt is None and not s.isEvent for s in spans):
        reasons.append("Export contains unfinished spans")
    inputs = root.input
    selector = _get_text_selector(root.input)
    if history_only:
        # #1050's source tag describes a memory-dependent snapshot, including
        # system and supplied messages. The operator supplies the history-only
        # constraint because getTrace does not export the full memory config.
        inputs = {
            "mastra_conversation_context": {
                "version": 1,
                "source": "recalled",
                "complete": not reasons,
                "messages": messages,
                **({"reason": "; ".join(reasons)} if reasons else {}),
            },
            "supplied_messages": root.input,
        }
        selector = f"/supplied_messages{selector}" if selector is not None else None
    external_id = trace["traceId"]
    if namespace:
        external_id = hashlib.sha256(
            json.dumps([namespace, external_id]).encode()
        ).hexdigest()
    session = ImportedSession(
        external_id=external_id,
        framework="mastra",
        name=root.name,
        status=SessionStatus(nodes[0].status.value),
        error=_get_error(root),
        started_at=root.startedAt,
        ended_at=root.endedAt,
        inputs=inputs,
        outputs=root.output,
        input_text_selector=selector,
        output_text_selector=_get_text_selector(root.output),
        nodes=nodes,
        metadata={
            "mastra": {
                "format": "getTrace",
                "verified_core_version": "1.51.0",
                "trace_id": trace["traceId"],
                "source_namespace": namespace,
                "conversation_id": context_id,
                "invocation_started_at": root.startedAt.isoformat(),
                "instructions": attributes.get("instructions"),
                "replay": {
                    "eligible": not reasons,
                    "history_only_declared": history_only,
                    "reasons": reasons,
                    "context_span_id": context_span_id,
                    "context_input_pointer": "/messages" if context_span_id else None,
                },
            }
        },
    )
    flatten_nodes(session.nodes)
    session.model_dump_json()
    return session


def parse(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse one full getTrace response or an array of selected responses.

    Args:
        payload: UTF-8 JSON exported from Mastra's full getTrace API.
        params: Optional source_namespace and replay_context="history-only".
            The latter asserts an unredacted history-only invocation without
            advanced memory, user input processors, or prepareStep.

    Yields:
        Sessions in invocation start order and isolated trace failures.

    Raises:
        InvalidImport: The payload envelope or parameters are invalid.
    """
    if set(params) - {"source_namespace", "replay_context"}:
        raise InvalidImport("Only source_namespace and replay_context are supported")
    mode = params.get("replay_context")
    if mode is not None and mode != "history-only":
        raise InvalidImport('replay_context must be "history-only" when provided')
    namespace = params.get("source_namespace")
    if namespace is not None and (
        not isinstance(namespace, str) or not namespace.strip()
    ):
        raise InvalidImport("source_namespace must be a nonempty string")
    if len(payload) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Mastra export exceeds the 64 MiB upload limit")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise InvalidImport("Mastra export must be UTF-8 JSON") from exc
    records = value if isinstance(value, list) else [value]
    if not records:
        raise InvalidImport("Mastra export contains no traces")
    sessions: list[ImportedSession] = []
    seen: dict[str, str] = {}
    conflicting_ids: set[str] = set()
    for line, record in enumerate(records, 1):
        external_id = None
        try:
            if not isinstance(record, dict):
                raise ValueError("trace must be an object")
            external_id = record.get("traceId")
            if not isinstance(external_id, str) or not external_id.strip():
                external_id = None
                raise ValueError("traceId must be a nonempty string")
            signature = json.dumps(record, sort_keys=True, allow_nan=False)
            if external_id in seen:
                if signature == seen[external_id]:
                    continue
                conflicting_ids.add(external_id)
                raise ValueError("conflicting duplicate traceId in export")
            seen[external_id] = signature
            session = _normalize(record, namespace, mode == "history-only")
            sessions.append(session)
        except (
            ValueError,
            TypeError,
            RecursionError,
            InvalidOperation,
            PydanticSerializationError,
        ) as exc:
            yield ImportFailure(
                line=line,
                external_id=(
                    external_id.encode("utf-8", errors="backslashreplace").decode()
                    if external_id is not None
                    else None
                ),
                error=str(exc).encode("utf-8", errors="backslashreplace").decode(),
            )
    sessions.sort(key=lambda session: (session.started_at, session.external_id))
    for session in sessions:
        if session.metadata["mastra"]["trace_id"] not in conflicting_ids:
            yield session
