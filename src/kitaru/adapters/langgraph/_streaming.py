"""Best-effort live events for LangGraph v2 streaming."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast, get_args

import kitaru.events as kitaru_events
from kitaru.adapters._streaming_utils import (
    BaseStreamPublisher,
    clip_stream_text,
    safe_string,
)
from kitaru.errors import KitaruUsageError

from ._policy import LangGraphStreamPolicy
from ._serialization import redact_config
from ._types import LangGraphStreamMode

LANGGRAPH_STREAM_STARTED = "langgraph.stream.started"
LANGGRAPH_STREAM_COMPLETED = "langgraph.stream.completed"
LANGGRAPH_STREAM_FAILED = "langgraph.stream.failed"
LANGGRAPH_STREAM_MESSAGES = "langgraph.stream.messages"
LANGGRAPH_STREAM_UPDATES = "langgraph.stream.updates"
LANGGRAPH_STREAM_CUSTOM = "langgraph.stream.custom"
LANGGRAPH_STREAM_VALUES = "langgraph.stream.values"
LANGGRAPH_STREAM_CHECKPOINTS = "langgraph.stream.checkpoints"
LANGGRAPH_STREAM_TASKS = "langgraph.stream.tasks"
LANGGRAPH_STREAM_DEBUG = "langgraph.stream.debug"

LANGGRAPH_STREAM_EVENT_KINDS = (
    LANGGRAPH_STREAM_STARTED,
    LANGGRAPH_STREAM_MESSAGES,
    LANGGRAPH_STREAM_UPDATES,
    LANGGRAPH_STREAM_CUSTOM,
    LANGGRAPH_STREAM_VALUES,
    LANGGRAPH_STREAM_CHECKPOINTS,
    LANGGRAPH_STREAM_TASKS,
    LANGGRAPH_STREAM_DEBUG,
    LANGGRAPH_STREAM_COMPLETED,
    LANGGRAPH_STREAM_FAILED,
)
LANGGRAPH_STREAM_TERMINAL_EVENT_KINDS = (
    LANGGRAPH_STREAM_COMPLETED,
    LANGGRAPH_STREAM_FAILED,
)

STREAM_VERSION = "v2"
SUPPORTED_STREAM_MODES = cast(
    tuple[LangGraphStreamMode, ...], get_args(LangGraphStreamMode)
)
_RECONSTRUCTION_MODE: LangGraphStreamMode = "values"
_MODE_EVENT_KINDS: dict[LangGraphStreamMode, str] = {
    "messages": LANGGRAPH_STREAM_MESSAGES,
    "updates": LANGGRAPH_STREAM_UPDATES,
    "custom": LANGGRAPH_STREAM_CUSTOM,
    "values": LANGGRAPH_STREAM_VALUES,
    "checkpoints": LANGGRAPH_STREAM_CHECKPOINTS,
    "tasks": LANGGRAPH_STREAM_TASKS,
    "debug": LANGGRAPH_STREAM_DEBUG,
}
_SAFE_MESSAGE_METADATA_KEYS = {
    "thread_id",
    "langgraph_node",
    "langgraph_step",
    "langgraph_path",
    "langgraph_checkpoint_ns",
    "ls_provider",
    "ls_model_type",
    "tags",
}
_MAX_ERROR_CHARS = 500
_MAX_STREAM_METADATA_ITEMS = 20
_MAX_STREAM_SUMMARY_DEPTH = 4
_MAX_STREAM_TOTAL_ITEMS = 256
_MAX_STREAM_APPROX_CHARS = 20_000
_MAX_STREAM_LABEL_CHARS = 200


@dataclass(frozen=True)
class LangGraphStreamOptions:
    """Resolved public and internal LangGraph stream options."""

    requested_modes: tuple[LangGraphStreamMode, ...]
    upstream_modes: tuple[LangGraphStreamMode, ...]
    # Today this matches requested_modes. Keep it separate so Kitaru can later
    # request an upstream mode for internal use without publishing it as a live event.
    published_modes: tuple[LangGraphStreamMode, ...]
    subgraphs: bool = False
    version: str = STREAM_VERSION

    def cache_identity(self) -> dict[str, object]:
        return {
            "stream_modes_requested": list(self.requested_modes),
            "stream_modes_upstream": list(self.upstream_modes),
            "stream_version": self.version,
            "stream_subgraphs": self.subgraphs,
        }

    def tracker_metadata(self) -> dict[str, object]:
        return self.cache_identity()


@dataclass
class LangGraphStreamStats:
    """Compact counters for one drained LangGraph stream."""

    counts_by_mode: dict[str, int] = field(default_factory=dict)
    part_count: int = 0

    def record(self, mode: str | None) -> None:
        self.part_count += 1
        key = mode or "unknown"
        self.counts_by_mode[key] = self.counts_by_mode.get(key, 0) + 1

    def metadata(self) -> dict[str, object]:
        return {
            "stream_event_counts_by_mode": dict(sorted(self.counts_by_mode.items())),
            "stream_part_count": self.part_count,
        }


class LangGraphStreamPublisher(BaseStreamPublisher):
    """Normalize LangGraph v2 stream parts and publish Kitaru live events.

    Live events are observation-only. Publishing is best effort: if the runtime
    cannot publish from the current context, graph execution still continues.
    """

    _started_kind = LANGGRAPH_STREAM_STARTED
    _completed_kind = LANGGRAPH_STREAM_COMPLETED
    _failed_kind = LANGGRAPH_STREAM_FAILED
    _normalization_failed_kind = LANGGRAPH_STREAM_DEBUG
    _started_display = "LangGraph stream started"
    _completed_display_prefix = "LangGraph stream completed"
    _failed_display_prefix = "LangGraph stream failed"
    _normalization_failed_category = "stream_part_normalization_failed"
    _normalization_failed_display = "LangGraph stream part"
    _error_message_limit = _MAX_ERROR_CHARS

    def __init__(
        self,
        *,
        graph_name: str,
        thread_id: str,
        policy: LangGraphStreamPolicy,
        options: LangGraphStreamOptions,
    ) -> None:
        super().__init__(
            publish=lambda *args, **kwargs: kitaru_events.publish(*args, **kwargs)
        )
        self._graph_name = graph_name
        self._thread_id = thread_id
        self._policy = policy
        self._options = options

    def part(self, part: Any) -> None:
        self._publish_stream_item(part)

    def completed(self, *, status: str, stats: LangGraphStreamStats) -> None:
        self._publish_completed(status=status, **stats.metadata())

    def failed(self, error: BaseException, *, stats: LangGraphStreamStats) -> None:
        self._publish_failed(error, **stats.metadata())

    def _normalize_publishable(self, item: Any) -> tuple[str, dict[str, Any]]:
        return self.normalize_part(item)

    def _normalization_failed_payload(self, item: Any) -> dict[str, Any]:
        return self._base_payload(
            category="stream_part_normalization_failed",
            mode="debug",
            display="LangGraph stream part",
            event_type=type(item).__name__,
        )

    def normalize_part(self, part: Any) -> tuple[str, dict[str, Any]]:
        if not isinstance(part, Mapping):
            return (
                LANGGRAPH_STREAM_DEBUG,
                self._base_payload(
                    category="stream_part_normalization_failed",
                    mode="debug",
                    display="Malformed LangGraph stream part",
                    event_type=type(part).__name__,
                ),
            )
        mode = safe_string(part.get("type"))
        if mode not in _MODE_EVENT_KINDS:
            return (
                LANGGRAPH_STREAM_DEBUG,
                self._base_payload(
                    category="stream_part_unknown_mode",
                    mode="debug",
                    display=f"Unknown LangGraph stream mode: {mode or 'unknown'}",
                    event_type=mode or type(part).__name__,
                    namespace=_namespace(part),
                ),
            )
        stream_mode = mode
        payload = self._normalize_known_mode(stream_mode, part)
        return _MODE_EVENT_KINDS[stream_mode], payload

    def _normalize_known_mode(
        self, mode: LangGraphStreamMode, part: Mapping[str, Any]
    ) -> dict[str, Any]:
        data = part.get("data")
        if mode == "messages":
            return self._normalize_messages(part, data)
        if mode == "updates":
            return self._normalize_updates(part, data)
        if mode == "custom":
            return self._normalize_custom(part, data)
        return self._normalize_structural(mode, part, data)

    def _normalize_messages(self, part: Mapping[str, Any], data: Any) -> dict[str, Any]:
        chunk: Any | None = None
        metadata: Any = None
        if isinstance(data, tuple | list) and len(data) == 2:
            chunk, metadata = data
        text_delta = _message_content_text(
            chunk,
            limit=self._policy.max_display_chars,
        )
        message_type = safe_string(getattr(chunk, "type", None)) or safe_string(
            _mapping_get(chunk, "type")
        )
        safe_metadata = _safe_metadata(
            metadata,
            string_limit=self._policy.max_display_chars,
        )
        payload = self._base_payload(
            category="stream_part",
            mode="messages",
            display=clip_stream_text(
                text_delta
                if self._policy.include_message_text_deltas and text_delta
                else message_type or "Message chunk",
                self._policy.max_display_chars,
            ),
            namespace=_namespace(part),
        )
        if self._policy.include_message_text_deltas and text_delta is not None:
            payload["text_delta"] = clip_stream_text(
                text_delta, self._policy.max_display_chars
            )
        if message_type is not None:
            payload["message_type"] = message_type
        if safe_metadata:
            payload["metadata"] = safe_metadata
        node_name = safe_metadata.get("langgraph_node") if safe_metadata else None
        if isinstance(node_name, str):
            payload["node_name"] = node_name
        return payload

    def _normalize_updates(self, part: Mapping[str, Any], data: Any) -> dict[str, Any]:
        node_names: list[str] = []
        updated_keys_by_node: dict[str, list[str]] = {}
        updated_key_counts_by_node: dict[str, int] = {}
        value_types_by_node: dict[str, dict[str, str] | str] = {}
        node_count = _safe_len(data) if isinstance(data, Mapping) else 0
        if isinstance(data, Mapping):
            for index, (node_name, update) in enumerate(data.items()):
                if index >= _MAX_STREAM_METADATA_ITEMS:
                    break
                node_label = _safe_label(node_name, limit=_MAX_STREAM_LABEL_CHARS)
                node_names.append(node_label)
                if isinstance(update, Mapping):
                    updated_key_counts_by_node[node_label] = _safe_len(update)
                    updated_keys_by_node[node_label] = _first_string_labels(update)
                    value_types_by_node[node_label] = {
                        _safe_label(key, limit=_MAX_STREAM_LABEL_CHARS): type(
                            value
                        ).__qualname__
                        for key, value in _first_mapping_items(update)
                    }
                else:
                    value_types_by_node[node_label] = type(update).__qualname__
        payload = self._base_payload(
            category="stream_part",
            mode="updates",
            display=clip_stream_text(
                _updates_display(node_names), self._policy.max_display_chars
            ),
            namespace=_namespace(part),
            node_count=node_count,
            node_names=node_names,
            nodes_truncated=max(node_count - len(node_names), 0),
            updated_keys_by_node=updated_keys_by_node,
            updated_key_counts_by_node=updated_key_counts_by_node,
            value_types_by_node=value_types_by_node,
        )
        if self._policy.include_raw_payloads:
            payload["raw"] = redact_config(
                _bounded_json_safe(
                    data,
                    string_limit=self._policy.max_display_chars,
                )
            )
        return payload

    def _normalize_custom(self, part: Mapping[str, Any], data: Any) -> dict[str, Any]:
        if self._policy.include_custom_payload:
            safe_data = redact_config(
                _bounded_json_safe(data, string_limit=self._policy.max_display_chars)
            )
            payload = self._base_payload(
                category="stream_part",
                mode="custom",
                display=_display_text(safe_data, limit=self._policy.max_display_chars),
                namespace=_namespace(part),
                custom_summary=_structural_summary(safe_data),
                custom=safe_data,
            )
            return payload

        return self._base_payload(
            category="stream_part",
            mode="custom",
            display="Custom stream payload",
            namespace=_namespace(part),
            custom_summary=_structural_summary(data, include_keys=False),
        )

    def _normalize_structural(
        self,
        mode: LangGraphStreamMode,
        part: Mapping[str, Any],
        data: Any,
    ) -> dict[str, Any]:
        summary = _structural_summary(data)
        payload = self._base_payload(
            category="stream_part",
            mode=mode,
            display=clip_stream_text(
                f"LangGraph {mode}: {summary['summary']}",
                self._policy.max_display_chars,
            ),
            namespace=_namespace(part),
            summary=summary,
        )
        interrupts = part.get("interrupts")
        if interrupts:
            safe_interrupts = _bounded_json_safe(
                interrupts,
                string_limit=self._policy.max_display_chars,
            )
            payload["interrupt_count"] = _safe_len(interrupts)
            payload["interrupts"] = safe_interrupts
        if self._policy.include_raw_payloads:
            payload["raw"] = redact_config(
                _bounded_json_safe(
                    data,
                    string_limit=self._policy.max_display_chars,
                )
            )
        return payload

    def _base_payload(
        self,
        *,
        category: str,
        display: str,
        mode: str | None = None,
        namespace: list[str] | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        ns = _bounded_string_sequence(namespace or [])
        payload: dict[str, Any] = {
            "adapter": "langgraph",
            "graph_name": self._graph_name,
            "thread_id": self._thread_id,
            "category": category,
            "display": display,
            "langgraph": {
                "stream_version": self._options.version,
                "namespace": ns,
                "subgraph": bool(ns),
            },
            **fields,
        }
        if mode is not None:
            payload["mode"] = mode
        return payload


def resolve_stream_options(
    stream_mode: LangGraphStreamMode | Sequence[LangGraphStreamMode] | None,
    *,
    policy: LangGraphStreamPolicy,
    subgraphs: bool,
) -> LangGraphStreamOptions:
    """Resolve public stream modes plus Kitaru's internal reconstruction mode."""
    requested = (
        policy.default_modes if stream_mode is None else _normalize_modes(stream_mode)
    )
    for mode in requested:
        if mode == "debug" and not policy.allow_debug:
            raise KitaruUsageError(
                "LangGraph stream mode 'debug' can expose large or sensitive "
                "runtime internals. Set LangGraphStreamPolicy(allow_debug=True) "
                "to request it explicitly."
            )
    upstream = requested
    if _RECONSTRUCTION_MODE not in upstream:
        upstream = (*upstream, _RECONSTRUCTION_MODE)
    return LangGraphStreamOptions(
        requested_modes=requested,
        upstream_modes=upstream,
        published_modes=requested,
        subgraphs=subgraphs,
    )


def stream_part_mode(part: Any) -> str | None:
    if isinstance(part, Mapping):
        mode = part.get("type")
        if isinstance(mode, str):
            return mode
    return None


def stream_part_output_candidate(part: Any) -> tuple[bool, Any]:
    """Return whether ``part`` carries the internal final-output candidate."""
    if not isinstance(part, Mapping) or part.get("type") != _RECONSTRUCTION_MODE:
        return False, None
    if _namespace(part):
        return False, None
    if "data" not in part:
        return False, None
    data = part.get("data")
    interrupts = part.get("interrupts")
    if interrupts:
        if isinstance(data, Mapping):
            return True, {**dict(data), "__interrupt__": interrupts}
        return True, {"value": data, "__interrupt__": interrupts}
    return True, data


def _normalize_modes(
    value: LangGraphStreamMode | Sequence[LangGraphStreamMode],
) -> tuple[LangGraphStreamMode, ...]:
    raw_modes: list[Any] = [value] if isinstance(value, str) else list(value)
    if not raw_modes:
        raise KitaruUsageError("stream_mode must include at least one mode.")
    modes: list[LangGraphStreamMode] = []
    for raw_mode in raw_modes:
        if raw_mode not in SUPPORTED_STREAM_MODES:
            supported = "', '".join(SUPPORTED_STREAM_MODES)
            raise KitaruUsageError(
                f"Unsupported LangGraph stream mode {raw_mode!r}. Supported "
                f"`.stream(..., version=\"v2\")` modes are: '{supported}'. "
                "`stream_events(...)` style modes such as 'events' and "
                "'messages-tuple' are intentionally deferred."
            )
        mode = cast(LangGraphStreamMode, raw_mode)
        if mode not in modes:
            modes.append(mode)
    return tuple(modes)


def _namespace(part: Mapping[str, Any]) -> list[str]:
    raw = part.get("ns")
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, Sequence):
        return _bounded_string_sequence(raw)
    return [_safe_label(raw, limit=_MAX_STREAM_LABEL_CHARS)]


def _mapping_get(value: Any, key: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(key)
    return None


def _message_content_text(chunk: Any, *, limit: int) -> str | None:
    content = getattr(chunk, "content", None)
    if content is None:
        content = _mapping_get(chunk, "content")
    if content is None:
        return None
    if isinstance(content, str):
        return clip_stream_text(content, limit)
    if isinstance(content, list | tuple):
        pieces: list[str] = []
        current_length = 0
        for item in content:
            text: str | None = None
            if isinstance(item, str):
                text = item
            elif isinstance(item, Mapping):
                nested = item.get("text") or item.get("content")
                if isinstance(nested, str):
                    text = nested
            if text is None:
                continue
            remaining = limit - current_length
            if remaining <= 0:
                break
            pieces.append(text[: remaining + 1])
            current_length += len(pieces[-1])
            if current_length > limit:
                break
        return clip_stream_text("".join(pieces), limit) if pieces else None
    return clip_stream_text(str(content), limit)


def _safe_metadata(metadata: Any, *, string_limit: int) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        return {}
    safe: dict[str, Any] = {}
    for key in _SAFE_MESSAGE_METADATA_KEYS:
        if key in metadata:
            safe[key] = _bounded_json_safe(metadata[key], string_limit=string_limit)
    return safe


def _updates_display(node_names: list[str]) -> str:
    if not node_names:
        return "Graph update"
    if len(node_names) == 1:
        return f"Graph update: {node_names[0]}"
    return f"Graph updates: {', '.join(node_names)}"


def _structural_summary(value: Any, *, include_keys: bool = True) -> dict[str, Any]:
    if isinstance(value, Mapping):
        key_count = _safe_len(value)
        summary: dict[str, Any] = {
            "python_type": type(value).__qualname__,
            "key_count": key_count,
            "summary": f"mapping with {key_count} keys",
        }
        if include_keys:
            keys = _first_string_labels(value)
            summary["keys"] = keys
            summary["keys_truncated"] = max(key_count - len(keys), 0)
        return summary
    if isinstance(value, list | tuple):
        return {
            "python_type": type(value).__qualname__,
            "item_count": len(value),
            "summary": f"sequence with {len(value)} items",
        }
    return {
        "python_type": type(value).__qualname__,
        "summary": type(value).__qualname__,
    }


def _display_text(value: Any, *, limit: int) -> str:
    if isinstance(value, Mapping):
        for key in ("message", "event", "status", "type"):
            nested = value.get(key)
            if isinstance(nested, str):
                return clip_stream_text(nested, limit)
    if isinstance(value, str):
        return clip_stream_text(value, limit)
    return clip_stream_text(repr(value), limit)


def _first_mapping_items(value: Mapping[Any, Any]) -> list[tuple[Any, Any]]:
    items: list[tuple[Any, Any]] = []
    for index, item in enumerate(value.items()):
        if index >= _MAX_STREAM_METADATA_ITEMS:
            break
        items.append(item)
    return items


def _safe_label(value: Any, *, limit: int) -> str:
    try:
        text = str(value)
    except Exception:
        text = f"<unprintable key {type(value).__qualname__}>"
    return clip_stream_text(text, limit)


def _first_string_labels(value: Mapping[Any, Any]) -> list[str]:
    labels: list[str] = []
    for key, _ in _first_mapping_items(value):
        labels.append(_safe_label(key, limit=_MAX_STREAM_LABEL_CHARS))
    return labels


def _bounded_string_sequence(value: Sequence[Any]) -> list[str]:
    labels: list[str] = []
    for index, item in enumerate(value):
        if index >= _MAX_STREAM_METADATA_ITEMS:
            break
        labels.append(_safe_label(item, limit=_MAX_STREAM_LABEL_CHARS))
    return labels


@dataclass
class _JsonSafeBudget:
    remaining_items: int
    remaining_chars: int

    def take_item(self) -> bool:
        if self.remaining_items <= 0 or self.remaining_chars <= 0:
            return False
        self.remaining_items -= 1
        return True

    def take_chars(self, count: int) -> int:
        allowed = min(count, self.remaining_chars)
        self.remaining_chars -= allowed
        return allowed

    def exhausted(self) -> bool:
        return self.remaining_items <= 0 or self.remaining_chars <= 0


def _json_safe_truncated_marker(value: Any) -> dict[str, str]:
    return {
        "_kitaru_truncated": "stream_payload_budget_exhausted",
        "python_type": type(value).__qualname__,
    }


def _bounded_json_safe_string(
    value: str,
    *,
    string_limit: int,
    budget: _JsonSafeBudget,
) -> Any:
    clipped = clip_stream_text(value, string_limit)
    allowed_chars = budget.take_chars(len(clipped))
    if allowed_chars <= 0:
        return _json_safe_truncated_marker(value)
    if allowed_chars >= len(clipped):
        return clipped
    return clip_stream_text(clipped[:allowed_chars], allowed_chars)


def _bounded_json_safe_key(
    key: Any,
    *,
    string_limit: int,
    budget: _JsonSafeBudget,
) -> str | None:
    text = _safe_label(key, limit=string_limit)
    allowed_chars = budget.take_chars(len(text))
    if allowed_chars <= 0:
        return None
    if allowed_chars >= len(text):
        return text
    return clip_stream_text(text[:allowed_chars], allowed_chars)


def _bounded_json_safe(
    value: Any,
    *,
    string_limit: int,
    _depth: int = 0,
    _budget: _JsonSafeBudget | None = None,
) -> Any:
    budget = _budget or _JsonSafeBudget(
        remaining_items=_MAX_STREAM_TOTAL_ITEMS,
        remaining_chars=_MAX_STREAM_APPROX_CHARS,
    )
    if not budget.take_item():
        return _json_safe_truncated_marker(value)
    if _depth > _MAX_STREAM_SUMMARY_DEPTH:
        return {
            "summary": "max_depth_exceeded",
            "python_type": type(value).__qualname__,
        }
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return _bounded_json_safe_string(
            value,
            string_limit=string_limit,
            budget=budget,
        )
    if isinstance(value, Mapping):
        item_count = _safe_len(value)
        bounded: dict[str, Any] = {}
        retained_count = 0
        for key, nested in _first_mapping_items(value):
            if budget.exhausted():
                bounded["_kitaru_truncated"] = "stream_payload_budget_exhausted"
                break
            bounded_key = _bounded_json_safe_key(
                key,
                string_limit=string_limit,
                budget=budget,
            )
            if bounded_key is None:
                bounded["_kitaru_truncated"] = "stream_payload_budget_exhausted"
                break
            bounded[bounded_key] = _bounded_json_safe(
                nested,
                string_limit=string_limit,
                _depth=_depth + 1,
                _budget=budget,
            )
            retained_count += 1
        omitted = item_count - retained_count
        if omitted > 0:
            bounded["_kitaru_omitted_keys"] = omitted
        return bounded
    if isinstance(value, list | tuple):
        bounded_items: list[Any] = []
        retained_count = 0
        for item in value[:_MAX_STREAM_METADATA_ITEMS]:
            if budget.exhausted():
                bounded_items.append(
                    {"_kitaru_truncated": "stream_payload_budget_exhausted"}
                )
                break
            bounded_items.append(
                _bounded_json_safe(
                    item,
                    string_limit=string_limit,
                    _depth=_depth + 1,
                    _budget=budget,
                )
            )
            retained_count += 1
        omitted = len(value) - retained_count
        if omitted > 0:
            bounded_items.append({"_kitaru_omitted_items": omitted})
        return bounded_items
    return {
        "python_type": type(value).__qualname__,
        "serialization_error": "unsupported_stream_value",
    }


def _safe_len(value: Any) -> int:
    try:
        return len(value)
    except TypeError:
        return 1
