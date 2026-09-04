#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Deterministic profiling for post-import insight generation."""

import hashlib
import json
import math
import re
import uuid
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from itertools import pairwise
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.insight import (
    Bin,
    BinnedInsightData,
    CategoricalInsightData,
    CategoryValue,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.insights.models import (
    MAX_CONTRIBUTING_SESSIONS,
    MAX_EVIDENCE_LOCATORS,
    MAX_INVESTIGATION_PROMPT_LENGTH,
    Coverage,
    CoverageTruncation,
    EvidenceLocator,
)
from kitaru.redaction import redact_data

ANALYSIS_VERSION = "2026-09-04.1"
MAX_LABEL_LENGTH = 120

_CORRECTION_PATTERN = re.compile(
    r"\b(?:that(?:'s| is) not|you (?:didn't|did not)|doesn't work|does not work|"
    r"not what i|try again|wrong)\b",
    flags=re.IGNORECASE,
)
_PUNCTUATION_PATTERN = re.compile(r"[!?]{3,}")
_PROFANITY_PATTERN = re.compile(
    r"\b(?:fuck(?:ed|er|ers|ing|s)?|shit(?:ty|ting|s)?|bullshit|asshole|"
    r"bastard|bitch(?:es)?)\b",
    flags=re.IGNORECASE,
)
_CONTROL_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
_PRIVATE_KEY_PATTERN_SUFFIX = "PRIVATE" + r" KEY-----"
_CREDENTIAL_PATTERNS = (
    re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b", re.IGNORECASE),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b", re.IGNORECASE),
    re.compile(r"\bxox[a-z]-[A-Za-z0-9-]{20,}\b", re.IGNORECASE),
    re.compile(r"\b(?:AK" r"IA|AS" r"IA)[A-Z0-9]{16}\b"),
    re.compile(r"\be" r"yJ[A-Za-z0-9_-]+\.e" r"yJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"),
    re.compile(r"-----BEGIN [A-Z ]*" + _PRIVATE_KEY_PATTERN_SUFFIX, re.IGNORECASE),
    re.compile(r"\bauthorization\s*:\s*(?:bearer|basic)\s+\S+", re.IGNORECASE),
    re.compile(
        r"\b(?:authorization|proxy-authorization|x-api-key|api-key|token|secret)"
        r"\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:password|passwd|pwd|credentials?|client[_-]?secret|private[_-]?key)"
        r"\s*[:=]\s*\S+",
        re.IGNORECASE,
    ),
    re.compile(r"https?://[^\s/@:]+:[^\s/@]+@", re.IGNORECASE),
    re.compile(
        r"https?://[^\s?#]+[?&](?:api[_-]?key|access[_-]?token|token|secret)="
        r"[^\s&#]+",
        re.IGNORECASE,
    ),
)
_INSTRUCTION_PATTERN = re.compile(
    r"\b(?:ignore|disregard|override)\s+(?:(?:all|any|every|the)\s+)?"
    r"(?:previous|prior|above|later|following)?\s*"
    r"(?:instructions?|directions?|prompts?|rules?)\b|"
    r"\breveal\s+(?:the\s+)?system\s+prompt\b|"
    r"\byou\s+are\s+now\s+(?:an?|the)\b",
    re.IGNORECASE,
)


class _ProfilingModel(BaseModel):
    """Base for strict immutable profiler output."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProfilingConfig(_ProfilingModel):
    """Hard bounds for one deterministic profiling run."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    max_sessions: int = Field(default=250, ge=1, le=10_000)
    max_nodes: int = Field(default=25_000, ge=1, le=1_000_000)
    max_text_bytes: int = Field(default=256_000, ge=1, le=10_000_000)
    max_payload_items: int = Field(default=100_000, ge=1, le=10_000_000)
    max_payload_bytes: int = Field(default=2_000_000, ge=1, le=100_000_000)
    max_payload_depth: int = Field(default=32, ge=1, le=100)
    max_evidence_per_candidate: int = Field(default=20, ge=1, le=20)
    max_candidates: int = Field(default=24, ge=1, le=100)
    max_contributing_sessions: int = Field(
        default=MAX_CONTRIBUTING_SESSIONS,
        ge=1,
        le=MAX_CONTRIBUTING_SESSIONS,
    )
    max_projection_bytes: int = Field(default=512_000, ge=1_000, le=10_000_000)


class DeterministicFact(_ProfilingModel):
    """One computed fact safe to include in a model projection."""

    name: str = Field(min_length=1, max_length=80)
    value: int | float | str


class CandidateCoverage(_ProfilingModel):
    """Signal-specific coverage and bounded-reference accounting."""

    sessions_analyzed: int = Field(ge=0)
    affected_sessions: int = Field(ge=0)
    occurrences: int = Field(ge=0)
    evidence_available: int = Field(ge=0)
    evidence_retained: int = Field(ge=0)
    contributing_sessions_available: int = Field(ge=0)
    contributing_sessions_retained: int = Field(ge=0)

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        """Keep retained and affected counts within their available totals."""
        if self.affected_sessions > self.sessions_analyzed:
            raise ValueError("affected sessions exceed analyzed sessions")
        if self.evidence_retained > self.evidence_available:
            raise ValueError("retained evidence exceeds available evidence")
        if self.contributing_sessions_retained > self.contributing_sessions_available:
            raise ValueError("retained contributions exceed available contributions")
        return self


class CandidateFinding(_ProfilingModel):
    """One chart-backed finding eligible for model selection."""

    id: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    family: str = Field(min_length=1, max_length=80)
    rank: int = Field(ge=0)
    eyebrow: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=255)
    fallback_description: str = Field(min_length=1, max_length=1000)
    caveat: str | None = Field(default=None, min_length=1, max_length=1000)
    data: CategoricalInsightData | BinnedInsightData
    facts: list[DeterministicFact] = Field(default_factory=list, max_length=20)
    coverage: CandidateCoverage
    contributing_session_ids: list[uuid.UUID] = Field(
        min_length=1, max_length=MAX_CONTRIBUTING_SESSIONS
    )
    evidence: list[EvidenceLocator] = Field(
        default_factory=list, max_length=MAX_EVIDENCE_LOCATORS
    )
    investigation_prompt: str = Field(
        min_length=1, max_length=MAX_INVESTIGATION_PROMPT_LENGTH
    )

    @model_validator(mode="after")
    def _validate_references(self) -> Self:
        """Keep contribution and evidence references consistent."""
        contributing = set(self.contributing_session_ids)
        if len(contributing) != len(self.contributing_session_ids):
            raise ValueError("contributing session IDs must be unique")
        if any(item.session_id not in contributing for item in self.evidence):
            raise ValueError("evidence session must be a contributing session")
        return self


class ProfilingResult(_ProfilingModel):
    """Stable candidate envelope produced without a model or evaluations."""

    analysis_version: Literal["2026-09-04.1"] = ANALYSIS_VERSION
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    coverage: Coverage
    candidates: list[CandidateFinding]


@dataclass
class _Aggregate:
    """Mutable aggregate retained only while profiling."""

    count: int = 0
    sessions: set[uuid.UUID] = field(default_factory=set)
    categories: Counter[str] = field(default_factory=Counter)
    evidence: list[EvidenceLocator] = field(default_factory=list)


@dataclass
class _PayloadBudget:
    """Global bound for inspecting caller-controlled payload structures."""

    max_items: int
    max_bytes: int
    max_depth: int
    items: int = 0
    bytes: int = 0
    truncated: bool = False

    def consume(self, value: Any, *, depth: int) -> bool:
        """Account for one value, rejecting it before an over-budget traversal."""
        if depth > self.max_depth or self.items >= self.max_items:
            self.truncated = True
            return False
        size = len(value.encode("utf-8")) if isinstance(value, str) else 0
        if self.bytes + size > self.max_bytes:
            self.truncated = True
            return False
        self.items += 1
        self.bytes += size
        return True

    def can_enter(self, size: int) -> bool:
        """Reject a container whose immediate children exceed the remaining budget."""
        if size > self.max_items - self.items:
            self.truncated = True
            return False
        return True


@dataclass(frozen=True)
class _DistributionSpec:
    """One chart definition with the exact sessions supplying observations."""

    candidate_id: str
    family: str
    rank: int
    eyebrow: str
    title: str
    description: str
    values: Sequence[float]
    session_ids: set[uuid.UUID]
    sessions_analyzed: int
    bounds: Sequence[float]
    unit: str


@dataclass
class _State:
    """Mutable state for a bounded deterministic scan."""

    config: ProfilingConfig
    payload_budget: _PayloadBudget = field(init=False)
    analyzed_session_ids: set[uuid.UUID] = field(default_factory=set)
    node_session_ids: set[uuid.UUID] = field(default_factory=set)
    signals: dict[str, _Aggregate] = field(
        default_factory=lambda: defaultdict(_Aggregate)
    )
    statuses: Counter[str] = field(default_factory=Counter)
    status_sessions: dict[str, set[uuid.UUID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    tool_counts: list[int] = field(default_factory=list)
    model_counts: list[int] = field(default_factory=list)
    activity_counts: list[int] = field(default_factory=list)
    durations: list[float] = field(default_factory=list)
    duration_session_ids: set[uuid.UUID] = field(default_factory=set)
    models: Counter[str] = field(default_factory=Counter)
    model_sessions: dict[str, set[uuid.UUID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    text_bytes_available: int = 0
    inspected_text_bytes: int = 0
    text_available: bool = False
    text_truncated: bool = False
    timing_available: int = 0
    identity_available: int = 0
    tool_calls: int = 0
    contribution_truncated: bool = False
    maximum_contributors_available: int = 0

    def __post_init__(self) -> None:
        """Initialize the traversal budget from the immutable config."""
        self.payload_budget = _PayloadBudget(
            max_items=self.config.max_payload_items,
            max_bytes=self.config.max_payload_bytes,
            max_depth=self.config.max_payload_depth,
        )


def sanitize_label(value: str | None) -> str | None:
    """Return a bounded exact label, or omit it when it may contain a secret."""
    if value is None:
        return None
    candidate = value.strip()
    if not candidate or len(candidate) > MAX_LABEL_LENGTH:
        return None
    redacted = redact_data(candidate)
    if not isinstance(redacted, str) or redacted != candidate or "***" in redacted:
        return None
    if _CONTROL_PATTERN.search(candidate):
        return None
    if any(pattern.search(candidate) for pattern in _CREDENTIAL_PATTERNS):
        return None
    if _INSTRUCTION_PATTERN.search(candidate):
        return None
    return candidate


def _normalize_observed(
    value: Any, budget: _PayloadBudget, *, depth: int = 0
) -> tuple[Any, bool]:
    """Normalize finite JSON-like values for exact tool-call identity."""
    if not budget.consume(value, depth=depth):
        return None, False
    if value is None or isinstance(value, (str, bool, int)):
        return value, True
    if isinstance(value, float):
        return (value, True) if math.isfinite(value) else (None, False)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None, False
        formatted = format(value, "f")
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0", True
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None, False
        return (
            value.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
            True,
        )
    if isinstance(value, uuid.UUID):
        return str(value), True
    if isinstance(value, Enum):
        return _normalize_observed(value.value, budget, depth=depth + 1)
    if isinstance(value, (list, tuple)):
        if not budget.can_enter(len(value)):
            return None, False
        normalized: list[Any] = []
        complete = True
        for item in value:
            converted, available = _normalize_observed(item, budget, depth=depth + 1)
            normalized.append(converted)
            complete = complete and available
        return normalized, complete
    if isinstance(value, dict):
        if not budget.can_enter(len(value)):
            return None, False
        if not all(isinstance(key, str) for key in value):
            return None, False
        normalized_dict: dict[str, Any] = {}
        complete = True
        for key in sorted(value):
            if not budget.consume(key, depth=depth + 1):
                return None, False
            converted, available = _normalize_observed(
                value[key], budget, depth=depth + 1
            )
            normalized_dict[key] = converted
            complete = complete and available
        return normalized_dict, complete
    return None, False


def _tool_identity(
    node: SessionNodeResponse, budget: _PayloadBudget
) -> tuple[str, str] | None:
    """Return evaluator-compatible exact tool name and canonical inputs."""
    if node.tool_name is None or node.inputs is None:
        return None
    normalized, complete = _normalize_observed(node.inputs, budget)
    if not complete:
        return None
    return node.tool_name, json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _is_empty_result(value: Any) -> bool:
    """Match the deterministic evaluator's non-null empty result definition."""
    return isinstance(value, (str, list, dict)) and len(value) == 0


def _mostly_uppercase(value: str) -> bool:
    """Return whether a substantial message is predominantly uppercase."""
    letters = [character for character in value if character.isalpha()]
    return (
        len(letters) >= 12
        and sum(character.isupper() for character in letters) / len(letters) >= 0.8
    )


def _decode_pointer_part(part: str) -> str | None:
    """Decode one conservative RFC 6901 pointer component."""
    if re.search(r"~(?:[^01]|$)", part):
        return None
    return part.replace("~1", "/").replace("~0", "~")


def _resolve_pointer(
    document: Any, pointer: str | None, budget: _PayloadBudget
) -> tuple[bool, Any]:
    """Resolve a valid RFC 6901 pointer without raising on source data."""
    if pointer is None:
        return False, None
    if pointer == "":
        return True, document
    if not pointer.startswith("/"):
        return False, None
    current = document
    for depth, raw_part in enumerate(pointer[1:].split("/")):
        if not budget.consume(current, depth=depth):
            return False, None
        part = _decode_pointer_part(raw_part)
        if part is None:
            return False, None
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif (
            isinstance(current, list)
            and part.isascii()
            and part.isdigit()
            and (part == "0" or not part.startswith("0"))
            and int(part) < len(current)
        ):
            current = current[int(part)]
        else:
            return False, None
    return True, current


def _text_parts(value: Any, budget: _PayloadBudget) -> list[str]:
    """Extract strings from common structured message text containers."""
    texts: list[str] = []
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if not budget.consume(current, depth=depth):
            continue
        if isinstance(current, str):
            texts.append(current)
        elif isinstance(current, list):
            if budget.can_enter(len(current)):
                stack.extend((item, depth + 1) for item in reversed(current))
        elif isinstance(current, dict):
            if current.get("type") in {"text", "input_text"}:
                content = current.get("content", current.get("text"))
                if isinstance(content, str):
                    stack.append((content, depth + 1))
                continue
            children = [
                current[key] for key in ("content", "parts", "text") if key in current
            ]
            if budget.can_enter(len(children)):
                stack.extend((item, depth + 1) for item in reversed(children))
    return texts


def _user_messages(value: Any, budget: _PayloadBudget) -> list[str]:
    """Find explicitly user-authored messages in a nested recorded input."""
    messages: list[str] = []
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if not budget.consume(current, depth=depth):
            continue
        if isinstance(current, list):
            if budget.can_enter(len(current)):
                stack.extend((item, depth + 1) for item in reversed(current))
            continue
        if not isinstance(current, dict):
            continue
        if str(current.get("role", "")).lower() == "user":
            for key in ("content", "parts", "text"):
                if key in current:
                    messages.extend(_text_parts(current[key], budget))
            continue
        keys = sorted(key for key in current if isinstance(key, str))
        if budget.can_enter(len(keys)):
            stack.extend((current[key], depth + 1) for key in reversed(keys))
    return messages


def _selected_user_texts(
    state: _State,
    session: SessionWithNodesResponse,
    nodes: list[SessionNodeResponse],
) -> list[tuple[str, uuid.UUID | None]]:
    """Select user text using explicit selectors before conservative recursion."""
    deduplicated: dict[str, uuid.UUID | None] = {}

    def add_texts(texts: Sequence[str], node_id: uuid.UUID | None) -> None:
        for text in texts:
            normalized = text.strip()
            if normalized:
                deduplicated.setdefault(normalized, node_id)

    found, value = _resolve_pointer(
        session.session.inputs,
        session.session.input_text_selector,
        state.payload_budget,
    )
    session_texts = (
        _text_parts(value, state.payload_budget)
        if found
        else _user_messages(session.session.inputs, state.payload_budget)
    )
    add_texts(session_texts, None)
    for node in nodes:
        if node.node_type is not NodeType.LLM_CALL:
            continue
        found, value = _resolve_pointer(
            node.inputs, node.input_text_selector, state.payload_budget
        )
        node_texts = (
            _text_parts(value, state.payload_budget)
            if found
            else _user_messages(node.inputs, state.payload_budget)
        )
        add_texts(node_texts, node.id)
    return sorted(deduplicated.items(), key=lambda item: (item[0], str(item[1] or "")))


def _record(
    state: _State,
    signal: str,
    session_id: uuid.UUID,
    *,
    category: str,
    node_id: uuid.UUID | None,
) -> None:
    """Record one content-free signal occurrence."""
    aggregate = state.signals[signal]
    aggregate.count += 1
    aggregate.sessions.add(session_id)
    aggregate.categories[category] += 1
    if len(aggregate.evidence) < state.config.max_evidence_per_candidate:
        aggregate.evidence.append(
            EvidenceLocator(session_id=session_id, node_id=node_id, signal=signal)
        )


def _cycle_findings(
    calls: list[SessionNodeResponse],
    identities: list[tuple[str, str] | None],
) -> list[tuple[int, int, list[str]]]:
    """Detect evaluator-compatible period-2-to-5 cycles repeated three times."""
    candidates: list[tuple[int, int, int, list[str]]] = []
    for period in range(2, 6):
        match_start: int | None = None
        for position in range(len(identities) - period + 1):
            matches = (
                position < len(identities) - period
                and identities[position] is not None
                and identities[position] == identities[position + period]
            )
            if matches and match_start is None:
                match_start = position
            if matches:
                continue
            if match_start is not None:
                end = position + period
                if position - match_start >= 2 * period:
                    candidates.append(
                        (
                            match_start,
                            end - 1,
                            period,
                            [
                                call.tool_name or "unavailable"
                                for call in calls[match_start : match_start + period]
                            ],
                        )
                    )
                match_start = None
    candidates.sort(key=lambda item: (item[0], -(item[1] - item[0]), item[2]))
    retained: list[tuple[int, int, list[str]]] = []
    maximum_end: int | None = None
    for start, end, _, tools in candidates:
        if maximum_end is not None and end <= maximum_end:
            continue
        retained.append((start, end, tools))
        maximum_end = end
    return retained


def _scan_session(
    state: _State,
    session: SessionWithNodesResponse,
    nodes: list[SessionNodeResponse],
    *,
    nodes_complete: bool,
) -> None:
    """Add one bounded normalized session to aggregate state."""
    session_id = session.session.id
    state.analyzed_session_ids.add(session_id)
    state.statuses[session.session.status.value] += 1
    state.status_sessions[session.session.status.value].add(session_id)
    calls = (
        [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
        if nodes_complete
        else []
    )
    llm_calls = (
        [node for node in nodes if node.node_type is NodeType.LLM_CALL]
        if nodes_complete
        else []
    )
    if nodes_complete:
        state.node_session_ids.add(session_id)
        state.tool_calls += len(calls)
        state.tool_counts.append(len(calls))
        state.model_counts.append(len(llm_calls))
        state.activity_counts.append(len(nodes))

    if session.session.started_at is not None and session.session.ended_at is not None:
        duration = (
            session.session.ended_at - session.session.started_at
        ).total_seconds()
        if duration >= 0:
            state.durations.append(duration)
            state.duration_session_ids.add(session_id)
            state.timing_available += 1

    for node in llm_calls:
        label = sanitize_label(node.model or node.requested_model)
        if label is not None:
            state.models[label] += 1
            state.model_sessions[label].add(session_id)

    identities = [_tool_identity(call, state.payload_budget) for call in calls]
    state.identity_available += sum(identity is not None for identity in identities)
    for call in calls:
        label = sanitize_label(call.tool_name) or "Unavailable tool"
        if call.status is NodeStatus.FAILED:
            _record(
                state,
                "tool-errors",
                session_id,
                category=label,
                node_id=call.id,
            )
        if call.outputs is None:
            _record(
                state,
                "null-tool-results",
                session_id,
                category=label,
                node_id=call.id,
            )
        elif _is_empty_result(call.outputs):
            _record(
                state,
                "empty-tool-results",
                session_id,
                category=label,
                node_id=call.id,
            )

    for position, (first, second) in enumerate(pairwise(calls)):
        first_identity = identities[position]
        second_identity = identities[position + 1]
        label = sanitize_label(first.tool_name) or "Unavailable tool"
        if first_identity is not None and first_identity == second_identity:
            _record(
                state,
                "adjacent-identical-calls",
                session_id,
                category=label,
                node_id=first.id,
            )
            if first.status is NodeStatus.FAILED:
                _record(
                    state,
                    "failed-identical-retries",
                    session_id,
                    category=label,
                    node_id=first.id,
                )
        if (
            first.status is NodeStatus.FAILED
            and second.status is NodeStatus.FAILED
            and first.tool_name is not None
            and first.tool_name == second.tool_name
        ):
            _record(
                state,
                "adjacent-same-tool-failures",
                session_id,
                category=label,
                node_id=first.id,
            )

    for start, _, tools in _cycle_findings(calls, identities):
        safe_tools = [sanitize_label(tool) or "Unavailable tool" for tool in tools]
        _record(
            state,
            "short-tool-cycles",
            session_id,
            category=" -> ".join(safe_tools),
            node_id=calls[start].id,
        )

    messages = _selected_user_texts(state, session, nodes if nodes_complete else [])
    state.text_available = state.text_available or bool(messages)
    state.text_bytes_available += sum(
        len(message.encode("utf-8")) for message, _ in messages
    )
    for message, node_id in messages:
        encoded = message.encode("utf-8")
        remaining = state.config.max_text_bytes - state.inspected_text_bytes
        if remaining <= 0:
            state.text_truncated = True
            break
        inspected = encoded[:remaining].decode("utf-8", errors="ignore")
        state.inspected_text_bytes += len(inspected.encode("utf-8"))
        if len(encoded) > remaining:
            state.text_truncated = True
        if _CORRECTION_PATTERN.search(inspected):
            _record(
                state,
                "correction-language",
                session_id,
                category="Literal correction marker",
                node_id=node_id,
            )
        if _PUNCTUATION_PATTERN.search(inspected):
            _record(
                state,
                "repeated-punctuation",
                session_id,
                category="Repeated punctuation",
                node_id=node_id,
            )
        if _mostly_uppercase(inspected):
            _record(
                state,
                "mostly-uppercase-messages",
                session_id,
                category="Mostly uppercase",
                node_id=node_id,
            )
        if _PROFANITY_PATTERN.search(inspected):
            _record(
                state,
                "possible-profanity",
                session_id,
                category="Literal profanity marker",
                node_id=node_id,
            )
        if len(encoded) > remaining:
            break


def _percent(numerator: int, denominator: int) -> int:
    """Return a whole percentage without dividing by zero."""
    return round(100 * numerator / denominator) if denominator else 0


def _contributions(state: _State, session_ids: set[uuid.UUID]) -> list[uuid.UUID]:
    """Return a bounded stable contribution set."""
    ordered = sorted(session_ids, key=str)
    state.maximum_contributors_available = max(
        state.maximum_contributors_available, len(ordered)
    )
    if len(ordered) > state.config.max_contributing_sessions:
        state.contribution_truncated = True
    return ordered[: state.config.max_contributing_sessions]


def _prompt(subject: str) -> str:
    """Build a deterministic inspect-to-experiment prompt."""
    return (
        f"Use Kitaru to investigate {subject}. Inspect representative affected "
        "sessions and a comparable unaffected set. Define or refine a cohort that "
        "captures the pattern. Form a concrete hypothesis, then propose one or more "
        "controlled experiments that change one factor at a time. Compare outcome "
        "quality, recurrence, latency, cost, and tool use. Treat this pattern as a "
        "lead, not proof of a bad result."
    )


def _categorical(values: Counter[str], *, unit: str) -> CategoricalInsightData:
    """Build categorical data with deterministic count and label ordering."""
    ordered = sorted(values.items(), key=lambda item: (-item[1], item[0]))
    return CategoricalInsightData(
        unit=unit,
        values=[CategoryValue(label=label, value=value) for label, value in ordered],
    )


def _binned(
    values: Sequence[float], bounds: Sequence[float], *, unit: str
) -> BinnedInsightData:
    """Count values into contiguous half-open bins."""
    counts = [0] * (len(bounds) + 1)
    for value in values:
        index = bisect_right(bounds, value)
        counts[index] += 1
    bins = [
        Bin(
            lower_bound=None if index == 0 else bounds[index - 1],
            upper_bound=bounds[index] if index < len(bounds) else None,
            count=count,
        )
        for index, count in enumerate(counts)
    ]
    return BinnedInsightData(unit=unit, bins=bins)


def _signal_candidate(
    state: _State,
    *,
    signal: str,
    candidate_id: str,
    family: str,
    rank: int,
    eyebrow: str,
    title_pattern: str,
    description_pattern: str,
    subject: str,
    caveat: str,
) -> CandidateFinding | None:
    """Turn one non-empty signal aggregate into a candidate finding."""
    aggregate = state.signals.get(signal)
    if aggregate is None or not aggregate.sessions:
        return None
    contributions = _contributions(state, aggregate.sessions)
    contribution_ids = set(contributions)
    eligible_session_ids = (
        state.analyzed_session_ids if family == "language" else state.node_session_ids
    )
    share = _percent(len(aggregate.sessions), len(eligible_session_ids))
    values = aggregate.categories
    unit = "occurrences"
    if len(values) == 1:
        values = Counter(
            {
                "Matching sessions": len(aggregate.sessions),
                "Other sessions": max(
                    len(eligible_session_ids) - len(aggregate.sessions), 0
                ),
            }
        )
        unit = "sessions"
    top = sorted(aggregate.categories.items(), key=lambda item: (-item[1], item[0]))[0][
        0
    ]
    return CandidateFinding(
        id=candidate_id,
        family=family,
        rank=rank,
        eyebrow=eyebrow,
        title=title_pattern.format(share=share, affected=len(aggregate.sessions)),
        fallback_description=description_pattern.format(
            count=aggregate.count,
            affected=len(aggregate.sessions),
            top=top,
        ),
        caveat=caveat,
        data=_categorical(values, unit=unit),
        facts=[
            DeterministicFact(name="occurrences", value=aggregate.count),
            DeterministicFact(name="affected_sessions", value=len(aggregate.sessions)),
            DeterministicFact(name="affected_share_percent", value=share),
        ],
        coverage=CandidateCoverage(
            sessions_analyzed=len(eligible_session_ids),
            affected_sessions=len(aggregate.sessions),
            occurrences=aggregate.count,
            evidence_available=aggregate.count,
            evidence_retained=sum(
                locator.session_id in contribution_ids for locator in aggregate.evidence
            ),
            contributing_sessions_available=len(aggregate.sessions),
            contributing_sessions_retained=len(contributions),
        ),
        contributing_session_ids=contributions,
        evidence=[
            locator
            for locator in aggregate.evidence
            if locator.session_id in contribution_ids
        ],
        investigation_prompt=_prompt(subject),
    )


def _build_candidates(state: _State) -> list[CandidateFinding]:
    """Build the complete ranked candidate collection."""
    candidates: list[CandidateFinding] = []
    signal_specs = (
        (
            "failed-identical-retries",
            "failed-identical-retries",
            "trajectory",
            10,
            "RETRIES AFTER ERRORS",
            "{share}% of sessions immediately retry the same failed call",
            "The profiler found {count} exact retries across {affected} sessions; "
            "{top} appears most often.",
            "exact tool calls repeated immediately after a recorded failure",
            "A recorded failure may be recovered later and is not the same as a "
            "failed session.",
        ),
        (
            "short-tool-cycles",
            "short-tool-cycles",
            "trajectory",
            20,
            "REPEATING TOOL CYCLES",
            "{share}% of sessions contain a repeating tool-call cycle",
            "Two-to-five-call sequences repeated at least three times in {affected} "
            "sessions; {top} appears most often.",
            "repeated exact tool-call cycles",
            "Exact cycles require recorded tool names and canonically encodable "
            "inputs.",
        ),
        (
            "adjacent-same-tool-failures",
            "adjacent-same-tool-failures",
            "tool_health",
            30,
            "REPEATED TOOL FAILURES",
            "{share}% of sessions hit the same failing tool twice in a row",
            "The pattern appears {count} times across {affected} sessions; {top} is "
            "the largest group.",
            "back-to-back recorded failures from the same tool",
            "A recorded failure may be recovered later and is not the same as a "
            "failed session.",
        ),
        (
            "adjacent-identical-calls",
            "adjacent-identical-calls",
            "trajectory",
            40,
            "REPEATED TOOL CALLS",
            "{share}% of sessions repeat the same tool call back to back",
            "The profiler found {count} exact repeated pairs across {affected} "
            "sessions; {top} appears most often.",
            "back-to-back tool calls with the same name and exact inputs",
            "Exact repetition requires a recorded tool name and canonically encodable "
            "input.",
        ),
        (
            "tool-errors",
            "tool-error-mix",
            "tool_health",
            50,
            "ERRORS BY TOOL",
            "Recorded tool errors affect {affected} sessions",
            "The profiler found {count} recorded errors across {affected} sessions; "
            "{top} is the largest group.",
            "recorded tool errors grouped by exact tool name",
            "A recorded tool error may be recovered later and is not the same as a "
            "failed session.",
        ),
        (
            "correction-language",
            "correction-language",
            "language",
            60,
            "USERS CORRECTING THE AGENT",
            "{share}% of sessions include explicit correction language",
            "A literal correction marker appears {count} times across {affected} "
            "sessions.",
            "user messages containing literal correction phrases",
            "Language markers are literal leads, not judgments of sentiment or intent.",
        ),
        (
            "null-tool-results",
            "null-tool-results",
            "tool_health",
            70,
            "MISSING TOOL RESULTS",
            "{share}% of sessions contain a null tool result",
            "The profiler found {count} null results across {affected} sessions; "
            "{top} appears most often.",
            "tool calls with a null recorded result",
            "Null results may reflect instrumentation rather than agent behavior.",
        ),
        (
            "empty-tool-results",
            "empty-tool-results",
            "tool_health",
            80,
            "EMPTY TOOL RESULTS",
            "{share}% of sessions contain an empty tool result",
            "The profiler found {count} empty results across {affected} sessions; "
            "{top} appears most often.",
            "tool calls with an empty string or container result",
            "Empty results may reflect instrumentation rather than agent behavior.",
        ),
        (
            "repeated-punctuation",
            "repeated-punctuation",
            "language",
            90,
            "REPEATED PUNCTUATION",
            "{share}% of sessions include repeated exclamation or question marks",
            "The literal punctuation marker appears {count} times across {affected} "
            "sessions.",
            "user messages containing three or more consecutive exclamation or "
            "question marks",
            "Language markers are literal leads, not judgments of sentiment or intent.",
        ),
        (
            "mostly-uppercase-messages",
            "mostly-uppercase-messages",
            "language",
            100,
            "MOSTLY-UPPERCASE MESSAGES",
            "{share}% of sessions include a mostly-uppercase user message",
            "The literal capitalization marker appears {count} times across "
            "{affected} sessions.",
            "substantial user messages written mostly in uppercase",
            "Language markers are literal leads, not judgments of sentiment or intent.",
        ),
        (
            "possible-profanity",
            "possible-profanity",
            "language",
            110,
            "POSSIBLE PROFANITY",
            "{share}% of sessions match the literal profanity monitor",
            "A small literal word list matched {count} messages across {affected} "
            "sessions.",
            "user messages matched by the literal profanity monitor",
            "Language markers are literal leads, not judgments of sentiment or intent.",
        ),
    )
    for spec in signal_specs:
        candidate = _signal_candidate(
            state,
            signal=spec[0],
            candidate_id=spec[1],
            family=spec[2],
            rank=spec[3],
            eyebrow=spec[4],
            title_pattern=spec[5],
            description_pattern=spec[6],
            subject=spec[7],
            caveat=spec[8],
        )
        if candidate is not None:
            candidates.append(candidate)

    analyzed_sessions = len(state.analyzed_session_ids)
    failed_ids = state.status_sessions.get(SessionStatus.FAILED.value, set())
    if analyzed_sessions and failed_ids:
        affected_ids = failed_ids
        contributions = _contributions(state, affected_ids)
        contribution_ids = set(contributions)
        status_evidence = [
            EvidenceLocator(
                session_id=session_id,
                signal="session-status",
            )
            for session_id in sorted(affected_ids, key=str)[
                : state.config.max_evidence_per_candidate
            ]
            if session_id in contribution_ids
        ]
        candidates.append(
            CandidateFinding(
                id="session-outcomes",
                family="outcome",
                rank=55,
                eyebrow="SESSION OUTCOMES",
                title=(
                    f"{_percent(state.statuses.get('failed', 0), analyzed_sessions)}% "
                    "of sessions are recorded failed"
                ),
                fallback_description=(
                    "Recorded session statuses show where to begin comparing failed "
                    "and completed runs."
                ),
                caveat=(
                    "Recorded status describes the session boundary, not the cause of "
                    "the outcome."
                ),
                data=_categorical(state.statuses, unit="sessions"),
                facts=[
                    DeterministicFact(
                        name="failed_sessions",
                        value=state.statuses.get("failed", 0),
                    ),
                    DeterministicFact(name="sessions", value=analyzed_sessions),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=analyzed_sessions,
                    affected_sessions=len(affected_ids),
                    occurrences=len(affected_ids),
                    evidence_available=len(affected_ids),
                    evidence_retained=len(status_evidence),
                    contributing_sessions_available=len(affected_ids),
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=status_evidence,
                investigation_prompt=_prompt("recorded failed session outcomes"),
            )
        )

    distribution_specs = (
        _DistributionSpec(
            candidate_id="tool-call-distribution",
            family="activity",
            rank=120,
            eyebrow="TOOL-CALL DISTRIBUTION",
            title="Tool calls per session have a measurable spread",
            description=(
                "The distribution shows sessions where repeated tool use may warrant "
                "closer inspection."
            ),
            values=state.tool_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
            bounds=[3, 6, 10, 15],
            unit="calls",
        ),
        _DistributionSpec(
            candidate_id="model-call-distribution",
            family="activity",
            rank=130,
            eyebrow="MODEL-CALL DISTRIBUTION",
            title="Model calls per session have a measurable spread",
            description=(
                "The distribution shows how model-call activity varies across the "
                "imported sessions."
            ),
            values=state.model_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
            bounds=[2, 3, 5],
            unit="calls",
        ),
        _DistributionSpec(
            candidate_id="total-activity-distribution",
            family="activity",
            rank=140,
            eyebrow="RECORDED ACTIVITY",
            title="Recorded node activity varies across sessions",
            description=(
                "The distribution counts normalized nodes per session without treating "
                "activity as outcome quality."
            ),
            values=state.activity_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
            bounds=[10, 20, 30, 50],
            unit="nodes",
        ),
        _DistributionSpec(
            candidate_id="recorded-duration-distribution",
            family="timing",
            rank=150,
            eyebrow="RECORDED DURATION",
            title="Recorded session duration has a measurable spread",
            description=(
                "The distribution uses valid session start and end timestamps and may "
                "omit uninstrumented work."
            ),
            values=state.durations,
            session_ids=state.duration_session_ids,
            sessions_analyzed=analyzed_sessions,
            bounds=[5, 15, 30, 60],
            unit="seconds",
        ),
    )
    # Generic distributions are eligible with two observations, even if all values
    # land in one bin, because their chart is still a reproducible baseline.
    for spec in distribution_specs:
        if len(spec.values) < 2:
            continue
        contributing = _contributions(state, spec.session_ids)
        if not contributing:
            continue
        candidates.append(
            CandidateFinding(
                id=spec.candidate_id,
                family=spec.family,
                rank=spec.rank,
                eyebrow=spec.eyebrow,
                title=spec.title,
                fallback_description=spec.description,
                caveat=(
                    "Recorded timestamps may omit uninstrumented work."
                    if spec.family == "timing"
                    else "Activity volume is not evidence of outcome quality."
                ),
                data=_binned(
                    [float(value) for value in spec.values],
                    spec.bounds,
                    unit=spec.unit,
                ),
                facts=[
                    DeterministicFact(name="observations", value=len(spec.values)),
                    DeterministicFact(name="maximum", value=max(spec.values)),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=spec.sessions_analyzed,
                    affected_sessions=len(spec.session_ids),
                    occurrences=len(spec.values),
                    evidence_available=len(spec.values),
                    evidence_retained=0,
                    contributing_sessions_available=len(spec.session_ids),
                    contributing_sessions_retained=len(contributing),
                ),
                contributing_session_ids=contributing,
                evidence=[],
                investigation_prompt=_prompt(spec.title.lower()),
            )
        )

    if state.models:
        model_sessions = set().union(*state.model_sessions.values())
        contributions = _contributions(state, model_sessions)
        candidates.append(
            CandidateFinding(
                id="model-mix",
                family="model",
                rank=160,
                eyebrow="MODEL MIX",
                title=(
                    f"{len(state.models)} recorded model "
                    f"{'appears' if len(state.models) == 1 else 'appear'} in these "
                    "sessions"
                ),
                fallback_description=(
                    "This exact model mix can seed a cohort for cost, latency, or "
                    "quality comparisons."
                ),
                caveat=(
                    "Requested and served model fields may be absent from some "
                    "recorded calls."
                ),
                data=_categorical(state.models, unit="calls"),
                facts=[
                    DeterministicFact(name="models", value=len(state.models)),
                    DeterministicFact(
                        name="model_calls", value=sum(state.models.values())
                    ),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=len(state.node_session_ids),
                    affected_sessions=len(model_sessions),
                    occurrences=sum(state.models.values()),
                    evidence_available=sum(state.models.values()),
                    evidence_retained=0,
                    contributing_sessions_available=len(model_sessions),
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=[],
                investigation_prompt=_prompt("the recorded model mix"),
            )
        )
    candidates.sort(key=lambda candidate: (candidate.rank, candidate.id))
    return candidates


def _content_hash(
    config: ProfilingConfig,
    coverage: Coverage,
    candidates: Sequence[CandidateFinding],
) -> str:
    """Hash the bounded content-free envelope using canonical JSON."""
    envelope = {
        "config": config.model_dump(mode="json"),
        "coverage": coverage.model_dump(mode="json"),
        "candidates": [candidate.model_dump(mode="json") for candidate in candidates],
    }
    serialized = json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def profile_sessions(
    sessions: list[SessionWithNodesResponse],
    *,
    config: ProfilingConfig | None = None,
) -> ProfilingResult:
    """Profile caller-scoped normalized sessions into stable candidate findings."""
    selected_config = config or ProfilingConfig()
    selected_sessions = sorted(sessions, key=lambda item: str(item.session.id))[
        : selected_config.max_sessions
    ]
    nodes_available = sum(len(session.nodes) for session in sessions)
    remaining_nodes = selected_config.max_nodes
    selected: list[
        tuple[SessionWithNodesResponse, list[SessionNodeResponse], bool]
    ] = []
    for session in selected_sessions:
        if len(session.nodes) > remaining_nodes:
            selected.append((session, [], False))
            remaining_nodes = 0
            continue
        ordered_nodes = sorted(
            session.nodes, key=lambda node: (node.index, str(node.id))
        )
        selected.append((session, ordered_nodes, True))
        remaining_nodes -= len(ordered_nodes)

    state = _State(config=selected_config)
    for session, nodes, nodes_complete in selected:
        _scan_session(state, session, nodes, nodes_complete=nodes_complete)

    candidates = _build_candidates(state)

    truncations: list[CoverageTruncation] = []
    if len(selected_sessions) < len(sessions):
        truncations.append(
            CoverageTruncation(
                dimension="sessions",
                available=len(sessions),
                analyzed=len(selected_sessions),
            )
        )
    nodes_analyzed = sum(
        len(nodes) for _, nodes, nodes_complete in selected if nodes_complete
    )
    if nodes_analyzed < nodes_available:
        truncations.append(
            CoverageTruncation(
                dimension="nodes", available=nodes_available, analyzed=nodes_analyzed
            )
        )
    if state.text_truncated:
        truncations.append(
            CoverageTruncation(
                dimension="text_bytes",
                available=state.text_bytes_available,
                analyzed=state.inspected_text_bytes,
            )
        )
    if state.contribution_truncated:
        truncations.append(
            CoverageTruncation(
                dimension="contributing_sessions",
                available=state.maximum_contributors_available,
                analyzed=selected_config.max_contributing_sessions,
            )
        )

    if len(candidates) > selected_config.max_candidates:
        truncations.append(
            CoverageTruncation(
                dimension="candidates",
                available=len(candidates),
                analyzed=selected_config.max_candidates,
            )
        )
        candidates = candidates[: selected_config.max_candidates]

    caveats = [
        "The profiler uses normalized sessions and does not read persisted "
        "evaluation results."
    ]
    if any(not nodes_complete for _, _, nodes_complete in selected):
        caveats.append(
            "Node-derived profiling excludes sessions whose node lists were "
            "truncated by the configured node limit."
        )
    if state.timing_available < len(selected_sessions):
        caveats.append(
            "Timing coverage is incomplete because valid session bounds are missing."
        )
    if state.identity_available < state.tool_calls:
        caveats.append(
            "Tool identity coverage is incomplete because names or finite inputs are "
            "missing."
        )
    if not state.text_available:
        caveats.append(
            "User text coverage is unavailable because no explicit user-authored "
            "input was found."
        )
    if state.text_truncated:
        caveats.append("User text inspection stopped at the configured byte limit.")
    if state.payload_budget.truncated:
        caveats.append(
            "Payload inspection reached a configured item, byte, or depth limit; "
            "tool identity and user text coverage may be incomplete."
        )

    coverage = Coverage(
        sessions_available=len(sessions),
        sessions_analyzed=len(selected_sessions),
        nodes_available=nodes_available,
        nodes_analyzed=nodes_analyzed,
        inspected_text_bytes=state.inspected_text_bytes,
        truncations=truncations,
        caveats=caveats,
    )
    result = ProfilingResult(
        content_hash=_content_hash(selected_config, coverage, candidates),
        coverage=coverage,
        candidates=candidates,
    )

    # Drop lower-ranked candidates until the entire model-facing envelope fits.
    original_count = len(result.candidates)
    projection_bytes = len(result.model_dump_json().encode("utf-8"))
    oversized_projection_bytes = projection_bytes
    while result.candidates and projection_bytes > selected_config.max_projection_bytes:
        candidates = list(result.candidates[:-1])
        projection_truncation = CoverageTruncation(
            dimension="projection_bytes",
            available=oversized_projection_bytes,
            analyzed=selected_config.max_projection_bytes,
        )
        coverage = result.coverage.model_copy(
            update={
                "truncations": [
                    item
                    for item in result.coverage.truncations
                    if item.dimension != "projection_bytes"
                ]
                + [projection_truncation]
            }
        )
        result = result.model_copy(
            update={"coverage": coverage, "candidates": candidates}
        )
        projection_bytes = len(result.model_dump_json().encode("utf-8"))
    if original_count and not result.candidates:
        # The configured byte ceiling can be lower than the irreducible coverage
        # envelope. Keep the empty bounded result and state that explicitly.
        caveats = [
            *result.coverage.caveats,
            "No candidate fit within the configured projection byte limit.",
        ]
        result = result.model_copy(
            update={"coverage": result.coverage.model_copy(update={"caveats": caveats})}
        )
    return result.model_copy(
        update={
            "content_hash": _content_hash(
                selected_config, result.coverage, result.candidates
            )
        }
    )
