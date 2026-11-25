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
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from functools import partial
from itertools import pairwise
from types import TracebackType
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.insight import (
    CategoricalArtifact,
    CycleStep,
    InsightArtifact,
    RateArtifact,
    Severity,
    StatGridArtifact,
    SuggestedQuestion,
    Tone,
    ToolCycleArtifact,
)
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights.briefings import (
    build_distribution_briefing as _build_distribution_briefing,
)
from kitaru_post_import_insights.briefings import (
    build_model_mix_briefing as _build_model_mix_briefing,
)
from kitaru_post_import_insights.briefings import (
    build_outcome_briefing as _build_outcome_briefing,
)
from kitaru_post_import_insights.briefings import (
    build_signal_briefing as _build_signal_briefing,
)
from kitaru_post_import_insights.findings import (
    CandidateCoverage,
    CandidateFinding,
    DeterministicFact,
    build_binned,
    build_categorical,
    build_split_bar,
    calculate_percent,
    format_briefing,
    pluralize,
)
from kitaru_post_import_insights.labels import (
    MAX_SELECTOR_LENGTH,
    MAX_SELECTOR_SEGMENTS,
    UNAVAILABLE_TOOL,
    sanitize_label,
)
from kitaru_post_import_insights.models import (
    DISTRIBUTION_TOP_BIN_SIGNAL,
    MAX_CONTRIBUTING_SESSIONS,
    Coverage,
    CoverageTruncation,
    EvidenceLocator,
)
from kitaru_post_import_insights.profiling_state import (
    CountsStore,
    HighestValueSessions,
    Histogram,
    LabelCounts,
    SessionReferences,
)
from kitaru_post_import_insights.resource_inputs import FindingLimits
from kitaru_post_import_insights.resource_profile import ResourceProfile

ANALYSIS_VERSION = "2026-11-09.1"
MAX_DISTRIBUTION_EXAMPLES = 5
MAX_ROLE_LENGTH = 32
_DECIMAL_BASE_SIZE = Decimal(0).__sizeof__()
_JSON_STRING_ENCODER = json.JSONEncoder(ensure_ascii=False)

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


class _ProfilingModel(BaseModel):
    """Base for strict immutable profiler output."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProfilingConfig(_ProfilingModel):
    """Per-session payload safety limits and bounded result projection."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

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


class ProfilingResult(_ProfilingModel):
    """Stable candidate envelope produced without a model or evaluations."""

    analysis_version: Literal["2026-11-09.1"] = ANALYSIS_VERSION
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    coverage: Coverage
    candidates: list[CandidateFinding]


@dataclass
class _Aggregate:
    """Mutable aggregate retained only while profiling."""

    sessions: SessionReferences
    categories: LabelCounts
    count: int = 0
    evidence: list[EvidenceLocator] = field(default_factory=list)


def _calculate_json_string_size(value: str, limit: int) -> int | None:
    """Return exact UTF-8 JSON string size, or None past a limit or on a surrogate."""
    # Every character encodes to at least one byte, so the length alone rejects
    # an oversized string before the escaped copy is built.
    if len(value) + 2 > limit:
        return None
    try:
        size = len(_JSON_STRING_ENCODER.encode(value).encode("utf-8"))
    except UnicodeEncodeError:
        return None
    return size if size <= limit else None


def _get_bounded_decimal_text(value: Decimal, limit: int) -> str | None:
    """Format a finite Decimal only when its canonical fixed form fits."""
    if not value.is_finite():
        return None
    # CPython's Decimal stores coefficient digits inline or in the allocation
    # reported here. Bounding that allocation before as_tuple() keeps coefficient
    # copying and trailing-zero inspection proportional to the payload budget.
    if Decimal.__sizeof__(value) > _DECIMAL_BASE_SIZE + limit:
        return None
    if not value:
        return "0" if limit >= 3 else None
    sign, digits, exponent = value.as_tuple()
    if len(digits) > limit:
        return None
    decimal_exponent = int(exponent)
    digit_count = len(digits)
    while decimal_exponent < 0 and digit_count > 1 and digits[digit_count - 1] == 0:
        digit_count -= 1
        decimal_exponent += 1
    if decimal_exponent >= 0:
        text_length = digit_count + decimal_exponent
    elif digit_count + decimal_exponent > 0:
        text_length = digit_count + 1
    else:
        text_length = 2 - decimal_exponent
    text_length += sign
    if text_length + 2 > limit:
        return None
    formatted = format(value, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted or "0"


def _calculate_scalar_json_size(value: Any, limit: int) -> int | None:
    """Return the bounded immediate JSON size of a supported value."""
    if value is None:
        return 4 if limit >= 4 else None
    if isinstance(value, bool):
        size = 4 if value else 5
        return size if size <= limit else None
    if isinstance(value, str):
        return _calculate_json_string_size(value, limit)
    if isinstance(value, int):
        if value:
            estimated_digits = int((abs(value).bit_length() - 1) * math.log10(2)) + 1
        else:
            estimated_digits = 1
        estimated_size = estimated_digits + (value < 0)
        if estimated_size > limit:
            return None
        try:
            encoded = str(value)
        except ValueError:
            return None
        return len(encoded) if len(encoded) <= limit else None
    if isinstance(value, float):
        if not math.isfinite(value):
            return 0
        size = len(repr(value))
        return size if size <= limit else None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return 0
        text = _get_bounded_decimal_text(value, limit)
        return None if text is None else _calculate_json_string_size(text, limit)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return 0
        text = (
            value.astimezone(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        return _calculate_json_string_size(text, limit)
    if isinstance(value, uuid.UUID):
        return 38 if limit >= 38 else None
    if isinstance(value, (list, tuple)):
        size = 2 + max(len(value) - 1, 0)
        return size if size <= limit else None
    if isinstance(value, dict):
        size = 2 + len(value) + max(len(value) - 1, 0)
        if size > limit:
            return None
        if not all(isinstance(key, str) for key in value):
            return 0
        for key in value:
            key_size = _calculate_json_string_size(key, limit - size)
            if key_size is None:
                return None
            size += key_size
        return size
    return 0


@dataclass
class _PayloadBudget:
    """Per-session bound for inspecting caller-controlled payload structures."""

    max_items: int
    max_bytes: int
    max_depth: int
    items: int = 0
    bytes: int = 0
    truncated: bool = False
    truncation_count: int = 0

    def record_truncation(self) -> None:
        """Record one rejected traversal step."""
        self.truncated = True
        self.truncation_count += 1

    def consume(self, value: Any, *, depth: int) -> bool:
        """Account for one value, rejecting it before an over-budget traversal."""
        if depth > self.max_depth or self.items >= self.max_items:
            self.record_truncation()
            return False
        if isinstance(value, (list, tuple, dict)) and not self.can_consume_container(
            len(value)
        ):
            return False
        remaining = self.max_bytes - self.bytes
        size = _calculate_scalar_json_size(value, remaining)
        if size is None:
            self.record_truncation()
            return False
        self.items += 1
        self.bytes += size
        return True

    def can_enter(self, size: int) -> bool:
        """Reject a container whose immediate children exceed the remaining budget."""
        if size > self.max_items - self.items:
            self.record_truncation()
            return False
        return True

    def can_consume_container(self, size: int) -> bool:
        """Reject a container before inspecting it when its children cannot fit."""
        if size >= self.max_items - self.items:
            self.record_truncation()
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
    values: Histogram
    highest_values: HighestValueSessions
    session_ids: SessionReferences
    sessions_analyzed: int
    unit: str
    caveat: str = "Activity volume is not evidence of outcome quality."


@dataclass
class _State:
    """Mutable state for a bounded deterministic scan."""

    config: ProfilingConfig
    payload_budget: _PayloadBudget = field(init=False)
    counts_store: CountsStore = field(default_factory=CountsStore)
    analyzed_session_ids: SessionReferences = field(init=False)
    node_session_ids: SessionReferences = field(init=False)
    tool_count_session_ids: SessionReferences = field(init=False)
    signals: dict[str, _Aggregate] = field(default_factory=dict)
    statuses: Counter[str] = field(default_factory=Counter)
    status_sessions: dict[str, SessionReferences] = field(default_factory=dict)
    tool_counts: Histogram = field(default_factory=lambda: Histogram((3, 6, 10, 15)))
    model_counts: Histogram = field(default_factory=lambda: Histogram((2, 3, 5)))
    activity_counts: Histogram = field(
        default_factory=lambda: Histogram((10, 20, 30, 50))
    )
    durations: Histogram = field(default_factory=lambda: Histogram((5, 15, 30, 60)))
    highest_tool_counts: HighestValueSessions = field(init=False)
    highest_model_counts: HighestValueSessions = field(init=False)
    highest_activity_counts: HighestValueSessions = field(init=False)
    highest_durations: HighestValueSessions = field(init=False)
    duration_session_ids: SessionReferences = field(init=False)
    models: LabelCounts = field(init=False)
    model_sessions: SessionReferences = field(init=False)
    text_bytes_available: int = 0
    inspected_text_bytes: int = 0
    text_inspected_session_ids: SessionReferences = field(init=False)
    session_inspected_text_bytes: int = 0
    nodes_analyzed: int = 0
    payload_truncation_count: int = 0
    text_available: bool = False
    text_truncated: bool = False
    timing_available: int = 0
    identity_available: int = 0
    tool_calls: int = 0
    tool_requests_available: bool = False
    ambiguous_tool_outputs: int = 0
    contribution_truncated: bool = False
    maximum_contributors_available: int = 0
    min_started_at: datetime | None = None
    max_started_at: datetime | None = None
    cycle_max_repeats: dict[str, int] = field(default_factory=dict)
    cycle_steps: dict[str, list[tuple[str, bool]]] = field(default_factory=dict)
    cycle_session_sets: dict[str, set[uuid.UUID]] = field(
        default_factory=lambda: defaultdict(set)
    )
    resources: ResourceProfile = field(init=False)

    def __post_init__(self) -> None:
        """Initialize the traversal budget from the immutable config."""
        self.analyzed_session_ids = SessionReferences(
            self.config.max_contributing_sessions
        )
        self.node_session_ids = SessionReferences(self.config.max_contributing_sessions)
        self.tool_count_session_ids = SessionReferences(
            self.config.max_contributing_sessions
        )
        self.duration_session_ids = SessionReferences(
            self.config.max_contributing_sessions
        )
        self.model_sessions = SessionReferences(self.config.max_contributing_sessions)
        self.text_inspected_session_ids = SessionReferences(
            self.config.max_contributing_sessions
        )
        evidence_limit = min(
            MAX_DISTRIBUTION_EXAMPLES,
            self.config.max_evidence_per_candidate,
            self.config.max_contributing_sessions,
        )
        self.highest_tool_counts = HighestValueSessions(evidence_limit)
        self.highest_model_counts = HighestValueSessions(evidence_limit)
        self.highest_activity_counts = HighestValueSessions(evidence_limit)
        self.highest_durations = HighestValueSessions(evidence_limit)
        self.models = LabelCounts(self.counts_store, "models")
        self.resources = ResourceProfile(
            FindingLimits.from_bounds(
                max_evidence=self.config.max_evidence_per_candidate,
                max_contributors=self.config.max_contributing_sessions,
                max_text_bytes=self.config.max_text_bytes,
            )
        )
        self.reset_payload_budget()

    def reset_payload_budget(self) -> None:
        """Give each session an independent payload and text safety budget."""
        self.session_inspected_text_bytes = 0
        self.payload_budget = _PayloadBudget(
            max_items=self.config.max_payload_items,
            max_bytes=self.config.max_payload_bytes,
            max_depth=self.config.max_payload_depth,
        )


def _validate_exact_name(value: str | None) -> str | None:
    """Return a safe bounded name without changing exact identity semantics."""
    if value is None or sanitize_label(value) is None:
        return None
    return value


def _normalize_observed(
    value: Any, budget: _PayloadBudget, *, depth: int = 0
) -> tuple[Any, bool]:
    """Normalize finite JSON-like values for exact tool-call identity."""
    while isinstance(value, Enum):
        value = value.value
    if not budget.consume(value, depth=depth):
        return None, False
    if value is None or isinstance(value, (str, bool, int)):
        return value, True
    if isinstance(value, float):
        return (value, True) if math.isfinite(value) else (None, False)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None, False
        formatted = _get_bounded_decimal_text(value, budget.max_bytes)
        return (formatted, True) if formatted is not None else (None, False)
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
            converted, available = _normalize_observed(
                value[key], budget, depth=depth + 1
            )
            normalized_dict[key] = converted
            complete = complete and available
        return normalized_dict, complete
    return None, False


def _get_tool_identity(
    node: SessionNodeResponse, budget: _PayloadBudget
) -> tuple[str, str] | None:
    """Return evaluator-compatible exact tool name and canonical inputs."""
    tool_name = _validate_exact_name(node.tool_name)
    if tool_name is None or node.inputs is None:
        return None
    normalized, complete = _normalize_observed(node.inputs, budget)
    if not complete:
        return None
    canonical = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return tool_name, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _is_empty_result(value: Any) -> bool:
    """Match the deterministic evaluator's non-null empty result definition."""
    return isinstance(value, (str, list, dict)) and len(value) == 0


def _is_mostly_uppercase(value: str) -> bool:
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
) -> tuple[bool, Any, int]:
    """Resolve a valid RFC 6901 pointer without raising on source data."""
    if pointer is None:
        return False, None, 0
    if not isinstance(pointer, str):
        budget.record_truncation()
        return False, None, 0
    if len(pointer) > MAX_SELECTOR_LENGTH or pointer.count("/") > MAX_SELECTOR_SEGMENTS:
        budget.record_truncation()
        return False, None, 0
    if pointer == "":
        return True, document, 0
    if not pointer.startswith("/"):
        return False, None, 0
    current = document
    for depth, raw_part in enumerate(pointer[1:].split("/")):
        if not budget.consume(current, depth=depth):
            return False, None, 0
        part = _decode_pointer_part(raw_part)
        if part is None:
            return False, None, 0
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
            return False, None, 0
    return True, current, len(pointer[1:].split("/"))


def _collect_text_parts(
    value: Any, budget: _PayloadBudget, *, start_depth: int = 0
) -> list[str]:
    """Extract strings from common structured message text containers."""
    texts: list[str] = []
    stack: list[tuple[Any, int]] = [(value, start_depth)]
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
            discriminator = current.get("type")
            if isinstance(discriminator, str) and discriminator in {
                "text",
                "input_text",
            }:
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


def _get_user_messages(value: Any, budget: _PayloadBudget) -> list[str]:
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
        role = current.get("role")
        if isinstance(role, str):
            if len(role) > MAX_ROLE_LENGTH:
                budget.record_truncation()
                continue
            if not budget.consume(role, depth=depth + 1):
                continue
        if isinstance(role, str) and role.lower() == "user":
            for key in ("content", "parts", "text"):
                if key in current:
                    messages.extend(
                        _collect_text_parts(current[key], budget, start_depth=depth + 1)
                    )
            continue
        keys = sorted(key for key in current if isinstance(key, str) and key != "role")
        if budget.can_enter(len(keys)):
            stack.extend((current[key], depth + 1) for key in reversed(keys))
    return messages


def _select_user_texts(
    state: _State,
    session: SessionWithNodesResponse,
    nodes: list[SessionNodeResponse],
) -> tuple[list[tuple[str, uuid.UUID | None]], bool]:
    """Select user text using explicit selectors before conservative recursion."""
    mirrored: dict[tuple[str, int], uuid.UUID | None] = {}
    selected_node_turns: dict[str, list[uuid.UUID]] = defaultdict(list)
    truncations_before = state.payload_budget.truncation_count

    def add_mirrored_texts(texts: Sequence[str], node_id: uuid.UUID | None) -> None:
        occurrences: Counter[str] = Counter()
        for text in texts:
            normalized = text.strip()
            if normalized:
                occurrence = occurrences[normalized]
                occurrences[normalized] += 1
                # The ordinal preserves repeated turns within one representation;
                # setdefault merges that same history when another payload mirrors it.
                mirrored.setdefault((normalized, occurrence), node_id)

    def add_selected_node_turns(texts: Sequence[str], node_id: uuid.UUID) -> None:
        for text in texts:
            normalized = text.strip()
            if normalized:
                selected_node_turns[normalized].append(node_id)

    found, value, value_depth = _resolve_pointer(
        session.session.inputs,
        session.session.input_text_selector,
        state.payload_budget,
    )
    session_texts = (
        _collect_text_parts(value, state.payload_budget, start_depth=value_depth)
        if found
        else _get_user_messages(session.session.inputs, state.payload_budget)
    )
    add_mirrored_texts(session_texts, None)
    for node in nodes:
        if node.node_type is not NodeType.LLM_CALL:
            continue
        found, value, value_depth = _resolve_pointer(
            node.inputs, node.input_text_selector, state.payload_budget
        )
        node_texts = (
            _collect_text_parts(value, state.payload_budget, start_depth=value_depth)
            if found
            else _get_user_messages(node.inputs, state.payload_budget)
        )
        if found:
            add_selected_node_turns(node_texts, node.id)
        else:
            add_mirrored_texts(node_texts, node.id)
    deduplicated = dict(mirrored)
    for text, node_ids in selected_node_turns.items():
        for occurrence, node_id in enumerate(node_ids):
            # A selector identifies the current turn for a particular model call.
            # Overwriting the same ordinal from a mirrored history keeps that turn
            # distinct without adding the history twice.
            deduplicated[(text, occurrence)] = node_id
    selected = [
        (text, node_id)
        for (text, _), node_id in sorted(
            deduplicated.items(),
            key=lambda item: (item[0][0], item[0][1], str(item[1] or "")),
        )
    ]
    return selected, state.payload_budget.truncation_count == truncations_before


def _record(
    state: _State,
    signal: str,
    session_id: uuid.UUID,
    *,
    category: str,
    node_id: uuid.UUID | None,
) -> None:
    """Record one content-free signal occurrence."""
    if signal not in state.signals:
        state.signals[signal] = _Aggregate(
            sessions=SessionReferences(state.config.max_contributing_sessions),
            categories=LabelCounts(state.counts_store, signal),
        )
    aggregate = state.signals[signal]
    aggregate.count += 1
    aggregate.sessions.add(session_id)
    aggregate.categories.add(category)
    if (
        len(aggregate.evidence) == state.config.max_evidence_per_candidate
        and session_id >= aggregate.evidence[-1].session_id
    ):
        return
    aggregate.evidence.append(
        EvidenceLocator(session_id=session_id, node_id=node_id, signal=signal)
    )
    # Stable sorting preserves within-session event order while keeping the
    # same smallest session IDs regardless of input order or batch boundaries.
    aggregate.evidence.sort(key=lambda locator: locator.session_id)
    del aggregate.evidence[state.config.max_evidence_per_candidate :]


def _find_cycles(
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


def _count_output_tool_requests(
    nodes: Sequence[SessionNodeResponse], budget: _PayloadBudget
) -> int | None:
    """Count requests in fully inspected assistant outputs, or return unknown."""
    if not nodes:
        return None
    count = 0
    for node in nodes:
        if not isinstance(node.outputs, (list, dict)):
            return None
        assistant_seen = False
        stack: list[tuple[Any, int, bool]] = [(node.outputs, 0, False)]
        while stack:
            current, depth, in_parts = stack.pop()
            if not budget.consume(current, depth=depth):
                return None
            if not isinstance(current, (list, dict)):
                return None
            if isinstance(current, list):
                if not budget.can_enter(len(current)):
                    return None
                stack.extend((item, depth + 1, in_parts) for item in current)
            elif in_parts:
                if current.get("type") not in ("text", "tool_call", "tool_result"):
                    return None
                count += current.get("type") == "tool_call"
            elif current.get("role") == "assistant":
                if not isinstance(current.get("parts"), list):
                    return None
                assistant_seen = True
                stack.append((current.get("parts"), depth + 1, True))
            elif "role" not in current and "messages" in current:
                stack.append((current["messages"], depth + 1, False))
            elif "role" not in current:
                return None
        if not assistant_seen:
            return None
    return count


def _scan_session(
    state: _State,
    session: SessionWithNodesResponse,
    nodes: list[SessionNodeResponse],
) -> None:
    """Add one bounded normalized session to aggregate state."""
    session_id = session.session.id
    state.analyzed_session_ids.add(session_id)
    state.statuses[session.session.status.value] += 1
    status = session.session.status.value
    if status not in state.status_sessions:
        state.status_sessions[status] = SessionReferences(
            state.config.max_contributing_sessions
        )
    state.status_sessions[status].add(session_id)
    calls = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
    llm_calls = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
    state.node_session_ids.add(session_id)
    state.tool_calls += len(calls)
    tool_count = (
        len(calls)
        if calls
        else _count_output_tool_requests(llm_calls, state.payload_budget)
    )
    if tool_count is not None:
        state.tool_count_session_ids.add(session_id)
        state.tool_counts.add(tool_count)
        state.highest_tool_counts.add(session_id, tool_count)
        if not calls and tool_count:
            state.tool_requests_available = True
    state.model_counts.add(len(llm_calls))
    state.highest_model_counts.add(session_id, len(llm_calls))
    state.activity_counts.add(len(nodes))
    state.highest_activity_counts.add(session_id, len(nodes))

    started_at = session.session.started_at
    ended_at = session.session.ended_at
    if (
        started_at is not None
        and started_at.tzinfo is not None
        and started_at.utcoffset() is not None
    ):
        started_utc = started_at.astimezone(UTC)
        if state.min_started_at is None or started_utc < state.min_started_at:
            state.min_started_at = started_utc
        if state.max_started_at is None or started_utc > state.max_started_at:
            state.max_started_at = started_utc
    if started_at is not None and ended_at is not None:
        started_aware = (
            started_at.tzinfo is not None and started_at.utcoffset() is not None
        )
        ended_aware = ended_at.tzinfo is not None and ended_at.utcoffset() is not None
        if started_aware == ended_aware:
            duration = (ended_at - started_at).total_seconds()
            if duration >= 0:
                state.durations.add(duration)
                state.highest_durations.add(session_id, duration)
                state.duration_session_ids.add(session_id)
                state.timing_available += 1

    for node in llm_calls:
        label = sanitize_label(node.model or node.requested_model)
        if label is not None:
            state.models.add(label)
            state.model_sessions.add(session_id)

    identities = [_get_tool_identity(call, state.payload_budget) for call in calls]
    state.identity_available += sum(identity is not None for identity in identities)
    for call in calls:
        label = sanitize_label(call.tool_name) or UNAVAILABLE_TOOL
        if call.status is NodeStatus.FAILED:
            _record(
                state,
                "tool-errors",
                session_id,
                category=label,
                node_id=call.id,
            )
        if call.outputs is None:
            state.ambiguous_tool_outputs += 1
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
        first_tool_name = _validate_exact_name(first.tool_name)
        second_tool_name = _validate_exact_name(second.tool_name)
        label = sanitize_label(first.tool_name) or UNAVAILABLE_TOOL
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
            and first_tool_name is not None
            and first_tool_name == second_tool_name
        ):
            _record(
                state,
                "adjacent-same-tool-failures",
                session_id,
                category=label,
                node_id=first.id,
            )

    for start, end, tools in _find_cycles(calls, identities):
        safe_tools = [sanitize_label(tool) or UNAVAILABLE_TOOL for tool in tools]
        cycle_label = " -> ".join(safe_tools)
        period = len(safe_tools)
        repeats = max(2, (end - start + 1) // period)
        state.cycle_max_repeats[cycle_label] = max(
            state.cycle_max_repeats.get(cycle_label, 0), repeats
        )
        step_errors = [
            any(
                calls[idx].status is NodeStatus.FAILED
                for idx in range(start + offset, end + 1, period)
            )
            for offset in range(period)
        ]
        existing_steps = state.cycle_steps.get(cycle_label)
        if existing_steps is None:
            state.cycle_steps[cycle_label] = list(
                zip(safe_tools, step_errors, strict=True)
            )
        else:
            state.cycle_steps[cycle_label] = [
                (name, prev_err or cur_err)
                for (name, prev_err), cur_err in zip(
                    existing_steps, step_errors, strict=True
                )
            ]
        state.cycle_session_sets[cycle_label].add(session_id)
        _record(
            state,
            "short-tool-cycles",
            session_id,
            category=cycle_label,
            node_id=calls[start].id,
        )

    messages, payload_complete = _select_user_texts(state, session, nodes)
    state.text_available = state.text_available or bool(messages)
    if not payload_complete:
        return
    state.text_bytes_available += sum(
        len(message.encode("utf-8")) for message, _ in messages
    )
    fully_inspected = bool(messages)
    language_matches: list[tuple[str, str, uuid.UUID | None]] = []
    for message, node_id in messages:
        encoded = message.encode("utf-8")
        remaining = state.config.max_text_bytes - state.session_inspected_text_bytes
        if remaining <= 0:
            state.text_truncated = True
            fully_inspected = False
            break
        inspected = encoded[:remaining].decode("utf-8", errors="ignore")
        state.inspected_text_bytes += len(inspected.encode("utf-8"))
        state.session_inspected_text_bytes += len(inspected.encode("utf-8"))
        if len(encoded) > remaining:
            state.text_truncated = True
            fully_inspected = False
        if _CORRECTION_PATTERN.search(inspected):
            language_matches.append(
                (
                    "correction-language",
                    "Literal correction marker",
                    node_id,
                )
            )
        if _PUNCTUATION_PATTERN.search(inspected):
            language_matches.append(
                (
                    "repeated-punctuation",
                    "Repeated punctuation",
                    node_id,
                )
            )
        if _is_mostly_uppercase(inspected):
            language_matches.append(
                (
                    "mostly-uppercase-messages",
                    "Mostly uppercase",
                    node_id,
                )
            )
        if _PROFANITY_PATTERN.search(inspected):
            language_matches.append(
                (
                    "possible-profanity",
                    "Literal profanity marker",
                    node_id,
                )
            )
        if len(encoded) > remaining:
            break
    if fully_inspected:
        state.text_inspected_session_ids.add(session_id)
        for signal, category, node_id in language_matches:
            _record(
                state,
                signal,
                session_id,
                category=category,
                node_id=node_id,
            )


def _get_contributions(
    state: _State,
    session_ids: SessionReferences,
    *,
    reserved_ids: Sequence[uuid.UUID] = (),
) -> list[uuid.UUID]:
    """Return a bounded stable contribution set."""
    ordered = list(session_ids)
    state.maximum_contributors_available = max(
        state.maximum_contributors_available, len(session_ids)
    )
    if len(session_ids) > state.config.max_contributing_sessions:
        state.contribution_truncated = True
    reserved = set(reserved_ids)
    remaining = [session_id for session_id in ordered if session_id not in reserved]
    return sorted(
        [
            *reserved,
            *remaining[: state.config.max_contributing_sessions - len(reserved)],
        ]
    )


def _build_categorical(
    values: Counter[str] | LabelCounts,
    *,
    unit: str,
    highlight_top_tone: Tone | None = None,
) -> CategoricalArtifact | StatGridArtifact:
    """Build categorical data with deterministic count and label ordering."""
    ordered = (
        values.top()
        if isinstance(values, LabelCounts)
        else sorted(values.items(), key=lambda item: (-item[1], item[0]))
    )
    return build_categorical(ordered, unit=unit, highlight_top_tone=highlight_top_tone)


def _build_signal_artifacts(
    state: _State,
    *,
    signal: str,
    family: str,
    aggregate: _Aggregate,
    affected: int,
    eligible: int,
) -> tuple[InsightArtifact, list[InsightArtifact]]:
    """Choose the Zuri-aligned lead artifact and optional companion chart."""
    rate_value = min(1.0, affected / eligible) if eligible else 0.0
    if signal == "short-tool-cycles" and state.cycle_steps:
        top_cycle = aggregate.categories.top(1)[0][0]
        steps = state.cycle_steps.get(top_cycle)
        if steps:
            max_repeats = max(2, state.cycle_max_repeats.get(top_cycle, 3))
            cycle_sessions = max(1, len(state.cycle_session_sets.get(top_cycle, ())))
            has_multiple = len(aggregate.categories) > 1
            lead: InsightArtifact = ToolCycleArtifact(
                title="Most frequent tool loop" if has_multiple else None,
                steps=[
                    CycleStep(name=name[:120], status="error" if err else "ok")
                    for name, err in steps[:6]
                ],
                repeat_count=max_repeats,
                session_count=cycle_sessions,
                note=(
                    f"Repeated up to {max_repeats}x across "
                    f"{pluralize(cycle_sessions, 'session')}"
                ),
            )
            extras: list[InsightArtifact] = (
                [
                    _build_categorical(
                        aggregate.categories, unit="occurrences"
                    ).model_copy(update={"title": "Repeating cycles by sequence"})
                ]
                if has_multiple
                else []
            )
            return lead, extras

    if signal == "tool-errors" and len(aggregate.categories) > 1:
        return (
            _build_categorical(
                aggregate.categories, unit="occurrences", highlight_top_tone="danger"
            ),
            [],
        )

    lead_rate: InsightArtifact = RateArtifact(
        presentation="share_bar",
        rate=round(rate_value, 4),
        numerator=affected,
        denominator=max(eligible, 1),
        label=f"of {eligible} sessions",
        basis=f"{affected} of {eligible} sessions",
        tone="warn" if family == "language" else "risk",
    )
    extras = []
    if len(aggregate.categories) > 1:
        breakdown_title = (
            "Breakdown by marker" if family == "language" else "Breakdown by tool"
        )
        extras.append(
            _build_categorical(
                aggregate.categories,
                unit="occurrences",
                highlight_top_tone="risk",
            ).model_copy(update={"title": breakdown_title})
        )
    return lead_rate, extras


def _build_signal_candidate(
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
    contributions = _get_contributions(state, aggregate.sessions)
    contribution_ids = set(contributions)
    eligible_session_ids = (
        state.text_inspected_session_ids
        if family == "language"
        else state.node_session_ids
    )
    affected = len(aggregate.sessions)
    eligible = len(eligible_session_ids)
    share = calculate_percent(affected, eligible)
    lead_artifact, extra_artifacts = _build_signal_artifacts(
        state,
        signal=signal,
        family=family,
        aggregate=aggregate,
        affected=affected,
        eligible=eligible,
    )
    candidate_caveat = caveat
    if len(aggregate.categories) > 8:
        candidate_caveat += (
            " The breakdown chart shows the top 7 categories and folds the "
            "remaining categories into Other."
        )
    excluded_sessions = len(state.analyzed_session_ids) - eligible
    if family == "language" and excluded_sessions:
        noun = "session was" if excluded_sessions == 1 else "sessions were"
        candidate_caveat = (
            f"{candidate_caveat} {excluded_sessions} analyzed {noun} excluded "
            "because user text was missing or not fully inspected."
        )
    leading = aggregate.categories.top(1)
    briefing = _build_signal_briefing(
        family=family,
        subject=subject,
        affected=affected,
        analyzed=eligible,
        share=share,
        occurrences=aggregate.count,
        chart_groups=len(aggregate.categories.top()),
        leading_category_count=leading[0][1] if leading else 0,
        excluded_sessions=excluded_sessions if family == "language" else 0,
    )
    severity: Severity = (
        "high"
        if share >= 20 or (family in ("trajectory", "tool_health") and affected >= 10)
        else ("medium" if share >= 5 else "low")
    )
    questions = [
        SuggestedQuestion(label=f"Show example sessions for {eyebrow.lower()}"[:80]),
        SuggestedQuestion(label="What happened right before this signal triggered?"),
    ]
    return CandidateFinding(
        id=candidate_id,
        family=family,
        rank=rank,
        tier="primary",
        severity=severity,
        eyebrow=eyebrow,
        title=title_pattern.format(share=share, affected=affected),
        fallback_description=description_pattern.format(
            count=aggregate.count,
            affected=affected,
        ),
        caveat=candidate_caveat,
        data=lead_artifact,
        extra_artifacts=extra_artifacts,
        facts=[
            DeterministicFact(name="occurrences", value=aggregate.count),
            DeterministicFact(name="affected_sessions", value=affected),
            DeterministicFact(name="affected_share_percent", value=share),
        ],
        coverage=CandidateCoverage(
            sessions_analyzed=eligible,
            affected_sessions=affected,
            occurrences=aggregate.count,
            evidence_available=aggregate.count,
            evidence_retained=sum(
                locator.session_id in contribution_ids for locator in aggregate.evidence
            ),
            contributing_sessions_available=affected,
            contributing_sessions_retained=len(contributions),
        ),
        contributing_session_ids=contributions,
        evidence=[
            locator
            for locator in aggregate.evidence
            if locator.session_id in contribution_ids
        ],
        investigation_prompt=briefing,
        suggested_questions=questions,
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
            "The profiler found {count} exact retries across {affected} sessions. "
            "Compare the chart groups to choose a starting point.",
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
            "sessions. Compare the chart groups to choose a starting point.",
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
            "The pattern appears {count} times across {affected} sessions. "
            "Compare the chart groups to choose a starting point.",
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
            "sessions. Compare the chart groups to choose a starting point.",
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
            "The profiler found {count} recorded errors across {affected} sessions. "
            "Compare the chart groups to choose a starting point.",
            "tool errors",
            "A recorded tool error may be recovered later and is not the same as a "
            "failed session.",
        ),
        (
            "correction-language",
            "correction-language",
            "language",
            60,
            "LITERAL CORRECTION MARKERS",
            "{share}% of sessions with fully inspected selected text contain literal "
            "correction markers",
            "A literal correction marker appears {count} times across {affected} "
            "sessions.",
            "selected input text containing literal correction phrases",
            "Language markers are literal leads, not judgments of sentiment or intent. "
            "Recorded user-role messages can include agent policy or harness "
            "instructions.",
        ),
        (
            "empty-tool-results",
            "empty-tool-results",
            "tool_health",
            80,
            "EMPTY TOOL RESULTS",
            "{share}% of sessions contain an empty tool result",
            "The profiler found {count} empty results across {affected} sessions. "
            "Compare the chart groups to choose a starting point.",
            "tool calls with an empty string or container result",
            "Empty results may reflect instrumentation rather than agent behavior.",
        ),
        (
            "repeated-punctuation",
            "repeated-punctuation",
            "language",
            90,
            "REPEATED PUNCTUATION",
            "{share}% of sessions with fully inspected user text include repeated "
            "exclamation or question marks",
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
            "{share}% of sessions with fully inspected user text include a "
            "mostly-uppercase user message",
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
            "{share}% of sessions with fully inspected user text match the literal "
            "profanity monitor",
            "A small literal word list matched {count} messages across {affected} "
            "sessions.",
            "user messages matched by the literal profanity monitor",
            "Language markers are literal leads, not judgments of sentiment or intent.",
        ),
    )
    for spec in signal_specs:
        candidate = _build_signal_candidate(
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
    failed_ids = state.status_sessions.get(SessionStatus.FAILED.value)
    status_tones: dict[str, Tone] = {
        SessionStatus.COMPLETED.value: "good",
        SessionStatus.FAILED.value: "danger",
        SessionStatus.IN_PROGRESS.value: "warn",
    }
    ordered_statuses = sorted(
        state.statuses.items(), key=lambda item: (-item[1], item[0])
    )
    if analyzed_sessions and failed_ids:
        affected_ids = failed_ids
        contributions = _get_contributions(state, affected_ids)
        contribution_ids = set(contributions)
        completed_sessions = state.statuses.get(SessionStatus.COMPLETED.value, 0)
        failed_count = state.statuses.get("failed", 0)
        failed_percent = calculate_percent(failed_count, analyzed_sessions)
        outcome_description = (
            "Recorded session statuses show where to begin comparing failed and "
            "completed runs."
            if completed_sessions
            else "Recorded session statuses show where to investigate failures and "
            "define a useful comparison group."
        )
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
        outcome_tier: Literal["primary", "secondary"] = (
            "primary" if failed_percent >= 5 else "secondary"
        )
        outcome_severity: Severity = (
            "high"
            if failed_percent >= 20
            else ("medium" if failed_percent >= 5 else "low")
        )
        candidates.append(
            CandidateFinding(
                id="session-outcomes",
                family="outcome",
                rank=55,
                tier=outcome_tier,
                severity=outcome_severity,
                eyebrow="SESSION OUTCOMES",
                title=(f"{failed_percent}% of sessions are recorded failed"),
                fallback_description=outcome_description,
                caveat=(
                    "Recorded status describes the session boundary, not the cause of "
                    "the outcome."
                ),
                data=build_split_bar(
                    ordered_statuses, unit="sessions", tones=status_tones
                ),
                facts=[
                    DeterministicFact(
                        name="failed_sessions",
                        value=failed_count,
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
                investigation_prompt=_build_outcome_briefing(
                    failed=len(affected_ids),
                    completed=completed_sessions,
                    analyzed=analyzed_sessions,
                    failed_percent=failed_percent,
                    statuses=len(state.statuses),
                ),
                suggested_questions=[
                    SuggestedQuestion(
                        label="What do the failed sessions have in common?"
                    ),
                    SuggestedQuestion(
                        label="Compare failed sessions with completed ones"
                    ),
                ],
            )
        )
    elif analyzed_sessions and len(ordered_statuses) >= 2:
        all_contributions = _get_contributions(state, state.analyzed_session_ids)
        candidates.append(
            CandidateFinding(
                id="session-outcomes",
                family="outcome",
                rank=118,
                tier="secondary",
                eyebrow="SESSION OUTCOMES",
                title="How sessions ended",
                fallback_description=(
                    "Baseline breakdown of recorded session statuses across the "
                    "analyzed run."
                ),
                caveat=(
                    "Recorded status describes the session boundary, not the cause of "
                    "the outcome."
                ),
                data=build_split_bar(
                    ordered_statuses, unit="sessions", tones=status_tones
                ),
                facts=[DeterministicFact(name="sessions", value=analyzed_sessions)],
                coverage=CandidateCoverage(
                    sessions_analyzed=analyzed_sessions,
                    affected_sessions=analyzed_sessions,
                    occurrences=analyzed_sessions,
                    evidence_available=0,
                    evidence_retained=0,
                    contributing_sessions_available=analyzed_sessions,
                    contributing_sessions_retained=len(all_contributions),
                ),
                contributing_session_ids=all_contributions,
                evidence=[],
                investigation_prompt=format_briefing(
                    odd=f"Sessions span {len(ordered_statuses)} recorded statuses.",
                    look_first="Compare sessions across each recorded status.",
                    cohort="sessions grouped by recorded status.",
                    hypothesis=(
                        "Check whether non-completed sessions share a stop point."
                    ),
                    held="non-completed sessions decline on replay.",
                ),
            )
        )

    tool_unit = "requests" if state.tool_requests_available else "calls"
    tool_caveat = "Activity volume is not evidence of outcome quality."
    if state.tool_requests_available:
        tool_caveat += (
            " Counts use recorded tool nodes where available, otherwise explicit tool "
            "requests in model outputs. Tool requests may not have executed."
        )
    if len(state.tool_count_session_ids) < len(state.node_session_ids):
        tool_caveat += (
            " Sessions with incompletely inspected tool-request payloads were excluded."
        )
    distribution_specs = (
        _DistributionSpec(
            candidate_id="tool-call-distribution",
            family="activity",
            rank=120,
            eyebrow=(
                "TOOL-REQUEST DISTRIBUTION"
                if state.tool_requests_available
                else "TOOL-CALL DISTRIBUTION"
            ),
            title=f"Tool {tool_unit} per session have a measurable spread",
            description=(
                "The distribution shows sessions where repeated tool use may warrant "
                "closer inspection."
            ),
            values=state.tool_counts,
            highest_values=state.highest_tool_counts,
            session_ids=state.tool_count_session_ids,
            sessions_analyzed=len(state.tool_count_session_ids),
            unit=tool_unit,
            caveat=tool_caveat,
        ),
        _DistributionSpec(
            candidate_id="model-call-distribution",
            family="activity",
            rank=130,
            eyebrow="MODEL-CALL DISTRIBUTION",
            title="Model calls per session have a measurable spread",
            description=(
                "The distribution shows how model-call activity varies across the "
                "analyzed sessions."
            ),
            values=state.model_counts,
            highest_values=state.highest_model_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
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
            highest_values=state.highest_activity_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
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
            highest_values=state.highest_durations,
            session_ids=state.duration_session_ids,
            sessions_analyzed=analyzed_sessions,
            unit="seconds",
            caveat="Recorded timestamps may omit uninstrumented work.",
        ),
    )
    for spec in distribution_specs:
        if len(spec.values) < 2 or spec.values.minimum == spec.values.maximum:
            continue
        highest_bin = spec.values.get_highest_occupied_bin()
        assert highest_bin is not None
        evidence = [
            EvidenceLocator(session_id=session_id, signal=DISTRIBUTION_TOP_BIN_SIGNAL)
            for session_id, value in spec.highest_values.get_entries()
            if spec.values.get_bin_index(value) == highest_bin
        ]
        contributing = _get_contributions(
            state, spec.session_ids, reserved_ids=[item.session_id for item in evidence]
        )
        if not contributing:
            continue
        candidates.append(
            CandidateFinding(
                id=spec.candidate_id,
                family=spec.family,
                rank=spec.rank,
                tier="secondary",
                eyebrow=spec.eyebrow,
                title=spec.title,
                fallback_description=spec.description,
                caveat=spec.caveat,
                data=build_binned(
                    spec.values,
                    unit=spec.unit,
                    presentation="micro",
                    focus_top_bin=True,
                ),
                facts=[
                    DeterministicFact(name="observations", value=len(spec.values)),
                    DeterministicFact(name="minimum", value=spec.values.minimum),
                    DeterministicFact(name="maximum", value=spec.values.maximum),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=spec.sessions_analyzed,
                    affected_sessions=len(spec.session_ids),
                    occurrences=len(spec.values),
                    evidence_available=spec.values.bins[highest_bin],
                    evidence_retained=len(evidence),
                    contributing_sessions_available=len(spec.session_ids),
                    contributing_sessions_retained=len(contributing),
                ),
                contributing_session_ids=contributing,
                evidence=evidence,
                investigation_prompt=_build_distribution_briefing(
                    family=spec.family,
                    values=spec.values,
                    sessions_analyzed=spec.sessions_analyzed,
                    unit=spec.unit,
                ),
            )
        )

    if len(state.models) > 1:
        model_sessions = state.model_sessions
        contributions = _get_contributions(state, model_sessions)
        candidates.append(
            CandidateFinding(
                id="model-mix",
                family="model",
                rank=160,
                tier="secondary",
                eyebrow="MODEL MIX",
                title=(
                    f"{len(state.models)} recorded model identifiers appear in "
                    "these sessions"
                ),
                fallback_description=(
                    "This identifier mix can seed a cohort for cost, latency, or "
                    "quality comparisons."
                ),
                caveat=(
                    "Identifiers prefer the served model, falling back to the "
                    "requested model. Different identifiers may refer to the same "
                    "model."
                    + (
                        " The chart shows the 20 most frequent identifiers and "
                        "combines the remaining identifiers."
                        if len(state.models) > 20
                        else ""
                    )
                ),
                data=_build_categorical(state.models, unit="calls"),
                facts=[
                    DeterministicFact(name="models", value=len(state.models)),
                    DeterministicFact(name="model_calls", value=state.models.total),
                ],
                coverage=CandidateCoverage(
                    sessions_analyzed=len(state.node_session_ids),
                    affected_sessions=len(model_sessions),
                    occurrences=state.models.total,
                    evidence_available=state.models.total,
                    evidence_retained=0,
                    contributing_sessions_available=len(model_sessions),
                    contributing_sessions_retained=len(contributions),
                ),
                contributing_session_ids=contributions,
                evidence=[],
                investigation_prompt=_build_model_mix_briefing(
                    models=len(state.models),
                    model_calls=state.models.total,
                    leading_model_calls=state.models.top(1)[0][1],
                    affected=len(model_sessions),
                    analyzed=len(state.node_session_ids),
                ),
            )
        )
    candidates.extend(
        state.resources.build_candidates(partial(_get_contributions, state))
    )
    candidates.sort(key=lambda candidate: (candidate.rank, candidate.id))
    return candidates


def _calculate_content_hash(
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


class SessionProfiler:
    """Scan sessions incrementally with bounded memory and exact aggregate counts.

    Use as a context manager to release temporary storage if iteration fails.
    Each session ID must be supplied exactly once. No session DTO is retained
    after consume returns; one individual trace must still fit in memory.
    """

    def __init__(self, config: ProfilingConfig | None = None) -> None:
        """Initialize an empty full-import scan."""
        self._state = _State(config=config or ProfilingConfig())
        self._closed = False

    def __enter__(self) -> Self:
        """Return the profiler for a resource-scoped scan."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close temporary storage when leaving the scan context."""
        self.close()

    def close(self) -> None:
        """Release temporary storage; no further consumption is allowed."""
        self._state.counts_store.close()
        self._state.resources.close()
        self._closed = True

    def consume(self, session: SessionWithNodesResponse) -> None:
        """Profile one session, rejecting duplicate IDs and closing on error."""
        if self._closed:
            raise ValueError("SessionProfiler is closed")
        try:
            state = self._state
            if (
                state.counts_store.increment(
                    "seen-session-ids", str(session.session.id)
                )
                != 1
            ):
                raise ValueError("session IDs must be unique")
            state.reset_payload_budget()
            # The server returns nodes ordered by position already.
            ordered_nodes = list(session.nodes)
            _scan_session(state, session, ordered_nodes)
            state.resources.consume(session, ordered_nodes)
            state.nodes_analyzed += len(ordered_nodes)
            state.payload_truncation_count += state.payload_budget.truncation_count
        except BaseException:
            self.close()
            raise

    def finish(self, source_session_count: int | None = None) -> ProfilingResult:
        """Build the bounded result and release storage, including on failure."""
        if self._closed:
            raise ValueError("SessionProfiler is closed")
        try:
            return _finish_profile(self._state, source_session_count)
        finally:
            self.close()


def profile_sessions(
    sessions: Iterable[SessionWithNodesResponse],
    *,
    config: ProfilingConfig | None = None,
    source_session_count: int | None = None,
) -> ProfilingResult:
    """Profile every caller-scoped session without materializing the iterable."""
    with SessionProfiler(config=config) as profiler:
        for session in sessions:
            profiler.consume(session)
        return profiler.finish(source_session_count=source_session_count)


def _finish_profile(state: _State, source_session_count: int | None) -> ProfilingResult:
    """Project the complete scan into bounded findings and honest coverage."""
    selected_config = state.config
    sessions_analyzed = len(state.analyzed_session_ids)
    sessions_available = sessions_analyzed
    if source_session_count is not None:
        if (
            type(source_session_count) is not int
            or source_session_count < sessions_analyzed
        ):
            raise ValueError(
                "source_session_count must be an integer >= analyzed sessions"
            )
        sessions_available = source_session_count
    candidates = _build_candidates(state)

    truncations: list[CoverageTruncation] = []
    if sessions_analyzed < sessions_available:
        truncations.append(
            CoverageTruncation(
                dimension="sessions",
                available=sessions_available,
                analyzed=sessions_analyzed,
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
    if sessions_available > sessions_analyzed:
        caveats[0] += (
            " Some source sessions were not supplied for profiling. nodes_available "
            "counts only nodes in the supplied sessions, not every source session."
        )
    if state.timing_available < sessions_analyzed:
        caveats.append(
            "Timing coverage is incomplete because valid session bounds are missing."
        )
    unknown_tool_sessions = sessions_analyzed - len(state.tool_count_session_ids)
    if unknown_tool_sessions:
        caveats.append(
            f"Tool activity coverage is incomplete for {unknown_tool_sessions} "
            "sessions because tool nodes or fully inspected supported assistant "
            "outputs were unavailable. These sessions were excluded from tool "
            "counts; zero counts mean no recorded requests, not proof of no tool use."
        )
    if state.identity_available < state.tool_calls:
        caveats.append(
            "Tool identity coverage is incomplete because names or finite inputs are "
            "missing."
        )
    if state.ambiguous_tool_outputs:
        caveats.append(
            "Tool outputs recorded as null were not profiled because the normalized "
            "contract cannot distinguish an explicit null from an omitted payload."
        )
    if not state.text_available:
        caveats.append(
            "User text coverage is unavailable because no explicit user-authored "
            "input was found."
        )
    excluded_text_sessions = sessions_analyzed - len(state.text_inspected_session_ids)
    if excluded_text_sessions:
        noun = "session" if excluded_text_sessions == 1 else "sessions"
        caveats.append(
            f"Language-signal profiling excluded {excluded_text_sessions} analyzed "
            f"{noun} because user text was missing or not fully inspected."
        )
    if state.text_truncated:
        caveats.append(
            "User text inspection reached the configured per-session byte limit."
        )
    if state.payload_truncation_count:
        caveats.append(
            "Payload inspection reached a configured per-session item, byte, or "
            "depth limit; "
            "tool identity and user text coverage may be incomplete."
        )

    coverage = Coverage(
        sessions_available=sessions_available,
        sessions_analyzed=sessions_analyzed,
        nodes_available=state.nodes_analyzed,
        nodes_analyzed=state.nodes_analyzed,
        inspected_text_bytes=state.inspected_text_bytes,
        started_after=state.min_started_at,
        started_before=state.max_started_at,
        truncations=truncations,
        caveats=caveats,
    )
    result = ProfilingResult(
        content_hash=_calculate_content_hash(selected_config, coverage, candidates),
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
        caveats = [
            *result.coverage.caveats,
            "No candidate fit within the configured projection byte limit.",
        ]
        result = result.model_copy(
            update={"coverage": result.coverage.model_copy(update={"caveats": caveats})}
        )
    projection_bytes = len(result.model_dump_json().encode("utf-8"))
    if projection_bytes > selected_config.max_projection_bytes:
        raise ValueError(
            "Coverage envelope exceeds max_projection_bytes even without candidates "
            f"({projection_bytes} > {selected_config.max_projection_bytes}); "
            "increase max_projection_bytes to retain coverage details."
        )
    return result.model_copy(
        update={
            "content_hash": _calculate_content_hash(
                selected_config, result.coverage, result.candidates
            )
        }
    )
