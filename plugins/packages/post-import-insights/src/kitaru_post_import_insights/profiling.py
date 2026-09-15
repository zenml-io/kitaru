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
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from itertools import pairwise
from types import TracebackType
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
from kitaru.redaction import redact_data
from kitaru_post_import_insights.models import (
    DISTRIBUTION_TOP_BIN_SIGNAL,
    MAX_CONTRIBUTING_SESSIONS,
    MAX_EVIDENCE_LOCATORS,
    MAX_INVESTIGATION_PROMPT_LENGTH,
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

ANALYSIS_VERSION = "2026-09-09.1"
MAX_DISTRIBUTION_EXAMPLES = 5
MAX_LABEL_LENGTH = 120
MAX_ROLE_LENGTH = 32
MAX_SELECTOR_LENGTH = 1_024
MAX_SELECTOR_SEGMENTS = 64
_DECIMAL_BASE_SIZE = Decimal(0).__sizeof__()

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

    analysis_version: Literal["2026-09-09.1"] = ANALYSIS_VERSION
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
    """Return exact UTF-8 JSON string size, stopping once it exceeds a limit."""
    size = 2  # Opening and closing quotes.
    short_escapes = {"\b", "\t", "\n", "\f", "\r", '"', "\\"}
    for character in value:
        codepoint = ord(character)
        if character in short_escapes:
            size += 2
        elif codepoint < 0x20:
            size += 6
        elif codepoint < 0x80:
            size += 1
        elif codepoint < 0x800:
            size += 2
        elif codepoint < 0x10000:
            if 0xD800 <= codepoint <= 0xDFFF:
                return None
            size += 3
        else:
            size += 4
        if size > limit:
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


@dataclass
class _State:
    """Mutable state for a bounded deterministic scan."""

    config: ProfilingConfig
    payload_budget: _PayloadBudget = field(init=False)
    counts_store: CountsStore = field(default_factory=CountsStore)
    analyzed_session_ids: SessionReferences = field(init=False)
    node_session_ids: SessionReferences = field(init=False)
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
    ambiguous_tool_outputs: int = 0
    contribution_truncated: bool = False
    maximum_contributors_available: int = 0

    def __post_init__(self) -> None:
        """Initialize the traversal budget from the immutable config."""
        self.analyzed_session_ids = SessionReferences(
            self.config.max_contributing_sessions
        )
        self.node_session_ids = SessionReferences(self.config.max_contributing_sessions)
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
        self.reset_payload_budget()

    def reset_payload_budget(self) -> None:
        """Give each session an independent payload and text safety budget."""
        self.session_inspected_text_bytes = 0
        self.payload_budget = _PayloadBudget(
            max_items=self.config.max_payload_items,
            max_bytes=self.config.max_payload_bytes,
            max_depth=self.config.max_payload_depth,
        )


def sanitize_label(value: str | None) -> str | None:
    """Return a bounded exact label, or omit it when it may contain a secret."""
    if value is None:
        return None
    if len(value) > MAX_LABEL_LENGTH:
        return None
    candidate = value
    if not candidate.strip():
        return None
    try:
        candidate.encode("utf-8")
    except UnicodeEncodeError:
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


def _validate_exact_name(value: str | None) -> str | None:
    """Return a safe bounded name without changing exact identity semantics."""
    if value is None or sanitize_label(value) is None:
        return None
    if _CONTROL_PATTERN.search(value):
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
    state.tool_counts.add(len(calls))
    state.highest_tool_counts.add(session_id, len(calls))
    state.model_counts.add(len(llm_calls))
    state.highest_model_counts.add(session_id, len(llm_calls))
    state.activity_counts.add(len(nodes))
    state.highest_activity_counts.add(session_id, len(nodes))

    started_at = session.session.started_at
    ended_at = session.session.ended_at
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

    for start, _, tools in _find_cycles(calls, identities):
        safe_tools = [sanitize_label(tool) or "Unavailable tool" for tool in tools]
        _record(
            state,
            "short-tool-cycles",
            session_id,
            category=" -> ".join(safe_tools),
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


def _calculate_percent(numerator: int, denominator: int) -> int | float:
    """Round percentages, preserving the scale of very sparse shares."""
    if not denominator:
        return 0
    exact = Decimal(numerator) * 100 / Decimal(denominator)
    quantum = Decimal("0.01")
    value = exact.quantize(quantum, rounding=ROUND_HALF_UP)
    while 0 < numerator < denominator and value in (0, 100):
        quantum /= 10
        value = exact.quantize(quantum, rounding=ROUND_HALF_UP)
    if value == value.to_integral_value():
        return int(value)
    return float(value)


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


_BRIEFING_CAUTION = (
    "This pattern is a lead, not proof of a bad result. The finding data at the "
    "end of this prompt carries the exact session IDs, the chart, and any "
    "evidence locators; use those, not this summary, when choosing sessions."
)


def _format_briefing(
    *,
    odd: str,
    look_first: str,
    cohort: str,
    hypothesis: str,
    held: str,
) -> str:
    """Lay out one finding briefing in the fixed section order."""
    return (
        f"What is odd: {odd}\n"
        f"Where to look first: {look_first}\n"
        f"Candidate cohort: {cohort}\n"
        f"One hypothesis to test: {hypothesis}\n"
        f"The hypothesis held if: {held}\n"
        f"{_BRIEFING_CAUTION}"
    )


def _pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """Return the count with the matching noun form."""
    noun = singular if count == 1 else (plural or f"{singular}s")
    return f"{count} {noun}"


def _format_ratio(numerator: int, denominator: int) -> str:
    """Format a per-session average with at most one decimal place."""
    if not denominator:
        return "0"
    return f"{numerator / denominator:.1f}".removesuffix(".0")


def _build_signal_briefing(
    *,
    family: str,
    subject: str,
    affected: int,
    analyzed: int,
    share: int | float,
    occurrences: int,
    chart_groups: int,
    leading_category_count: int,
    excluded_sessions: int,
) -> str:
    """Build the briefing for a trajectory, tool-health, or language signal."""
    odd = (
        f"{_pluralize(occurrences, 'occurrence')} of {subject} across "
        f"{_pluralize(affected, 'session')}, which is {share}% of the "
        f"{_pluralize(analyzed, 'session')} analyzed for this marker."
    )
    if chart_groups > 1:
        odd += (
            f" The chart splits them into {chart_groups} groups; the leading group "
            f"alone accounts for {leading_category_count} occurrences."
        )
    else:
        odd += (
            " All occurrences fall in one group, so the chart only compares "
            "matching sessions with the other sessions."
        )
    if excluded_sessions:
        odd += (
            f" {_pluralize(excluded_sessions, 'session was', 'sessions were')} left "
            "out because their user text was missing or not fully inspected."
        )
    marker_look_first = (
        "Open the sessions named in evidence_locators; each locator points at "
        "the node where the marker was recorded. Compare them with sessions "
        "from the same import that have no occurrence"
    )
    if family == "language":
        look_first = (
            "Open the sessions named in evidence_locators; when a locator's node_id "
            "is null the matching text is in the session's recorded inputs, "
            "otherwise it is in that node's inputs. Compare them with sessions from "
            "the same import that have no occurrence, and read the agent turn "
            "immediately before each flagged user message."
        )
        cohort = (
            f"sessions with at least one user message matching this marker (the "
            f"{affected} listed), versus sessions with fully inspected user text and "
            "no match."
        )
        hypothesis = (
            "Check whether the flagged messages follow one recognizable agent turn, "
            "such as an unanswered question, a wrong tool result, or a repeated "
            "clarification request. If they do, one agent change aimed at that turn "
            "is the candidate to replay."
        )
        held = (
            "the marker recurs in fewer replayed sessions than the "
            f"{_pluralize(affected, 'session')} it appears in now, and the "
            "failed-session rate for the cohort does not rise."
        )
    elif family == "tool_health":
        look_first = marker_look_first + (
            ", starting with the chart's leading group when it dominates the count."
            if chart_groups > 1
            else "."
        )
        cohort = (
            f"sessions with at least one recorded occurrence of {subject} (the "
            f"{affected} listed)"
        )
        cohort += (
            ", optionally narrowed to the chart's leading group."
            if chart_groups > 1
            else "."
        )
        hypothesis = (
            "Check whether the affected calls fail or come back empty for the same "
            "inputs every time. If they do, the tool contract or the way the agent "
            "builds its arguments is the single factor to change and replay."
        )
        held = (
            f"recorded occurrences per session fall below the current average of "
            f"{_format_ratio(occurrences, affected)}, the affected share drops below "
            f"{share}%, and the failed-session rate does not rise."
        )
    else:
        look_first = (
            marker_look_first
            + ", and check what the agent saw between one call and its repeat."
        )
        cohort = (
            f"sessions with at least one recorded occurrence of {subject} (the "
            f"{affected} listed), versus sessions with tool calls and no repeat."
        )
        hypothesis = (
            "Check whether the repeat follows a tool result the agent could not act "
            "on, such as an error with no guidance or an unchanged answer. If it "
            "does, the tool's error message or the agent's retry instruction is the "
            "single factor to change and replay."
        )
        held = (
            f"repeated calls per affected session fall below the current average of "
            f"{_format_ratio(occurrences, affected)}, tool calls per session drop, "
            "and the failed-session rate does not rise."
        )
    return _format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=hypothesis,
        held=held,
    )


def _build_outcome_briefing(
    *,
    failed: int,
    completed: int,
    analyzed: int,
    failed_percent: int | float,
    statuses: int,
) -> str:
    """Build the briefing for the recorded session outcome finding."""
    odd = (
        f"{_pluralize(failed, 'session')} out of {analyzed} analyzed "
        f"({failed_percent}%) {'carries' if failed == 1 else 'carry'} the recorded "
        "status failed."
    )
    if completed:
        odd += f" {completed} are recorded completed."
    if statuses > 2:
        odd += f" The chart shows {statuses} distinct recorded statuses in total."
    if completed:
        look_first = (
            "Open the failed sessions listed in contributing_session_ids and read "
            "their last recorded node. Compare each with a completed session from "
            "the same import that starts with a similar input."
        )
    else:
        look_first = (
            "Open the failed sessions listed in contributing_session_ids and read "
            "their last recorded node. No completed session exists in this import, "
            "so pick the comparison set from another import or agent version."
        )
    return _format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=f"sessions whose recorded status is failed (the {failed} listed).",
        hypothesis=(
            "Check whether the failed sessions share the same final step, such as "
            "one tool name and one error, right before the status flips to failed. "
            "If they do, that step is the single factor to change and replay."
        ),
        held=(
            f"the failed share of the replayed cohort falls below {failed_percent}% "
            "without a rise in duration or cost per session."
        ),
    )


def _build_flat_distribution_briefing(
    *, family: str, values: Histogram, unit: str, quantity: str
) -> str:
    """Build the briefing for a distribution whose observations share one value."""
    observations = len(values)
    minimum = f"{values.minimum:g}"
    recorded = "a recorded duration" if family == "timing" else f"recorded {unit}"
    return _format_briefing(
        odd=(
            f"All {observations} observations have exactly {minimum} {unit}; there "
            "is no tail to compare against the middle."
        ),
        look_first=(
            "Open a few of the sessions in contributing_session_ids to confirm the "
            "value is real rather than a fixed instrumentation default."
        ),
        cohort=f"every session with {recorded} (the {observations} listed).",
        hypothesis=(
            "Treat this as a baseline: check that one intended agent change moves "
            f"{quantity} away from {minimum} {unit} in the direction you expect."
        ),
        held=(
            f"the replayed cohort's {quantity} differs from {minimum} {unit} in the "
            "expected direction while the failed-session rate does not rise."
        ),
    )


def _build_distribution_briefing(
    *,
    family: str,
    values: Histogram,
    sessions_analyzed: int,
    unit: str,
) -> str:
    """Build the briefing for an activity or timing distribution finding."""
    quantity = "recorded duration" if family == "timing" else f"{unit} per session"
    reading = "recorded duration" if family == "timing" else f"{unit} count"
    observations = len(values)
    minimum = f"{values.minimum:g}"
    maximum = f"{values.maximum:g}"
    if values.minimum == values.maximum:
        return _build_flat_distribution_briefing(
            family=family, values=values, unit=unit, quantity=quantity
        )

    tail_index = values.get_highest_occupied_bin()
    assert tail_index is not None
    tail_count = values.bins[tail_index]
    tail_lower = f"{values.bounds[tail_index - 1]:g}" if tail_index else None
    # Name the bin the way the chart labels it: the last bin is open-ended, a
    # lower occupied bin has an upper bound and empty bins above it.
    if tail_index == len(values.bounds):
        bin_name, bin_adjective = "top bin", "top-bin"
        bin_label = f"{tail_lower} or more {unit}"
    else:
        bin_name, bin_adjective = "highest occupied bin", "highest-bin"
        bin_label = (
            f"at least {tail_lower} and " if tail_lower is not None else ""
        ) + f"less than {values.bounds[tail_index]:g} {unit}"
    rest = observations - tail_count
    odd = (
        f"{observations} sessions range from {minimum} to {maximum} {unit}. "
        f"{_pluralize(tail_count, 'session sits', 'sessions sit')} in the "
        f"{bin_name} ({bin_label})"
        + (
            f"; the other {rest} sit below it."
            if rest
            else "; all observations share this bin despite their different values."
        )
    )
    if observations < sessions_analyzed:
        odd += (
            f" Of the {sessions_analyzed} sessions analyzed, "
            f"{sessions_analyzed - observations} had no {reading}."
        )
    look_first = (
        "Open evidence_locators in their listed order: they point to sessions with "
        f"the highest recorded values in the {bin_name}, ranked by value descending "
        "and session UUID ascending for ties. Check evidence_scope for available "
        "versus retained counts and truncation. Other contributing_session_ids "
        "describe the wider distribution; they are not guaranteed to belong to "
        "this bin or form a comparison group. "
        + (
            "Compare them with sessions from lower occupied bins; read each "
            f"comparison session's {reading} to place it."
            if rest
            else "All observations share one bin; compare their actual recorded values."
        )
    )
    interval = f"at least {tail_lower}" if tail_lower is not None else ""
    if tail_index < len(values.bounds):
        interval += (" and " if interval else "") + (
            f"less than {values.bounds[tail_index]:g}"
        )
    if family == "timing":
        cohort = (
            f"sessions whose recorded duration is {interval} seconds (the {bin_name})."
        )
        hypothesis = (
            "Check whether the long sessions spend their time in a few slow nodes, "
            "such as one tool wait or one model call, rather than in more steps. "
            "If they do, that node is the single factor to change and replay."
        )
        held = (
            f"the {bin_adjective} cohort's recorded duration falls below {tail_lower} "
            "seconds for most sessions while the failed-session rate and cost per "
            "session do not rise."
        )
    else:
        cohort = f"sessions with {interval} {unit} (the {bin_name})."
        hypothesis = (
            f"Check whether the {bin_adjective} sessions reach the same outcome as the "
            f"lower-value sessions or whether the extra {unit} are repeats and "
            "retries. If they are repeats, the step that triggers them is the "
            "single factor to change and replay."
        )
        held = (
            f"the {bin_adjective} cohort's {unit} per session fall below {tail_lower} "
            "for most sessions while the failed-session rate does not rise."
        )
    if not rest:
        hypothesis = (
            f"Check whether sessions with higher {quantity} contain avoidable waits "
            "or repeated steps. If confirmed, change that one factor and replay "
            "the cohort against its recorded baseline."
        )
        held = (
            f"the cohort's {quantity} decreases against its recorded baseline while "
            "the failed-session rate does not rise."
        )
    return _format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=hypothesis,
        held=held,
    )


def _build_model_mix_briefing(
    *,
    models: int,
    model_calls: int,
    leading_model_calls: int,
    affected: int,
    analyzed: int,
) -> str:
    """Build the briefing for the recorded model mix finding."""
    leading_share = _calculate_percent(leading_model_calls, model_calls)
    odd = (
        f"{_pluralize(models, 'distinct recorded model label')} across "
        f"{_pluralize(model_calls, 'model call')} in {affected} of {analyzed} "
        f"sessions. The most frequent label accounts for "
        f"{_pluralize(leading_model_calls, 'call')} ({leading_share}%)."
    )
    if models == 1:
        look_first = (
            "Open a few sessions from contributing_session_ids and read the model "
            "label on their model-call nodes. With a single label there is no "
            "in-import comparison; the comparison is one candidate replacement "
            "model on replay."
        )
        cohort = (
            f"sessions with a recorded model label (all {affected} listed), since "
            "every recorded call uses the same label."
        )
    else:
        look_first = (
            "Open sessions from contributing_session_ids and group them by the model "
            "label on their model-call nodes. Compare sessions with at least one "
            "model-call node recording the chart's leading label against sessions "
            "with none."
        )
        cohort = (
            "sessions with at least one model-call node recording the chart's "
            f"leading label ({leading_model_calls} of {model_calls} calls)."
        )
    return _format_briefing(
        odd=odd,
        look_first=look_first,
        cohort=cohort,
        hypothesis=(
            "Check whether swapping the leading model for one candidate changes "
            "outcome or cost. Run one replay with a single model override and no "
            "other change."
        ),
        held=(
            "cost per session or the failed-session rate falls on the replayed "
            "cohort while the other one does not rise."
        ),
    )


def _build_categorical(
    values: Counter[str] | LabelCounts, *, unit: str
) -> CategoricalInsightData:
    """Build categorical data with deterministic count and label ordering."""
    ordered = (
        values.top()
        if isinstance(values, LabelCounts)
        else sorted(values.items(), key=lambda item: (-item[1], item[0]))
    )
    return CategoricalInsightData(
        unit=unit,
        values=[CategoryValue(label=label, value=value) for label, value in ordered],
    )


def _build_binned(values: Histogram, *, unit: str) -> BinnedInsightData:
    """Count values into contiguous half-open bins."""
    bounds = values.bounds
    bins = [
        Bin(
            lower_bound=None if index == 0 else bounds[index - 1],
            upper_bound=bounds[index] if index < len(bounds) else None,
            count=count,
        )
        for index, count in enumerate(values.bins)
    ]
    return BinnedInsightData(unit=unit, bins=bins)


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
    share = _calculate_percent(len(aggregate.sessions), len(eligible_session_ids))
    values: Counter[str] | LabelCounts = aggregate.categories
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
    candidate_caveat = caveat
    if len(aggregate.categories) > 20:
        candidate_caveat += (
            " The chart shows the 20 most frequent categories and combines the "
            "remaining categories."
        )
    excluded_sessions = len(state.analyzed_session_ids) - len(eligible_session_ids)
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
        affected=len(aggregate.sessions),
        analyzed=len(eligible_session_ids),
        share=share,
        occurrences=aggregate.count,
        chart_groups=len(aggregate.categories.top()),
        leading_category_count=leading[0][1] if leading else 0,
        excluded_sessions=excluded_sessions if family == "language" else 0,
    )
    return CandidateFinding(
        id=candidate_id,
        family=family,
        rank=rank,
        eyebrow=eyebrow,
        title=title_pattern.format(share=share, affected=len(aggregate.sessions)),
        fallback_description=description_pattern.format(
            count=aggregate.count,
            affected=len(aggregate.sessions),
        ),
        caveat=candidate_caveat,
        data=_build_categorical(values, unit=unit),
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
        investigation_prompt=briefing,
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
            "USERS CORRECTING THE AGENT",
            "{share}% of sessions with fully inspected user text include explicit "
            "correction language",
            "A literal correction marker appears {count} times across {affected} "
            "sessions.",
            "user messages containing literal correction phrases",
            "Language markers are literal leads, not judgments of sentiment or intent.",
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
    if analyzed_sessions and failed_ids:
        affected_ids = failed_ids
        contributions = _get_contributions(state, affected_ids)
        contribution_ids = set(contributions)
        completed_sessions = state.statuses.get(SessionStatus.COMPLETED.value, 0)
        failed_percent = _calculate_percent(
            state.statuses.get("failed", 0), analyzed_sessions
        )
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
        candidates.append(
            CandidateFinding(
                id="session-outcomes",
                family="outcome",
                rank=55,
                eyebrow="SESSION OUTCOMES",
                title=(f"{failed_percent}% of sessions are recorded failed"),
                fallback_description=outcome_description,
                caveat=(
                    "Recorded status describes the session boundary, not the cause of "
                    "the outcome."
                ),
                data=_build_categorical(state.statuses, unit="sessions"),
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
                investigation_prompt=_build_outcome_briefing(
                    failed=len(affected_ids),
                    completed=completed_sessions,
                    analyzed=analyzed_sessions,
                    failed_percent=failed_percent,
                    statuses=len(state.statuses),
                ),
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
            highest_values=state.highest_tool_counts,
            session_ids=state.node_session_ids,
            sessions_analyzed=len(state.node_session_ids),
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
        ),
    )
    for spec in distribution_specs:
        if len(spec.values) < 2:
            continue
        uniform = spec.values.minimum == spec.values.maximum
        highest_bin = spec.values.get_highest_occupied_bin()
        assert highest_bin is not None
        evidence = (
            []
            if uniform
            else [
                EvidenceLocator(
                    session_id=session_id, signal=DISTRIBUTION_TOP_BIN_SIGNAL
                )
                for session_id, value in spec.highest_values.get_entries()
                if spec.values.get_bin_index(value) == highest_bin
            ]
        )
        contributing = _get_contributions(
            state, spec.session_ids, reserved_ids=[item.session_id for item in evidence]
        )
        if not contributing:
            continue
        title = (
            f"All recorded observations have {spec.values.minimum:g} {spec.unit}"
            if uniform
            else spec.title
        )
        description = (
            "This uniform baseline can anchor comparisons after an agent change."
            if uniform
            else spec.description
        )
        candidates.append(
            CandidateFinding(
                id=spec.candidate_id,
                family=spec.family,
                rank=spec.rank,
                eyebrow=spec.eyebrow,
                title=title,
                fallback_description=description,
                caveat=(
                    "Recorded timestamps may omit uninstrumented work."
                    if spec.family == "timing"
                    else "Activity volume is not evidence of outcome quality."
                ),
                data=_build_binned(spec.values, unit=spec.unit),
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

    if state.models:
        model_sessions = state.model_sessions
        contributions = _get_contributions(state, model_sessions)
        candidates.append(
            CandidateFinding(
                id="model-mix",
                family="model",
                rank=160,
                eyebrow="MODEL MIX",
                title=(
                    f"{len(state.models)} recorded model"
                    f"{' appears' if len(state.models) == 1 else 's appear'} in "
                    "these sessions"
                ),
                fallback_description=(
                    "This exact model mix can seed a cohort for cost, latency, or "
                    "quality comparisons."
                ),
                caveat=(
                    "Requested and served model fields may be absent from some "
                    "recorded calls."
                    + (
                        " The chart shows the 20 most frequent models and combines "
                        "the remaining models."
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
            "counts only nodes in the supplied sessions, not the entire source import."
        )
    if state.timing_available < sessions_analyzed:
        caveats.append(
            "Timing coverage is incomplete because valid session bounds are missing."
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
