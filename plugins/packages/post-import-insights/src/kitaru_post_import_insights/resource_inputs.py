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
"""Session readings of tool calls, model-call token counts, and finding bounds."""

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Self

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.api_models.v1.session_node import (
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_post_import_insights.findings import ContributionSelector
from kitaru_post_import_insights.labels import UNAVAILABLE_TOOL, sanitize_label
from kitaru_post_import_insights.models import EvidenceLocator
from kitaru_post_import_insights.profiling_state import Observation, SessionReferences

# Ten ranked node locators are enough to start an investigation, and they keep
# a copied prompt with 200 contributing session IDs inside its 16,000
# character bound.
MAX_RESOURCE_EVIDENCE = 10


@dataclass(frozen=True)
class FindingLimits:
    """Evidence, contribution, and system prompt bounds for one finding family."""

    evidence: int
    contributors: int
    max_prompt_characters: int

    @classmethod
    def from_bounds(
        cls, *, max_evidence: int, max_contributors: int, max_text_bytes: int
    ) -> Self:
        """Derive limits so that every evidence session can also be a contributor."""
        return cls(
            evidence=min(max_evidence, max_contributors, MAX_RESOURCE_EVIDENCE),
            contributors=max_contributors,
            max_prompt_characters=max_text_bytes,
        )


def _get_token_count(value: int | None) -> int | None:
    """Keep a recorded token count only when it is a nonnegative integer."""
    return value if value is not None and value >= 0 else None


@dataclass(frozen=True)
class ModelCallTokens:
    """Token counts of one model call, read under its source's cache convention.

    Sources disagree on whether ``input_tokens`` includes cache reads.
    OpenAI-style usage counts cache reads inside input tokens, while
    Anthropic-style usage, including Kitaru's Claude Agent SDK adapter, reports
    them separately. Kitaru stores both as recorded. ``cache_reads_separate`` is
    true only when the convention is established for this call's model in its
    session; otherwise it is unknown and the inclusive reading is used.
    """

    node: SessionNodeResponse
    model_key: str
    input_tokens: int | None
    cached_input_tokens: int | None
    cache_reads_separate: bool

    @property
    def prompt_tokens(self) -> int | None:
        """Return the call's total input size, including cache reads."""
        if self.input_tokens is None:
            return None
        if self.cache_reads_separate:
            return self.input_tokens + (self.cached_input_tokens or 0)
        return self.input_tokens


def read_model_calls(nodes: Iterable[SessionNodeResponse]) -> list[ModelCallTokens]:
    """Read each model call's token counts in node order.

    A call reporting more cache reads than input tokens can only come from a
    source that counts cache reads separately. One source records every call to
    a model the same way, so that evidence applies to every call to the same
    model in the session.
    """
    calls = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
    readings: list[tuple[SessionNodeResponse, str, int | None, int | None]] = []
    separate: set[str] = set()
    for node in calls:
        model_key = node.model or node.requested_model or ""
        tokens = node.tokens
        input_tokens = _get_token_count(tokens.input_tokens) if tokens else None
        cached = _get_token_count(tokens.cached_input_tokens) if tokens else None
        if input_tokens is not None and cached is not None and cached > input_tokens:
            separate.add(model_key)
        readings.append((node, model_key, input_tokens, cached))
    return [
        ModelCallTokens(
            node=node,
            model_key=model_key,
            input_tokens=input_tokens,
            cached_input_tokens=cached,
            cache_reads_separate=model_key in separate,
        )
        for node, model_key, input_tokens, cached in readings
    ]


@dataclass(frozen=True)
class SessionView:
    """One session's nodes split into tool calls and model calls."""

    session: SessionDetailResponse
    nodes: Sequence[SessionNodeResponse]
    tool_calls: list[SessionNodeResponse]
    model_calls: list[ModelCallTokens]

    @classmethod
    def from_session(
        cls, session: SessionWithNodesResponse, nodes: Sequence[SessionNodeResponse]
    ) -> Self:
        """Split nodes, already in position order, into tool and model calls."""
        return cls(
            session=session.session,
            nodes=nodes,
            tool_calls=[node for node in nodes if node.node_type is NodeType.TOOL_CALL],
            model_calls=read_model_calls(nodes),
        )


def get_tool_label(node: SessionNodeResponse) -> str:
    """Return a node's safe tool name, or the profiler's placeholder."""
    return sanitize_label(node.tool_name) or UNAVAILABLE_TOOL


def get_model_label(call: ModelCallTokens) -> str | None:
    """Return the call's safe served model, falling back to the requested one."""
    return sanitize_label(call.model_key)


def get_locators(rows: Iterable[Observation], signal: str) -> list[EvidenceLocator]:
    """Point evidence at the session and, when recorded, the node of each row."""
    return [
        EvidenceLocator(
            session_id=uuid.UUID(row.session),
            node_id=uuid.UUID(row.node) if row.node else None,
            signal=signal,
        )
        for row in rows
    ]


def build_references(
    session_ids: Iterable[uuid.UUID], *, available: int, limit: int
) -> SessionReferences:
    """Wrap a selection of at most ``limit`` sessions out of ``available`` in all."""
    retained = sorted(set(session_ids))[:limit]
    return SessionReferences(limit=limit, count=available, retained=retained)


def select_evidence(
    select: ContributionSelector,
    session_ids: SessionReferences,
    evidence: Sequence[EvidenceLocator],
) -> tuple[list[uuid.UUID], list[EvidenceLocator]]:
    """Keep every evidence session among the contributors, then drop the rest."""
    contributions = select(
        session_ids,
        reserved_ids=list(dict.fromkeys(item.session_id for item in evidence)),
    )
    retained = set(contributions)
    return contributions, [item for item in evidence if item.session_id in retained]


def format_count(value: int) -> str:
    """Format a count with thousands separators."""
    return f"{value:,}"
