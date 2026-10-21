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
"""Collect and sample the recorded model calls of an import."""

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from kitaru.api_models.v1.session import TokenUsage
from kitaru.api_models.v1.session_node import (
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.normalize import ReplayRequest, Skipped, build_replay_request
from kitaru_model_swap.params import ModelSwapParams

# Rough characters per token, used only when the recording has no token counts.
_CHARS_PER_TOKEN = 4
_DEFAULT_OUTPUT_TOKENS = 500
_RESERVE_OUTPUT_FACTOR = 4
_MIN_RESERVED_OUTPUT = 2048


@dataclass(frozen=True)
class RecordedCall:
    """A replayable recorded call and what it cost when it was recorded."""

    session_id: UUID
    node_id: UUID
    node_name: str
    model: str | None
    request: ReplayRequest
    usage: Usage
    cost: Decimal | None
    estimated_input_tokens: int


@dataclass
class Collection:
    """Replayable calls plus coverage and volume counts for an import.

    Sampling visits groups of node name and model round-robin, so it never
    takes more than `max_per_group` calls from one group, nor any call from
    a group first seen after `max_per_group` groups. Such calls are counted
    but not kept. `group_seen` counts every replayable call per group, so
    per-call costs can be weighted by how often each kind of call occurs.
    """

    max_per_group: int
    calls: list[RecordedCall] = field(default_factory=list)
    llm_calls_seen: int = 0
    sessions: int = 0
    sessions_not_fetched: int = 0
    replayable: Counter[str] = field(default_factory=Counter)
    skipped: Counter[str] = field(default_factory=Counter)
    first_started: datetime | None = None
    last_started: datetime | None = None
    group_seen: Counter[tuple[str, str | None]] = field(default_factory=Counter)
    _group_sizes: Counter[tuple[str, str | None]] = field(default_factory=Counter)

    def add_session(
        self, session: SessionWithNodesResponse, params: ModelSwapParams
    ) -> None:
        """Rebuild every model call of one session, counting what is skipped."""
        self.sessions += 1
        started = session.session.started_at
        if started is not None:
            if self.first_started is None or started < self.first_started:
                self.first_started = started
            if self.last_started is None or started > self.last_started:
                self.last_started = started
        for node in session.nodes:
            if node.node_type is not NodeType.LLM_CALL:
                continue
            self.llm_calls_seen += 1
            result = build_replay_request(
                node,
                tool_schemas=params.tool_schemas,
                tool_descriptions=params.tool_descriptions,
                use_recorded_tools=params.use_recorded_tools,
                infer_missing_schemas=params.infer_missing_schemas,
            )
            if isinstance(result, Skipped):
                self.skipped[result.reason] += 1
                continue
            self.replayable[result.schema_source] += 1
            group = (node.name, node.model or node.requested_model)
            self.group_seen[group] += 1
            kept = group in self._group_sizes
            if not kept and len(self._group_sizes) >= self.max_per_group:
                continue
            if self._group_sizes[group] < self.max_per_group:
                self._group_sizes[group] += 1
                self.calls.append(_recorded_call(session.session.id, node, result))


def _recorded_call(
    session_id: UUID, node: SessionNodeResponse, request: ReplayRequest
) -> RecordedCall:
    """Pair a rebuilt request with the recorded usage and cost."""
    tokens = node.tokens or TokenUsage()
    return RecordedCall(
        session_id=session_id,
        node_id=node.id,
        node_name=node.name,
        model=node.model or node.requested_model,
        request=request,
        usage=Usage(
            input_tokens=tokens.input_tokens or 0,
            cached_input_tokens=tokens.cached_input_tokens or 0,
            output_tokens=tokens.output_tokens or 0,
            reasoning_tokens=tokens.reasoning_tokens or 0,
        ),
        cost=node.cost,
        estimated_input_tokens=tokens.input_tokens
        or len(json.dumps(request.messages) + json.dumps(request.tools))
        // _CHARS_PER_TOKEN,
    )


def estimate_usage(
    call: RecordedCall, max_tokens: int | None, system_prompt: str | None = None
) -> Usage:
    """Estimate a replay's tokens from the recording."""
    output_tokens = call.usage.output_tokens or _DEFAULT_OUTPUT_TOKENS
    if max_tokens is not None:
        output_tokens = min(output_tokens, max_tokens)
    return Usage(
        input_tokens=_input_tokens(call, system_prompt), output_tokens=output_tokens
    )


def _input_tokens(call: RecordedCall, system_prompt: str | None) -> int:
    # A replacement prompt is counted on top of the recorded input rather than
    # instead of the recorded prompt, which keeps the estimate on the high side.
    extra = len(system_prompt) // _CHARS_PER_TOKEN if system_prompt else 0
    return call.estimated_input_tokens + extra


def reservation_usage(
    call: RecordedCall,
    max_tokens: int | None,
    *,
    strict: bool = False,
    system_prompt: str | None = None,
) -> Usage:
    """Estimate a replay's tokens on the high side, for the spend cap.

    With `strict` and an output limit, the reservation is the full limit, so
    no reply can cost more than reserved. Otherwise a candidate can still
    produce far more output than the recorded model, for example when it
    reasons and the recorded model did not, so the reservation assumes
    several times the recorded output, never more than the limit.
    """
    if strict and max_tokens is not None:
        output_tokens = max_tokens
    else:
        recorded = call.usage.output_tokens or _DEFAULT_OUTPUT_TOKENS
        output_tokens = max(recorded * _RESERVE_OUTPUT_FACTOR, _MIN_RESERVED_OUTPUT)
        if max_tokens is not None:
            output_tokens = min(output_tokens, max_tokens)
    return Usage(
        input_tokens=_input_tokens(call, system_prompt), output_tokens=output_tokens
    )


def sample_calls(calls: list[RecordedCall], max_calls: int) -> list[RecordedCall]:
    """Pick up to `max_calls`, spread evenly across node names and models.

    Groups are visited round-robin in recorded order, so a small sample still
    covers every kind of call and the selection is repeatable.
    """
    groups: dict[tuple[str, str | None], list[RecordedCall]] = defaultdict(list)
    for call in calls:
        groups[(call.node_name, call.model)].append(call)
    queues = list(groups.values())
    sampled: list[RecordedCall] = []
    index = 0
    while len(sampled) < max_calls and any(index < len(q) for q in queues):
        for queue in queues:
            if index < len(queue) and len(sampled) < max_calls:
                sampled.append(queue[index])
        index += 1
    return sampled
