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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Replay diff and experiment run summary computation."""

import uuid
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from statistics import fmean, median
from typing import Any

from kitaru.server.base import FrozenModel
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayOverride
from kitaru.server.domain.session import Session, TokenUsage
from kitaru.server.domain.session_node import NodeType, SessionNode


class DiffValue(FrozenModel):
    """Original and effective value pair."""

    original: Any = None
    effective: Any = None


class ReplayInputDiff(FrozenModel):
    """Replay input diff."""

    inputs: DiffValue
    model: DiffValue
    system_prompt: DiffValue


class TokenDeltas(FrozenModel):
    """Token count deltas."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None


class NodePairDiff(FrozenModel):
    """Node pair diff."""

    key: str
    node_type: NodeType
    original_node_id: uuid.UUID
    result_node_id: uuid.UUID
    cost_delta: float | None
    token_deltas: TokenDeltas
    duration_delta: float | None
    outputs_equal: bool
    mocked: bool
    cache_key_changed: bool | None


class DiffNode(FrozenModel):
    """Unmatched diff node."""

    id: uuid.UUID
    key: str
    node_type: NodeType
    name: str


class ScoreDelta(FrozenModel):
    """Score delta."""

    original: float | None = None
    replay: float | None = None
    delta: float | None = None


class ReplayDiff(FrozenModel):
    """Replay diff."""

    replay_id: uuid.UUID
    original_session_id: uuid.UUID
    result_session_id: uuid.UUID
    input_diff: ReplayInputDiff
    node_pairs: list[NodePairDiff]
    added_nodes: list[DiffNode]
    removed_nodes: list[DiffNode]
    score_deltas: dict[str, ScoreDelta]


def _is_mocked(node: SessionNode) -> bool:
    """Report whether a node records a mocked tool call.

    Args:
        node: Node to inspect.

    Returns:
        ``True`` when the node's attributes mark it mocked.
    """
    return bool(node.attributes.get("mocked"))


def _delta(
    original: int | float | Decimal | None, result: int | float | Decimal | None
) -> float | None:
    """Compute the delta between two optional numbers.

    Args:
        original: Original value.
        result: Result value.

    Returns:
        Result minus original with ``None`` as zero, ``None`` when both are
        ``None``.
    """
    if original is None and result is None:
        return None
    return float(result or 0) - float(original or 0)


def _int_delta(original: int | None, result: int | None) -> int | None:
    """Compute the integer delta between two optional counts.

    Args:
        original: Original count.
        result: Result count.

    Returns:
        Result minus original with ``None`` as zero, ``None`` when both are
        ``None``.
    """
    if original is None and result is None:
        return None
    return (result or 0) - (original or 0)


def _token_deltas(
    original: TokenUsage | None, result: TokenUsage | None
) -> TokenDeltas:
    """Compute token count deltas between two optional usages.

    Args:
        original: Original token usage.
        result: Result token usage.

    Returns:
        Per-kind token deltas.
    """
    original = original or TokenUsage()
    result = result or TokenUsage()
    return TokenDeltas(
        input_tokens=_int_delta(original.input_tokens, result.input_tokens),
        output_tokens=_int_delta(original.output_tokens, result.output_tokens),
        cached_input_tokens=_int_delta(
            original.cached_input_tokens, result.cached_input_tokens
        ),
        reasoning_tokens=_int_delta(original.reasoning_tokens, result.reasoning_tokens),
    )


def _duration_seconds(
    started_at: datetime | None, ended_at: datetime | None
) -> float | None:
    """Compute a duration in seconds from optional timestamps.

    Args:
        started_at: Start time.
        ended_at: End time.

    Returns:
        Duration, ``None`` when either timestamp is missing.
    """
    if started_at is None or ended_at is None:
        return None
    return (ended_at - started_at).total_seconds()


def _distinct_models(nodes: Sequence[SessionNode]) -> list[str]:
    """Collect the distinct models of a session's LLM call nodes.

    Args:
        nodes: Nodes of the session.

    Returns:
        Models in first-occurrence order.
    """
    models: list[str] = []
    for node in nodes:
        if (
            node.node_type is NodeType.LLM_CALL
            and node.model is not None
            and node.model not in models
        ):
            models.append(node.model)
    return models


def _effective_models(models: list[str], override: ReplayOverride | None) -> list[str]:
    """Apply a model override to a list of original models.

    Args:
        models: Original models.
        override: Execution override.

    Returns:
        Models after the override, element-wise.
    """
    if override is None or override.model is None:
        return list(models)
    if isinstance(override.model, str):
        replacement = override.model
        return [replacement for _ in models]
    return [override.model.get(model, model) for model in models]


def _align(
    original_nodes: Sequence[SessionNode], result_nodes: Sequence[SessionNode]
) -> tuple[list[tuple[SessionNode, SessionNode]], list[SessionNode], list[SessionNode]]:
    """Align nodes of two sessions by key.

    Args:
        original_nodes: Nodes of the original session in sequence order.
        result_nodes: Nodes of the result session in sequence order.

    Returns:
        Matched pairs in original order, added result nodes, and removed
        original nodes.
    """
    result_by_key = {node.key: node for node in result_nodes}
    pairs: list[tuple[SessionNode, SessionNode]] = []
    removed: list[SessionNode] = []
    for node in original_nodes:
        match = result_by_key.pop(node.key, None)
        if match is None:
            removed.append(node)
        else:
            pairs.append((node, match))
    added = [node for node in result_nodes if node.key in result_by_key]
    return pairs, added, removed


def _pair_diff(original: SessionNode, result: SessionNode) -> NodePairDiff:
    """Compute the diff of one aligned node pair.

    Args:
        original: Node of the original session.
        result: Node of the result session.

    Returns:
        Node pair diff.
    """
    cache_key_changed = None
    if NodeType.TOOL_CALL in (original.node_type, result.node_type):
        cache_key_changed = original.cache_key != result.cache_key
    return NodePairDiff(
        key=original.key,
        node_type=original.node_type,
        original_node_id=original.id,
        result_node_id=result.id,
        cost_delta=_delta(original.cost, result.cost),
        token_deltas=_token_deltas(original.tokens, result.tokens),
        duration_delta=_delta(
            _duration_seconds(original.started_at, original.ended_at),
            _duration_seconds(result.started_at, result.ended_at),
        ),
        outputs_equal=original.outputs == result.outputs,
        mocked=_is_mocked(result),
        cache_key_changed=cache_key_changed,
    )


def _diff_node(node: SessionNode) -> DiffNode:
    """Build the unmatched-node view of a node.

    Args:
        node: Unmatched node.

    Returns:
        Unmatched diff node.
    """
    return DiffNode(id=node.id, key=node.key, node_type=node.node_type, name=node.name)


def _score_deltas(
    original_scores: dict[str, float], replay_scores: dict[str, float]
) -> dict[str, ScoreDelta]:
    """Compare original session scores against replay scores.

    Args:
        original_scores: Scores of the original session.
        replay_scores: Scores of the replay.

    Returns:
        Score deltas keyed by scorer name.
    """
    deltas: dict[str, ScoreDelta] = {}
    for name in sorted(set(original_scores) | set(replay_scores)):
        original = original_scores.get(name)
        replay = replay_scores.get(name)
        delta = None
        if original is not None and replay is not None:
            delta = replay - original
        deltas[name] = ScoreDelta(original=original, replay=replay, delta=delta)
    return deltas


def compute_replay_diff(
    replay: Replay,
    override: ReplayOverride | None,
    original_session: Session,
    result_session: Session,
    original_nodes: Sequence[SessionNode],
    result_nodes: Sequence[SessionNode],
) -> ReplayDiff:
    """Compute the full diff between a replay's original and result session.

    Args:
        replay: Replay linking the two sessions.
        override: Execution override of the replay's config.
        original_session: Original session.
        result_session: Result session.
        original_nodes: Nodes of the original session in sequence order.
        result_nodes: Nodes of the result session in sequence order.

    Returns:
        Full replay diff.
    """
    pairs, added, removed = _align(original_nodes, result_nodes)
    original_models = _distinct_models(original_nodes)
    effective_inputs = original_session.inputs
    system_prompt = None
    if override is not None:
        if override.prompt is not None:
            effective_inputs = override.prompt
        system_prompt = override.system_prompt
    input_diff = ReplayInputDiff(
        inputs=DiffValue(original=original_session.inputs, effective=effective_inputs),
        model=DiffValue(
            original=original_models,
            effective=_effective_models(original_models, override),
        ),
        # The original system prompt is not recorded, only the override is
        # known.
        system_prompt=DiffValue(original=None, effective=system_prompt),
    )
    return ReplayDiff(
        replay_id=replay.id,
        original_session_id=original_session.id,
        result_session_id=result_session.id,
        input_diff=input_diff,
        node_pairs=[_pair_diff(original, result) for original, result in pairs],
        added_nodes=[_diff_node(node) for node in added],
        removed_nodes=[_diff_node(node) for node in removed],
        score_deltas=_score_deltas(original_session.scores, replay.scores or {}),
    )


def compute_diff_summary(
    replay_scores: dict[str, float],
    original_session: Session,
    result_session: Session,
    original_nodes: Sequence[SessionNode],
    result_nodes: Sequence[SessionNode],
) -> dict[str, Any]:
    """Compute the scalar diff summary stored on a completed replay.

    Args:
        replay_scores: Scores reported by the runner.
        original_session: Original session.
        result_session: Result session.
        original_nodes: Nodes of the original session in sequence order.
        result_nodes: Nodes of the result session in sequence order.

    Returns:
        Diff summary.
    """
    pairs, added, removed = _align(original_nodes, result_nodes)
    score_deltas: dict[str, float | None] = {}
    for name, score in replay_scores.items():
        original = original_session.scores.get(name)
        score_deltas[name] = None if original is None else score - original
    return {
        "cost_delta": _delta(original_session.cost, result_session.cost),
        "token_deltas": _token_deltas(
            original_session.tokens, result_session.tokens
        ).model_dump(),
        "duration_delta": _delta(
            _duration_seconds(original_session.started_at, original_session.ended_at),
            _duration_seconds(result_session.started_at, result_session.ended_at),
        ),
        "status_changed": original_session.status is not result_session.status,
        "tool_calls": {
            "matched": sum(
                1 for original, _ in pairs if original.node_type is NodeType.TOOL_CALL
            ),
            "mocked": sum(
                1
                for node in result_nodes
                if node.node_type is NodeType.TOOL_CALL and _is_mocked(node)
            ),
            "added": sum(1 for node in added if node.node_type is NodeType.TOOL_CALL),
            "removed": sum(
                1 for node in removed if node.node_type is NodeType.TOOL_CALL
            ),
        },
        "score_deltas": score_deltas,
    }


def compute_run_summary(
    replays: Sequence[Replay], sessions: dict[uuid.UUID, Session]
) -> dict[str, Any]:
    """Compute the aggregate summary stored on a finalized experiment run.

    Args:
        replays: All replays of the run.
        sessions: Original and result sessions keyed by id.

    Returns:
        Run summary.
    """
    counts: dict[str, int] = {}
    for replay in replays:
        counts[replay.status.value] = counts.get(replay.status.value, 0) + 1
    scored = [replay for replay in replays if replay.passed is not None]
    pass_rate = None
    if scored:
        pass_rate = sum(1 for replay in scored if replay.passed) / len(scored)
    originals = [
        session
        for replay in replays
        if (session := sessions.get(replay.original_session_id)) is not None
    ]
    results = [
        session
        for replay in replays
        if replay.result_session_id is not None
        and (session := sessions.get(replay.result_session_id)) is not None
    ]
    scorer_names: set[str] = set()
    for replay in replays:
        scorer_names.update(replay.scores or {})
    scores: dict[str, Any] = {}
    for name in sorted(scorer_names):
        baseline_values = [
            session.scores[name] for session in originals if name in session.scores
        ]
        replay_values = [
            replay.scores[name]
            for replay in replays
            if replay.scores is not None and name in replay.scores
        ]
        scores[name] = {
            "baseline": _score_stats(baseline_values),
            "replay": _score_stats(replay_values),
        }
    return {
        "replay_counts_by_status": counts,
        "pass_rate": pass_rate,
        "scores": scores,
        "total_cost": {
            "baseline": _total_cost(originals),
            "replay": _total_cost(results),
        },
    }


def _score_stats(values: list[float]) -> dict[str, float | None]:
    """Compute mean and median score statistics.

    Args:
        values: Score values.

    Returns:
        Mean and median, ``None`` when no values exist.
    """
    if not values:
        return {"mean": None, "median": None}
    return {"mean": fmean(values), "median": float(median(values))}


def _total_cost(sessions: Sequence[Session]) -> float | None:
    """Sum the costs of a set of sessions.

    Args:
        sessions: Sessions to sum over.

    Returns:
        Total cost, ``None`` when no session has a cost.
    """
    values = [float(session.cost) for session in sessions if session.cost is not None]
    if not values:
        return None
    return sum(values)
