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
"""Tests for replay diff and run summary computation."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from kitaru.server.domain.job import JobStatus, Replay
from kitaru.server.domain.replay_config import ReplayOverride
from kitaru.server.domain.replay_diff import (
    compute_diff_summary,
    compute_replay_diff,
    compute_run_summary,
)
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.server.domain.session_node import (
    NodeStatus,
    NodeType,
    SessionNode,
)

OWNER_ID = uuid.uuid4()
AGENT_ID = uuid.uuid4()

STARTED_AT = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 7, 1, 12, 5, tzinfo=UTC)
LATER_ENDED_AT = datetime(2026, 7, 1, 12, 7, tzinfo=UTC)


def session(**overrides: object) -> Session:
    """Build a completed session entity.

    Args:
        **overrides: Field overrides.

    Returns:
        Session entity.
    """
    values: dict[str, object] = {
        "owner_id": OWNER_ID,
        "agent_id": AGENT_ID,
        "origin": SessionOrigin.RECORDED,
        "status": SessionStatus.COMPLETED,
        "started_at": STARTED_AT,
        "ended_at": ENDED_AT,
        **overrides,
    }
    return Session.model_validate(values)


def node(session_id: uuid.UUID, key: str, **overrides: object) -> SessionNode:
    """Build a completed session node entity.

    Args:
        session_id: Id of the session.
        key: Node key.
        **overrides: Field overrides.

    Returns:
        Session node entity.
    """
    values: dict[str, object] = {
        "session_id": session_id,
        "key": key,
        "sequence": 0,
        "node_type": NodeType.SPAN,
        "name": "run",
        "status": NodeStatus.COMPLETED,
        **overrides,
    }
    return SessionNode.model_validate(values)


def replay(**overrides: object) -> Replay:
    """Build a replay entity with a linked result session.

    Args:
        **overrides: Field overrides.

    Returns:
        Replay entity.
    """
    values: dict[str, object] = {
        "replay_config_id": uuid.uuid4(),
        "agent_version_id": uuid.uuid4(),
        "original_session_id": uuid.uuid4(),
        "result_session_id": uuid.uuid4(),
        **overrides,
    }
    return Replay.model_validate(values)


def test_diff_aligns_nodes_by_key_with_occurrences() -> None:
    """Align nodes by key, including occurrence-indexed keys."""
    original = session(id=uuid.uuid4(), scores={"conciseness": 0.4})
    result = session(id=uuid.uuid4())
    original_nodes = [
        node(original.id, "span:run", sequence=0),
        node(
            original.id,
            "span:run/tool_call:search",
            sequence=1,
            node_type=NodeType.TOOL_CALL,
            name="search",
            tool_name="search",
            cache_key="a" * 64,
            outputs={"hits": 1},
        ),
        node(
            original.id,
            "span:run/tool_call:search#2",
            sequence=2,
            node_type=NodeType.TOOL_CALL,
            name="search",
            tool_name="search",
            cache_key="b" * 64,
        ),
    ]
    result_nodes = [
        node(result.id, "span:run", sequence=0),
        node(
            result.id,
            "span:run/tool_call:search",
            sequence=1,
            node_type=NodeType.TOOL_CALL,
            name="search",
            tool_name="search",
            cache_key="c" * 64,
            outputs={"hits": 1},
            attributes={"mocked": True, "policy": "history"},
        ),
        node(
            result.id,
            "span:run/llm_call:chat",
            sequence=2,
            node_type=NodeType.LLM_CALL,
            name="chat",
        ),
    ]
    subject = replay(
        original_session_id=original.id,
        result_session_id=result.id,
        scores={"conciseness": 0.7},
    )
    diff = compute_replay_diff(
        subject, None, original, result, original_nodes, result_nodes
    )
    assert [pair.key for pair in diff.node_pairs] == [
        "span:run",
        "span:run/tool_call:search",
    ]
    tool_pair = diff.node_pairs[1]
    assert tool_pair.node_type is NodeType.TOOL_CALL
    assert tool_pair.cache_key_changed is True
    assert tool_pair.outputs_equal is True
    assert tool_pair.mocked is True
    assert diff.node_pairs[0].cache_key_changed is None
    assert [added.key for added in diff.added_nodes] == ["span:run/llm_call:chat"]
    assert [removed.key for removed in diff.removed_nodes] == [
        "span:run/tool_call:search#2"
    ]
    assert diff.score_deltas["conciseness"].original == 0.4
    assert diff.score_deltas["conciseness"].replay == 0.7
    assert diff.score_deltas["conciseness"].delta is not None


def test_diff_input_diff_applies_override() -> None:
    """Report original inputs and models against the effective override."""
    original = session(id=uuid.uuid4(), inputs={"prompt": "hi"})
    result = session(id=uuid.uuid4())
    original_nodes = [
        node(
            original.id,
            "llm_call:chat",
            sequence=0,
            node_type=NodeType.LLM_CALL,
            name="chat",
            model="gpt-4o",
        ),
        node(
            original.id,
            "llm_call:classify",
            sequence=1,
            node_type=NodeType.LLM_CALL,
            name="classify",
            model="gpt-4o-mini",
        ),
    ]
    override = ReplayOverride(
        model={"gpt-4o": "claude-sonnet-5"}, system_prompt="Be brief."
    )
    diff = compute_replay_diff(
        replay(original_session_id=original.id, result_session_id=result.id),
        override,
        original,
        result,
        original_nodes,
        [],
    )
    assert diff.input_diff.inputs.original == {"prompt": "hi"}
    assert diff.input_diff.inputs.effective == {"prompt": "hi"}
    assert diff.input_diff.model.original == ["gpt-4o", "gpt-4o-mini"]
    assert diff.input_diff.model.effective == ["claude-sonnet-5", "gpt-4o-mini"]
    assert diff.input_diff.system_prompt.original is None
    assert diff.input_diff.system_prompt.effective == "Be brief."

    string_override = ReplayOverride(model="claude-sonnet-5", prompt="new task")
    diff = compute_replay_diff(
        replay(original_session_id=original.id, result_session_id=result.id),
        string_override,
        original,
        result,
        original_nodes,
        [],
    )
    assert diff.input_diff.inputs.effective == "new task"
    assert diff.input_diff.model.effective == ["claude-sonnet-5", "claude-sonnet-5"]


def test_diff_summary_scalars() -> None:
    """Compute the scalar summary stored on a completed replay."""
    original = session(
        id=uuid.uuid4(),
        cost=Decimal("0.30"),
        tokens=TokenUsage(input_tokens=100, output_tokens=50),
        scores={"conciseness": 0.4},
    )
    result = session(
        id=uuid.uuid4(),
        status=SessionStatus.FAILED,
        ended_at=LATER_ENDED_AT,
        cost=Decimal("0.10"),
        tokens=TokenUsage(input_tokens=80, output_tokens=70),
    )
    original_nodes = [
        node(
            original.id,
            "tool_call:search",
            sequence=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
        ),
        node(
            original.id,
            "tool_call:fetch",
            sequence=1,
            node_type=NodeType.TOOL_CALL,
            name="fetch",
        ),
    ]
    result_nodes = [
        node(
            result.id,
            "tool_call:search",
            sequence=0,
            node_type=NodeType.TOOL_CALL,
            name="search",
            attributes={"mocked": True},
        ),
        node(
            result.id,
            "tool_call:translate",
            sequence=1,
            node_type=NodeType.TOOL_CALL,
            name="translate",
        ),
    ]
    summary = compute_diff_summary(
        {"conciseness": 0.7, "accuracy": 0.9},
        original,
        result,
        original_nodes,
        result_nodes,
    )
    assert summary["cost"]["original"] == pytest.approx(0.3)
    assert summary["cost"]["replay"] == pytest.approx(0.1)
    assert summary["cost"]["delta"] == pytest.approx(-0.2)
    assert summary["tokens"]["original"] == {
        "input_tokens": 100,
        "output_tokens": 50,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert summary["tokens"]["replay"] == {
        "input_tokens": 80,
        "output_tokens": 70,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert summary["tokens"]["deltas"] == {
        "input_tokens": -20,
        "output_tokens": 20,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert summary["duration_delta"] == 120.0
    assert summary["status_changed"] is True
    assert summary["tool_calls"] == {
        "matched": 1,
        "mocked": 1,
        "added": 1,
        "removed": 1,
    }
    assert summary["score_deltas"]["conciseness"] == pytest.approx(0.3)
    assert summary["score_deltas"]["accuracy"] is None


def test_run_summary_aggregates() -> None:
    """Aggregate replay counts, pass rate, scores, and cost."""
    originals = [
        session(
            id=uuid.uuid4(),
            cost=Decimal("0.30"),
            tokens=TokenUsage(input_tokens=100, output_tokens=50),
            scores={"conciseness": 0.4},
        ),
        session(
            id=uuid.uuid4(),
            cost=Decimal("0.10"),
            tokens=TokenUsage(input_tokens=40),
            scores={"conciseness": 0.6},
        ),
    ]
    results = [
        session(
            id=uuid.uuid4(),
            cost=Decimal("0.20"),
            tokens=TokenUsage(input_tokens=80, output_tokens=70),
        ),
        session(id=uuid.uuid4(), cost=Decimal("0.05")),
    ]
    run_id = uuid.uuid4()
    replays = [
        replay(
            experiment_run_id=run_id,
            original_session_id=originals[0].id,
            result_session_id=results[0].id,
            status=JobStatus.COMPLETED,
            passed=True,
            score=0.8,
            scores={"conciseness": 0.8},
        ),
        replay(
            experiment_run_id=run_id,
            original_session_id=originals[1].id,
            result_session_id=results[1].id,
            status=JobStatus.COMPLETED,
            passed=False,
            score=0.2,
            scores={"conciseness": 0.2},
        ),
        replay(
            experiment_run_id=run_id,
            original_session_id=originals[1].id,
            result_session_id=None,
            status=JobStatus.FAILED,
        ),
        replay(
            experiment_run_id=run_id,
            result_session_id=None,
            status=JobStatus.TIMED_OUT,
        ),
        replay(
            experiment_run_id=run_id,
            result_session_id=None,
            status=JobStatus.CANCELED,
        ),
    ]
    sessions = {entity.id: entity for entity in originals + results}
    summary = compute_run_summary(replays, sessions)
    assert summary["replay_counts_by_status"] == {
        "completed": 2,
        "failed": 1,
        "timed_out": 1,
        "canceled": 1,
    }
    # One passed of the four finished replays, the canceled one excluded.
    assert summary["pass_rate"] == 0.25
    replay_stats = summary["scores"]["conciseness"]["replay"]
    assert replay_stats["mean"] == pytest.approx(0.5)
    assert replay_stats["median"] == pytest.approx(0.5)
    baseline = summary["scores"]["conciseness"]["baseline"]
    assert baseline["median"] == pytest.approx(0.6)
    assert summary["total_cost"]["replay"] == pytest.approx(0.25)
    assert summary["total_cost"]["baseline"] > 0.25
    assert summary["total_tokens"]["baseline"] == {
        "input_tokens": 180,
        "output_tokens": 50,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert summary["total_tokens"]["replay"] == {
        "input_tokens": 80,
        "output_tokens": 70,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }


def test_run_summary_without_scores() -> None:
    """Report null statistics when nothing was scored."""
    original = session(id=uuid.uuid4())
    subject = replay(
        experiment_run_id=uuid.uuid4(),
        original_session_id=original.id,
        result_session_id=None,
        status=JobStatus.CANCELED,
    )
    summary = compute_run_summary([subject], {original.id: original})
    assert summary["replay_counts_by_status"] == {"canceled": 1}
    assert summary["pass_rate"] is None
    assert summary["scores"] == {}
    assert summary["total_cost"] == {"baseline": None, "replay": None}
    empty_totals = {
        "input_tokens": None,
        "output_tokens": None,
        "cached_input_tokens": None,
        "reasoning_tokens": None,
    }
    assert summary["total_tokens"] == {
        "baseline": empty_totals,
        "replay": empty_totals,
    }
