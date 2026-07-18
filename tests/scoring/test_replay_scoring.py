"""Replay scoring retry contracts."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock, call

import pytest
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import OperationalLimitReason, RegressionLimits, Score, scorer
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.adapters.pydantic_ai import _agent as agent_module


@scorer(capability="pure")
def _objective(_: object) -> Score:
    return Score(value=1.0)


@dataclass(frozen=True)
class _ReplayResult:
    record: Any
    submission: Any
    runs: Any


def _replay_with_scoring(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: _ReplayResult,
    evaluation_service: Any,
    grounded_policy: Any | None = None,
    grounded_capabilities: dict[str, Any] | None = None,
) -> _ReplayResult:
    agent = KitaruAgent(Agent(TestModel(), name="scored-agent", output_type=str))
    binding = SimpleNamespace(
        project_id="project-id",
        manifest=SimpleNamespace(protections={}),
    )
    agent._registered_state = cast(Any, SimpleNamespace(binding=binding))
    flow_definition = MagicMock()
    monkeypatch.setattr(agent, "_registered_flow", lambda: flow_definition)
    monkeypatch.setattr(agent, "_preflight_registered_identity", Mock())
    monkeypatch.setattr(agent_module, "Client", lambda: object())
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )
    monkeypatch.setattr(
        agent_module,
        "preplan_replay_attempt",
        lambda *_a, **_k: object(),
    )
    monkeypatch.setattr(
        agent_module,
        "freeze_replay_attempt",
        lambda *_a, **_k: SimpleNamespace(
            spec=SimpleNamespace(
                experiment_id="exp-1",
                regression_limits=None,
                replay_inputs=SimpleNamespace(
                    flow_overrides=None,
                    checkpoint_overrides=None,
                    invocation_overrides=None,
                    skip=None,
                ),
                at="checkpoint",
                wait=True,
            )
        ),
    )
    monkeypatch.setattr(
        agent_module,
        "execute_replay_attempt",
        lambda *_a, **_k: result,
    )
    monkeypatch.setattr(agent_module, "ScoreEvaluationService", evaluation_service)

    return cast(
        _ReplayResult,
        agent.replay(
            "parent-run",
            at="checkpoint",
            on_error="fail",
            uncovered_policy="fail",
            idempotency_key="same-request",
            scorers=[_objective],
            grounded_policy=grounded_policy,
            grounded_capabilities=grounded_capabilities,
        ),
    )


def test_agent_evaluate_delegates_to_client_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = KitaruAgent(Agent(TestModel(), name="scored-agent", output_type=str))
    expected = object()
    evaluate = Mock(return_value=expected)
    monkeypatch.setattr(
        agent_module.kitaru,
        "KitaruClient",
        lambda: SimpleNamespace(executions=SimpleNamespace(evaluate=evaluate)),
    )

    actual = agent.evaluate(
        ["run-1"],
        [_objective],
        idempotency_key="score-request",
    )

    assert actual is expected
    assert evaluate.call_args.args == (["run-1"], [_objective])
    assert evaluate.call_args.kwargs["agent"] is agent
    assert evaluate.call_args.kwargs["idempotency_key"] == "score-request"


def test_bounded_rerun_reads_terminal_usage_before_stopping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = KitaruAgent(Agent(TestModel(), name="bounded-agent", output_type=str))

    @agent.protection("completed", capability="pure")
    def completed(_: object) -> Score:
        return Score(value=1.0)

    binding = SimpleNamespace(
        project_id="project-id",
        manifest=SimpleNamespace(protections=agent._protection_snapshots()),
    )
    agent._registered_state = cast(Any, SimpleNamespace(binding=binding))
    monkeypatch.setattr(agent, "_registered_flow", MagicMock())
    monkeypatch.setattr(agent, "_preflight_registered_identity", Mock())
    monkeypatch.setattr(agent_module, "Client", lambda: object())
    monkeypatch.setattr(agent_module, "_get_project_by_exact_selector", Mock())
    agent_info = SimpleNamespace(
        resolve_suite_rerun_request=lambda _selector, _key: (None, object()),
    )
    monkeypatch.setattr(
        agent_module,
        "_agent_info_from_project_model",
        lambda *_args, **_kwargs: agent_info,
    )
    monkeypatch.setattr(agent_module, "_active_project_id", lambda _client: None)
    limits = RegressionLimits(max_trials=2, max_cost_usd=0.5)
    spec = SimpleNamespace(
        experiment_id="exp-bounded",
        regression_limits=limits,
        replay_inputs=SimpleNamespace(
            flow_overrides=None,
            checkpoint_overrides=None,
            invocation_overrides=None,
            skip=None,
        ),
        grounded_policy=None,
        at="checkpoint",
        wait=True,
    )
    monkeypatch.setattr(
        agent_module,
        "plan_suite_rerun",
        lambda *_args, **kwargs: (
            SimpleNamespace(spec=spec)
            if kwargs["limits"] == limits
            else pytest.fail("limits were not passed to rerun planning")
        ),
    )

    usage_get = Mock(side_effect=RuntimeError("usage metadata was unavailable"))
    monkeypatch.setattr(
        agent_module.kitaru,
        "KitaruClient",
        lambda: SimpleNamespace(
            executions=SimpleNamespace(_get_llm_usage_summary=usage_get)
        ),
    )

    frozen_outcome: Any | None = None

    def execute(_plan: Any, **kwargs: Any) -> _ReplayResult:
        nonlocal frozen_outcome
        reason = kwargs["observe_trial"](
            SimpleNamespace(),
            SimpleNamespace(results=[SimpleNamespace(replay_exec_id="child-1")]),
        )
        assert reason is OperationalLimitReason.COST_UNVERIFIED
        frozen_outcome = kwargs["finalize_operational_limit"](1, "2026-07-18T10:00:00Z")
        return _ReplayResult(
            record=SimpleNamespace(
                score_aggregate=object(),
                operational_limit=frozen_outcome,
            ),
            submission=SimpleNamespace(results=[]),
            runs=Mock(),
        )

    monkeypatch.setattr(agent_module, "execute_replay_attempt", execute)

    result = agent.replay(
        experiment="regression-suite",
        idempotency_key="bounded-attempt",
        limits=limits,
    )

    usage_get.assert_called_once_with("child-1")
    assert result.record.operational_limit == frozen_outcome
    assert frozen_outcome is not None
    assert frozen_outcome.reason_code is OperationalLimitReason.COST_UNVERIFIED
    assert frozen_outcome.verified is False
    assert frozen_outcome.facts.remaining_trials == 1


def test_zero_verified_children_still_reach_hold_scoring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runs = Mock()
    result = _ReplayResult(
        record=SimpleNamespace(
            score_aggregate=None,
            counts=SimpleNamespace(verified=0),
            unverified_children=[],
        ),
        submission=SimpleNamespace(results=[]),
        runs=runs,
    )
    scored_record = SimpleNamespace(score_aggregate=object())
    service = Mock()
    service.evaluate_existing_attempt.return_value = SimpleNamespace(
        record=scored_record
    )
    evaluation_service = Mock(return_value=service)

    actual = _replay_with_scoring(
        monkeypatch,
        result=result,
        evaluation_service=evaluation_service,
    )

    assert actual.record is scored_record
    assert service.evaluate_existing_attempt.call_args.kwargs["executions"] == []
    runs.list.assert_not_called()


def test_scored_replay_retry_does_not_replace_existing_aggregate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aggregate_reference = object()
    runs = Mock()
    result = _ReplayResult(
        record=SimpleNamespace(score_aggregate=aggregate_reference),
        submission=SimpleNamespace(results=[]),
        runs=runs,
    )
    evaluation_service = Mock(side_effect=AssertionError("retry rescored the attempt"))

    actual = _replay_with_scoring(
        monkeypatch,
        result=result,
        evaluation_service=evaluation_service,
    )

    assert actual is result
    evaluation_service.assert_not_called()
    runs.list.assert_not_called()


def test_scored_replay_retry_recovers_all_verified_children(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    children = [SimpleNamespace(id=f"child-{index}") for index in range(51)]
    runs = Mock()
    runs.list.side_effect = [
        SimpleNamespace(items=children[:50]),
        SimpleNamespace(items=children[50:]),
    ]
    result = _ReplayResult(
        record=SimpleNamespace(
            score_aggregate=None,
            counts=SimpleNamespace(verified=51),
            unverified_children=[],
        ),
        submission=SimpleNamespace(results=[]),
        runs=runs,
    )
    scored_record = SimpleNamespace(score_aggregate=object())
    service = Mock()
    service.evaluate_existing_attempt.return_value = SimpleNamespace(
        record=scored_record
    )
    evaluation_service = Mock(return_value=service)

    grounded_policy = object()
    grounded_capabilities = {"lookup": object()}
    actual = _replay_with_scoring(
        monkeypatch,
        result=result,
        evaluation_service=evaluation_service,
        grounded_policy=grounded_policy,
        grounded_capabilities=grounded_capabilities,
    )

    assert actual.record is scored_record
    assert service.evaluate_existing_attempt.call_args.kwargs["executions"] == [
        f"child-{index}" for index in range(51)
    ]
    assert service.evaluate_existing_attempt.call_args.kwargs["record"] is result.record
    assert (
        service.evaluate_existing_attempt.call_args.kwargs["grounded_policy"]
        is grounded_policy
    )
    assert (
        service.evaluate_existing_attempt.call_args.kwargs["grounded_capabilities"]
        == grounded_capabilities
    )
    assert runs.list.call_args_list == [
        call(page=1, size=50),
        call(page=2, size=50),
    ]
