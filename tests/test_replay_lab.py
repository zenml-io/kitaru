"""Tests for the internal Replay Lab comparison helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

from kitaru._replay_lab import (
    build_case_delta,
    compare_replay_lab,
    extract_metrics,
    load_manifest,
    render_json_report,
    render_markdown_report,
    validate_candidate_descriptor,
    validate_candidate_descriptors,
)
from kitaru.client import ExecutionStatus


@dataclass(frozen=True)
class FakeArtifact:
    artifact_id: str
    name: str
    value: Any

    def load(self) -> Any:
        return self.value


class FakeExecutions:
    def __init__(self, executions: dict[str, Any]) -> None:
        self._executions = executions
        self.replay_calls: list[dict[str, Any]] = []

    def get(self, exec_id: str) -> Any:
        return self._executions[exec_id]

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Any:
        self.replay_calls.append(
            {
                "exec_id": exec_id,
                "from_": from_,
                "overrides": overrides,
                "flow_inputs": flow_inputs,
            }
        )
        if len(self.replay_calls) == 1:
            return self._executions["baseline"]
        candidate_index = len(self.replay_calls) - 1
        return self._executions.get(
            f"candidate-{candidate_index}", self._executions["candidate"]
        )


def _execution(
    exec_id: str,
    *,
    status: ExecutionStatus = ExecutionStatus.COMPLETED,
    scorecard: dict[str, Any] | None = None,
    final_response: Any | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    started_at = datetime(2026, 5, 13, 12, 0, tzinfo=UTC)
    artifacts: list[FakeArtifact] = []
    if scorecard is not None:
        artifacts.append(FakeArtifact(f"{exec_id}-scorecard", "scorecard", scorecard))
    if final_response is not None:
        artifacts.append(
            FakeArtifact(f"{exec_id}-final-response", "final_response", final_response)
        )
    checkpoint = SimpleNamespace(
        name="draft_answer",
        status=status,
        artifacts=artifacts,
    )
    return SimpleNamespace(
        exec_id=exec_id,
        status=status,
        started_at=started_at,
        ended_at=started_at + timedelta(seconds=3),
        metadata=metadata or {},
        checkpoints=[checkpoint],
        artifacts=artifacts,
        failure=None,
        status_reason=None,
    )


def test_load_manifest_validates_cases_defaults_and_aliases() -> None:
    manifest = load_manifest(
        {
            "cohort_name": "Support regression cohort",
            "description": "Recent expensive support replies",
            "default_replay_checkpoint": "draft_answer",
            "cases": [
                {
                    "id": "case-1",
                    "execution_id": "exec-1",
                    "reasons": ["cost spike", "customer complaint"],
                    "labels": {"tier": "enterprise"},
                }
            ],
        }
    )

    assert manifest.name == "Support regression cohort"
    assert manifest.default_from_checkpoint == "draft_answer"
    assert manifest.expected_artifacts == ["scorecard", "final_response"]
    assert manifest.cases[0].case_id == "case-1"
    assert manifest.cases[0].reason == "cost spike; customer complaint"
    assert manifest.cases[0].labels == {"tier": "enterprise"}


def test_candidate_descriptor_rejects_unknown_fields() -> None:
    descriptor = validate_candidate_descriptor(
        {
            "id": "cheap",
            "label": "cheaper prompt",
            "flow_inputs": {"temperature": 0},
            "checkpoint_overrides": {"draft_answer.prompt": "shorter"},
            "notes": "Try a terser response style.",
        }
    )

    assert descriptor.id == "cheap"
    assert descriptor.label == "cheaper prompt"
    assert descriptor.flow_inputs == {"temperature": 0}

    with pytest.raises(ValueError, match="Unsupported candidate descriptor"):
        validate_candidate_descriptor(
            {"id": "v2", "label": "v2", "deployment": "future"}
        )


def test_candidate_descriptors_reject_duplicates_and_non_objects() -> None:
    with pytest.raises(ValueError, match="Duplicate candidate descriptor id `cheap`"):
        validate_candidate_descriptors(
            [
                {"id": "cheap", "label": "Cheap"},
                {"id": "cheap", "label": "Cheap again"},
            ]
        )

    with pytest.raises(
        ValueError, match=r"candidate_descriptors\[1\]` must be an object"
    ):
        validate_candidate_descriptors(cast(Any, [None]))


def test_extract_metrics_prefers_scorecard_and_final_response_artifacts() -> None:
    execution = _execution(
        "exec-1",
        scorecard={
            "cost_usd": "0.42",
            "latency_ms": 1250,
            "quality_score": 0.82,
            "tool_call_count": 3,
            "llm_calls": 2,
        },
        final_response={"text": "Here is the support answer."},
    )

    metrics, limitations = extract_metrics(execution)

    assert metrics.cost == 0.42
    assert metrics.latency_seconds == 1.25
    assert metrics.quality_score == 0.82
    assert metrics.output_text == "Here is the support answer."
    assert metrics.tool_call_count == 3
    assert metrics.llm_call_count == 2
    assert metrics.checkpoint_count == 1
    assert limitations == []


def test_extract_metrics_marks_missing_scorecard_and_output_limitations() -> None:
    execution = _execution("exec-1")

    metrics, limitations = extract_metrics(execution)

    assert metrics.cost is None
    assert metrics.quality_score is None
    assert any("Missing `scorecard`" in limitation for limitation in limitations)
    assert any("Missing `final_response`" in limitation for limitation in limitations)


def test_extract_metrics_uses_logs_for_call_and_failure_counts() -> None:
    execution = _execution("exec-1", final_response="ok")
    log_entries = [
        SimpleNamespace(level="warning", message="tool search retried"),
        SimpleNamespace(level="error", message="model call failed once"),
        SimpleNamespace(level="info", message="llm call completed"),
    ]

    metrics, _ = extract_metrics(execution, log_entries=log_entries)

    assert metrics.warning_log_count == 1
    assert metrics.error_log_count == 1
    assert metrics.tool_call_count == 1
    assert metrics.llm_call_count == 2


def test_delta_calculation_keeps_absolute_and_percent_change() -> None:
    baseline = _execution("baseline", scorecard={"cost": 0.50, "quality_score": 0.70})
    candidate = _execution("candidate", scorecard={"cost": 0.25, "quality_score": 0.80})
    baseline_metrics, _ = extract_metrics(baseline)
    candidate_metrics, _ = extract_metrics(candidate)

    delta = build_case_delta(baseline_metrics, candidate_metrics)

    assert delta.cost.absolute == -0.25
    assert delta.cost.percent == -50
    assert delta.quality_score.absolute == pytest.approx(0.10)


def test_compare_replay_lab_runs_three_lanes_and_detects_changed_output() -> None:
    observed = _execution(
        "observed",
        scorecard={"cost": 0.40, "quality_score": 0.70},
        final_response="Original answer",
    )
    baseline = _execution(
        "baseline",
        scorecard={"cost": 0.30, "quality_score": 0.70},
        final_response="Original answer",
    )
    candidate = _execution(
        "candidate",
        scorecard={"cost": 0.20, "quality_score": 0.80},
        final_response="Candidate answer",
    )
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [
                {
                    "case_id": "case-1",
                    "exec_id": "observed",
                    "reason": "cost spike",
                }
            ],
        },
        candidate_descriptor={
            "id": "cheap",
            "label": "cheaper answer",
            "flow_inputs": {"style": "brief"},
            "checkpoint_overrides": {"draft_answer.prompt": "short"},
        },
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    case = report.cases[0]
    assert report.candidates[0].id == "cheap"
    assert set(case.lanes) == {"observed", "baseline_replay"}
    assert case.replay_drift.cost.absolute == pytest.approx(-0.10)
    candidate_result = case.candidate_results[0]
    assert candidate_result.candidate_id == "cheap"
    assert candidate_result.effect_vs_baseline.cost.absolute == pytest.approx(-0.10)
    assert candidate_result.output_changed_vs_baseline is True
    assert fake_client.executions.replay_calls == [
        {
            "exec_id": "observed",
            "from_": "draft_answer",
            "overrides": None,
            "flow_inputs": {},
        },
        {
            "exec_id": "observed",
            "from_": "draft_answer",
            "overrides": {"draft_answer.prompt": "short"},
            "flow_inputs": {"style": "brief"},
        },
    ]


def test_compare_replay_lab_marks_timeout_lane_and_keeps_processing() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution(
        "baseline",
        status=ExecutionStatus.RUNNING,
        scorecard={"cost": 0.30},
        final_response="a",
    )
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="a")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [
                {
                    "case_id": "case-1",
                    "exec_id": "observed",
                    "reason": "timeout case",
                }
            ],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    case = report.cases[0]
    assert case.lanes["baseline_replay"].status == "timeout"
    assert case.lanes["baseline_replay"].timed_out is True
    assert case.candidate_results[0].lane.status == "completed"
    assert report.summary["failed_or_timed_out_lane_count"] == 1


def test_compare_replay_lab_marks_replay_start_failure_as_lane_failure() -> None:
    class FailingCandidateExecutions(FakeExecutions):
        def replay(
            self,
            exec_id: str,
            *,
            from_: str,
            overrides: dict[str, Any] | None = None,
            **flow_inputs: Any,
        ) -> Any:
            if self.replay_calls:
                self.replay_calls.append(
                    {
                        "exec_id": exec_id,
                        "from_": from_,
                        "overrides": overrides,
                        "flow_inputs": flow_inputs,
                    }
                )
                raise RuntimeError("candidate crashed before launch")
            return super().replay(
                exec_id,
                from_=from_,
                overrides=overrides,
                **flow_inputs,
            )

    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    fake_client = SimpleNamespace(
        executions=FailingCandidateExecutions(
            {"observed": observed, "baseline": baseline}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [
                {
                    "case_id": "case-1",
                    "exec_id": "observed",
                    "reason": "candidate failure",
                }
            ],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    case = report.cases[0]
    candidate_lane = case.candidate_results[0].lane
    assert candidate_lane.status == "failed"
    assert candidate_lane.error_message is not None
    assert "candidate crashed" in candidate_lane.error_message
    assert report.summary["failed_or_timed_out_lane_count"] == 1


def test_compare_replay_lab_accepts_plural_candidates() -> None:
    observed = _execution("observed", scorecard={"cost": 0.30}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    cheap = _execution("candidate-1", scorecard={"cost": 0.20}, final_response="b")
    quality = _execution("candidate-2", scorecard={"cost": 0.35}, final_response="a")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {
                "observed": observed,
                "baseline": baseline,
                "candidate-1": cheap,
                "candidate-2": quality,
                "candidate": cheap,
            }
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "matrix"}],
        },
        candidate_descriptors=[
            {"id": "cheap", "label": "Cheap alias", "flow_inputs": {"model": "cheap"}},
            {
                "id": "quality",
                "label": "Quality alias",
                "flow_inputs": {"model": "quality"},
            },
        ],
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    case = report.cases[0]
    assert set(case.lanes) == {"observed", "baseline_replay"}
    assert [result.candidate_id for result in case.candidate_results] == [
        "cheap",
        "quality",
    ]
    assert report.summary["candidate_ids"] == ["cheap", "quality"]
    assert report.summary["candidates"]["cheap"]["changed_output_count"] == 1
    assert report.summary["candidates"]["cheap"]["efficiency_win_count"] == 1
    assert report.summary["candidates"]["cheap"]["aggregate_verdict"] == "caution"
    assert report.summary["candidates"]["quality"]["changed_output_count"] == 0
    assert report.summary["candidate_ranking"][0]["candidate_id"] == "cheap"
    assert report.summary["replay_trust"]["label"] == "Replay trust: steady"
    assert "safe enough" not in report.summary["overall_recommendation"]
    assert fake_client.executions.replay_calls[1]["flow_inputs"] == {"model": "cheap"}
    assert fake_client.executions.replay_calls[2]["flow_inputs"] == {"model": "quality"}


def test_compare_replay_lab_rejects_singular_and_plural_candidates_together() -> None:
    with pytest.raises(ValueError, match="Provide only one"):
        compare_replay_lab(
            manifest={
                "name": "Support cohort",
                "default_from_checkpoint": "draft_answer",
                "cases": [
                    {"case_id": "case-1", "exec_id": "observed", "reason": "matrix"}
                ],
            },
            candidate_descriptor={"id": "cheap", "label": "Cheap"},
            candidate_descriptors=[{"id": "quality", "label": "Quality"}],
        )


def test_evaluator_callable_overrides_quality_and_keeps_runtime_facts() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution(
        "candidate",
        scorecard={"cost": 0.20, "quality_score": 0.50},
        final_response="b",
    )
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    def evaluator(request: Any) -> dict[str, Any]:
        return {
            "evaluator_id": "callable-v1",
            "quality_score": 0.91,
            "scorecard": {"has_summary": True},
            "limitations": [f"review {request.lane_name}"],
            "cost": 999,
            "cost_usd": 999,
            "latency_ms": 1,
        }

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "eval"}],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        evaluator=evaluator,
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    candidate_lane = report.cases[0].candidate_results[0].lane
    assert candidate_lane.metrics.cost == 0.20
    assert candidate_lane.metrics.quality_score == 0.91
    assert candidate_lane.metrics.evaluation == {
        "evaluator_id": "callable-v1",
        "quality_score": 0.91,
        "scorecard": {"has_summary": True},
        "limitations": ["review candidate_replay"],
    }
    assert candidate_lane.metrics.evaluation is not None
    assert "cost_usd" not in candidate_lane.metrics.evaluation
    assert "latency_ms" not in candidate_lane.metrics.evaluation
    assert "review candidate_replay" in candidate_lane.limitations


def test_evaluator_recomputes_replay_drift_and_candidate_effect() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    def evaluator(request: Any) -> dict[str, Any]:
        scores = {"observed": 0.9, "baseline_replay": 0.5, "candidate_replay": 0.4}
        return {"quality_score": scores[request.lane_name]}

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "eval"}],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        evaluator=evaluator,
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    case = report.cases[0]
    assert case.replay_drift.quality_score.absolute == pytest.approx(-0.4)
    assert case.candidate_results[
        0
    ].effect_vs_baseline.quality_score.absolute == pytest.approx(-0.1)
    assert case.candidate_results[0].verdict == "caution"


def test_evaluator_fill_missing_does_not_replace_existing_quality() -> None:
    observed = _execution(
        "observed", scorecard={"quality_score": 0.70}, final_response="a"
    )
    baseline = _execution(
        "baseline", scorecard={"quality_score": 0.60}, final_response="a"
    )
    candidate = _execution(
        "candidate", scorecard={"quality_score": 0.50}, final_response="b"
    )
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "eval"}],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        evaluator=lambda request: {"quality_score": 0.99},
        evaluator_precedence="fill_missing",
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    assert report.cases[0].candidate_results[0].lane.metrics.quality_score == 0.50


def test_evaluator_warns_on_error_by_default() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "eval"}],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        evaluator=lambda request: (_ for _ in ()).throw(RuntimeError("bad score")),
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    assert any(
        "Evaluator failed" in limitation for limitation in report.cases[0].limitations
    )


def test_evaluator_fail_policy_raises() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    with pytest.raises(RuntimeError, match="bad score"):
        compare_replay_lab(
            manifest={
                "name": "Support cohort",
                "default_from_checkpoint": "draft_answer",
                "cases": [
                    {"case_id": "case-1", "exec_id": "observed", "reason": "eval"}
                ],
            },
            candidate_descriptor={"id": "cheap", "label": "candidate"},
            evaluator=lambda request: (_ for _ in ()).throw(RuntimeError("bad score")),
            evaluator_on_error="fail",
            client=fake_client,
            timeout_seconds=0,
            poll_interval_seconds=0.01,
            sleep=lambda _: None,
        )


def test_evaluator_descriptor_loads_module_function(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_path = tmp_path / "replay_eval.py"
    module_path.write_text(
        "def evaluate(request):\n"
        "    return {'quality_score': 0.77, 'lane_name': request.lane_name}\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [{"case_id": "case-1", "exec_id": "observed", "reason": "eval"}],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        evaluator_descriptor={
            "target": "replay_eval:evaluate",
            "id": "descriptor-v1",
        },
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    evaluation = report.cases[0].candidate_results[0].lane.metrics.evaluation
    assert evaluation == {
        "quality_score": 0.77,
        "lane_name": "candidate_replay",
        "evaluator_id": "descriptor-v1",
    }


def test_replay_lab_compare_tracks_safe_aggregate_analytics() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )

    with patch("kitaru._replay_lab.track") as mock_track:
        compare_replay_lab(
            manifest={
                "name": "Support cohort",
                "default_from_checkpoint": "draft_answer",
                "cases": [
                    {"case_id": "case-1", "exec_id": "observed", "reason": "eval"}
                ],
            },
            candidate_descriptor={"id": "cheap", "label": "candidate"},
            evaluator=lambda request: {"quality_score": 0.99},
            client=fake_client,
            timeout_seconds=0,
            poll_interval_seconds=0.01,
            source="/tmp/private-user-path",
            sleep=lambda _: None,
        )

    requested = mock_track.call_args_list[0].args
    completed = mock_track.call_args_list[1].args
    assert requested[0] == "Kitaru Replay Lab compare requested"
    assert requested[1] == {
        "case_count": 1,
        "candidate_count": 1,
        "has_evaluator": True,
        "source": "other",
    }
    assert completed[0] == "Kitaru Replay Lab compare completed"
    assert completed[1] == {
        "case_count": 1,
        "candidate_count": 1,
        "has_evaluator": True,
        "source": "other",
        "completed": True,
        "failed_or_timed_out_lane_count": 0,
    }


def test_report_rendering_outputs_json_and_markdown() -> None:
    observed = _execution("observed", scorecard={"cost": 0.40}, final_response="a")
    baseline = _execution("baseline", scorecard={"cost": 0.30}, final_response="a")
    candidate = _execution("candidate", scorecard={"cost": 0.20}, final_response="b")
    fake_client = SimpleNamespace(
        executions=FakeExecutions(
            {"observed": observed, "baseline": baseline, "candidate": candidate}
        )
    )
    report = compare_replay_lab(
        manifest={
            "name": "Support cohort",
            "default_from_checkpoint": "draft_answer",
            "cases": [
                {
                    "case_id": "case-1",
                    "exec_id": "observed",
                    "reason": "render case",
                }
            ],
        },
        candidate_descriptor={"id": "cheap", "label": "candidate"},
        client=fake_client,
        timeout_seconds=0,
        poll_interval_seconds=0.01,
        sleep=lambda _: None,
    )

    json_payload = json.loads(render_json_report(report))
    markdown = render_markdown_report(report)

    assert json_payload["name"] == "Support cohort"
    assert "candidate" not in json_payload
    assert json_payload["candidates"][0]["id"] == "cheap"
    assert "candidate_replay" not in json_payload["cases"][0]["lanes"]
    assert (
        json_payload["cases"][0]["candidate_results"][0]["output_changed_vs_baseline"]
        is True
    )
    assert "# Replay Lab Report: Support cohort" in markdown
    assert "case-1" in markdown
