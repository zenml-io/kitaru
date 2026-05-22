"""Cheap tests for the Replay Lab end-to-end demo files."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from examples.end_to_end.replay_lab import verdict_renderer
from examples.end_to_end.replay_lab.langgraph_requirements_triage import (
    evaluator as requirements_evaluator,
)
from examples.end_to_end.replay_lab.langgraph_requirements_triage import (
    requirements_cases,
    requirements_flow,
)
from examples.end_to_end.replay_lab.langgraph_requirements_triage import (
    run_replay_lab as requirements_replay,
)
from examples.end_to_end.replay_lab.render_report import render_html_report
from examples.end_to_end.replay_lab.scenarios import (
    DEFAULT_VARIANTS_PER_BASE,
    build_draft_response,
    evaluate_draft,
    get_scenario,
    list_base_scenarios,
    list_seed_scenarios,
)
from examples.end_to_end.replay_lab.seed_observed import (
    build_manifest_payload,
    parse_args,
    select_case_ids,
)

from kitaru._replay_lab import validate_candidate_descriptor

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "end_to_end" / "replay_lab"
)


def test_candidate_descriptor_file_uses_supported_shape() -> None:
    payload = json.loads(
        (_EXAMPLE_DIR / "candidates" / "cheaper_support_agent.json").read_text()
    )

    descriptor = validate_candidate_descriptor(payload)

    assert descriptor.label == "Cheaper deterministic support agent"
    assert descriptor.flow_inputs == {"agent_profile": "candidate"}
    assert descriptor.checkpoint_overrides == {}


def test_base_scenarios_keep_the_original_three_case_ids() -> None:
    scenario_ids = [scenario["case_id"] for scenario in list_base_scenarios()]

    assert scenario_ids == [
        "support-refund-delay",
        "regulated-medical-claim",
        "shipping-tool-loop",
    ]
    assert get_scenario("support-refund-delay")["topic"] == "refund delay"


def test_seed_scenarios_generate_deterministic_history_variants() -> None:
    first = list_seed_scenarios(variants_per_base=DEFAULT_VARIANTS_PER_BASE)
    second = list_seed_scenarios(variants_per_base=DEFAULT_VARIANTS_PER_BASE)

    assert len(first) == 12
    assert [scenario["case_id"] for scenario in first] == [
        scenario["case_id"] for scenario in second
    ]
    assert [scenario["labels"] for scenario in first] == [
        scenario["labels"] for scenario in second
    ]
    assert "regulated-medical-claim--hist-02" in [
        scenario["case_id"] for scenario in first
    ]


def test_generated_history_case_resolves_during_replay() -> None:
    scenario = get_scenario("regulated-medical-claim--hist-02")

    assert scenario["case_id"] == "regulated-medical-claim--hist-02"
    assert scenario["base_case_id"] == "regulated-medical-claim"
    assert scenario["required_terms"] == [
        "human review",
        "medical advice",
        "safe response",
    ]
    assert scenario["risk_terms"] == ["diagnosis", "treatment plan"]
    assert scenario["labels"] == {
        "scenario_version": "replay_lab_support_v1",
        "base_case_id": "regulated-medical-claim",
        "variant_index": "02",
        "tier": "regulated",
        "topic": "medical claim routing",
        "trigger": "quality drop",
        "production_signal": "customer complaint",
        "company": "Northwind Health",
        "region": "eu",
        "channel": "chat",
        "urgency": "high",
    }


def test_candidate_is_cheaper_but_regulated_case_loses_a_guardrail() -> None:
    scenario = get_scenario("regulated-medical-claim")
    champion = build_draft_response(scenario, "champion")
    candidate = build_draft_response(scenario, "candidate")

    champion_scorecard = evaluate_draft(champion, scenario)
    candidate_scorecard = evaluate_draft(candidate, scenario)

    assert candidate_scorecard["cost_usd"] < champion_scorecard["cost_usd"]
    assert candidate_scorecard["quality_score"] < champion_scorecard["quality_score"]
    assert "human review" in candidate_scorecard["missing_required_terms"]


def test_candidate_variant_scoring_stays_deterministic() -> None:
    scenario = get_scenario("regulated-medical-claim--hist-02")
    champion = build_draft_response(scenario, "champion")
    candidate = build_draft_response(scenario, "candidate")

    champion_scorecard = evaluate_draft(champion, scenario)
    candidate_scorecard = evaluate_draft(candidate, scenario)

    assert champion_scorecard["case_id"] == "regulated-medical-claim--hist-02"
    assert candidate_scorecard["cost_usd"] == 0.26
    assert champion_scorecard["cost_usd"] == 0.54
    assert candidate_scorecard["cost_usd"] < champion_scorecard["cost_usd"]
    assert candidate_scorecard["quality_score"] < champion_scorecard["quality_score"]
    assert "human review" in candidate_scorecard["missing_required_terms"]


def test_seed_selection_defaults_to_richer_history_and_preserves_small_path() -> None:
    default_ids = select_case_ids(
        case_ids=None,
        small=False,
        count=None,
        variants_per_base=None,
    )
    small_ids = select_case_ids(
        case_ids=None,
        small=True,
        count=None,
        variants_per_base=None,
    )
    capped_ids = select_case_ids(
        case_ids=None,
        small=False,
        count=5,
        variants_per_base=None,
    )

    assert len(default_ids) == 12
    assert small_ids == [
        "support-refund-delay",
        "regulated-medical-claim",
        "shipping-tool-loop",
    ]
    assert capped_ids == default_ids[:5]
    assert select_case_ids(
        case_ids=["shipping-tool-loop--hist-01", "support-refund-delay"],
        small=False,
        count=None,
        variants_per_base=None,
    ) == ["shipping-tool-loop--hist-01", "support-refund-delay"]


def test_seed_cli_rejects_too_many_variants_per_base() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--variants-per-base", "99"])


def test_seed_manifest_payload_points_replay_at_draft_response() -> None:
    manifest = build_manifest_payload(
        [
            {
                "case_id": "support-refund-delay",
                "exec_id": "exec-123",
                "reason": "cost spike",
                "labels": {"tier": "enterprise"},
            }
        ]
    )

    assert manifest["default_from_checkpoint"] == "draft_response"
    assert manifest["expected_artifacts"] == ["scorecard", "final_response"]
    assert manifest["cases"] == [
        {
            "case_id": "support-refund-delay",
            "exec_id": "exec-123",
            "reason": "cost spike",
            "labels": {"tier": "enterprise"},
        }
    ]


def test_seed_manifest_payload_keeps_variant_labels_flat_strings() -> None:
    scenario = get_scenario("regulated-medical-claim--hist-02")

    manifest = build_manifest_payload(
        [
            {
                "case_id": scenario["case_id"],
                "exec_id": "exec-variant-123",
                "reason": scenario["reason"],
                "labels": scenario["labels"] | {"retry_count": 2},
            }
        ]
    )

    labels = manifest["cases"][0]["labels"]
    assert manifest["cases"][0]["case_id"] == "regulated-medical-claim--hist-02"
    assert labels["company"] == "Northwind Health"
    assert labels["retry_count"] == "2"
    assert all(isinstance(value, str) for value in labels.values())


def test_static_html_report_renders_plural_demo_sections(tmp_path: Path) -> None:
    report = {
        "name": "Support Replay Lab demo",
        "candidates": [
            {
                "id": "cheap",
                "label": "Cheaper deterministic support agent",
                "flow_inputs": {"agent_profile": "candidate"},
                "checkpoint_overrides": {},
                "notes": "Shorter candidate profile.",
            }
        ],
        "summary": {
            "case_count": 1,
            "candidate_count": 1,
            "candidate_ids": ["cheap"],
            "candidates": {
                "cheap": {
                    "label": "Cheaper deterministic support agent",
                    "completed_count": 1,
                    "changed_output_count": 1,
                    "failed_or_timed_out_lane_count": 0,
                    "average_cost": 0.25,
                    "average_latency_seconds": 2.7,
                    "average_quality_score": 1.0,
                }
            },
            "failed_or_timed_out_lane_count": 0,
            "replay_drift_warning_count": 0,
        },
        "cases": [
            {
                "case_id": "support-refund-delay",
                "source_exec_id": "observed-1",
                "from_checkpoint": "draft_response",
                "reason": "cost spike",
                "labels": {},
                "replay_drift": {
                    "cost": {"absolute": 0.0, "percent": 0.0},
                    "duration_seconds": {"absolute": 0.0, "percent": 0.0},
                    "latency_seconds": {"absolute": 0.0, "percent": 0.0},
                    "quality_score": {"absolute": 0.0, "percent": 0.0},
                },
                "candidate_results": [
                    {
                        "candidate_id": "cheap",
                        "candidate_label": "Cheaper deterministic support agent",
                        "output_changed_vs_baseline": True,
                        "verdict": "caution",
                        "limitations": [],
                        "effect_vs_baseline": {
                            "cost": {"absolute": -0.17, "percent": -40.5},
                            "duration_seconds": {"absolute": 1.0, "percent": 6.7},
                            "latency_seconds": {"absolute": -2.1, "percent": -43.7},
                            "quality_score": {"absolute": 0.0, "percent": 0.0},
                        },
                        "lane": {
                            "exec_id": "candidate-1",
                            "status": "completed",
                            "metrics": {
                                "cost": 0.25,
                                "latency_seconds": 2.7,
                                "quality_score": 1.0,
                                "tool_call_count": 2,
                                "llm_call_count": 1,
                                "output_text": "Candidate reply",
                            },
                        },
                    }
                ],
                "limitations": [],
                "lanes": {
                    "observed": {
                        "exec_id": "observed-1",
                        "status": "completed",
                        "metrics": {
                            "cost": 0.42,
                            "latency_seconds": 4.8,
                            "quality_score": 1.0,
                            "tool_call_count": 3,
                            "llm_call_count": 1,
                            "output_text": "Baseline reply",
                        },
                    },
                    "baseline_replay": {
                        "exec_id": "baseline-1",
                        "status": "completed",
                        "metrics": {
                            "cost": 0.42,
                            "latency_seconds": 4.8,
                            "quality_score": 1.0,
                            "tool_call_count": 3,
                            "llm_call_count": 1,
                            "output_text": "Baseline reply",
                        },
                    },
                },
            }
        ],
    }

    html = verdict_renderer.build_html_report(report)
    assert "Replay Lab Report: Support Replay Lab demo" in html
    assert "Cheaper deterministic support agent" in html
    assert "observed-1" in html
    assert "candidate-1" in html
    assert "Changed output: cheap" in html

    json_path = tmp_path / "report.json"
    html_path = tmp_path / "report.html"
    json_path.write_text(json.dumps(report), encoding="utf-8")

    assert render_html_report(json_path, html_path) == html_path
    assert html_path.read_text(encoding="utf-8").startswith("<!doctype html>")


def test_html_renderer_prefers_canonical_summary_verdicts() -> None:
    report = {
        "name": "Canonical verdict demo",
        "candidates": [{"id": "cheap", "label": "Cheap alias"}],
        "summary": {
            "case_count": 1,
            "candidate_count": 1,
            "failed_or_timed_out_lane_count": 0,
            "replay_drift_warning_count": 0,
            "replay_trust": {"label": "Replay trust: steady", "detail": "Core detail."},
            "overall_recommendation": "Ship candidate `cheap` for a guarded trial.",
            "candidate_ranking": [
                {
                    "candidate_id": "cheap",
                    "label": "Cheap alias",
                    "aggregate_verdict": "ship",
                    "completed_count": 1,
                    "changed_output_count": 0,
                    "failed_or_timed_out_lane_count": 0,
                    "efficiency_win_count": 1,
                    "quality_loss_count": 0,
                    "cases_to_inspect": [],
                }
            ],
        },
        "cases": [],
    }

    verdict = verdict_renderer.build_report_verdict(report)

    assert verdict.overall == "Ship candidate `cheap` for a guarded trial."
    assert verdict.candidates[0].verdict == "ship"


def test_html_renderer_truncates_changed_outputs() -> None:
    long_output = "x" * 3000
    report = {
        "name": "Long output demo",
        "candidates": [{"id": "cheap", "label": "Cheap alias"}],
        "summary": {
            "case_count": 1,
            "candidate_count": 1,
            "failed_or_timed_out_lane_count": 0,
            "replay_drift_warning_count": 0,
            "replay_trust": {"label": "Replay trust: steady", "detail": "Core detail."},
            "overall_recommendation": "Caution: inspect outputs.",
            "candidate_ranking": [
                {
                    "candidate_id": "cheap",
                    "label": "Cheap alias",
                    "aggregate_verdict": "caution",
                    "completed_count": 1,
                    "changed_output_count": 1,
                    "failed_or_timed_out_lane_count": 0,
                    "efficiency_win_count": 0,
                    "quality_loss_count": 0,
                    "cases_to_inspect": ["case-1"],
                }
            ],
        },
        "cases": [
            {
                "case_id": "case-1",
                "reason": "large response",
                "replay_drift_warning": False,
                "lanes": {
                    "observed": {"metrics": {}},
                    "baseline_replay": {
                        "exec_id": "base",
                        "status": "completed",
                        "metrics": {"output_text": long_output},
                    },
                },
                "candidate_results": [
                    {
                        "candidate_id": "cheap",
                        "candidate_label": "Cheap alias",
                        "output_changed_vs_baseline": True,
                        "verdict": "caution",
                        "effect_vs_baseline": {},
                        "lane": {
                            "exec_id": "candidate",
                            "status": "completed",
                            "metrics": {"output_text": long_output},
                        },
                    }
                ],
            }
        ],
    }

    html = verdict_renderer.build_html_report(report)

    assert "truncated 500 characters in HTML" in html
    assert long_output not in html


def test_requirements_flow_uses_expected_runner_and_anchor_names() -> None:
    assert requirements_flow.RUNNER_NAME == "requirements_triage"
    assert requirements_flow.REPLAY_ANCHOR == "requirements_triage_langgraph_call"


def test_requirements_seed_selection_defaults_to_three_and_small_to_two() -> None:
    default_ids = requirements_cases.select_case_ids(
        case_ids=None,
        small=False,
        count=None,
    )
    small_ids = requirements_cases.select_case_ids(
        case_ids=None,
        small=True,
        count=None,
    )
    capped_ids = requirements_cases.select_case_ids(
        case_ids=None,
        small=False,
        count=1,
    )

    assert default_ids == requirements_cases.list_case_ids()
    assert len(default_ids) == 3
    assert small_ids == default_ids[:2]
    assert capped_ids == default_ids[:1]


def test_requirements_candidate_matrix_uses_aliases_only() -> None:
    candidates = requirements_replay.load_candidate_descriptors(
        matrix_path=requirements_replay.DEFAULT_MATRIX_PATH,
        candidate_paths=None,
    )

    assert [candidate["id"] for candidate in candidates] == [
        "cheap",
        "balanced",
        "quality",
    ]
    assert all("/" not in candidate["flow_inputs"]["model"] for candidate in candidates)
    assert (
        requirements_replay.load_candidate_descriptors(
            matrix_path=requirements_replay.DEFAULT_MATRIX_PATH,
            candidate_paths=None,
            candidate_limit=2,
        )
        == candidates[:2]
    )


def test_requirements_candidate_loading_rejects_mixed_inputs(tmp_path: Path) -> None:
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(
        json.dumps(
            {
                "id": "cheap",
                "label": "Cheap alias",
                "flow_inputs": {"model": "cheap"},
                "checkpoint_overrides": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="either --matrix-path"):
        requirements_replay.load_candidate_descriptors(
            matrix_path=requirements_replay.DEFAULT_MATRIX_PATH,
            candidate_paths=[candidate_path],
        )


def test_requirements_evaluator_scores_sectioned_text() -> None:
    class Metrics:
        output_text = (
            "Summary\nLooks clear.\n\n"
            "Known requirements\n- One\n\n"
            "Missing information\n- Two\n\n"
            "Risks\n- Three\n\n"
            "Recommended next action\nAsk a concrete question."
        )

    class Lane:
        metrics = Metrics()

    class Request:
        lane = Lane()

    result = requirements_evaluator.evaluate_requirements_triage(Request())

    assert result["quality_score"] == 1.0
    assert result["scorecard"]["lists_missing_information"] is True
    assert result["limitations"] == []


def test_requirements_evaluator_flags_missing_sections() -> None:
    result = requirements_evaluator.evaluate_requirements_triage(
        {
            "lane": {
                "metrics": {
                    "output_text": (
                        "Summary\nLooks complete.\n\n"
                        "Known requirements\n- One\n\n"
                        "Recommended next action\nShip it."
                    )
                }
            }
        }
    )

    assert result["quality_score"] < 1.0
    assert result["scorecard"]["lists_risks"] is False
    assert "Missing Risks section." in result["limitations"]


def test_verdict_renderer_flags_section_drift() -> None:
    report = json.loads(
        (
            _EXAMPLE_DIR
            / "langgraph_requirements_triage"
            / "reports"
            / "requirements-triage-sample.json"
        ).read_text(encoding="utf-8")
    )
    report["summary"].pop("candidate_ranking", None)
    report["summary"].pop("overall_recommendation", None)
    report["summary"].pop("replay_trust", None)
    case = report["cases"][0]
    case.pop("replay_drift_warning", None)
    case["lanes"]["baseline_replay"]["metrics"]["evaluation"]["scorecard"][
        "lists_risks"
    ] = False

    assert verdict_renderer.case_has_high_replay_drift(case)
    verdict = verdict_renderer.build_report_verdict(report)
    assert verdict.trust_label == "Replay trust: inspect first"


def test_committed_requirements_sample_renders(tmp_path: Path) -> None:
    sample_json = (
        _EXAMPLE_DIR
        / "langgraph_requirements_triage"
        / "reports"
        / "requirements-triage-sample.json"
    )
    sample_html = (
        _EXAMPLE_DIR
        / "langgraph_requirements_triage"
        / "reports"
        / "requirements-triage-sample.html"
    )
    rendered = tmp_path / "sample.html"

    assert render_html_report(sample_json, rendered) == rendered
    html = rendered.read_text(encoding="utf-8")
    assert "Replay Lab Report: Requirements triage LangGraph demo" in html
    assert "Balanced alias" in html
    assert "requirements_triage_v1" in html
    assert sample_html.read_text(encoding="utf-8").startswith("<!doctype html>")
