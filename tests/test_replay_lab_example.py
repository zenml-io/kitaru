"""Cheap tests for the Replay Lab end-to-end demo files."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from examples.end_to_end.replay_lab.render_report import (
    build_html_report,
    render_html_report,
)
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


def test_static_html_report_renders_demo_sections(tmp_path: Path) -> None:
    report = {
        "name": "Support Replay Lab demo",
        "candidate": {
            "label": "Cheaper deterministic support agent",
            "notes": "Shorter candidate profile.",
        },
        "summary": {
            "case_count": 1,
            "candidate_completed_count": 1,
            "changed_output_count": 1,
            "replay_drift_warning_count": 0,
        },
        "cases": [
            {
                "case_id": "support-refund-delay",
                "reason": "cost spike",
                "output_changed_vs_baseline": True,
                "candidate_effect": {
                    "cost": {"absolute": -0.17, "percent": -40.5},
                    "quality_score": {"absolute": 0.0, "percent": 0.0},
                },
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
                        },
                    },
                    "candidate_replay": {
                        "exec_id": "candidate-1",
                        "status": "completed",
                        "metrics": {
                            "cost": 0.25,
                            "latency_seconds": 2.7,
                            "quality_score": 1.0,
                            "tool_call_count": 2,
                        },
                    },
                },
            }
        ],
    }

    html = build_html_report(report)
    assert "Replay Lab Report: Support Replay Lab demo" in html
    assert "Candidate is cheaper" in html
    assert "observed-1" in html
    assert "candidate-1" in html

    json_path = tmp_path / "report.json"
    html_path = tmp_path / "report.html"
    json_path.write_text(json.dumps(report), encoding="utf-8")

    assert render_html_report(json_path, html_path) == html_path
    assert html_path.read_text(encoding="utf-8").startswith("<!doctype html>")
