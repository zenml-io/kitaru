"""Tests for the durable (flow-wrapped) imported-input verifier demo."""

import json
from pathlib import Path

import pytest

_FIXTURE = Path(
    "examples/replay_verify_imported_cases/fixtures/"
    "support_copilot_imported_cases.jsonl"
)


def _fixture_rows() -> list[dict]:
    return [
        json.loads(line) for line in _FIXTURE.read_text().splitlines() if line.strip()
    ]


def _eligible_row() -> dict:
    return next(
        row for row in _fixture_rows() if row["case_id"] == "rv-model-only-eligible"
    )


def _invocation_payload(row: dict, role: str) -> dict:
    from examples.replay_verify_imported_cases.prompt_config import BASELINE_CONFIG

    from kitaru._replay_verify_imported_models import (
        DEFAULT_COMPARISON_FIELDS,
        IMPORTED_INPUT_EXECUTION_MODE,
        imported_case_from_mapping,
    )

    case = imported_case_from_mapping(row)
    return {
        "case_id": case.case_id,
        "role": role,
        "runner_id": role,
        "root_input": case.root_input,
        "available_tools": list(case.trace_contract.available_tools or ()),
        "config": {**BASELINE_CONFIG, "agent_id": "support-copilot-v1"},
        "comparison_fields": list(DEFAULT_COMPARISON_FIELDS),
        "execution_mode": IMPORTED_INPUT_EXECUTION_MODE,
    }


def test_execute_lane_payload_returns_output_envelope() -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        execute_lane_payload,
    )

    row = _eligible_row()
    envelope = execute_lane_payload(
        role="baseline",
        runner_mode="deterministic",
        case_payload=row,
        invocation_payload=_invocation_payload(row, "baseline"),
    )

    assert set(envelope) == {"output"}
    output = envelope["output"]
    assert output["payload"]["policy_label"]
    assert output["payload"]["risk_status"]
    assert output["unsafe_live_execution_count"] == 0


def test_execute_lane_payload_returns_error_envelope_instead_of_raising() -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        execute_lane_payload,
    )

    row = _eligible_row()
    envelope = execute_lane_payload(
        role="baseline",
        runner_mode="no-such-mode",
        case_payload=row,
        invocation_payload=_invocation_payload(row, "baseline"),
    )

    assert set(envelope) == {"error"}
    assert "no-such-mode" in envelope["error"]


def test_lane_id_is_sanitized_and_unique_per_role() -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import lane_id

    assert lane_id("baseline", "rv-model-only-eligible") == (
        "baseline_rv_model_only_eligible"
    )
    assert lane_id("candidate", "rv-model-only-eligible") != lane_id(
        "baseline", "rv-model-only-eligible"
    )


def _manual_artifact_names(exec_id: str) -> set[str]:
    from zenml.client import Client
    from zenml.enums import ArtifactSaveType

    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    hydrated = run.get_hydrated_version()
    names: set[str] = set()
    for step in hydrated.steps.values():
        for output_name, artifacts in step.outputs.items():
            for artifact in artifacts:
                if artifact.save_type == ArtifactSaveType.MANUAL:
                    names.add(output_name)
    return names


def test_durable_flow_runs_cohort_and_persists_artifacts(primed_zenml) -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        run_durable_demo,
    )

    result = run_durable_demo()

    summary = result["summary"]
    assert summary["overall_verdict"] == "hold"
    assert summary["imported_count"] == 8
    assert summary["eligible_count"] == 4
    assert summary["stopped_count"] == 4
    assert summary["candidate_executions_for_stopped_cases"] == 0
    assert summary["unsafe_live_execution_count"] == 0
    # Regression: the durable flow must validate against the same SAFE_TOOL_NAMES
    # registry as the non-durable demo. With the broader default registry this
    # stop reason disappears (send_email is allowed there) and the two demo
    # paths diverge on which tools may run.
    assert (
        "unknown_tool:send_email"
        in summary["stopped_case_reasons"]["rv-unsafe-live-write-stopped"]
    )

    artifact_names = _manual_artifact_names(result["exec_id"])
    assert {
        "imported_cases",
        "fidelity_report",
        "verification_report",
        "verification_report_html",
    } <= artifact_names


def test_durable_flow_creates_no_lane_checkpoints_for_stopped_cases(
    primed_zenml,
) -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        run_durable_demo,
    )
    from zenml.client import Client

    result = run_durable_demo()
    run = Client().get_pipeline_run(result["exec_id"], allow_name_prefix_match=False)
    step_names = set(run.get_hydrated_version().steps.keys())

    # Stopped fixture cases must never get a lane checkpoint, in either role.
    assert not any("rv_missing_output_stopped" in name for name in step_names)
    assert not any("rv_unsafe_live_write_stopped" in name for name in step_names)
    # Eligible cases get both lanes.
    assert any(name.endswith("baseline_rv_model_only_eligible") for name in step_names)
    assert any(name.endswith("candidate_rv_model_only_eligible") for name in step_names)


def test_second_run_with_changed_candidate_reuses_baseline_lanes(
    primed_zenml,
) -> None:
    from examples.replay_verify_imported_cases.durable_verify_flow import (
        run_durable_demo,
    )
    from zenml.client import Client
    from zenml.enums import ExecutionStatus

    first = run_durable_demo(candidate="support-copilot-v2")
    second = run_durable_demo(candidate="support-copilot-v3")

    run = Client().get_pipeline_run(second["exec_id"], allow_name_prefix_match=False)
    steps = run.get_hydrated_version().steps
    baseline_statuses = {
        name: step.status for name, step in steps.items() if "baseline_rv_" in name
    }
    candidate_statuses = {
        name: step.status for name, step in steps.items() if "candidate_rv_" in name
    }

    assert baseline_statuses, "expected baseline lane steps in the second run"
    assert all(
        status == ExecutionStatus.CACHED for status in baseline_statuses.values()
    ), f"baseline lanes were not cached: {baseline_statuses}"
    assert candidate_statuses and not any(
        status == ExecutionStatus.CACHED for status in candidate_statuses.values()
    ), f"candidate lanes unexpectedly cached: {candidate_statuses}"
    assert first["summary"]["overall_verdict"] == second["summary"]["overall_verdict"]


def test_durable_cli_parses_args_and_rejects_unknown_mode() -> None:
    from examples.replay_verify_imported_cases.run_durable_demo import parse_args

    args = parse_args(["--runner", "deterministic"])
    assert args.runner == "deterministic"
    assert args.baseline_model is None

    with pytest.raises(SystemExit):
        parse_args(["--runner", "nonsense"])
