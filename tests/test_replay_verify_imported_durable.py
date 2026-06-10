"""Tests for the durable (flow-wrapped) imported-input verifier demo."""

import json
from pathlib import Path

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
