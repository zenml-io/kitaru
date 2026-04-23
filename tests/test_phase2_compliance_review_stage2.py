"""Guarded tests for the compliance review Stage 2 multi-checkpoint flow."""

from __future__ import annotations

import importlib
import time
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from zenml.client import Client
from zenml.enums import ArtifactSaveType

from kitaru import KitaruClient
from kitaru.client import ExecutionStatus
from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    configure_fake_claude_home,
    fake_claude_response,
    install_fake_claude_agent_sdk,
)


@pytest.fixture
def stage2_module(monkeypatch, tmp_path):
    """Import Stage 2 with a fake Claude SDK module."""
    configure_fake_claude_home(monkeypatch, tmp_path)
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules(
        "examples.compliance_review.stage_2_multi_domain",
    )
    return importlib.import_module("examples.compliance_review.stage_2_multi_domain")


def _find_artifact_by_name(
    *,
    outputs_by_step: list[dict[str, list[Any]]],
    name: str,
) -> Any:
    """Return the first artifact in step outputs with a given name."""
    for step_outputs in outputs_by_step:
        for artifacts in step_outputs.values():
            for artifact in artifacts:
                if artifact.name == name:
                    return artifact
    raise AssertionError(f"No artifact named '{name}' found in step outputs.")


def _patch_audit_company(
    monkeypatch: pytest.MonkeyPatch,
    stage2_module: Any,
    *,
    result: Any | None = None,
) -> Mock:
    """Replace stage 2's `audit_company` flow with a Mock and return it."""
    stub = result or stage2_module.ClaudeAgentResult(
        session_id="s",
        cwd=str(stage2_module.EXAMPLE_DIR),
        transcript_path="/tmp/s.jsonl",
        result="ok",
        num_turns=1,
    )
    fake_flow = Mock(run=Mock(return_value=Mock(wait=Mock(return_value=stub))))
    monkeypatch.setattr(stage2_module, "audit_company", fake_flow)
    return fake_flow


@pytest.mark.parametrize(
    ("kwargs", "expected_cache"),
    [({}, False), ({"cache": True}, True)],
    ids=["defaults-to-cache-disabled", "can-opt-into-cache"],
)
def test_stage2_run_workflow_forwards_cache_to_audit_company(
    monkeypatch,
    stage2_module,
    kwargs: dict[str, Any],
    expected_cache: bool,
) -> None:
    """Cache defaults to off for honest demos; callers can still opt in."""
    fake_flow = _patch_audit_company(monkeypatch, stage2_module)

    stage2_module.run_workflow(**kwargs)

    fake_flow.run.assert_called_once_with(stack=None, cache=expected_cache)


def test_stage2_required_result_text_surfaces_diagnostics(
    stage2_module,
) -> None:
    """The empty-result error should expose session + stop metadata."""
    empty = stage2_module.ClaudeAgentResult(
        session_id="746c3a14-session",
        cwd="/tmp",
        transcript_path="/tmp/t.jsonl",
        result=None,
        stop_reason="end_turn",
        num_turns=14,
    )

    with pytest.raises(ValueError) as exc_info:
        stage2_module._required_result_text(empty, domain="insurance")

    message = str(exc_info.value)
    assert "insurance checkpoint returned no result text" in message
    assert "session_id='746c3a14-session'" in message
    assert "stop_reason='end_turn'" in message
    assert "num_turns=14" in message


def test_stage2_run_workflow_can_opt_into_runtime_secret_environment(
    monkeypatch,
    stage2_module,
) -> None:
    """run_workflow() should forward stack and secret-env image overrides."""
    expected = stage2_module.ClaudeAgentResult(
        session_id="stage-2-test-session",
        cwd=str(stage2_module.EXAMPLE_DIR),
        transcript_path="/tmp/stage-2-test-session.jsonl",
        result="Stubbed flow result",
        num_turns=1,
    )
    fake_flow = _patch_audit_company(monkeypatch, stage2_module, result=expected)

    result = stage2_module.run_workflow(
        stack="prod-k8s",
        use_secret_environment=True,
    )

    assert result == expected
    fake_flow.run.assert_called_once_with(
        stack="prod-k8s",
        cache=False,
        image={
            "requirements": [
                stage2_module.CLAUDE_AGENT_SDK_REQUIREMENT,
                stage2_module.KITARU_REQUIREMENT,
            ],
            "secret_environment_from": [stage2_module.ANTHROPIC_SECRET_NAME],
        },
    )
    fake_flow.run.return_value.wait.assert_called_once_with()


@pytest.mark.usefixtures("primed_zenml")
def test_stage2_flow_runs_domain_checkpoints_synthesis_and_saves_report(
    monkeypatch,
    stage2_module,
) -> None:
    """The real Stage 2 flow should run five checkpoints and save the report."""
    calls: list[dict[str, Any]] = []
    domain_outputs = {
        stage2_module.HR_COMPLIANCE_PROMPT: "HR finding: parental leave gap.",
        stage2_module.IT_SECURITY_PROMPT: "IT finding: retention schedule gap.",
        stage2_module.VENDOR_CONTRACTS_PROMPT: (
            "Vendor finding: Alpha passes; Beta lacks indemnification and cap."
        ),
        stage2_module.INSURANCE_PROMPT: "Insurance finding: cyber coverage gap.",
    }
    final_report = (
        "# Acme Corp Compliance Report\n\n"
        "Overall risk score: 7/10.\n\n"
        "- HR: parental leave gap.\n"
        "- IT Security: retention schedule gap.\n"
        "- Vendor Contracts: Beta contract gaps.\n"
        "- Insurance: missing cyber coverage.\n"
    )

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        cwd: Path,
    ) -> dict[str, Any]:
        call_index = len(calls)
        calls.append({"prompt": prompt, "allowed_tools": allowed_tools, "cwd": cwd})
        result = domain_outputs.get(prompt, final_report)
        return fake_claude_response(
            prompt=prompt,
            cwd=cwd,
            session_id=f"stage-2-test-session-{call_index}",
            result=result,
        )

    monkeypatch.setattr(stage2_module, "run_agent_turn", fake_run_agent_turn)

    handle = stage2_module.audit_company.run()
    result = handle.wait()

    assert isinstance(result, stage2_module.ClaudeAgentResult)
    assert result.result == final_report
    assert [call["prompt"] for call in calls[:4]] == [
        stage2_module.HR_COMPLIANCE_PROMPT,
        stage2_module.IT_SECURITY_PROMPT,
        stage2_module.VENDOR_CONTRACTS_PROMPT,
        stage2_module.INSURANCE_PROMPT,
    ]
    assert len(calls) == 5
    synthesis_prompt = calls[4]["prompt"]
    assert "HR finding: parental leave gap." in synthesis_prompt
    assert "IT finding: retention schedule gap." in synthesis_prompt
    assert "Vendor finding: Alpha passes" in synthesis_prompt
    assert "Insurance finding: cyber coverage gap." in synthesis_prompt
    assert {tuple(call["allowed_tools"]) for call in calls} == {
        tuple(stage2_module.DEFAULT_ALLOWED_TOOLS)
    }
    assert {call["cwd"] for call in calls} == {stage2_module.EXAMPLE_DIR}

    run = Client().get_pipeline_run(handle.exec_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()
    assert list(hydrated_run.steps) == [
        "check_hr_compliance",
        "check_it_security",
        "check_vendor_contracts",
        "check_insurance",
        "synthesize_report",
    ]

    synthesis_step = hydrated_run.steps["synthesize_report"]
    assert synthesis_step.run_metadata["domain"] == "synthesis"
    assert synthesis_step.run_metadata["artifact"] == stage2_module.REPORT_ARTIFACT_NAME

    saved_report_artifact = _find_artifact_by_name(
        outputs_by_step=[step.outputs for step in hydrated_run.steps.values()],
        name=stage2_module.REPORT_ARTIFACT_NAME,
    )
    assert saved_report_artifact.save_type == ArtifactSaveType.MANUAL


@pytest.mark.usefixtures("primed_zenml")
def test_stage2_replay_from_checkpoint_completes_independently(
    monkeypatch,
    stage2_module,
) -> None:
    """Replay from ``check_insurance`` should produce a distinct completed run.

    This pins the Stage 2 README's replay claim at the machinery level: the
    replay endpoint accepts ``from_="check_insurance"`` against a Stage 2
    execution, yields a new execution id, and reaches a terminal COMPLETED
    state. The cache-hit semantics (HR/IT/vendor reuse vs. re-execution) are
    governed by ZenML step caching and live override inputs, which this test
    does not exercise; see ``test_phase16_replay_example.py`` for that shape.
    """
    calls: list[dict[str, Any]] = []
    domain_outputs = {
        stage2_module.HR_COMPLIANCE_PROMPT: "HR finding: parental leave gap.",
        stage2_module.IT_SECURITY_PROMPT: "IT finding: retention schedule gap.",
        stage2_module.VENDOR_CONTRACTS_PROMPT: (
            "Vendor finding: Alpha passes; Beta has gaps."
        ),
        stage2_module.INSURANCE_PROMPT: "Insurance finding: cyber coverage gap.",
    }
    final_report = "# Replay synthesis report"

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        cwd: Path,
    ) -> dict[str, Any]:
        call_index = len(calls)
        calls.append({"prompt": prompt, "allowed_tools": allowed_tools, "cwd": cwd})
        result = domain_outputs.get(prompt, final_report)
        return fake_claude_response(
            prompt=prompt,
            cwd=cwd,
            session_id=f"stage-2-replay-session-{call_index}",
            result=result,
        )

    monkeypatch.setattr(stage2_module, "run_agent_turn", fake_run_agent_turn)

    first_handle = stage2_module.audit_company.run()
    first_result = first_handle.wait()
    assert isinstance(first_result, stage2_module.ClaudeAgentResult)

    client = KitaruClient()
    replayed = client.executions.replay(
        first_handle.exec_id,
        from_="check_insurance",
    )
    assert replayed.exec_id != first_handle.exec_id

    deadline = time.time() + 120
    while True:
        execution = client.executions.get(replayed.exec_id)
        if execution.status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            break
        if time.time() > deadline:
            raise TimeoutError("Stage 2 replay did not complete within 120s.")
        time.sleep(0.5)

    assert execution.status == ExecutionStatus.COMPLETED
