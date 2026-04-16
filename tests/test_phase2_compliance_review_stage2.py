"""Guarded tests for the compliance review Stage 2 multi-checkpoint flow."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest
from zenml.client import Client
from zenml.enums import ArtifactSaveType

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


def test_stage2_flow_runs_domain_checkpoints_synthesis_and_saves_report(
    monkeypatch,
    primed_zenml,
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
