"""Integration test for the ``kitaru.llm()`` example workflow."""

from __future__ import annotations

from typing import Any, cast

from examples.llm.flow_with_llm import run_workflow
from zenml.client import Client

from kitaru.config import register_model_alias


def test_phase12_llm_example_runs_end_to_end(
    monkeypatch,
) -> None:
    """Verify the LLM example runs with tracked metadata and artifacts."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "Mocked LLM response.")

    execution_id, result = run_workflow("kitaru")

    assert execution_id
    assert result == "Mocked LLM response."

    # Verify LLM call metadata persisted on ZenML step metadata.
    run = Client().get_pipeline_run(execution_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()
    step_llm_metadata = [
        cast(dict[str, Any], step.run_metadata["llm_calls"])
        for step in hydrated_run.steps.values()
        if "llm_calls" in step.run_metadata
    ]

    assert len(step_llm_metadata) == 2

    llm_call_names = {
        call_name for llm_calls in step_llm_metadata for call_name in llm_calls
    }
    assert "outline_call" in llm_call_names
    assert "draft_call" in llm_call_names

    for llm_calls in step_llm_metadata:
        for metadata in llm_calls.values():
            assert metadata["resolved_model"] == "openai/gpt-4o-mini"
            assert metadata["credential_source"] == "environment"
