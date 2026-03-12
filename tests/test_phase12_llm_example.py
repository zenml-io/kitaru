"""Integration test for the Phase 12 `kitaru.llm()` example workflow."""

from __future__ import annotations

from examples.llm.flow_with_llm import run_workflow

from kitaru.config import register_model_alias


def test_phase12_llm_example_runs_end_to_end(
    monkeypatch,
) -> None:
    """Verify the LLM example runs with tracked metadata and artifacts."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "Mocked LLM response.")

    execution_id, result, step_llm_metadata = run_workflow("kitaru")

    assert execution_id
    assert result == "Mocked LLM response."
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
