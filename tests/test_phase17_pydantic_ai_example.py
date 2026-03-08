"""Integration test for the Phase 17 PydanticAI adapter example."""

from __future__ import annotations

import pytest

pytest.importorskip("pydantic_ai")

from examples.pydantic_ai_adapter import run_workflow


def test_phase17_pydantic_ai_example_runs_end_to_end() -> None:
    """Verify wrapped-agent runs produce child-event metadata."""
    execution_id, result, child_events, run_summaries = run_workflow(topic="kitaru")

    assert execution_id
    assert isinstance(result, str)
    assert child_events
    assert any(event.get("type") == "llm_call" for event in child_events.values())
    assert any(event.get("type") == "tool_call" for event in child_events.values())
    assert run_summaries
