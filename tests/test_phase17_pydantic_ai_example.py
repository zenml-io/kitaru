"""Integration test for the PydanticAI adapter example."""

from __future__ import annotations

from typing import Any, cast

import pytest
from zenml.client import Client

pytest.importorskip("pydantic_ai")

from examples.pydantic_ai_agent.pydantic_ai_adapter import run_workflow


def test_phase17_pydantic_ai_example_runs_end_to_end() -> None:
    """Verify wrapped-agent runs produce child-event metadata."""
    execution_id, result = run_workflow(topic="kitaru")

    assert execution_id
    assert isinstance(result, str)

    # Verify PydanticAI adapter metadata persisted on ZenML step metadata.
    run = Client().get_pipeline_run(execution_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()

    child_events: dict[str, Any] = {}
    run_summaries: dict[str, Any] = {}

    for step in hydrated_run.steps.values():
        events_metadata = step.run_metadata.get("pydantic_ai_events")
        if isinstance(events_metadata, dict):
            child_events.update(cast(dict[str, Any], events_metadata))

        summaries_metadata = step.run_metadata.get("pydantic_ai_run_summaries")
        if isinstance(summaries_metadata, dict):
            run_summaries.update(cast(dict[str, Any], summaries_metadata))

    assert child_events
    assert any(event.get("type") == "llm_call" for event in child_events.values())
    assert any(event.get("type") == "tool_call" for event in child_events.values())
    assert run_summaries
