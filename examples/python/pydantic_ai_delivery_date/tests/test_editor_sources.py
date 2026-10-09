"""Captured evidence, source identity, and native restart contracts."""

import asyncio
from uuid import uuid4

import pytest
from kitaru.api_models.v1.session import SessionDetailResponse

from delivery_date.models import get_scenario
from delivery_date.runner import run_scenario
from delivery_date.simulation import (
    RunRequest,
    build_execution_scenario,
    present_result,
)
from delivery_date.sources import (
    derive_seed_histories,
    resolve_capture,
    to_editor,
    validate_capture,
)


@pytest.fixture
def snapshot():
    scenario = get_scenario("missing-date")
    return {
        "schema_version": "delivery-date-run.v1",
        "scenario_snapshot": scenario.model_dump(mode="json"),
        "scenario_sha256": scenario.calculate_hash(),
    }


def test_imported_capture_preserves_exact_root_inputs(snapshot):
    session = SessionDetailResponse.model_construct(
        inputs={
            "schema_version": 1,
            "turns": [
                {
                    "source_trace_id": "trace-1",
                    "inputs": snapshot,
                    "outputs": {"messages": []},
                }
            ],
        },
        outputs=None,
    )
    inputs, outputs = resolve_capture(session)
    assert inputs == snapshot
    assert outputs == {"messages": []}
    assert validate_capture(inputs).shipping.estimated_delivery is None


def test_import_without_scenario_is_rejected_without_inventing_state():
    session = SessionDetailResponse.model_construct(
        inputs={"turns": [{"inputs": {"customer": "hello"}}]}, outputs=None
    )
    with pytest.raises(ValueError, match="captured scenario"):
        resolve_capture(session)


def test_multiple_captures_are_not_silently_combined(snapshot):
    session = SessionDetailResponse.model_construct(
        inputs={"turns": [{"inputs": snapshot}, {"inputs": snapshot}]}, outputs=None
    )
    with pytest.raises(ValueError, match="captured scenario"):
        resolve_capture(session)


def test_changed_capture_hash_is_rejected(snapshot):
    snapshot["scenario_snapshot"]["shipping"]["status"] = "Delivered"
    with pytest.raises(ValueError, match="content hash"):
        validate_capture(snapshot)


def test_new_conversation_needs_no_captured_messages(snapshot):
    source = validate_capture(snapshot)
    derive_seed_histories(source, None)
    request = RunRequest(
        title="New conversation", sourceId=uuid4(), scenario=to_editor(source, snapshot)
    )
    assert build_execution_scenario(request, source).seed_histories == {"full": []}
    request.scenario.start = "tool-boundary"
    with pytest.raises(ValueError, match="starting boundary"):
        build_execution_scenario(request, source)


def test_edited_fixture_is_used_in_captured_tool_boundary(snapshot):
    scenario = validate_capture(snapshot)
    from pydantic_ai.messages import ModelMessagesTypeAdapter

    from delivery_date.runner import build_seed

    scenario.seed_histories = {
        mode: ModelMessagesTypeAdapter.dump_python(
            build_seed(scenario, mode), mode="json"
        )
        for mode in ("full", "n-minus-one", "tool-boundary")
    }
    editor = to_editor(scenario, snapshot).model_copy(
        update={"estimate": "2026-10-09", "start": "tool-boundary"}
    )
    native = build_execution_scenario(
        RunRequest(title="new evidence", sourceId=uuid4(), scenario=editor), scenario
    )
    tool_return = native.seed_histories["tool-boundary"][-1]["parts"][0]["content"]
    assert tool_return["estimated_delivery"] == "2026-10-09"
    assert scenario.shipping.estimated_delivery is None


def test_restart_uses_actual_followup_and_excludes_original_final_answer(snapshot):
    async def check():
        result = await run_scenario(
            get_scenario("missing-date"), backend="scripted", variant="fix"
        )
        native = result.scenario.model_copy(deep=True)
        captured = result.model_dump(mode="json")
        captured["messages"][-1]["parts"][0]["content"] = "ORIGINAL FINAL ANSWER"
        derive_seed_histories(native, captured)
        seed = native.seed_histories["n-minus-one"]
        assert seed[-1]["parts"][0]["content"] == result.visible_transcript[-2].content
        assert "ORIGINAL FINAL ANSWER" not in str(seed)
        assert (
            native.seed_histories["tool-boundary"][-1]["parts"][0]["part_kind"]
            == "tool-return"
        )
        editor = to_editor(native, snapshot)
        displayed = present_result(result, editor, source_id="source")
        assert displayed["status"] == "completed"
        assert [row["role"] for row in displayed["messages"]] == [
            "customer",
            "tool",
            "agent",
            "customer",
            "agent",
        ]

    asyncio.run(check())
