"""Resolve captured delivery inputs without guessing missing business state."""

from typing import Any
from uuid import UUID

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.client import KitaruAPIClient
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ToolReturnPart,
    UserPromptPart,
)

from .generation import Scenario as EditorScenario
from .models import RunResult, Scenario
from .persistence import SCHEMA_VERSION
from .runner import validate_history


def resolve_capture(
    session: SessionDetailResponse,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Find one canonical delivery capture in recorded or imported inputs."""
    inputs = session.inputs
    if isinstance(inputs, dict) and inputs.get("schema_version") == SCHEMA_VERSION:
        return inputs, session.outputs if isinstance(session.outputs, dict) else None
    turns = inputs.get("turns", []) if isinstance(inputs, dict) else []
    captures = [
        turn
        for turn in turns
        if isinstance(turn, dict)
        and isinstance(turn.get("inputs"), dict)
        and turn["inputs"].get("schema_version") == SCHEMA_VERSION
    ]
    if len(captures) != 1:
        raise ValueError(
            "Select a delivery session with a captured scenario and tool state."
        )
    return captures[0]["inputs"], captures[0].get("outputs")


def validate_capture(inputs: dict[str, Any]) -> Scenario:
    """Check the canonical snapshot and its content hash."""
    scenario = Scenario.model_validate(inputs.get("scenario_snapshot"))
    if scenario.calculate_hash() != inputs.get("scenario_sha256"):
        raise ValueError("The captured scenario does not match its content hash.")
    return scenario


def derive_seed_histories(scenario: Scenario, outputs: dict[str, Any] | None) -> None:
    """Use captured native messages at complete tool and user boundaries."""
    scenario.seed_histories = {"full": []}
    if not outputs or "messages" not in outputs:
        return
    messages = ModelMessagesTypeAdapter.validate_python(outputs["messages"])
    seeds: dict[str, list[dict[str, Any]]] = {"full": []}
    for index, message in enumerate(messages):
        if isinstance(message, ModelRequest):
            if any(isinstance(part, ToolReturnPart) for part in message.parts):
                prefix = messages[: index + 1]
                validate_history(prefix)
                seeds["tool-boundary"] = ModelMessagesTypeAdapter.dump_python(
                    prefix, mode="json"
                )
                break
    for index in range(len(messages) - 1, 0, -1):
        message = messages[index]
        if isinstance(message, ModelRequest) and any(
            isinstance(part, UserPromptPart) for part in message.parts
        ):
            prefix = messages[: index + 1]
            validate_history(prefix)
            seeds["n-minus-one"] = ModelMessagesTypeAdapter.dump_python(
                prefix, mode="json"
            )
            break
    scenario.seed_histories = seeds


def to_editor(scenario: Scenario, inputs: dict[str, Any]) -> EditorScenario:
    """Show captured scenario fields in the guided editor."""
    if isinstance(inputs.get("editor_snapshot"), dict):
        return EditorScenario.model_validate(inputs["editor_snapshot"])
    return EditorScenario.model_validate(
        {
            "goal": scenario.goal,
            "knownFacts": "\n".join(scenario.customer_known_facts.values()),
            "opening": scenario.opening_message,
            "tone": "frustrated"
            if "frustrated" in scenario.customer_policy.lower()
            else "calm",
            "persistence": "asks-once",
            "status": scenario.shipping.status,
            "estimate": scenario.shipping.estimated_delivery or "",
            "tracking": scenario.shipping.tracking_url,
            "acceptance": "\n".join(scenario.acceptance_criteria),
            "maxTurns": min(scenario.max_agent_turns, 5),
            "start": "full",
        }
    )


async def load_case(client: KitaruAPIClient, session_id: UUID) -> dict[str, Any]:
    """Load the source evidence and editable draft for one delivery session."""
    session = await client.sessions.get(session_id)
    inputs, outputs = resolve_capture(session)
    native = validate_capture(inputs)
    editor = to_editor(native, inputs)
    preview = None
    if outputs and "messages" in outputs:
        from .simulation import present_result

        result = RunResult.model_validate(outputs)
        preview = present_result(result, editor, source_id=str(session_id))
    return {
        "id": str(session.id),
        "title": session.name or native.name,
        "sourceLabel": "Langfuse session"
        if session.imported_from == "langfuse"
        else "Recorded session",
        "sourceKind": "imported" if session.imported_from else "recorded",
        "scenario": editor.model_dump(),
        "preview": preview,
    }


async def load_execution_source(client: KitaruAPIClient, source_id: UUID) -> Scenario:
    """Read and validate the source snapshot and captured restart histories."""
    session = await client.sessions.get(source_id)
    inputs, outputs = resolve_capture(session)
    scenario = validate_capture(inputs)
    derive_seed_histories(scenario, outputs)
    return scenario
