"""Execute an edited delivery case and record the resolved inputs in Kitaru."""

import asyncio
import json
from typing import Any, Literal
from uuid import UUID

from kitaru.client import KitaruAPIClient
from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    TextPart,
    ToolReturnPart,
    UserPromptPart,
)

from .generation import MODEL
from .generation import Scenario as EditorScenario
from .models import RunResult, Scenario
from .persistence import record_result
from .policy import Policy
from .runner import run_scenario, validate_history
from .sources import load_execution_source


class RunRequest(BaseModel):
    """An explicit edited case and agent-policy choice."""

    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=1, max_length=300)
    sourceId: UUID
    scenario: EditorScenario
    variant: Literal["fix", "control"] = "fix"
    policy: Policy | None = None


def build_execution_scenario(request: RunRequest, source: Scenario) -> Scenario:
    """Copy the source state and apply the fields reviewed in the editor."""
    editor = request.scenario
    scenario = source.model_copy(deep=True)
    scenario.name = "known-date" if editor.estimate else "missing-date"
    scenario.shipping.status = editor.status
    scenario.shipping.estimated_delivery = editor.estimate or None
    scenario.shipping.tracking_url = editor.tracking
    scenario.goal = editor.goal
    scenario.opening_message = editor.opening
    scenario.customer_known_facts = {"provided_facts": editor.knownFacts}
    scenario.customer_policy = f"Tone: {editor.tone}. Persistence: {editor.persistence}. Stay within the provided customer facts. Do not infer hidden tool evidence."
    scenario.acceptance_criteria = [editor.acceptance]
    scenario.max_agent_turns = editor.maxTurns
    if editor.start not in scenario.seed_histories:
        raise ValueError(
            "This conversation does not contain the selected starting boundary."
        )
    for name, serialized in list(scenario.seed_histories.items()):
        history = ModelMessagesTypeAdapter.validate_python(serialized)
        opening_updated = False
        for message in history:
            for part in message.parts:
                if isinstance(part, UserPromptPart) and not opening_updated:
                    part.content = editor.opening
                    opening_updated = True
                elif (
                    isinstance(part, ToolReturnPart)
                    and part.tool_name == "check_shipping"
                ):
                    part.content = scenario.shipping.model_dump()
        validate_history(history)
        scenario.seed_histories[name] = ModelMessagesTypeAdapter.dump_python(
            history, mode="json"
        )
    return scenario


def present_result(
    result: RunResult,
    editor: EditorScenario,
    *,
    source_id: str,
    session_id: UUID | None = None,
) -> dict[str, Any]:
    """Return visible messages and separate execution and date-check outcomes."""
    messages = []
    for index, message in enumerate(result.messages):
        for part in message.parts:
            historical = index < result.seed_message_count
            if isinstance(part, UserPromptPart):
                messages.append(
                    {
                        "role": "customer",
                        "text": str(part.content),
                        "historical": historical,
                    }
                )
            elif isinstance(part, ToolReturnPart):
                messages.append(
                    {
                        "role": "tool",
                        "text": f"{part.tool_name} → {json.dumps(part.content)}",
                        "historical": historical,
                    }
                )
            elif isinstance(part, TextPart) and part.content:
                messages.append(
                    {"role": "agent", "text": part.content, "historical": historical}
                )
    return {
        "runId": str(result.run_id),
        "sessionId": str(session_id) if session_id else None,
        "model": result.model_name,
        "variant": result.variant,
        "policy": {"name": result.policy_name, "prompt": result.policy_prompt}
        if result.policy_prompt is not None
        else None,
        "policyHash": result.policy_hash,
        "scenarioHash": result.scenario_hash,
        "scenario": editor.model_dump(),
        "status": result.status,
        "messages": messages,
        "agentTurns": result.agent_turns,
        "modelRequests": result.model_requests,
        "checks": result.verdict.model_dump(),
        "error": "The agent or customer continuation failed." if result.error else None,
        "sourceId": source_id,
    }


async def run(request: RunRequest, client: KitaruAPIClient) -> dict[str, Any]:
    """Run a fresh conversation and retain its source and resolved scenario."""
    source = await load_execution_source(client, request.sourceId)
    scenario = build_execution_scenario(request, source)
    result = await asyncio.wait_for(
        run_scenario(
            scenario,
            backend="openai",
            model=MODEL,
            variant=request.variant,
            custom_policy=request.policy,
            mode=request.scenario.start,
            continue_after_boundary=True,
        ),
        timeout=180,
    )
    session_id = await record_result(
        result,
        server_url=str(client.base_url),
        editor_snapshot=request.scenario.model_dump(),
        source_session_id=request.sourceId,
        title=request.title,
        continue_after_boundary=True,
        client=client,
    )
    return present_result(
        result, request.scenario, source_id=str(request.sourceId), session_id=session_id
    )
