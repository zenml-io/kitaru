"""Save executed conversations and retrieve their frozen scenario inputs."""

import hashlib
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from uuid import UUID

from kitaru.api_models.v1.agent import AgentCreateRequest, AgentListParams
from kitaru.api_models.v1.evaluation import EvaluationListParams, EvaluationResult
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionEvaluationsRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)

from .models import RunResult, Scenario

SCHEMA_VERSION = "delivery-date-run.v1"


def get_runner_revision() -> str:
    """Pin the native prompts, execution code, fixture model, and date checks."""
    directory = Path(__file__).parent
    digest = hashlib.sha256()
    for name in (
        "runner.py",
        "models.py",
        "dates.py",
        "policy.py",
        "worker_run.py",
        "evaluator.py",
    ):
        path = directory / name
        if path.exists():
            digest.update(name.encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


async def _save_evaluations(
    client: KitaruAPIClient, session_id: UUID, result: RunResult
) -> None:
    """Reconcile oracle rows even when recording was interrupted after finalization."""
    complete = result.status in {"completed", "boundary-completed"}
    expected = [
        EvaluationResult(
            name=name,
            score=value,
            passed=value,
            explanation="Fixture oracle checks ISO and English month-name dates only; not a general hallucination judge.",
        )
        if complete
        else EvaluationResult(
            name=name,
            value="unavailable",
            explanation=f"Incomplete simulation: {result.status}",
        )
        for name, value in result.verdict.model_dump().items()
    ]
    params = EvaluationListParams(
        filter=FilterCondition(
            field="session_id", op=FilterOp.EQ, value=str(session_id)
        )
    )
    existing = {row.name: row async for row in client.evaluations.iter(params)}
    missing = []
    for row in expected:
        stored = existing.get(row.name)
        if stored is None:
            missing.append(row)
        elif (stored.score, stored.value, stored.passed) != (
            row.score,
            row.value,
            row.passed,
        ):
            raise ValueError(f"Conflicting stored oracle result: {row.name}")
    if missing:
        await client.sessions.create_evaluations(
            session_id, SessionEvaluationsRequest(evaluations=missing)
        )


async def get_agent_id(client: KitaruAPIClient, name: str) -> UUID:
    """Find or create the example's recording agent."""
    params = AgentListParams(
        filter=FilterCondition(field="name", op=FilterOp.EQ, value=name)
    )
    agents = await client.agents.list(params)
    if agents.items:
        return agents.items[0].id
    try:
        agent = await client.agents.create(
            AgentCreateRequest(
                name=name,
                description="Bounded synthetic delivery conversations; no worker replay contract.",
            )
        )
        return agent.id
    except APIError as exc:
        if exc.status_code != 409:
            raise
        agents = await client.agents.list(params)
        if not agents.items:
            raise
        return agents.items[0].id


def _get_nodes(
    result: RunResult, terminal_status: NodeStatus
) -> list[SessionNodeCreateRequest]:
    """Build target-agent nodes without counting historical tools as new calls."""
    nodes = [
        SessionNodeCreateRequest(
            external_id="conversation",
            node_type=NodeType.SPAN,
            name="Delivery conversation",
            status=terminal_status,
            error=result.error if terminal_status == NodeStatus.FAILED else None,
            inputs={
                "scenario_sha256": result.scenario_hash,
                "mode": result.mode,
                "policy_name": result.policy_name,
                "policy_hash": result.policy_hash,
            },
            outputs={"status": result.status, "agent_turns": result.agent_turns},
            attributes={},
            started_at=result.started_at,
            ended_at=result.ended_at,
        )
    ]
    calls: dict[str, ToolCallPart] = {}
    for index, message in enumerate(result.messages):
        if index < result.seed_message_count:
            continue
        if isinstance(message, ModelResponse):
            text = "\n".join(
                p.content for p in message.parts if isinstance(p, TextPart)
            )
            input_selector = None
            for history_index in range(index - 1, -1, -1):
                for part_index, part in enumerate(result.messages[history_index].parts):
                    if isinstance(part, UserPromptPart) and isinstance(
                        part.content, str
                    ):
                        input_selector = (
                            f"/history/{history_index}/parts/{part_index}/content"
                        )
                        break
                if input_selector is not None:
                    break
            nodes.append(
                SessionNodeCreateRequest(
                    external_id=f"response-{index}",
                    parent_external_id="conversation",
                    node_type=NodeType.SPAN
                    if result.backend == "scripted"
                    else NodeType.LLM_CALL,
                    name="Scripted response"
                    if result.backend == "scripted"
                    else "Agent model response",
                    status=NodeStatus.COMPLETED,
                    inputs={
                        "instructions": result.policy_prompt,
                        "history": ModelMessagesTypeAdapter.dump_python(
                            result.messages[:index], mode="json"
                        ),
                    },
                    outputs={
                        "text": text,
                        "message": ModelMessagesTypeAdapter.dump_python(
                            [message], mode="json"
                        )[0],
                    },
                    input_text_selector=input_selector,
                    output_text_selector="/text" if text else None,
                    model=message.model_name if result.backend != "scripted" else None,
                    attributes={},
                )
            )
        for part in message.parts:
            if isinstance(part, ToolCallPart):
                calls[part.tool_call_id] = part
            elif isinstance(part, ToolReturnPart):
                call = calls.pop(part.tool_call_id)
                nodes.append(
                    SessionNodeCreateRequest(
                        external_id=f"tool-{part.tool_call_id}",
                        parent_external_id="conversation",
                        node_type=NodeType.TOOL_CALL,
                        name=part.tool_name,
                        tool_name=part.tool_name,
                        status=NodeStatus.COMPLETED,
                        inputs=call.args_as_dict(),
                        outputs=part.content,
                        attributes={},
                    )
                )
    return nodes


async def record_result(
    result: RunResult,
    *,
    server_url: str | None,
    agent_name: str = "delivery-date-demo",
    api_key: str | None = None,
    editor_snapshot: dict[str, Any] | None = None,
    source_session_id: UUID | None = None,
    title: str | None = None,
    continue_after_boundary: bool = False,
    client: KitaruAPIClient | None = None,
) -> UUID:
    """Persist a real execution, its resolved scenario, and narrow oracle results."""
    complete = result.status in {"completed", "boundary-completed"}
    transcript = "\n".join(f"{m.role}: {m.content}" for m in result.visible_transcript)
    outputs = result.model_dump(mode="json") | {"transcript_text": transcript}
    context = (
        nullcontext(client)
        if client is not None
        else KitaruAPIClient(base_url=server_url, api_key=api_key)
    )
    async with context as client:
        agent_id = await get_agent_id(client, agent_name)
        session = await client.sessions.create(
            SessionCreateRequest(
                agent_id=agent_id,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.IN_PROGRESS,
                name=title
                or f"{result.scenario.name}: {result.variant} / {result.mode}",
                framework="pydantic-ai",
                inputs={
                    "schema_version": SCHEMA_VERSION,
                    "scenario_snapshot": result.scenario.model_dump(mode="json"),
                    "scenario_sha256": result.scenario_hash,
                    "input_seed": result.input_seed,
                    "variant": result.variant,
                    "policy_name": result.policy_name,
                    "policy_prompt": result.policy_prompt,
                    "policy_hash": result.policy_hash,
                    "mode": result.mode,
                    "backend": result.backend,
                    "model_name": result.model_name,
                    "editor_snapshot": editor_snapshot,
                    "source_session_id": str(source_session_id)
                    if source_session_id
                    else None,
                    "continue_after_boundary": continue_after_boundary,
                    "runner_revision": get_runner_revision(),
                },
                outputs=None,
                metadata={
                    "demo": "delivery-date",
                    "backend": result.backend,
                    "scenario_sha256": result.scenario_hash,
                    "source_session_id": str(source_session_id)
                    if source_session_id
                    else None,
                    "runner_revision": get_runner_revision(),
                },
                started_at=result.started_at,
            ),
            idempotency_key=f"delivery-date-{result.run_id}",
        )
        stored = await client.sessions.get(session.id)
        if stored.status != SessionStatus.IN_PROGRESS:
            await _save_evaluations(client, session.id, result)
            return session.id
        await client.sessions.ingest_nodes(
            session.id,
            SessionNodeBatchRequest(
                nodes=_get_nodes(
                    result, NodeStatus.COMPLETED if complete else NodeStatus.FAILED
                )
            ),
        )
        await client.sessions.update(
            session.id,
            SessionUpdateRequest(
                status=SessionStatus.COMPLETED if complete else SessionStatus.FAILED,
                outputs=outputs,
                output_text_selector="/transcript_text",
                error=None
                if complete
                else result.error or f"Simulation ended with {result.status}",
                ended_at=result.ended_at,
            ),
        )
        await _save_evaluations(client, session.id, result)
        return session.id


async def load_scenario(
    *, session_id: UUID, server_url: str | None, api_key: str | None = None
) -> Scenario:
    """Read and verify an executed scenario snapshot without exporting a file."""
    async with KitaruAPIClient(base_url=server_url, api_key=api_key) as client:
        session = await client.sessions.get(session_id)
    inputs = session.inputs
    if not isinstance(inputs, dict) or inputs.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Session does not contain a delivery-date scenario snapshot")
    scenario = Scenario.model_validate(inputs.get("scenario_snapshot"))
    if scenario.calculate_hash() != inputs.get("scenario_sha256"):
        raise ValueError("Stored scenario snapshot does not match its SHA256")
    return scenario
