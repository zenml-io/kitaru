"""Execute one task-authenticated conversation for a native Kitaru replay."""

import asyncio
import os
from typing import Any
from uuid import UUID

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
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
from kitaru.api_models.v1.task import AgentTaskDetails
from kitaru.client import KitaruAPIClient
from kitaru.task.task_io import write_task_result
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)

from .models import RunResult, Scenario
from .persistence import SCHEMA_VERSION, get_runner_revision
from .policy import Policy
from .runner import run_scenario, validate_history


def read_worker_inputs(inputs: Any) -> tuple[Scenario, str, bool]:
    """Validate the complete frozen replay input before calling either model."""
    if not isinstance(inputs, dict) or inputs.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Task input is not a recorded delivery case")
    scenario = Scenario.model_validate(inputs.get("scenario_snapshot"))
    if scenario.calculate_hash() != inputs.get("scenario_sha256"):
        raise ValueError("Task scenario hash differs from its recorded snapshot")
    if inputs.get("runner_revision") != get_runner_revision():
        raise ValueError("Task case uses a different runner revision")
    mode = inputs.get("mode")
    if mode not in {"full", "n-minus-one", "tool-boundary"}:
        raise ValueError("Invalid recorded start mode")
    seed = scenario.seed_histories.get(mode)
    if seed is None or seed != inputs.get("input_seed"):
        raise ValueError("Task seed differs from the frozen native history")
    validate_history(ModelMessagesTypeAdapter.validate_python(seed))
    continuation = inputs.get("continue_after_boundary", False)
    if type(continuation) is not bool:
        raise ValueError("Invalid recorded continuation flag")
    return scenario, mode, continuation


def get_conversation_nodes(result: RunResult) -> list[SessionNodeCreateRequest]:
    """Record each newly executed assistant response and mocked shipping call."""
    nodes = []
    calls: dict[str, ToolCallPart] = {}
    for index, message in enumerate(
        result.messages[result.seed_message_count :], start=result.seed_message_count
    ):
        if isinstance(message, ModelResponse):
            text = "\n".join(
                part.content for part in message.parts if isinstance(part, TextPart)
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
                    node_type=NodeType.LLM_CALL,
                    name="Agent model response",
                    status=NodeStatus.COMPLETED,
                    inputs={
                        "history": ModelMessagesTypeAdapter.dump_python(
                            result.messages[:index], mode="json"
                        ),
                        "instructions": result.policy_prompt,
                    },
                    input_text_selector=input_selector,
                    outputs={"text": text},
                    output_text_selector="/text" if text else None,
                    model=message.model_name,
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


async def execute_task(client: KitaruAPIClient, task_id: UUID) -> UUID:
    """Create exactly one task-linked replay recording and finalize its execution."""
    spec = await client.tasks.get_spec(task_id)
    if not isinstance(spec.details, AgentTaskDetails) or spec.details.replay_id is None:
        raise ValueError("Delivery worker requires an agent replay task")
    inputs = spec.details.inputs
    scenario, mode, continuation = read_worker_inputs(inputs)
    policy = Policy.model_validate_json(os.environ["DELIVERY_POLICY_JSON"])
    revision = os.environ["DELIVERY_RUNNER_REVISION"]
    if revision != get_runner_revision():
        raise ValueError("Registered agent version uses a different runner revision")
    session = await client.sessions.create(
        SessionCreateRequest(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.IN_PROGRESS,
            inputs=inputs,
            outputs=None,
            name=policy.name,
            framework="pydantic-ai",
            metadata={
                "demo": "delivery-date",
                "policy_hash": policy.calculate_hash(),
                "runner_revision": revision,
            },
        ),
        idempotency_key=f"delivery-task-{task_id}",
    )
    stored = await client.sessions.get(session.id)
    if stored.status == SessionStatus.COMPLETED:
        previous = RunResult.model_validate(stored.outputs)
        if (
            previous.status not in {"completed", "boundary-completed"}
            or previous.policy_hash != policy.calculate_hash()
            or previous.policy_prompt != policy.prompt
            or previous.policy_name != policy.name
            or previous.backend != "openai"
            or previous.model_name != os.environ.get("DELIVERY_MODEL", "gpt-6-luna")
            or previous.mode != mode
            or previous.scenario_hash != scenario.calculate_hash()
            or previous.input_seed != inputs["input_seed"]
        ):
            raise ValueError(
                "Existing completed replay differs from its task configuration"
            )
        write_task_result({"session_id": str(session.id)})
        return session.id
    if stored.status == SessionStatus.FAILED or stored.metadata.get(
        "execution_started"
    ):
        raise RuntimeError("Existing replay execution cannot be safely repeated")
    await client.sessions.update(
        session.id,
        SessionUpdateRequest(metadata=stored.metadata | {"execution_started": True}),
    )
    try:
        result = await run_scenario(
            scenario,
            mode=mode,
            variant="fix",
            backend="openai",
            model=os.environ.get("DELIVERY_MODEL", "gpt-6-luna"),
            continue_after_boundary=continuation,
            custom_policy=policy,
        )
        complete = result.status in {"completed", "boundary-completed"}
        await client.sessions.ingest_nodes(
            session.id, SessionNodeBatchRequest(nodes=get_conversation_nodes(result))
        )
        transcript = "\n".join(
            f"{message.role}: {message.content}"
            for message in result.visible_transcript
        )
        await client.sessions.update(
            session.id,
            SessionUpdateRequest(
                status=SessionStatus.COMPLETED if complete else SessionStatus.FAILED,
                outputs=result.model_dump(mode="json")
                | {"transcript_text": transcript},
                output_text_selector="/transcript_text",
                ended_at=result.ended_at,
                error=None if complete else result.error or result.status,
            ),
        )
        if not complete:
            raise RuntimeError(f"Simulation incomplete: {result.status}")
    except Exception as exc:
        await client.sessions.update(
            session.id,
            SessionUpdateRequest(status=SessionStatus.FAILED, error=str(exc)),
        )
        raise
    write_task_result({"session_id": str(session.id)})
    return session.id


async def main() -> None:
    """Authenticate with the worker-provided task token and execute its replay."""
    token = os.environ["KITARU_API_TOKEN"]
    async with KitaruAPIClient(
        base_url=os.environ["KITARU_API_URL"], api_key=token
    ) as client:
        await execute_task(client, UUID(os.environ["KITARU_TASK_ID"]))


if __name__ == "__main__":
    os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")
    asyncio.run(main())
