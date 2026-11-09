#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Reply text selectors on recorded OpenAI Agents model calls."""

import uuid
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest
from agents import (
    Agent,
    AgentOutputSchemaBase,
    Handoff,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    RunConfig,
    Tool,
    TResponseInputItem,
    Usage,
)
from openai.types.responses import (
    ResponseOutputMessage,
    ResponseOutputRefusal,
    ResponseOutputText,
    ResponseReasoningItem,
)
from openai.types.responses.response_prompt_param import ResponsePromptParam
from openai.types.responses.response_reasoning_item import Content, Summary

import kitaru_openai_agents.recording as recording_module
from kitaru.api_models.v1.session_node import NodeType
from kitaru.json_pointer import resolve_json_pointer
from kitaru_openai_agents import KitaruRunner

from .conftest import DeterministicModel

LONG_REPLY = "x" * (recording_module.MAX_STRING_BYTES + 1)


class _RecordingClient:
    """Keep the node batches a run writes."""

    instances: ClassVar[list["_RecordingClient"]] = []

    def __init__(self) -> None:
        self.batches: list[Any] = []
        self.sessions = self
        type(self).instances.append(self)

    async def create(self, request: Any) -> Any:
        return SimpleNamespace(id=uuid.uuid4())

    async def ingest_nodes(self, session_id: uuid.UUID, request: Any) -> list[Any]:
        self.batches.append(request)
        return []

    async def update(self, session_id: uuid.UUID, request: Any) -> None:
        return None

    async def close(self) -> None:
        return None


class _OutputModel(DeterministicModel):
    """Answer with a fixed list of Responses API output parts."""

    def __init__(self, parts: list[Any]) -> None:
        super().__init__()
        self.parts = parts

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: ResponsePromptParam | None,
    ) -> ModelResponse:
        reasoning = ResponseReasoningItem(
            id="reasoning-1",
            type="reasoning",
            summary=[
                Summary(text="first thought", type="summary_text"),
                Summary(text=LONG_REPLY, type="summary_text"),
            ],
            content=[Content(text="raw thought", type="reasoning_text")],
        )
        message = ResponseOutputMessage(
            id="message-1",
            content=self.parts,
            role="assistant",
            status="completed",
            type="message",
        )
        return ModelResponse(
            output=[reasoning, message], usage=Usage(), response_id="response-1"
        )


def _text(text: str) -> ResponseOutputText:
    return ResponseOutputText(
        annotations=[], text=text, type="output_text", logprobs=[]
    )


@pytest.mark.parametrize(
    ("parts", "selector", "reply"),
    [
        ([_text("Shipped.")], "/1/content/0/text", "Shipped."),
        (
            [ResponseOutputRefusal(refusal="I can't help.", type="refusal")],
            "/1/content/0/refusal",
            "I can't help.",
        ),
        ([_text(LONG_REPLY)], "/1/content/0/text/value", LONG_REPLY[:-1]),
        ([_text("First."), _text("Second.")], None, None),
    ],
    ids=["one-text-part", "refusal", "truncated-text", "several-text-parts"],
)
async def test_model_call_points_at_its_only_reply_part(
    monkeypatch: pytest.MonkeyPatch,
    parts: list[Any],
    selector: str | None,
    reply: str | None,
) -> None:
    monkeypatch.setattr(recording_module, "KitaruAPIClient", _RecordingClient)
    _RecordingClient.instances.clear()
    agent = Agent[None](name="support", model=_OutputModel(parts))

    await KitaruRunner(agent_id=uuid.uuid4()).run(
        agent,
        "Where is ORD-1007?",
        run_config=RunConfig(tracing_disabled=True),
        # A refusal raises unless the application handles it.
        error_handlers={"model_refusal": lambda _: "refused"},
    )

    model = next(
        node
        for batch in _RecordingClient.instances[0].batches
        for node in batch.nodes
        if node.node_type is NodeType.LLM_CALL
    )
    assert model.output_text_selector == selector
    if selector is not None:
        assert resolve_json_pointer(model.outputs, selector) == (True, reply)
    reasoning = [
        resolve_json_pointer(model.outputs, pointer)[1]
        for pointer in model.reasoning_selectors
    ]
    # The long summary was shortened, so its pointer reaches into the wrapper.
    assert reasoning == ["first thought", LONG_REPLY[:-1], "raw thought"]
