#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Public OpenAI Agents SDK fakes for adapter contract tests."""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from agents import (
    Agent,
    AgentOutputSchemaBase,
    Handoff,
    Model,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    Tool,
    TResponseInputItem,
    Usage,
    set_tracing_disabled,
)
from openai.types.responses import ResponseOutputMessage, ResponseOutputText
from openai.types.responses.response_prompt_param import ResponsePromptParam


class DeterministicModel(Model):
    """Return one public Responses API message without provider I/O."""

    def __init__(self, output: str = "deterministic result") -> None:
        self.output = output
        self.running_loops: list[Any] = []

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
        """Return a deterministic final assistant message."""
        import asyncio

        self.running_loops.append(asyncio.get_running_loop())
        return ModelResponse(
            output=[
                ResponseOutputMessage(
                    id=f"message-{len(self.running_loops)}",
                    content=[
                        ResponseOutputText(
                            annotations=[],
                            text=self.output,
                            type="output_text",
                            logprobs=[],
                        )
                    ],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ],
            usage=Usage(),
            response_id=f"response-{len(self.running_loops)}",
        )

    def stream_response(
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
    ) -> AsyncIterator[Any]:
        """Reject streaming because the first adapter release is non-streaming."""
        raise AssertionError("The deterministic model must not be streamed")


@pytest.fixture(autouse=True)
def _isolate_process_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    set_tracing_disabled(True)
    yield
    set_tracing_disabled(False)


@pytest.fixture
def deterministic_model() -> DeterministicModel:
    """Create a deterministic implementation of the public model interface."""
    return DeterministicModel()


@pytest.fixture
def deterministic_agent(deterministic_model: DeterministicModel) -> Agent[None]:
    """Create an agent backed by the deterministic public model fake."""
    return Agent(name="deterministic", model=deterministic_model)
