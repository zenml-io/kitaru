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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Helpers for the model-swap analyzer tests."""

import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_langfuse_importer.importer import LangfuseJSONLImporter

REPO_ROOT = Path(__file__).resolve().parents[4]
QUICKSTART_TRACES = (
    REPO_ROOT
    / "examples/python/pydantic_ai_ticket_resolver/traces/langfuse-traces.jsonl"
)
NOW = datetime(2026, 10, 20, tzinfo=UTC)
OWNER_ID = uuid.UUID("01990000-0000-7000-8000-000000000001")
AGENT_ID = uuid.UUID("01990000-0000-7000-8000-000000000002")
IMPORT_ID = uuid.UUID("01990000-0000-7000-8000-000000000003")


def to_response(imported: ImportedSession, number: int) -> SessionWithNodesResponse:
    """Store an imported session the way the server returns it."""
    session_id = uuid.uuid5(IMPORT_ID, imported.external_id)
    nodes = [
        SessionNodeResponse.model_validate(
            {
                **create.model_dump(),
                "id": uuid.uuid5(session_id, create.external_id),
                "session_id": session_id,
            }
        )
        for create in flatten_nodes(imported.nodes)
    ]
    return SessionWithNodesResponse(
        session=SessionDetailResponse(
            id=session_id,
            owner_id=OWNER_ID,
            created=NOW,
            updated=NOW,
            agent_id=AGENT_ID,
            number=number,
            import_id=IMPORT_ID,
            imported_from="langfuse",
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.COMPLETED,
            started_at=imported.started_at,
            inputs=imported.inputs,
            outputs=imported.outputs,
            metadata={},
            llm_call_count=sum(n.node_type is NodeType.LLM_CALL for n in nodes),
            tool_call_count=sum(n.node_type is NodeType.TOOL_CALL for n in nodes),
        ),
        nodes=nodes,
    )


def load_quickstart_sessions() -> list[SessionWithNodesResponse]:
    """Import the quickstart Langfuse export with the real importer."""
    parsed = LangfuseJSONLImporter().parse(
        QUICKSTART_TRACES.read_bytes(), {"source_instance": "demo"}
    )
    imported = [item for item in parsed if isinstance(item, ImportedSession)]
    return [to_response(item, index) for index, item in enumerate(imported, 1)]


class StubClient:
    """Serve stored sessions in place of the Kitaru API."""

    def __init__(self, sessions: list[SessionWithNodesResponse]) -> None:
        """Index sessions by id."""
        self.sessions = self
        self._by_id = {item.session.id: item for item in sessions}

    async def __aenter__(self) -> "StubClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_with_nodes(self, session_id: uuid.UUID) -> SessionWithNodesResponse:
        return self._by_id[session_id]


Reply = Callable[[dict[str, Any]], httpx.Response]


def completion(
    *,
    tool_calls: list[tuple[str, Any]] | None = None,
    text: str | None = None,
    prompt_tokens: int = 1000,
    completion_tokens: int = 200,
    reasoning_tokens: int = 0,
) -> httpx.Response:
    """Build an OpenAI chat-completions response."""
    message: dict[str, Any] = {"role": "assistant", "content": text}
    if tool_calls:
        message["tool_calls"] = [
            {
                "id": f"call_{index}",
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": (
                        arguments
                        if isinstance(arguments, str)
                        else json.dumps(arguments)
                    ),
                },
            }
            for index, (name, arguments) in enumerate(tool_calls)
        ]
    return httpx.Response(
        200,
        json={
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "completion_tokens_details": {"reasoning_tokens": reasoning_tokens},
            },
        },
    )


class MockOpenAIServer:
    """An OpenAI-compatible endpoint whose replies depend on the model name."""

    def __init__(self, models: dict[str, Reply]) -> None:
        """Map each model name to a reply function."""
        self.models = models
        self.requests: list[dict[str, Any]] = []
        self.headers: list[httpx.Headers] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/chat/completions")
        body = json.loads(request.content)
        self.requests.append(body)
        self.headers.append(request.headers)
        return self.models[body["model"]](body)
