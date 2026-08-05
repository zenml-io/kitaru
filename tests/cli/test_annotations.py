#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Annotation CLI behavior over the existing SDK resource."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.cli import annotations
from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.schema import describe_schema


@dataclass
class StubModel:
    """Small response exposing the Pydantic serialization surface."""

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


class StubAnnotationClient:
    """Protocol-shaped client recording annotation SDK calls."""

    def __init__(self) -> None:
        self.annotation = StubModel(
            uuid.uuid4(),
            {
                "session_id": str(uuid.uuid4()),
                "investigation_session_id": None,
                "question_key": None,
                "selector": None,
                "value": {"label": "failure"},
            },
        )
        self.create_calls: list[
            ManualAnnotationCreateRequest | InvestigationAnswerCreateRequest
        ] = []
        self.list_calls: list[AnnotationListParams] = []
        self.get_calls: list[uuid.UUID] = []
        self.update_calls: list[tuple[uuid.UUID, AnnotationUpdateRequest]] = []
        self.deleted: list[uuid.UUID] = []
        self.annotations = self._Annotations(self)

    class _Annotations:
        def __init__(self, owner: "StubAnnotationClient") -> None:
            self.owner = owner

        async def create(
            self,
            request: ManualAnnotationCreateRequest | InvestigationAnswerCreateRequest,
        ) -> StubModel:
            self.owner.create_calls.append(request)
            return self.owner.annotation

        async def list(self, params: AnnotationListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(
                items=[self.owner.annotation], next_cursor="next-annotation"
            )

        async def get(self, annotation_id: uuid.UUID) -> StubModel:
            self.owner.get_calls.append(annotation_id)
            assert annotation_id == self.owner.annotation.id
            return self.owner.annotation

        async def update(
            self, annotation_id: uuid.UUID, request: AnnotationUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((annotation_id, request))
            return self.owner.annotation

        async def delete(self, annotation_id: uuid.UUID) -> None:
            self.owner.deleted.append(annotation_id)


async def test_create_supports_manual_annotations_and_investigation_answers() -> None:
    """Mutually exclusive targets construct the two existing request variants."""
    client = StubAnnotationClient()
    session_id = uuid.uuid4()
    node_id = uuid.uuid4()

    manual = await annotations.create_annotation(
        client,
        value='{"label":"failure","confidence":0.8}',
        session_id=session_id,
        investigation_session_id=None,
        question_key=None,
        selector=json.dumps(
            {"node_id": str(node_id), "part": "output", "path": "/message"}
        ),
    )
    request = client.create_calls[-1]
    assert isinstance(request, ManualAnnotationCreateRequest)
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "session_id": str(session_id),
        "selector": {
            "node_id": str(node_id),
            "part": "output",
            "path": "/message",
        },
        "value": {"label": "failure", "confidence": 0.8},
    }
    assert manual.item["id"] == str(client.annotation.id)

    investigation_session_id = uuid.uuid4()
    await annotations.create_annotation(
        client,
        value='"yes"',
        session_id=None,
        investigation_session_id=investigation_session_id,
        question_key="cause",
        selector=None,
    )
    request = client.create_calls[-1]
    assert isinstance(request, InvestigationAnswerCreateRequest)
    assert request.model_dump(mode="json", exclude_unset=True) == {
        "investigation_session_id": str(investigation_session_id),
        "question_key": "cause",
        "value": "yes",
    }


@pytest.mark.parametrize(
    ("session_id", "investigation_session_id", "question_key", "message"),
    [
        (None, None, None, "exactly one annotation target"),
        (uuid.UUID(int=1), uuid.UUID(int=2), "cause", "exactly one annotation target"),
        (None, uuid.UUID(int=2), None, "--question-key is required"),
        (uuid.UUID(int=1), None, "cause", "only valid with --investigation-session"),
    ],
)
async def test_create_rejects_ambiguous_or_incomplete_targets(
    session_id: uuid.UUID | None,
    investigation_session_id: uuid.UUID | None,
    question_key: str | None,
    message: str,
) -> None:
    """Target validation fails before any remote create request."""
    client = StubAnnotationClient()

    with pytest.raises(CLIError, match=message):
        await annotations.create_annotation(
            client,
            value="true",
            session_id=session_id,
            investigation_session_id=investigation_session_id,
            question_key=question_key,
            selector=None,
        )

    assert client.create_calls == []


async def test_value_and_selector_require_valid_json() -> None:
    """JSON-valued inputs use stable CLI validation errors."""
    client = StubAnnotationClient()

    with pytest.raises(CLIError, match="--value is not valid JSON"):
        await annotations.create_annotation(
            client,
            value="not-json",
            session_id=uuid.uuid4(),
            investigation_session_id=None,
            question_key=None,
            selector=None,
        )
    with pytest.raises(CLIError, match="--selector must contain a JSON object"):
        await annotations.create_annotation(
            client,
            value="null",
            session_id=uuid.uuid4(),
            investigation_session_id=None,
            question_key=None,
            selector="[]",
        )
    assert client.create_calls == []


async def test_annotation_crud_maps_to_sdk_and_requires_force() -> None:
    """List/get/update/delete preserve paging, JSON values, and safe deletion."""
    client = StubAnnotationClient()
    annotation_id = client.annotation.id

    listed = await annotations.list_annotations(
        client, size=4, cursor="page", sort="created:desc", filter=None
    )
    assert client.list_calls[-1].model_dump(mode="json", exclude_unset=True) == {
        "cursor": "page",
        "size": 4,
        "sort": "created:desc",
        "filter": None,
    }
    assert listed.page is not None
    assert listed.page["next_cursor"] == "next-annotation"

    fetched = await annotations.get_annotation(client, annotation_id)
    assert fetched.item["id"] == str(annotation_id)

    await annotations.update_annotation(client, annotation_id, value="[1,2,3]")
    _, request = client.update_calls[-1]
    assert request.value == [1, 2, 3]

    with pytest.raises(CLIError, match="requires --force"):
        await annotations.delete_annotation(client, annotation_id, force=False)
    assert client.deleted == []
    deleted = await annotations.delete_annotation(client, annotation_id, force=True)
    assert deleted.item == {"id": str(annotation_id), "deleted": True}


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubAnnotationClient:
    """Route public CLI invocations through one recording client."""
    client = StubAnnotationClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_argv_and_schema_cover_annotation_crud(
    argv_client: StubAnnotationClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every public annotation leaf is registered with structured output."""
    client = argv_client
    annotation_id = str(client.annotation.id)
    commands = [
        (
            [
                "annotation",
                "create",
                "--session",
                str(uuid.uuid4()),
                "--value",
                '"failure"',
            ],
            "create",
        ),
        (["annotation", "list"], "list"),
        (["annotation", "get", annotation_id], "get"),
        (
            ["annotation", "update", annotation_id, "--value", "false"],
            "update",
        ),
        (["annotation", "delete", annotation_id, "--force"], "delete"),
    ]
    for argv, command in commands:
        assert app_module.main(argv) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["command"] == f"annotation.{command}"

    specs = {item["command"]: item for item in describe_schema(("annotation",))}
    assert set(specs) == {
        "annotation.create",
        "annotation.delete",
        "annotation.get",
        "annotation.list",
        "annotation.update",
    }
    create_parameters = {
        parameter["name"]: parameter
        for parameter in specs["annotation.create"]["parameters"]
    }
    assert create_parameters["--session"]["required"] is False
    assert create_parameters["--investigation-session"]["required"] is False
    assert specs["annotation.delete"]["side_effects"]["deletes_remote_state"]
