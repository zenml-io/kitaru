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
"""Import response compatibility across server and worker upgrades."""

import uuid
from datetime import UTC, datetime
from typing import Literal

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.base import JsonValue, OwnedResponseModel, ResponseModel
from kitaru.api_models.v1.imports import (
    ApiImportSource,
    BlobImportSource,
    ImportQuery,
    ImportResponse,
    ImportStats,
)
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.api_models.v1.task import (
    ApiImportSourceSpec,
    BlobImportSourceSpec,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    PluginSpec,
)


class LegacyImportResponse(OwnedResponseModel):
    """Import response contract before API sources were introduced."""

    id: uuid.UUID
    job_id: uuid.UUID | None = None
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    importer_version_id: uuid.UUID | None = None
    payload_blob_id: uuid.UUID
    params: dict[str, JsonValue]
    evaluators: list[EvaluatorConfig]
    stats: ImportStats | None = None
    error: str | None = None


class LegacyImportTaskDetails(ResponseModel):
    """Worker task details contract before API sources were introduced."""

    kind: Literal["importer"] = "importer"
    plugin: PluginSpec
    payload: PayloadSpec
    provider: str | None = None
    agent_id: uuid.UUID
    params: dict[str, JsonValue]


def test_blob_import_response_supports_old_and_new_clients() -> None:
    """An old SDK reads a new response; a new SDK reads an old server response."""
    blob_id = uuid.uuid4()
    response = ImportResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
        source=BlobImportSource(blob_id=blob_id),
        params={},
        evaluators=[],
    )
    legacy = LegacyImportResponse.model_validate_json(response.model_dump_json())
    assert legacy.payload_blob_id == blob_id

    restored = ImportResponse.model_validate_json(legacy.model_dump_json())
    assert restored.source == response.source
    assert restored.model_dump()["payload_blob_id"] == blob_id


def test_blob_task_details_support_old_and_new_workers() -> None:
    """Both worker versions can read blob tasks emitted by either server version."""
    response = ImportTaskDetails(
        plugin=PackagePluginSpec(entrypoint="example:parse", requirement="example==1"),
        source=BlobImportSourceSpec(blob_id=uuid.uuid4(), sha256="abc"),
        agent_id=uuid.uuid4(),
        params={"delimiter": ","},
    )
    legacy = LegacyImportTaskDetails.model_validate_json(response.model_dump_json())
    assert isinstance(response.source, BlobImportSourceSpec)
    assert legacy.payload.blob_id == response.source.blob_id
    assert legacy.payload.sha256 == response.source.sha256

    restored = ImportTaskDetails.model_validate_json(legacy.model_dump_json())
    assert restored.source == response.source
    assert restored.params == response.params


def test_api_import_responses_do_not_fabricate_blob_payloads() -> None:
    """API sources retain null legacy fields, which cannot stand in for a blob."""
    query = ImportQuery(trace_ids=["trace-1"])
    response = ImportResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
        source=ApiImportSource(query=query),
        params={},
        evaluators=[],
    )
    details = ImportTaskDetails(
        plugin=PackagePluginSpec(entrypoint="example:fetch", requirement="example==1"),
        source=ApiImportSourceSpec(query=query),
        agent_id=response.agent_id,
        params={},
    )
    assert response.model_dump()["payload_blob_id"] is None
    assert details.model_dump()["payload"] is None


@pytest.mark.parametrize("model", [ImportResponse, ImportTaskDetails])
def test_legacy_normalization_does_not_accept_missing_sources(
    model: type[ImportResponse] | type[ImportTaskDetails],
) -> None:
    """A response with neither representation remains invalid."""
    with pytest.raises(ValidationError, match="source"):
        model.model_validate({})
