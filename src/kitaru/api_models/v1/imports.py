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
"""Import API models."""

import uuid
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    DiscriminatedRequestModel,
    JsonValue,
    OwnedResponseModel,
    PlainStr,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.replay_config import EvaluatorConfig

MAX_IMPORT_FAILURES = 20


class BlobImportSource(DiscriminatedRequestModel):
    """Blob import source."""

    type: Literal["blob"] = Field(default="blob")
    blob_id: uuid.UUID = Field(description="Blob holding the payload to parse.")


class ApiImportSource(DiscriminatedRequestModel):
    """API import source."""

    type: Literal["api"] = Field(default="api")
    query: dict[str, JsonValue] = Field(
        default_factory=dict, description="Importer-defined selection of what to fetch."
    )


ImportSource = Annotated[
    BlobImportSource | ApiImportSource, Field(discriminator="type")
]


class ImportCreateRequest(RequestModel):
    """Import create request."""

    importer: PlainStr = Field(description="Importer name.")
    agent_id: uuid.UUID = Field(
        description="Agent imported sessions are created under."
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Agent version recorded on the imported sessions.",
    )
    version: int | None = Field(
        default=None,
        description="Importer version, an omitted value resolves to latest.",
    )
    source: ImportSource | None = Field(
        default=None, description="Where the payload comes from."
    )
    payload_blob_id: uuid.UUID | None = Field(
        default=None,
        deprecated="Use source instead.",
        description="Blob holding the payload to parse.",
    )
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Parameters passed to the importer."
    )
    evaluators: list[EvaluatorConfig] = Field(
        default_factory=list,
        description="Evaluators run against every imported session.",
    )


class ImportListParams(FilterableListParams):
    """Import list params."""

    @model_validator(mode="after")
    def _source_xor_payload_blob_id(self) -> Self:
        """Require exactly one of source and payload_blob_id.

        Raises:
            ValueError: Both or neither field was set.

        Returns:
            The validated request.
        """
        legacy = (
            "payload_blob_id" in self.model_fields_set
            and self.payload_blob_id is not None
        )
        if self.source is not None and legacy:
            raise ValueError("source and payload_blob_id are mutually exclusive")
        if self.source is None and not legacy:
            raise ValueError("source is required")
        return self

    def get_source(self) -> "BlobImportSource | ApiImportSource":
        """Return the import source, mapping the deprecated blob id to it.

        Returns:
            Import source.
        """
        if self.source is not None:
            return self.source
        blob_id = self.payload_blob_id
        assert blob_id is not None
        return BlobImportSource(blob_id=blob_id)


class ImportFailure(ResponseModel):
    """Import failure."""

    line: int = Field(description="Line the failure occurred at.")
    external_id: str | None = Field(
        default=None, description="External id of the failed item."
    )
    error: str = Field(description="Failure reason.")


class ImportStats(ResponseModel):
    """Import stats."""

    created: int = Field(description="Sessions created.")
    skipped: int = Field(description="Sessions skipped as duplicates.")
    failed: int = Field(description="Items that failed to import.")
    failures: list[ImportFailure] = Field(
        default_factory=list,
        max_length=MAX_IMPORT_FAILURES,
        description="Sample of failures.",
    )


class ImportResponse(OwnedResponseModel):
    """Import response."""

    id: uuid.UUID = Field(description="Import id.")
    job_id: uuid.UUID | None = Field(
        default=None, description="Job running the import."
    )
    agent_id: uuid.UUID = Field(
        description="Agent imported sessions are created under."
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Agent version recorded on the imported sessions.",
    )
    importer_version_id: uuid.UUID | None = Field(
        default=None, description="Importer version run."
    )
    payload_blob_id: uuid.UUID = Field(description="Blob holding the payload parsed.")
    params: dict[str, JsonValue] = Field(
        description="Parameters passed to the importer."
    )
    evaluators: list[EvaluatorConfig] = Field(
        description="Evaluators run against every imported session."
    )
    stats: ImportStats | None = Field(
        default=None, description="Stats from a completed import."
    )
    error: str | None = Field(default=None, description="Error from a failed import.")
