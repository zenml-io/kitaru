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
from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, ConfigDict, Field, model_validator

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
DEFAULT_FETCH_CONCURRENCY = 4


class BlobImportSource(DiscriminatedRequestModel):
    """Blob import source."""

    type: Literal["blob"] = Field(default="blob")
    blob_id: uuid.UUID = Field(description="Blob holding the payload to parse.")


class ImportQuery(RequestModel):
    """Import query."""

    model_config = ConfigDict(extra="allow")

    trace_ids: list[str] | None = Field(
        default=None,
        description="Exact trace ids to fetch, instead of a time window.",
    )
    since: AwareDatetime | None = Field(
        default=None, description="Start of the time window to fetch."
    )
    until: AwareDatetime | None = Field(
        default=None, description="End of the time window to fetch."
    )
    concurrency: int = Field(
        default=DEFAULT_FETCH_CONCURRENCY,
        ge=1,
        description="Fetches the importer runs at once.",
    )

    @model_validator(mode="after")
    def _check_window(self) -> Self:
        """Require since without trace ids and reject an inverted window.

        Raises:
            ValueError: Neither trace_ids nor since is set, or until is
                before since.

        Returns:
            The validated query.
        """
        if self.trace_ids is None and self.since is None:
            raise ValueError("since is required when trace_ids is absent")
        if (
            self.since is not None
            and self.until is not None
            and (self.until < self.since)
        ):
            raise ValueError("until must not be before since")
        return self

    def get_window(self) -> tuple[datetime, datetime]:
        """Return the time window, with until defaulting to now.

        Returns:
            Window bounds.
        """
        assert self.since is not None
        return self.since, self.until or datetime.now(UTC)


class ApiImportSource(DiscriminatedRequestModel):
    """API import source."""

    type: Literal["api"] = Field(default="api")
    query: ImportQuery = Field(
        description="Importer-defined selection of what to fetch."
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

    def get_source(self) -> ImportSource:
        """Return the import source, mapping the deprecated blob id to it.

        Returns:
            Import source.
        """
        if self.source is not None:
            return self.source
        blob_id = self.payload_blob_id
        assert blob_id is not None
        return BlobImportSource(blob_id=blob_id)


class ImportListParams(FilterableListParams):
    """Import list params."""


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
    source: ImportSource = Field(description="Where the payload comes from.")
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
