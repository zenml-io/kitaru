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

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue, RequestModel, ResponseModel

MAX_IMPORT_FAILURES = 20


class ImportCreateRequest(RequestModel):
    """Import create request."""

    importer: str = Field(description="Importer name.")
    agent_id: uuid.UUID = Field(
        description="Agent imported sessions are created under."
    )
    version: int | None = Field(
        default=None,
        description="Importer version, an omitted value resolves to latest.",
    )
    payload_blob_id: uuid.UUID = Field(description="Blob holding the payload to parse.")
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Parameters passed to the importer."
    )


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
