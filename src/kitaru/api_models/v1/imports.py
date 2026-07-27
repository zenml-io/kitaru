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

from kitaru.api_models.v1.base import JsonValue, RequestModel


class ImportCreateRequest(RequestModel):
    """Import create request."""

    importer: str = Field(description="Name of the registered importer.")
    agent_id: uuid.UUID = Field(description="Id of the agent the sessions bind to.")
    version: int | None = Field(
        default=None,
        description="Registered version to run, the latest one when omitted.",
    )
    payload_blob_id: uuid.UUID = Field(description="Id of the payload blob.")
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Keyword arguments for the importer."
    )
