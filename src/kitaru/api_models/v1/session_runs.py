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
"""Session run API models."""

import uuid
from typing import Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.agent_versions import ExecutionTarget
from kitaru.api_models.v1.base import JsonValue, RequestModel


class SessionRunCreateRequest(RequestModel):
    """Session run create request."""

    agent_id: uuid.UUID | None = Field(
        default=None, description="Id of the agent to execute."
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the agent version to execute, the latest runnable "
        "version of the agent when omitted.",
    )
    inputs: JsonValue = Field(default=None, description="Session inputs.")
    name: str | None = Field(
        default=None, max_length=255, description="Session run name."
    )
    execution_target: ExecutionTarget | None = Field(
        default=None,
        description="Execution target, the agent version's default when omitted.",
    )

    @model_validator(mode="after")
    def validate_agent_reference(self) -> Self:
        """Validate that an agent or agent version is referenced.

        Raises:
            ValueError: Neither an agent id nor an agent version id is set.

        Returns:
            The validated request.
        """
        if self.agent_id is None and self.agent_version_id is None:
            raise ValueError("Session run requires an agent_id or an agent_version_id")
        return self
