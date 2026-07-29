"""Session run command API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue, RequestModel


class SessionRunCreateRequest(RequestModel):
    """Session run create request."""

    agent_version_id: uuid.UUID = Field(description="Agent version id.")
    inputs: JsonValue = Field(description="Agent inputs.")
    name: str | None = Field(default=None, description="Result session name.")
