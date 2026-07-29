"""Import command API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue, RequestModel, ResponseModel

MAX_IMPORT_FAILURES = 20


class ImportCreateRequest(RequestModel):
    """Import create request."""

    importer: str = Field(description="Importer name.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    version: int | None = Field(default=None, description="Importer version.")
    payload_blob_id: uuid.UUID = Field(description="Payload blob id.")
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Importer parameters."
    )


class ImportFailure(ResponseModel):
    """One failed imported record."""

    line: int | None = Field(default=None, description="Input line number.")
    external_id: str | None = Field(default=None, description="External record id.")
    error: str = Field(description="Failure detail.")


class ImportStats(ResponseModel):
    """Import task result."""

    created: int = Field(description="Created session count.")
    skipped: int = Field(description="Skipped session count.")
    failed: int = Field(description="Failed session count.")
    failures: list[ImportFailure] = Field(
        max_length=MAX_IMPORT_FAILURES, description="Sample failures."
    )
