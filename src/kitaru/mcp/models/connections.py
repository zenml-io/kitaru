#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict connection read and management inputs."""

import uuid
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import PlainSerializedSecretStr
from kitaru.mcp.models.common import IDEMPOTENCY_KEY_DESCRIPTION, MCPModel, PageOptions


class ConnectionListRequest(PageOptions):
    """List one page of connections, optionally for one provider."""

    operation: Literal["list"]
    provider: str | None = None


class ConnectionGetRequest(MCPModel):
    """Get a connection by UUID or exact case-sensitive name."""

    operation: Literal["get"]
    reference: str = Field(min_length=1)


ConnectionReadRequest = Annotated[
    ConnectionListRequest | ConnectionGetRequest,
    Field(discriminator="operation"),
]


class ConnectionCreate(MCPModel):
    """Create a connection carrying provider environment and secret values."""

    operation: Literal["create"]
    name: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    env: dict[str, str] = Field(default_factory=dict)
    secrets: dict[str, PlainSerializedSecretStr] = Field(default_factory=dict)
    default: bool = False
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )


class ConnectionUpdate(MCPModel):
    """Replace environment values, secret values, or the default flag."""

    operation: Literal["update"]
    connection_id: uuid.UUID
    env: dict[str, str] | None = None
    secrets: dict[str, PlainSerializedSecretStr] | None = None
    default: bool | None = None

    @model_validator(mode="after")
    def _validate_update(self) -> Self:
        for field in ("env", "secrets", "default"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        if not {"env", "secrets", "default"} & self.model_fields_set:
            raise ValueError("connection update must change at least one field")
        return self


class ConnectionSetDefault(MCPModel):
    """Make one connection the default for its provider."""

    operation: Literal["set_default"]
    connection_id: uuid.UUID


ConnectionsManageRequest = Annotated[
    ConnectionCreate | ConnectionUpdate | ConnectionSetDefault,
    Field(discriminator="operation"),
]
