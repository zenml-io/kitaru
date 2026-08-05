#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Validated settings for the local Kitaru MCP runtime."""

import math
import os
from collections.abc import Mapping
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class CapabilityMode(StrEnum):
    """Cumulative tool capability modes."""

    READ_ONLY = "read-only"
    STANDARD = "standard"
    DESTRUCTIVE = "destructive"

    @property
    def rank(self) -> int:
        """Return the cumulative capability rank."""
        return list(type(self)).index(self)


class MCPSettings(BaseModel):
    """Immutable MCP process settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: CapabilityMode = CapabilityMode.READ_ONLY
    server_url: str | None = None
    timeout: float = Field(default=30.0, gt=0)
    handler_timeout: float = Field(default=120.0, gt=0)
    pool_size: int = Field(default=20, ge=1, le=1000)
    max_concurrency: int = Field(default=10, ge=1, le=1000)
    debug: bool = False

    @field_validator("server_url")
    @classmethod
    def _reject_blank_target(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("explicit server target must not be blank")
        return value

    @model_validator(mode="after")
    def _validate_settings(self) -> "MCPSettings":
        if not math.isfinite(self.timeout) or not math.isfinite(self.handler_timeout):
            raise ValueError("timeouts must be finite")
        return self

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None, **overrides: object
    ) -> "MCPSettings":
        """Load MCP-specific environment values with explicit overrides."""
        env = os.environ if environment is None else environment
        values: dict[str, object] = {}
        mapping = {
            "mode": "KITARU_MCP_MODE",
            "server_url": "KITARU_MCP_SERVER",
            "timeout": "KITARU_MCP_TIMEOUT",
            "handler_timeout": "KITARU_MCP_HANDLER_TIMEOUT",
            "pool_size": "KITARU_MCP_POOL_SIZE",
            "max_concurrency": "KITARU_MCP_MAX_CONCURRENCY",
            "debug": "KITARU_MCP_DEBUG",
        }
        for field, variable in mapping.items():
            value = env.get(variable)
            if value is not None:
                values[field] = value
        if "server_url" not in values and env.get("KITARU_API_URL") is not None:
            values["server_url"] = env["KITARU_API_URL"]
        values.update(
            {key: value for key, value in overrides.items() if value is not None}
        )
        return cls.model_validate(values)
