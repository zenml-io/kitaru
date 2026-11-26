#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Validated settings for the local Kitaru MCP runtime."""

import math
import os
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


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

    # `label_key` is a secret, so a refused value never shows in the error.
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    mode: CapabilityMode = CapabilityMode.READ_ONLY
    server_url: str | None = None
    timeout: float = Field(default=30.0, gt=0)
    handler_timeout: float = Field(default=120.0, gt=0)
    pool_size: int = Field(default=20, ge=1, le=1000)
    max_concurrency: int = Field(default=10, ge=1, le=1000)
    viewers_dir: Path | None = None
    debug: bool = False
    # Turn on for a server that runs as several copies behind one address: a
    # follow-up call can reach another copy, so no state is kept in memory
    # between calls. The failure matrix then returns no snapshot_id and each
    # cell reads its sessions again. Custom viewers are still read from each
    # copy's own `viewers_dir`.
    stateless: bool = False
    # Keys the digest that tells apart state labels redaction made equal. Copies
    # that serve one matrix and its cells must share it; when unset, each
    # process draws its own.
    label_key: SecretStr | None = Field(default=None, min_length=32)

    @field_validator("server_url")
    @classmethod
    def _reject_blank_target(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("explicit server target must not be blank")
        return value

    @field_validator("viewers_dir", mode="before")
    @classmethod
    def _ignore_blank_viewers_dir(cls, value: object) -> object:
        # An empty variable would otherwise become `Path(".")`, the host's cwd.
        if isinstance(value, str) and not value.strip():
            return None
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
            "viewers_dir": "KITARU_MCP_VIEWERS_DIR",
            "debug": "KITARU_MCP_DEBUG",
            "stateless": "KITARU_MCP_STATELESS",
            "label_key": "KITARU_MCP_LABEL_KEY",
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
