#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict inputs for one local experiment export."""

import re
import uuid
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.exports.models import ExportError, RewardSelector

ExportFormat = Literal["harbor", "verifiers-v1"]
TraceFormat = Literal["atif", "kitaru"]
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_environment_names(values: Iterable[str]) -> tuple[str, ...]:
    """Validate, sort, and freeze required environment variable names."""
    names = tuple(values)
    if len(set(names)) != len(names):
        raise ValueError("required environment names must be unique")
    if any(not _ENVIRONMENT_NAME.fullmatch(name) for name in names):
        raise ValueError("required environment names must be valid variable names")
    return tuple(sorted(names))


class ExportRequest(BaseModel):
    """Validated identifiers and local options for an export."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    format: ExportFormat
    source_root: Path
    destination: Path
    primary_reward: str
    required_environment_names: tuple[str, ...] = Field(default=(), max_length=100)
    trace_format: TraceFormat | None = None
    trace_path: str | None = None
    archive: bool = False
    dry_run: bool = False

    @field_validator("primary_reward")
    @classmethod
    def _validate_reward(cls, value: str) -> str:
        try:
            RewardSelector.parse(value)
        except ExportError as error:
            raise ValueError(str(error)) from error
        return value

    @field_validator("required_environment_names")
    @classmethod
    def _validate_environment_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return normalize_environment_names(value)

    @model_validator(mode="after")
    def _validate_target_options(self) -> "ExportRequest":
        if self.format == "harbor":
            if self.trace_format is None or self.trace_path is None:
                raise ValueError("Harbor exports require trace_format and trace_path")
            trace_path = PurePosixPath(self.trace_path)
            if not trace_path.is_absolute() or ".." in trace_path.parts:
                raise ValueError("trace_path must be an absolute in-sandbox path")
        elif self.trace_format is not None or self.trace_path is not None:
            raise ValueError("trace_format and trace_path apply only to Harbor exports")
        return self
