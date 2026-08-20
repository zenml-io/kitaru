#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict request and receipt models for local experiment exports."""

import uuid
from pathlib import PurePosixPath
from typing import Literal

from pydantic import Field, field_validator, model_validator

from kitaru.exports.config import normalize_environment_names
from kitaru.exports.models import ContentPolicy, EnvironmentPolicy, SourcePolicy
from kitaru.exports.operation import ExportReceipt
from kitaru.mcp.models.common import MCPModel, ToolResult


class ExperimentExportRequest(MCPModel):
    """Export exact Kitaru resources and a local source tree."""

    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    format: Literal["harbor", "verifiers-v1"]
    source_root: str = Field(min_length=1, max_length=4096)
    destination: str = Field(min_length=1, max_length=4096)
    primary_reward: str = Field(
        pattern=r"^[^:]+:[^:]+:(score|passed)$",
        description="EVALUATOR:RESULT:score or EVALUATOR:RESULT:passed.",
    )
    required_environment_names: list[str] = Field(default_factory=list, max_length=100)
    content_policy: ContentPolicy = Field(default_factory=ContentPolicy)
    environment_policy: EnvironmentPolicy = Field(default_factory=EnvironmentPolicy)
    source_policy: SourcePolicy = Field(default_factory=SourcePolicy)
    trace_format: Literal["atif", "kitaru"] | None = None
    trace_path: str | None = Field(default=None, max_length=1024)
    archive: bool = False
    dry_run: bool = False

    @field_validator("required_environment_names")
    @classmethod
    def _validate_environment_names(cls, value: list[str]) -> list[str]:
        return list(normalize_environment_names(value))

    @model_validator(mode="after")
    def _validate_trace_options(self) -> "ExperimentExportRequest":
        if self.format == "harbor":
            if self.trace_format is None or self.trace_path is None:
                raise ValueError("Harbor exports require trace_format and trace_path")
            trace_path = PurePosixPath(self.trace_path)
            if not trace_path.is_absolute() or ".." in trace_path.parts:
                raise ValueError("trace_path must be an absolute in-sandbox path")
        elif self.trace_format is not None or self.trace_path is not None:
            raise ValueError("trace options apply only to Harbor exports")
        return self


class ExperimentExportReceipt(ExportReceipt):
    """Receipt for a local export preflight or publication."""

    operation: Literal["experiment_export"] = "experiment_export"


class ExperimentExportResult(ToolResult):
    """Typed experiment export result."""

    data: ExperimentExportReceipt | None = None
