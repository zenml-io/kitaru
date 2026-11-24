#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Strict models for CLI agent registration spec documents."""

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest
from kitaru.cli.output import CLIError


class _SpecModel(BaseModel):
    """Strict base for CLI-only spec documents."""

    model_config = ConfigDict(extra="forbid")


class AgentParentSpec(_SpecModel):
    """Parent fields accepted in an agent registration spec."""

    description: str | None = Field(
        default=None,
        description="Agent description, independent of version.description.",
    )


class AgentRegistrationSpec(_SpecModel):
    """Agent parent and initial-version spec document."""

    description: str | None = Field(
        default=None,
        description=(
            "Shorthand for parent.description. Omission leaves the nested value "
            "unchanged; explicit null means no description. Both fields must match "
            "when supplied together."
        ),
    )
    parent: AgentParentSpec = Field(default_factory=AgentParentSpec)
    version: AgentVersionCreateRequest

    def get_agent_description(self) -> str | None:
        """Resolve the parent description, rejecting conflicting explicit values."""
        description = self.parent.description
        if "description" in self.model_fields_set:
            if (
                "description" in self.parent.model_fields_set
                and self.description != description
            ):
                raise CLIError(
                    "invalid_arguments",
                    "Spec description conflicts with parent.description.",
                    hint=(
                        "Use description or parent.description for the agent, or give "
                        "both the same value (including null). Use version.description "
                        "for the version."
                    ),
                )
            description = self.description
        return description
