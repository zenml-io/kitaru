"""Plugin source API models."""

import uuid
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import DiscriminatedRequestModel


class ScriptPluginSource(DiscriminatedRequestModel):
    """Uploaded script plugin source."""

    type: Literal["script"] = "script"
    blob_id: uuid.UUID = Field(description="Source blob id.")
    entrypoint: str = Field(description="Attribute in the script.")


class PackagePluginSource(DiscriminatedRequestModel):
    """Installed package plugin source."""

    type: Literal["package"] = "package"
    requirement: str = Field(description="Pinned PEP 508 requirement.")
    entrypoint: str = Field(description="Module and attribute entrypoint.")


PluginSource = Annotated[
    ScriptPluginSource | PackagePluginSource, Field(discriminator="type")
]
