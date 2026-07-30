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
"""Plugin source API models, shared by evaluators and importers."""

import uuid
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import DiscriminatedRequestModel


class ScriptPluginSource(DiscriminatedRequestModel):
    """Script plugin source."""

    type: Literal["script"] = Field(default="script")
    blob_id: uuid.UUID = Field(description="Blob holding the script.")
    entrypoint: str = Field(description="Attribute in the file.")


class PackagePluginSource(DiscriminatedRequestModel):
    """Package plugin source."""

    type: Literal["package"] = Field(default="package")
    requirement: str = Field(description="Pinned PEP 508 requirement.")
    entrypoint: str = Field(description="Module and attribute, as module:attribute.")


PluginSource = Annotated[
    ScriptPluginSource | PackagePluginSource, Field(discriminator="type")
]
