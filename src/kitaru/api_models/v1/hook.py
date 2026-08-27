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
"""Task hook API models."""

from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import DiscriminatedRequestModel


class CopyWorkdirHook(DiscriminatedRequestModel):
    """Copy workdir hook."""

    type: Literal["copy_workdir"] = Field(default="copy_workdir")


class SetupCommandHook(DiscriminatedRequestModel):
    """Setup command hook."""

    type: Literal["setup_command"] = Field(default="setup_command")
    command: str = Field(description="Shell command to run.")


class TeardownCommandHook(DiscriminatedRequestModel):
    """Teardown command hook."""

    type: Literal["teardown_command"] = Field(default="teardown_command")
    command: str = Field(description="Shell command to run.")
    on: Literal["success", "failure", "always"] = Field(
        default="success", description="Task process outcome the command runs on."
    )


TaskHook = Annotated[
    CopyWorkdirHook | SetupCommandHook | TeardownCommandHook,
    Field(discriminator="type"),
]
