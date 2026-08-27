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


class CommandHook(DiscriminatedRequestModel):
    """Command hook."""

    type: Literal["command"] = Field(default="command")
    command: str = Field(description="Shell command to run.")
    when: Literal["setup", "teardown"] = Field(description="Phase the command runs in.")
    run_on_failure: bool = Field(
        default=False,
        description="Whether a teardown command runs when the task process failed.",
    )


TaskHook = Annotated[CopyWorkdirHook | CommandHook, Field(discriminator="type")]
