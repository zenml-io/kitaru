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
"""Task hook value objects."""

from typing import Annotated, Literal

from pydantic import Field

from kitaru.base import FrozenModel


class CopyWorkdirHook(FrozenModel):
    """Copy workdir hook."""

    type: Literal["copy_workdir"] = "copy_workdir"


class CommandHook(FrozenModel):
    """Command hook."""

    type: Literal["command"] = "command"
    command: str
    when: Literal["setup", "teardown"]
    run_on_failure: bool = False


TaskHook = Annotated[CopyWorkdirHook | CommandHook, Field(discriminator="type")]
