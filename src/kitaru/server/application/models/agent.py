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
"""Agent filter and command models."""

from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class AgentFilter(ListFilter):
    """Agent list filter."""

    name: str | None = None


class AgentUpdate(FrozenModel):
    """Agent update command."""

    name: str | None = None
    description: str | None = None
