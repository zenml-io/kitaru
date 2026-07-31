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
"""Session filter and command models."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import AwareDatetime, Field

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class SessionFilter(ListFilter):
    """Session list filter."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    cohort_version_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    origin: SessionOrigin | None = None
    status: SessionStatus | None = None
    provider: str | None = None
    external_id: str | None = None
    name: str | None = None
    tag: str | None = None
    started_after: AwareDatetime | None = None
    started_before: AwareDatetime | None = None
    ended_after: AwareDatetime | None = None
    ended_before: AwareDatetime | None = None
    has_evaluation: bool | None = None
    min_cost: Decimal | None = None
    max_cost: Decimal | None = None


class SessionCreate(FrozenModel):
    """Session create command."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    task_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus | None = None
    name: str | None = None
    inputs: Any = None
    outputs: Any = None
    expected: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    provider: str | None = None
    framework: str | None = None
    adapter_version: str | None = None


class SessionUpdate(FrozenModel):
    """Session update command."""

    status: SessionStatus | None = None
    outputs: Any = None
    error: str | None = None
    ended_at: datetime | None = None
    name: str | None = None
    expected: Any = None
    metadata: dict[str, Any] | None = None
