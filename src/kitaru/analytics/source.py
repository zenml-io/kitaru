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
"""Analytics source and event context tracking."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any


class AnalyticsSource(StrEnum):
    """Analytics event source."""

    PYTHON = "kitaru-python"
    CLI = "kitaru-cli"
    MCP = "kitaru-mcp"
    TYPESCRIPT = "kitaru-typescript"
    API = "kitaru-api"
    UI = "kitaru-ui"


@dataclass(frozen=True)
class EventContext:
    """Event context."""

    source: AnalyticsSource = AnalyticsSource.PYTHON
    properties: Mapping[str, Any] = field(default_factory=dict)


_DEFAULT_CONTEXT = EventContext()

current_event_context: ContextVar[EventContext] = ContextVar(
    "kitaru_analytics_event_context", default=_DEFAULT_CONTEXT
)


@contextmanager
def analytics_event_context(**properties: Any) -> Iterator[None]:
    """Merge properties into every analytics event tracked inside the block."""
    current = current_event_context.get()
    token = current_event_context.set(
        replace(current, properties={**current.properties, **properties})
    )
    try:
        yield
    finally:
        current_event_context.reset(token)
