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
"""Analytics source tracking."""

from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum


class AnalyticsSource(StrEnum):
    """Analytics event source."""

    PYTHON = "kitaru-python"
    CLI = "kitaru-cli"
    MCP = "kitaru-mcp"
    TYPESCRIPT = "kitaru-typescript"
    API = "kitaru-api"
    UI = "kitaru-ui"


@dataclass(frozen=True)
class AnalyticsAttribution:
    """Analytics attribution."""

    source: AnalyticsSource = AnalyticsSource.PYTHON
    version: str | None = None
    skill: str | None = None


_DEFAULT_ATTRIBUTION = AnalyticsAttribution()

current_attribution: ContextVar[AnalyticsAttribution] = ContextVar(
    "kitaru_analytics_attribution", default=_DEFAULT_ATTRIBUTION
)
