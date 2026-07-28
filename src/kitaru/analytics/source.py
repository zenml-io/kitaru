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
from enum import StrEnum
from importlib.metadata import version

CLIENT_HEADER = "X-Kitaru-Client"


class AnalyticsSource(StrEnum):
    """Analytics event source."""

    PYTHON = "kitaru-python"
    CLI = "kitaru-cli"
    MCP = "kitaru-mcp"
    API = "kitaru-api"
    UI = "kitaru-ui"


current_source: ContextVar[AnalyticsSource] = ContextVar(
    "kitaru_analytics_source", default=AnalyticsSource.PYTHON
)


def format_client_header(source: AnalyticsSource) -> str:
    """Format the client identification header value.

    Args:
        source: Client sending the requests.

    Returns:
        ``<source>/<version>`` header value.
    """
    return f"{source.value}/{version('kitaru')}"


def parse_client_header(value: str) -> AnalyticsSource | None:
    """Parse the source from a client identification header value.

    Args:
        value: ``<source>/<version>`` header value.

    Returns:
        Parsed source, or None for an unknown client.
    """
    if not value:
        return None
    name, _, _ = value.partition("/")
    try:
        return AnalyticsSource(name)
    except ValueError:
        return None
