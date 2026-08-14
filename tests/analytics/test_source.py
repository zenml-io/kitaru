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
"""Tests for the analytics source helpers."""

from importlib.metadata import version

from kitaru.analytics.source import (
    AnalyticsSource,
    format_client_header,
    parse_client_header,
)


def test_format_client_header() -> None:
    """Format the source and package version into the header value."""
    assert format_client_header(AnalyticsSource.PYTHON) == (
        f"kitaru-python/{version('kitaru')}"
    )


def test_parse_client_header() -> None:
    """Parse the source from well-formed header values."""
    assert parse_client_header("kitaru-python/0.21.0") is AnalyticsSource.PYTHON
    assert parse_client_header("kitaru-typescript") is AnalyticsSource.TYPESCRIPT
    assert parse_client_header("kitaru-ui/1.2.3") is AnalyticsSource.UI
    assert parse_client_header("kitaru-cli") is AnalyticsSource.CLI


def test_parse_client_header_rejects_unknown_clients() -> None:
    """Return None for unknown or malformed header values."""
    assert parse_client_header("") is None
    assert parse_client_header("curl/8.0") is None
    assert parse_client_header("python-httpx/0.27.0") is None
