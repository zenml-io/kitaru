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
"""Tests for the request header helpers."""

import uuid
from importlib.metadata import version

from kitaru.analytics.source import AnalyticsSource
from kitaru.headers import format_client_header


def test_format_client_header() -> None:
    """Format the source and package version into the header value."""
    assert format_client_header(AnalyticsSource.PYTHON) == (
        f"kitaru-python/{version('kitaru')}"
    )


def test_format_client_header_with_analytics_id() -> None:
    """Append the analytics id as a third segment when given."""
    analytics_id = uuid.uuid4()

    assert format_client_header(AnalyticsSource.PYTHON, analytics_id) == (
        f"kitaru-python/{version('kitaru')}/{analytics_id}"
    )
