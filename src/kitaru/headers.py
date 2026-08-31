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
"""Kitaru request headers."""

import uuid
from importlib.metadata import version

from kitaru.analytics.source import AnalyticsSource

CLIENT_HEADER = "X-Kitaru-Client"
SKILL_HEADER = "X-Kitaru-Skill"


def format_client_header(
    source: AnalyticsSource, analytics_id: uuid.UUID | None = None
) -> str:
    """Format the client identification header value.

    Args:
        source: Client sending the requests.
        analytics_id: Anonymous analytics id, appended as a third segment
            when given.

    Returns:
        ``<source>/<version>`` header value, or ``<source>/<version>/<analytics_id>``
        when an analytics id is given.
    """
    value = f"{source.value}/{version('kitaru')}"
    if analytics_id is not None:
        value = f"{value}/{analytics_id}"
    return value
