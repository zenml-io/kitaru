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
"""Client identification parsed from request headers."""

import uuid
from dataclasses import dataclass

from kitaru.analytics.source import AnalyticsSource


@dataclass(frozen=True)
class ClientIdentity:
    """Client identity."""

    source: AnalyticsSource
    version: str | None
    analytics_id: uuid.UUID | None = None


def parse_client_identity(value: str) -> ClientIdentity | None:
    """Parse a client identification header value.

    Args:
        value: ``<source>/<version>`` or ``<source>/<version>/<analytics_id>``
            header value.

    Returns:
        Parsed identity, or None for an unknown client.
    """
    name, _, remainder = value.partition("/")
    try:
        source = AnalyticsSource(name)
    except ValueError:
        return None
    client_version, _, raw_analytics_id = remainder.partition("/")
    analytics_id: uuid.UUID | None
    try:
        analytics_id = uuid.UUID(raw_analytics_id)
    except ValueError:
        analytics_id = None
    return ClientIdentity(
        source=source, version=client_version or None, analytics_id=analytics_id
    )
