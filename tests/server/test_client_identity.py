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
"""Tests for client identification parsing."""

from kitaru.analytics.source import AnalyticsSource
from kitaru.server.client_identity import parse_client_identity


def test_parse_client_identity() -> None:
    """Read the source and the version out of the header."""
    identity = parse_client_identity("kitaru-python/0.21.0")
    assert identity is not None
    assert identity.source is AnalyticsSource.PYTHON
    assert identity.version == "0.21.0"


def test_parse_client_identity_without_a_version() -> None:
    """Read a source reported without a version."""
    identity = parse_client_identity("kitaru-cli")
    assert identity is not None
    assert identity.source is AnalyticsSource.CLI
    assert identity.version is None


def test_parse_client_identity_rejects_unknown_clients() -> None:
    """Report no identity for a caller that is not a Kitaru client."""
    assert parse_client_identity("") is None
    assert parse_client_identity("curl/8.0") is None
    assert parse_client_identity("python-httpx/0.27.0") is None
