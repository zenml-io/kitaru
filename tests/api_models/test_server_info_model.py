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
"""Tests for server info API models."""

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse


def test_server_info_accepts_payloads_from_older_servers() -> None:
    """Accept old server payloads that omit the optional limit."""
    response = ServerInfoResponse(version="1.0.0", auth_scheme=AuthScheme.LOCAL)

    assert response.max_blob_size_bytes is None


def test_server_info_accepts_zero_blob_limit() -> None:
    """A server may disable nonempty blob uploads with a zero-byte limit."""
    response = ServerInfoResponse(
        version="1.0.0",
        auth_scheme=AuthScheme.LOCAL,
        max_blob_size_bytes=0,
    )

    assert response.max_blob_size_bytes == 0


def test_server_info_rejects_negative_blob_limit() -> None:
    """Reject invalid negative upload limits."""
    with pytest.raises(ValidationError):
        ServerInfoResponse(
            version="1.0.0",
            auth_scheme=AuthScheme.LOCAL,
            max_blob_size_bytes=-1,
        )
