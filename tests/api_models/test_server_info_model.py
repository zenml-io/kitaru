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


@pytest.mark.parametrize("value", [0, -1])
def test_server_info_rejects_non_positive_blob_limits(value: int) -> None:
    """Require an advertised blob limit to be positive."""
    with pytest.raises(ValidationError):
        ServerInfoResponse(
            version="1.0.0",
            auth_scheme=AuthScheme.LOCAL,
            max_blob_size_bytes=value,
        )
