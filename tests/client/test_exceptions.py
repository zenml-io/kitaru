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
"""Tests for the typed client exceptions."""

import httpx
import pytest

from kitaru.client.exceptions import ConflictError, raise_for_response


def test_conflict_mapping() -> None:
    """Map HTTP 409 to ConflictError."""
    response = httpx.Response(409, json={"detail": "duplicate"})
    with pytest.raises(ConflictError) as exc_info:
        raise_for_response(response)
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "duplicate"
