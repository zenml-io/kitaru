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
"""Tests for import API models."""

import pytest

from kitaru.api_models.v1.imports import ImportQuery


def test_import_query_requires_since_without_trace_ids() -> None:
    """Require since when trace_ids is absent, but not otherwise."""
    with pytest.raises(ValueError, match="since is required"):
        ImportQuery.model_validate({})
    query = ImportQuery.model_validate({"trace_ids": ["t1"]})
    assert query.since is None


def test_import_query_allows_provider_extras_and_rejects_naive_datetimes() -> None:
    """Pass provider-specific keys through and reject a naive since."""
    query = ImportQuery.model_validate(
        {"since": "2026-01-01T00:00:00Z", "project_id": "proj-1"}
    )
    assert query.model_dump(mode="json", exclude_unset=True) == {
        "since": "2026-01-01T00:00:00Z",
        "project_id": "proj-1",
    }
    with pytest.raises(ValueError):
        ImportQuery.model_validate({"since": "2026-01-01T00:00:00"})
    with pytest.raises(ValueError):
        ImportQuery.model_validate({"trace_ids": "t1"})


def test_import_query_rejects_an_inverted_window() -> None:
    """Reject an until that lands before since."""
    with pytest.raises(ValueError, match="until must not be before since"):
        ImportQuery.model_validate(
            {"since": "2026-01-02T00:00:00Z", "until": "2026-01-01T00:00:00Z"}
        )


def test_import_query_window_defaults_until_to_now() -> None:
    """Default the window's end to now when until is unset."""
    query = ImportQuery.model_validate({"since": "2026-01-01T00:00:00Z"})
    since, until = query.get_window()
    assert since == query.since
    assert until.tzinfo is not None
    assert until > since
    query = ImportQuery.model_validate(
        {"since": "2026-01-01T00:00:00Z", "until": "2026-01-03T00:00:00Z"}
    )
    assert query.get_window() == (query.since, query.until)


def test_import_query_concurrency_defaults_and_rejects_zero() -> None:
    """Default concurrency to 4 and reject a non-positive value."""
    assert ImportQuery.model_validate({"trace_ids": []}).concurrency == 4
    with pytest.raises(ValueError):
        ImportQuery.model_validate({"trace_ids": [], "concurrency": 0})
