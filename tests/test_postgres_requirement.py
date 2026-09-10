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
"""Required PostgreSQL test sessions fail instead of silently skipping."""

from unittest.mock import AsyncMock

import pytest

import conftest


def test_required_postgres_unavailable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """A required database must be reachable before tests are collected."""
    monkeypatch.setenv("KITARU_TEST_REQUIRE_POSTGRES", "1")
    monkeypatch.setattr(conftest, "postgres_available", AsyncMock(return_value=False))

    with pytest.raises(pytest.UsageError, match="PostgreSQL is not reachable"):
        conftest.pytest_sessionstart()


def test_required_postgres_available_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful database probe allows the required session to start."""
    monkeypatch.setenv("KITARU_TEST_REQUIRE_POSTGRES", "1")
    probe = AsyncMock(return_value=True)
    monkeypatch.setattr(conftest, "postgres_available", probe)

    conftest.pytest_sessionstart()

    probe.assert_awaited_once()


@pytest.mark.parametrize("require_postgres", [None, "0"])
def test_optional_postgres_does_not_require_database(
    monkeypatch: pytest.MonkeyPatch, require_postgres: str | None
) -> None:
    """Ordinary local sessions do not require or probe PostgreSQL at startup."""
    if require_postgres is None:
        monkeypatch.delenv("KITARU_TEST_REQUIRE_POSTGRES", raising=False)
    else:
        monkeypatch.setenv("KITARU_TEST_REQUIRE_POSTGRES", require_postgres)
    probe = AsyncMock(side_effect=AssertionError("Unexpected database probe"))
    monkeypatch.setattr(conftest, "postgres_available", probe)

    conftest.pytest_sessionstart()

    probe.assert_not_awaited()
