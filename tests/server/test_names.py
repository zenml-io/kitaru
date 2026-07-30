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
"""Tests for restricted entity name validation."""

import pytest

from kitaru.server.domain.names import (
    RESERVED_SEPARATORS,
    InvalidName,
    validate_account_name,
    validate_evaluation_name,
    validate_name,
    validate_version_name,
)


@pytest.mark.parametrize(
    "name",
    ["a", "A", "7", "agent-1", "my_agent", "a-b_c", "a--b", "A9z"],
)
def test_valid_names_pass(name: str) -> None:
    """Accept names made of alphanumerics and default separators."""
    assert validate_name(name) == name


@pytest.mark.parametrize(
    "name",
    ["", "-agent", "agent-", "_agent", "a b", "a.b", "a/b", "a:b", "a@b", "ä"],
)
def test_invalid_names_rejected(name: str) -> None:
    """Reject empty names, boundary separators, and disallowed characters."""
    with pytest.raises(InvalidName):
        validate_name(name)


def test_name_length_limit() -> None:
    """Reject names longer than the maximum length."""
    assert validate_name("a" * 255)
    with pytest.raises(InvalidName):
        validate_name("a" * 256)
    with pytest.raises(InvalidName):
        validate_name("abc", max_length=2)


def test_custom_allowed_separators() -> None:
    """Apply a caller-supplied separator set instead of the default."""
    assert validate_name("a+b", allowed_separators=frozenset({"+"}))
    with pytest.raises(InvalidName):
        validate_name("a-b", allowed_separators=frozenset({"+"}))


@pytest.mark.parametrize("separator", sorted(RESERVED_SEPARATORS))
def test_reserved_separators_cannot_be_allowed(separator: str) -> None:
    """Refuse separator sets that include a reserved separator."""
    with pytest.raises(ValueError, match="reserved"):
        validate_name(f"a{separator}b", allowed_separators=frozenset(separator))


@pytest.mark.parametrize(
    "name",
    [
        "alice",
        "michael@zenml.io",
        "first.last@example.co.uk",
        "alice+ci@example.com",
        "a_b-c.d@e.f",
    ],
)
def test_valid_account_names_pass(name: str) -> None:
    """Accept the email addresses an OAuth2 control plane uses as usernames."""
    assert validate_account_name(name) == name


@pytest.mark.parametrize("name", ["", "@alice", "alice@", "a b", "a/b", "a:b", "ä"])
def test_invalid_account_names_rejected(name: str) -> None:
    """Reject names that are empty, boundary separated, or outside the set."""
    with pytest.raises(InvalidName):
        validate_account_name(name)


@pytest.mark.parametrize(
    "name",
    ["accuracy", "accuracy.v2", "accuracy_v2", "accuracy-v2", "a.b-c_d"],
)
def test_valid_evaluation_names_pass(name: str) -> None:
    """Accept the dot separator a qualified evaluator name may use."""
    assert validate_evaluation_name(name) == name


@pytest.mark.parametrize(
    "name", ["", ".accuracy", "accuracy.", "a b", "a/b", "a:b", "a@b", "ä"]
)
def test_invalid_evaluation_names_rejected(name: str) -> None:
    """Reject names that are empty, boundary separated, or outside the set."""
    with pytest.raises(InvalidName):
        validate_evaluation_name(name)


@pytest.mark.parametrize(
    "name",
    ["v1", "1.2.3", "1.2.3+build.5", "feature/branch-1", "v1_2-3.4"],
)
def test_valid_version_names_pass(name: str) -> None:
    """Accept the semver and branch-style separators a display version may use."""
    assert validate_version_name(name) == name


@pytest.mark.parametrize("name", ["", "/v1", "v1/", "v1 2", "v1:2", "v1@2", "ä"])
def test_invalid_version_names_rejected(name: str) -> None:
    """Reject names that are empty, boundary separated, or outside the set."""
    with pytest.raises(InvalidName):
        validate_version_name(name)
