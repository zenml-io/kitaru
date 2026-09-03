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
"""Tests for the bcrypt password hasher."""

import pytest

from kitaru.server.adapters.auth.passwords import (
    MAX_PASSWORD_BYTES,
    BcryptPasswordHasher,
)
from kitaru.server.domain.base import ValidationError


def test_hash_accepts_password_at_limit() -> None:
    """Hash a password of exactly 72 bytes and verify it."""
    hasher = BcryptPasswordHasher()
    password = "a" * MAX_PASSWORD_BYTES
    password_hash = hasher.hash(password)
    assert hasher.verify(password, password_hash)


def test_hash_rejects_password_over_limit() -> None:
    """Reject a password one byte over the bcrypt limit."""
    hasher = BcryptPasswordHasher()
    with pytest.raises(ValidationError, match="72 bytes"):
        hasher.hash("a" * (MAX_PASSWORD_BYTES + 1))


def test_hash_counts_bytes_not_characters() -> None:
    """Reject a short multibyte password whose UTF-8 encoding exceeds the limit."""
    hasher = BcryptPasswordHasher()
    password = "\u00e4" * (MAX_PASSWORD_BYTES // 2 + 1)
    assert len(password) < MAX_PASSWORD_BYTES
    with pytest.raises(ValidationError):
        hasher.hash(password)


def test_verify_rejects_password_over_limit() -> None:
    """Fail verification of an over-limit password without raising."""
    hasher = BcryptPasswordHasher()
    password_hash = hasher.hash("secret")
    assert hasher.verify("a" * (MAX_PASSWORD_BYTES + 1), password_hash) is False
