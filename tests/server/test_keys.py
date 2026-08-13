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
"""Tests for key material generation, hashing, and comparison."""

import re

from kitaru.server.domain.keys import (
    USER_CODE_ALPHABET,
    generate_secret,
    generate_user_code,
    hash_secret,
    verify_secret,
)


def test_generate_secret_is_random() -> None:
    """Generate distinct hex-encoded secrets."""
    assert generate_secret() != generate_secret()
    assert re.fullmatch(r"[0-9a-f]{64}", generate_secret())


def test_generate_user_code_shape() -> None:
    """Generate a code of the form XXXX-XXXX from the unambiguous alphabet."""
    code = generate_user_code()
    assert re.fullmatch(
        rf"[{USER_CODE_ALPHABET}]{{4}}-[{USER_CODE_ALPHABET}]{{4}}", code
    )


def test_generate_user_code_is_random() -> None:
    """Generate distinct codes across calls."""
    assert generate_user_code() != generate_user_code()


def test_hash_secret_is_deterministic() -> None:
    """Hash the same secret to the same digest."""
    secret = generate_secret()
    assert hash_secret(secret) == hash_secret(secret)
    assert hash_secret(secret) != hash_secret(generate_secret())


def test_verify_secret() -> None:
    """Accept the matching secret and reject a wrong one."""
    secret = generate_secret()
    secret_hash = hash_secret(secret)
    assert verify_secret(secret, secret_hash) is True
    assert verify_secret(generate_secret(), secret_hash) is False
