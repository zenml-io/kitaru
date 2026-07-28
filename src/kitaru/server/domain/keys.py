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
"""Random key material generation, hashing, and comparison."""

import hashlib
import secrets

# Digits and uppercase letters without the pairs a reader confuses when
# transcribing a code by hand: 0 and O, 1 and I, 2 and Z, 5 and S, 8 and B.
USER_CODE_ALPHABET = "34679ACDEFGHJKLMNPQRTUVWXY"


def generate_secret() -> str:
    """Generate a random secret.

    Returns:
        Hex-encoded 256-bit secret.
    """
    return secrets.token_hex(32)


def generate_user_code(length: int = 8, group_size: int = 4) -> str:
    """Generate a code short enough to read aloud and type by hand.

    Args:
        length: Number of characters drawn from the alphabet.
        group_size: Characters per dash-separated group.

    Returns:
        Code of the form ``XXXX-XXXX``.
    """
    characters = [secrets.choice(USER_CODE_ALPHABET) for _ in range(length)]
    groups = [
        "".join(characters[index : index + group_size])
        for index in range(0, length, group_size)
    ]
    return "-".join(groups)


def hash_secret(secret: str) -> str:
    """Hash a secret for storage.

    Args:
        secret: Plaintext secret.

    Returns:
        SHA-256 hex digest.
    """
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def verify_secret(secret: str, secret_hash: str) -> bool:
    """Compare a plaintext secret against a stored hash in constant time.

    Args:
        secret: Plaintext secret.
        secret_hash: Stored hash.

    Returns:
        Whether the secret matches the hash.
    """
    return secrets.compare_digest(hash_secret(secret), secret_hash)
