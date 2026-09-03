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
"""Bcrypt password hashing."""

import bcrypt

from kitaru.server.domain.base import ValidationError

# Bcrypt hash matching no password.
_DUMMY_PASSWORD_HASH = "$2b$12$KaouCaqMdtw0BrBrlObaCu2mYxFaYfBftAuhk79q6EqK/YbQevgTa"

# Bcrypt reads at most 72 bytes of input. Older releases silently truncate
# longer passwords and newer releases raise, so the limit is enforced here.
MAX_PASSWORD_BYTES = 72


class BcryptPasswordHasher:
    """Password hasher backed by bcrypt."""

    @property
    def dummy_hash(self) -> str:
        """Well-formed bcrypt hash matching no password.

        Returns:
            Hash for timing-uniform verification when no stored hash exists.
        """
        return _DUMMY_PASSWORD_HASH

    def hash(self, password: str) -> str:
        """Hash a password.

        Args:
            password: Plaintext password.

        Raises:
            ValidationError: The password is longer than 72 bytes.

        Returns:
            Bcrypt hash.
        """
        encoded = password.encode("utf-8")
        if len(encoded) > MAX_PASSWORD_BYTES:
            raise ValidationError(f"Password is longer than {MAX_PASSWORD_BYTES} bytes")
        return bcrypt.hashpw(encoded, bcrypt.gensalt()).decode("utf-8")

    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against a bcrypt hash.

        Args:
            password: Plaintext password.
            password_hash: Stored bcrypt hash.

        Returns:
            ``True`` when the password matches the hash.
        """
        try:
            return bcrypt.checkpw(
                password.encode("utf-8"), password_hash.encode("utf-8")
            )
        except ValueError:
            return False
