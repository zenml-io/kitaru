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
"""Password hasher interface."""

from typing import Protocol


class PasswordHasher(Protocol):
    """Password hashing operations."""

    @property
    def dummy_hash(self) -> str:
        """Well-formed hash of this scheme matching no password.

        Returns:
            Hash for timing-uniform verification when no stored hash exists.
        """
        ...

    def hash(self, password: str) -> str:
        """Hash a password.

        Args:
            password: Plaintext password.

        Returns:
            Password hash.
        """
        ...

    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against a hash.

        Args:
            password: Plaintext password.
            password_hash: Stored password hash.

        Returns:
            ``True`` when the password matches the hash.
        """
        ...
