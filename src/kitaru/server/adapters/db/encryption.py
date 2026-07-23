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
"""AES-GCM encryption for values stored at rest."""

import base64
import hashlib
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_NONCE_LENGTH = 12


class DecryptionError(Exception):
    """Raised when a token cannot be decrypted."""


class AesGcmCipher:
    """AES-GCM cipher keyed from a configured key string."""

    def __init__(self, key: str) -> None:
        """Initialize the cipher.

        Args:
            key: Configured key string, hashed into the AES-256 key.
        """
        self._aesgcm = AESGCM(hashlib.sha256(key.encode("utf-8")).digest())

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a plaintext string.

        Args:
            plaintext: String to encrypt.

        Returns:
            Base64-encoded nonce and ciphertext.
        """
        nonce = os.urandom(_NONCE_LENGTH)
        ciphertext = self._aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
        return base64.b64encode(nonce + ciphertext).decode("utf-8")

    def decrypt(self, token: str) -> str:
        """Decrypt an encrypted string.

        Args:
            token: Base64-encoded nonce and ciphertext.

        Raises:
            DecryptionError: The token was encrypted with a different key
                or is corrupted.

        Returns:
            Plaintext string.
        """
        try:
            raw = base64.b64decode(token)
            plaintext = self._aesgcm.decrypt(
                raw[:_NONCE_LENGTH], raw[_NONCE_LENGTH:], None
            )
        except (InvalidTag, ValueError) as exc:
            raise DecryptionError(
                "Decryption failed. The token was encrypted with a different "
                "key or is corrupted"
            ) from exc
        return plaintext.decode("utf-8")
