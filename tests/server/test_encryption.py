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
"""Tests for the AES-GCM cipher."""

import base64

import pytest

from kitaru.server.adapters.db.encryption import AesGcmCipher, DecryptionError


def test_encrypt_decrypt_round_trip() -> None:
    """Decrypt an encrypted string back into the plaintext."""
    cipher = AesGcmCipher("test-encryption-key")
    token = cipher.encrypt("hunter2")
    assert token != "hunter2"
    assert cipher.decrypt(token) == "hunter2"


def test_encrypt_is_randomized() -> None:
    """Encrypt the same plaintext to different tokens."""
    cipher = AesGcmCipher("test-encryption-key")
    first = cipher.encrypt("hunter2")
    second = cipher.encrypt("hunter2")
    assert first != second
    assert cipher.decrypt(first) == cipher.decrypt(second) == "hunter2"


def test_token_hides_plaintext() -> None:
    """Keep the plaintext out of the decoded token."""
    cipher = AesGcmCipher("test-encryption-key")
    token = cipher.encrypt("hunter2")
    assert b"hunter2" not in base64.b64decode(token)


def test_decrypt_wrong_key() -> None:
    """Reject a token encrypted under a different key."""
    token = AesGcmCipher("test-encryption-key").encrypt("hunter2")
    with pytest.raises(DecryptionError):
        AesGcmCipher("other-encryption-key").decrypt(token)


def test_decrypt_tampered_token() -> None:
    """Reject a token whose ciphertext was tampered with."""
    cipher = AesGcmCipher("test-encryption-key")
    raw = bytearray(base64.b64decode(cipher.encrypt("hunter2")))
    raw[-1] ^= 0x01
    tampered = base64.b64encode(bytes(raw)).decode("utf-8")
    with pytest.raises(DecryptionError):
        cipher.decrypt(tampered)
