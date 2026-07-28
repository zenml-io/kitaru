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
"""Tests for the API key codec."""

import base64
import json
import uuid

import pytest

from kitaru.server.domain.api_key import (
    API_KEY_PREFIX,
    InvalidApiKey,
    decode_api_key,
    encode_api_key,
)
from kitaru.server.domain.keys import generate_secret, hash_secret


def test_encode_decode_round_trip() -> None:
    """Decode an encoded key back into its id and secret."""
    key_id = uuid.uuid4()
    secret = generate_secret()
    encoded = encode_api_key(key_id, secret)
    assert encoded.startswith(API_KEY_PREFIX)
    decoded_id, decoded_secret = decode_api_key(encoded)
    assert decoded_id == key_id
    assert decoded_secret == secret


def test_hash_secret_is_deterministic() -> None:
    """Hash the same secret to the same digest."""
    secret = generate_secret()
    assert hash_secret(secret) == hash_secret(secret)
    assert hash_secret(secret) != hash_secret(generate_secret())


def test_decode_bad_prefix() -> None:
    """Reject a key without the expected prefix."""
    encoded = encode_api_key(uuid.uuid4(), generate_secret())
    with pytest.raises(InvalidApiKey):
        decode_api_key(encoded.removeprefix(API_KEY_PREFIX))
    with pytest.raises(InvalidApiKey):
        decode_api_key("OTHER_" + encoded.removeprefix(API_KEY_PREFIX))


def test_decode_bad_base64() -> None:
    """Reject a key whose payload is not valid base64."""
    with pytest.raises(InvalidApiKey):
        decode_api_key(API_KEY_PREFIX + "not base64!")


def test_decode_bad_json() -> None:
    """Reject a key whose payload is not valid JSON."""
    payload = base64.b64encode(b"not json").decode("utf-8")
    with pytest.raises(InvalidApiKey):
        decode_api_key(API_KEY_PREFIX + payload)


def test_decode_non_object_json() -> None:
    """Reject a key whose payload is not a JSON object."""
    payload = base64.b64encode(json.dumps(["id", "key"]).encode("utf-8")).decode(
        "utf-8"
    )
    with pytest.raises(InvalidApiKey):
        decode_api_key(API_KEY_PREFIX + payload)


def test_decode_missing_fields() -> None:
    """Reject a key whose payload misses the id or secret field."""
    for payload in [{}, {"id": str(uuid.uuid4())}, {"key": "secret"}]:
        encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
        with pytest.raises(InvalidApiKey):
            decode_api_key(API_KEY_PREFIX + encoded)


def test_decode_non_string_fields() -> None:
    """Reject a key whose payload fields are not strings."""
    payload = base64.b64encode(
        json.dumps({"id": 1, "key": "secret"}).encode("utf-8")
    ).decode("utf-8")
    with pytest.raises(InvalidApiKey):
        decode_api_key(API_KEY_PREFIX + payload)


def test_decode_bad_uuid() -> None:
    """Reject a key whose id field is not a UUID."""
    payload = base64.b64encode(
        json.dumps({"id": "not-a-uuid", "key": "secret"}).encode("utf-8")
    ).decode("utf-8")
    with pytest.raises(InvalidApiKey):
        decode_api_key(API_KEY_PREFIX + payload)


def test_decode_tampered_payload() -> None:
    """Reject a key whose base64 payload was tampered with."""
    encoded = encode_api_key(uuid.uuid4(), generate_secret())
    tampered = API_KEY_PREFIX + "@@" + encoded.removeprefix(API_KEY_PREFIX)[2:]
    with pytest.raises(InvalidApiKey):
        decode_api_key(tampered)
