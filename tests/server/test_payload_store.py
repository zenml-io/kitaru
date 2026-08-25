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
"""Tests for the payload store."""

import uuid

import pytest

from conftest import build_payload_store
from kitaru.server.domain.payload import Payload
from kitaru.server.domain.session import Session

OWNER_ID = uuid.uuid4()


async def test_offload_under_threshold_stays_inline() -> None:
    """Keep a value at or under the offload threshold inline, with no blob id."""
    fakes = build_payload_store()
    payload = Payload.from_text("short")
    await fakes.store.offload([payload], OWNER_ID)
    assert payload.blob_id is None
    assert payload.value == "short"


async def test_offload_over_threshold_offloads_and_keeps_the_value() -> None:
    """Offload a value over the threshold, keeping its value in memory."""
    fakes = build_payload_store(threshold_bytes=10)
    payload = Payload.from_text("x" * 50)
    await fakes.store.offload([payload], OWNER_ID)
    assert payload.blob_id is not None
    assert payload.value == "x" * 50


async def test_offload_threshold_zero_offloads_every_payload() -> None:
    """Offload every payload when the threshold is zero."""
    fakes = build_payload_store(threshold_bytes=0)
    payload = Payload.from_json({"a": 1})
    await fakes.store.offload([payload], OWNER_ID)
    assert payload.blob_id is not None
    assert payload.value == {"a": 1}


async def test_offload_dedupes_identical_values() -> None:
    """Share one blob between two payloads offloading the same value."""
    fakes = build_payload_store(threshold_bytes=10)
    shared_value = {"a": "i" * 50}
    first = Payload.from_json(shared_value)
    second = Payload.from_json(shared_value)
    await fakes.store.offload([first, second], OWNER_ID)
    assert first.blob_id is not None
    assert first.blob_id == second.blob_id


async def test_offload_keeps_byte_identical_values_apart_by_media_type() -> None:
    """Store a text value and a byte-identical JSON value as separate blobs."""
    fakes = build_payload_store(threshold_bytes=10)
    padded = "x" * 50
    # The JSON encoding of the plain string equals the raw text of the quoted
    # string, so the two payloads hash to the same sha256 despite carrying
    # different media types.
    text_payload = Payload.from_text(f'"{padded}"')
    json_payload = Payload.from_json(padded)
    await fakes.store.offload([text_payload, json_payload], OWNER_ID)
    assert text_payload.blob_id is not None
    assert json_payload.blob_id is not None
    assert text_payload.blob_id != json_payload.blob_id

    resolved_text = Payload.from_ref(text_payload.blob_id)
    resolved_json = Payload.from_ref(json_payload.blob_id)
    await fakes.store.resolve([resolved_text, resolved_json])
    assert resolved_text.value == f'"{padded}"'
    assert resolved_json.value == padded


async def test_offload_is_a_no_op_for_already_reffed_payloads() -> None:
    """Leave a payload that already carries a blob ref untouched."""
    fakes = build_payload_store(threshold_bytes=0)
    blob_id = uuid.uuid4()
    payload = Payload.from_ref(blob_id)
    payload.value = "already resolved"
    await fakes.store.offload([payload], OWNER_ID)
    assert payload.blob_id == blob_id
    assert payload.value == "already resolved"


async def test_resolve_fills_values_and_is_idempotent() -> None:
    """Resolve a ref-only payload and skip it on a second resolve call."""
    fakes = build_payload_store(threshold_bytes=10)
    value = {"a": 1, "b": "y" * 50}
    offloaded = Payload.from_json(value)
    await fakes.store.offload([offloaded], OWNER_ID)
    assert offloaded.blob_id is not None

    ref_only = Payload.from_ref(offloaded.blob_id)
    await fakes.store.resolve([ref_only])
    assert ref_only.value == value

    await fakes.store.resolve([ref_only])
    assert ref_only.value == value


async def test_unresolved_value_access_raises() -> None:
    """Raise when reading the value of an unresolved ref."""
    payload = Payload.from_ref(uuid.uuid4())
    with pytest.raises(RuntimeError):
        _ = payload.value


async def test_resolve_mutates_the_payload_assigned_to_a_session_field() -> None:
    """Resolve a payload in place without pydantic copying it on assignment."""
    fakes = build_payload_store(threshold_bytes=10)
    value = {"a": "z" * 50}
    offloaded = Payload.from_json(value)
    await fakes.store.offload([offloaded], OWNER_ID)
    assert offloaded.blob_id is not None

    ref_only = Payload.from_ref(offloaded.blob_id)
    session = Session(
        owner_id=OWNER_ID, agent_id=uuid.uuid4(), number=1, origin="recorded"
    )
    session.inputs = ref_only
    assert session.inputs is ref_only

    await fakes.store.resolve([session.inputs])
    assert session.inputs is ref_only
    assert session.inputs.value == value
