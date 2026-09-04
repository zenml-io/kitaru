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
"""Tests for shared API model base classes."""

import uuid

from pydantic import BaseModel, SecretStr

from kitaru.api_models.v1.base import PlainSerializedSecretStr
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay_config import PassthroughConfig


def test_discriminators_survive_exclude_unset() -> None:
    """Keep a discriminated model's default type field on an unset-only dump."""
    assert PassthroughConfig().model_dump(mode="json", exclude_unset=True) == {
        "type": "passthrough"
    }
    source = ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="evaluate")
    assert source.model_dump(mode="json", exclude_unset=True)["type"] == "script"


class _SecretHolder(BaseModel):
    token: PlainSerializedSecretStr


def test_plain_serialized_secret_reveals_on_json_only() -> None:
    """Emit the secret in JSON output and keep it redacted in Python output."""
    holder = _SecretHolder(token=SecretStr("plain"))
    assert holder.model_dump(mode="json") == {"token": "plain"}
    assert holder.model_dump()["token"].get_secret_value() == "plain"
    assert "plain" not in repr(holder)


def test_plain_serialized_secret_schema_is_write_only_on_input_only() -> None:
    """Declare the secret as a plain string in the serialization schema."""
    validation = _SecretHolder.model_json_schema(mode="validation")
    assert validation["properties"]["token"]["writeOnly"] is True
    assert validation["properties"]["token"]["format"] == "password"
    serialization = _SecretHolder.model_json_schema(mode="serialization")
    assert serialization["properties"]["token"] == {
        "title": "Token",
        "type": "string",
    }
