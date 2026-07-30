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

from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.api_models.v1.replay_config import PassthroughConfig


def test_discriminators_survive_exclude_unset() -> None:
    """Keep a discriminated model's default type field on an unset-only dump."""
    assert PassthroughConfig().model_dump(mode="json", exclude_unset=True) == {
        "type": "passthrough"
    }
    source = ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="evaluate")
    assert source.model_dump(mode="json", exclude_unset=True)["type"] == "script"
