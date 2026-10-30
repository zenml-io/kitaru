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
"""Langfuse importer token usage tests."""

import json

import pytest

from kitaru.api_models.v1.session import TokenUsage
from kitaru.task.importer import ImportedSession
from kitaru_langfuse_importer.importer import LangfuseJSONLImporter


def _get_tokens(usage_details: dict[str, int]) -> TokenUsage | None:
    record = {
        "id": "generation",
        "traceId": "trace-1",
        "sessionId": "conversation-1",
        "projectId": "project-1",
        "type": "GENERATION",
        "name": "generation",
        "startTime": "2026-07-24T10:00:00Z",
        "endTime": "2026-07-24T10:00:01Z",
        "usageDetails": usage_details,
    }
    [session] = LangfuseJSONLImporter().parse(json.dumps(record).encode(), {})
    assert isinstance(session, ImportedSession)
    return session.nodes[0].tokens


@pytest.mark.parametrize(
    "writes",
    [
        {"input_cache_write_tokens": 300},
        {"input_cache_creation": 300},
        {"cache_write_tokens": 300},
        {"cache_creation_input_tokens": 300},
        # LangChain's shape when Anthropic splits writes by cache lifetime.
        {
            "input_cache_creation": 0,
            "input_ephemeral_5m_input_tokens": 200,
            "input_ephemeral_1h_input_tokens": 100,
        },
    ],
)
def test_maps_cache_write_usage_details(writes: dict[str, int]) -> None:
    """Keep Langfuse's input count, which excludes cache reads and writes."""
    tokens = _get_tokens({"input": 1200, "input_cached_tokens": 800, **writes})

    assert tokens == TokenUsage(
        input_tokens=1200, cached_input_tokens=800, cache_write_input_tokens=300
    )


def test_leaves_cache_writes_unset_without_a_source_count() -> None:
    tokens = _get_tokens({"input": 5168, "input_cached_tokens": 5504})

    assert tokens is not None
    assert tokens.cache_write_input_tokens is None
