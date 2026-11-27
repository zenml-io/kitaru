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
"""Tests for call_fetcher user-facing error passthrough."""

from collections.abc import AsyncIterator
from typing import Any

import pytest

from kitaru.task.importer import SessionImportError, call_fetcher


async def test_call_fetcher_passes_through_session_import_error() -> None:
    """Keep a fetcher's SessionImportError text unchanged."""
    message = (
        "Langfuse rejected the API keys for https://cloud.langfuse.com. "
        "Check that the public and secret key come from the same project."
    )

    def fetcher(query: dict[str, Any]) -> Any:
        raise SessionImportError(message)

    with pytest.raises(SessionImportError) as caught:
        await anext(call_fetcher(fetcher, {}))
    assert str(caught.value) == message
    assert "Fetcher raised an error" not in str(caught.value)


async def test_call_fetcher_passes_through_mid_stream_session_import_error() -> None:
    """Keep a mid-stream SessionImportError text unchanged."""
    message = (
        "Couldn't reach Langfuse at https://cloud.langfuse.com. "
        "Check the host URL in the connection."
    )

    async def fetcher(query: dict[str, Any]) -> AsyncIterator[bytes]:
        yield b"first"
        raise SessionImportError(message)

    iterator = call_fetcher(fetcher, {})
    assert await anext(iterator) == b"first"
    with pytest.raises(SessionImportError) as caught:
        await anext(iterator)
    assert str(caught.value) == message
    assert "Fetcher raised an error" not in str(caught.value)
