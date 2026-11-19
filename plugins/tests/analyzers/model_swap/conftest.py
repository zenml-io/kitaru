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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Fixtures for the model-swap analyzer tests."""

import pytest

from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru_model_swap import analyzer as analyzer_module

from .helpers import StubClient, load_quickstart_sessions


@pytest.fixture(scope="session")
def quickstart_sessions() -> list[SessionWithNodesResponse]:
    """The 10 quickstart sessions as stored nodes."""
    return load_quickstart_sessions()


@pytest.fixture
def stub_client(
    monkeypatch: pytest.MonkeyPatch,
    quickstart_sessions: list[SessionWithNodesResponse],
) -> StubClient:
    """Patch the analyzer's API client with the quickstart sessions."""
    client = StubClient(quickstart_sessions)
    monkeypatch.setattr(analyzer_module, "KitaruAPIClient", lambda: client)
    return client
