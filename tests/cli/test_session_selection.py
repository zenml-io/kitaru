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
"""Shared CLI session-selection behavior."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.filter import FilterCondition
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.output import CLIError
from kitaru.cli.session_selection import select_session_ids


class SelectionClient:
    """Protocol-shaped client exposing selection resources."""

    def __init__(self) -> None:
        self.agent = SimpleNamespace(id=uuid.uuid4(), name="assistant")
        self.cohort = SimpleNamespace(
            id=uuid.uuid4(), name="regression", latest_version=1
        )
        self.cohort_version = SimpleNamespace(
            id=uuid.uuid4(), cohort_id=self.cohort.id, version=1
        )
        self.selected = [SimpleNamespace(id=uuid.uuid4())]
        self.params: SessionListParams | None = None
        self.agents = self._Agents(self)
        self.cohorts = self._Cohorts(self)
        self.cohort_versions = self._CohortVersions(self)
        self.sessions = self._Sessions(self)

    class _Agents:
        def __init__(self, owner: "SelectionClient") -> None:
            self.owner = owner

        async def iter(self):
            yield self.owner.agent

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            return SimpleNamespace(items=[self.owner.agent], next_cursor=None)

    class _Cohorts:
        def __init__(self, owner: "SelectionClient") -> None:
            self.owner = owner

        async def iter(self):
            yield self.owner.cohort

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            return SimpleNamespace(items=[self.owner.cohort], next_cursor=None)

        async def iter_versions(self, cohort_id: uuid.UUID):
            assert cohort_id == self.owner.cohort.id
            yield self.owner.cohort_version

    class _CohortVersions:
        def __init__(self, owner: "SelectionClient") -> None:
            self.owner = owner

        async def get(self, version_id: uuid.UUID):
            assert version_id == self.owner.cohort_version.id
            return self.owner.cohort_version

    class _Sessions:
        def __init__(self, owner: "SelectionClient") -> None:
            self.owner = owner

        async def iter(self, params: SessionListParams):
            self.owner.params = params
            for session in self.owner.selected:
                yield session


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"tag": "baseline"}, "tag"),
        ({"agent": "assistant"}, "agent_id"),
        ({"cohort": "regression@1"}, "cohort_version_id"),
        (
            {"filter": '{"field":"origin","op":"eq","value":"imported"}'},
            "origin",
        ),
    ],
)
async def test_select_session_ids_resolves_each_remote_selector(
    kwargs: dict[str, Any], field: str
) -> None:
    """Resolve supported selectors through one paginated session snapshot."""
    client = SelectionClient()

    selected = await select_session_ids(client, None, None, **kwargs)

    assert selected == [client.selected[0].id]
    assert client.params is not None
    session_filter = client.params.filter
    assert isinstance(session_filter, FilterCondition)
    assert session_filter.field == field


async def test_select_session_ids_rejects_mixed_or_empty_modes() -> None:
    """Require one selector mode before reading remote sessions."""
    client = SelectionClient()

    with pytest.raises(CLIError, match="--filter, or --all"):
        await select_session_ids(
            client,
            [str(uuid.uuid4())],
            None,
            tag="baseline",
        )
    with pytest.raises(CLIError, match="--filter, or --all"):
        await select_session_ids(client, None, None)
