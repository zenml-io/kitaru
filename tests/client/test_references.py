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
"""Tests for bounded neutral parent and plugin reference resolution."""

import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.references import (
    ParentKind,
    PluginKind,
    ReferenceResolutionError,
    resolve_parent,
    resolve_plugin_version,
)


@dataclass
class StubParent:
    """Minimal response surface required by the resolver."""

    name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


class StubResource:
    """Resource fake recording direct and bounded calls."""

    def __init__(self, items: list[StubParent]) -> None:
        """Initialize configured items and call records."""
        self.items = items
        self.get_calls: list[uuid.UUID] = []
        self.list_calls: list[Any] = []
        self.version_calls: list[tuple[uuid.UUID, int]] = []

    async def get(self, item_id: uuid.UUID) -> StubParent:
        """Return the matching configured UUID."""
        self.get_calls.append(item_id)
        return next(item for item in self.items if item.id == item_id)

    async def list(self, params: Any = None) -> Any:
        """Return one page without interpreting its filter."""
        self.list_calls.append(params)
        return SimpleNamespace(items=self.items[: params.size], next_cursor=None)

    async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
        """Record and return one direct plugin-version lookup."""
        self.version_calls.append((parent_id, version))
        return SimpleNamespace(parent_id=parent_id, version=version)


class StubClient:
    """Client fake exposing every supported parent family."""

    def __init__(self, resource: StubResource) -> None:
        """Bind the same resource fake to every family."""
        self.agents = resource
        self.cohorts = resource
        self.experiments = resource
        self.importers = resource
        self.evaluators = resource


def _client(resource: StubResource) -> KitaruAPIClient:
    """Cast a structural fake to the production client annotation."""
    return cast(KitaruAPIClient, StubClient(resource))


async def test_uuid_reference_uses_one_get_and_no_list() -> None:
    """A UUID is resolved directly without collection traversal."""
    item = StubParent("Example")
    resource = StubResource([item])

    resolved = await resolve_parent(_client(resource), ParentKind.AGENT, str(item.id))

    assert resolved is item
    assert resource.get_calls == [item.id]
    assert resource.list_calls == []


async def test_name_reference_uses_one_size_two_exact_filter() -> None:
    """A name performs one bounded list and rechecks case locally."""
    item = StubParent("Example")
    resource = StubResource([StubParent("example"), item])

    resolved = await resolve_parent(_client(resource), ParentKind.COHORT, " Example ")

    assert resolved is item
    assert resource.get_calls == []
    assert len(resource.list_calls) == 1
    params = resource.list_calls[0]
    assert params.size == 2
    assert params.filter == FilterCondition(
        field="name", op=FilterOp.EQ, value="Example"
    )


async def test_missing_and_ambiguous_names_are_bounded() -> None:
    """Missing and duplicate exact names produce neutral bounded errors."""
    missing_resource = StubResource([StubParent("example")])
    with pytest.raises(ReferenceResolutionError) as missing:
        await resolve_parent(
            _client(missing_resource), ParentKind.EXPERIMENT, "Example"
        )
    assert missing.value.code == "not_found"
    assert len(missing_resource.list_calls) == 1

    duplicates = [StubParent("same") for _ in range(3)]
    duplicate_resource = StubResource(duplicates)
    with pytest.raises(ReferenceResolutionError) as conflict:
        await resolve_parent(_client(duplicate_resource), ParentKind.EVALUATOR, "same")
    assert conflict.value.code == "conflict"
    assert conflict.value.details == {"ids": [str(item.id) for item in duplicates[:2]]}
    assert len(duplicate_resource.list_calls) == 1


async def test_blank_reference_fails_without_a_request() -> None:
    """Blank names are rejected before either lookup path runs."""
    resource = StubResource([])
    with pytest.raises(ReferenceResolutionError) as error:
        await resolve_parent(_client(resource), ParentKind.IMPORTER, "   ")
    assert error.value.code == "invalid_arguments"
    assert resource.get_calls == []
    assert resource.list_calls == []


async def test_plugin_version_uses_one_direct_endpoint() -> None:
    """Plugin versions never scan a version collection."""
    resource = StubResource([])
    parent_id = uuid.uuid4()

    result = await resolve_plugin_version(
        _client(resource), PluginKind.IMPORTER, parent_id, 3
    )

    assert result.version == 3
    assert resource.version_calls == [(parent_id, 3)]

    with pytest.raises(ReferenceResolutionError):
        await resolve_plugin_version(
            _client(resource), PluginKind.EVALUATOR, parent_id, 0
        )
    assert resource.version_calls == [(parent_id, 3)]
