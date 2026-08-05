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
"""Tests for explicit development-plugin seeding."""

import hashlib
import runpy
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

_SCRIPT = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "scripts/seed_default_plugins.py")
)
PluginDefinition = _SCRIPT["PluginDefinition"]
seed_plugin = _SCRIPT["seed_plugin"]


class FakeBlobs:
    """In-memory content-addressed blob resource."""

    def __init__(self) -> None:
        """Initialize an empty blob store."""
        self.items: dict[uuid.UUID, Any] = {}

    async def upload(
        self, content: bytes, media_type: str, filename: str
    ) -> SimpleNamespace:
        """Store source bytes and return their metadata."""
        del media_type, filename
        digest = hashlib.sha256(content).hexdigest()
        for blob in self.items.values():
            if blob.sha256 == digest:
                return blob
        blob = SimpleNamespace(id=uuid.uuid4(), sha256=digest)
        self.items[blob.id] = blob
        return blob

    async def get(self, blob_id: uuid.UUID) -> SimpleNamespace:
        """Return stored blob metadata."""
        return self.items[blob_id]


class FakePluginResource:
    """In-memory importer or evaluator resource."""

    def __init__(self) -> None:
        """Initialize empty parent and version stores."""
        self.parents: list[Any] = []
        self.versions: dict[tuple[uuid.UUID, int], Any] = {}
        self.list_params: list[Any] = []

    async def list(self, params: Any) -> SimpleNamespace:
        """Return parents matching the requested exact-name filter."""
        self.list_params.append(params)
        condition = params.filter
        items = [
            parent
            for parent in self.parents
            if getattr(parent, condition.field) == condition.value
        ]
        return SimpleNamespace(items=items[: params.size])

    async def create(self, request: Any) -> SimpleNamespace:
        """Create one plugin parent."""
        parent = SimpleNamespace(
            id=uuid.uuid4(),
            name=request.name,
            metadata=request.metadata,
            latest_version=0,
        )
        self.parents.append(parent)
        return parent

    async def create_version(
        self, parent_id: uuid.UUID, request: Any
    ) -> SimpleNamespace:
        """Create the next version for one parent."""
        parent = next(item for item in self.parents if item.id == parent_id)
        parent.latest_version += 1
        version = SimpleNamespace(
            version=parent.latest_version,
            display_version=request.display_version,
            source=request.source,
        )
        self.versions[(parent_id, version.version)] = version
        return version

    async def get_version(self, parent_id: uuid.UUID, version: int) -> Any:
        """Return one stored plugin version."""
        return self.versions[(parent_id, version)]


class FakeClient:
    """Client exposing the resources used by the seed workflow."""

    def __init__(self) -> None:
        """Initialize independent importer, evaluator, and blob stores."""
        self.blobs = FakeBlobs()
        self.importers = FakePluginResource()
        self.evaluators = FakePluginResource()


def _definition(path: Path) -> PluginDefinition:
    """Build one importer definition for a temporary source file."""
    return PluginDefinition(
        kind="importer",
        name="provider",
        path=path,
        entrypoint="parse",
        description="Import provider traces.",
        provider="provider",
    )


async def test_seed_creates_only_changed_versions(tmp_path: Path) -> None:
    """Create a parent once and a version for each distinct source digest."""
    source = tmp_path / "provider.py"
    source.write_text("def parse(content, params):\n    return iter(())\n")
    client = FakeClient()
    definition = _definition(source)

    first = await seed_plugin(client, definition, "commit-one")
    unchanged = await seed_plugin(client, definition, "commit-one")
    source.write_text("def parse(content, params):\n    yield from ()\n")
    changed = await seed_plugin(client, definition, "commit-two")

    assert first["action"] == "created_version"
    assert first["version"] == 1
    assert unchanged == {
        "kind": "importer",
        "name": "provider",
        "action": "unchanged",
        "version": 1,
    }
    assert changed["action"] == "created_version"
    assert changed["version"] == 2
    assert len(client.importers.parents) == 1
    assert all(params.filter.field == "name" for params in client.importers.list_params)
    assert all(params.size == 2 for params in client.importers.list_params)


async def test_seed_refuses_unmarked_existing_parent(tmp_path: Path) -> None:
    """Do not attach built-in code to a user-created plugin with the same name."""
    source = tmp_path / "provider.py"
    source.write_text("def parse(content, params):\n    return iter(())\n")
    client = FakeClient()
    client.importers.parents.append(
        SimpleNamespace(id=uuid.uuid4(), name="provider", metadata={}, latest_version=0)
    )

    with pytest.raises(RuntimeError, match="Refusing to modify unmarked importer"):
        await seed_plugin(client, _definition(source))
