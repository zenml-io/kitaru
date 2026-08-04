#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Tests for default local-server plugins."""

import uuid

from conftest import FakePluginRepository
from kitaru.server.application.services.default_plugins import (
    DEFAULT_EVALUATORS,
    DEFAULT_IMPORTERS,
    KITARU_VERSION,
    ensure_default_evaluators,
    ensure_default_importers,
)
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru.task.plugins import load_source_ref


def test_default_entrypoints_ship_in_kitaru() -> None:
    """Load every default implementation from the main distribution."""
    entrypoints = [item[2] for item in DEFAULT_IMPORTERS.values()]
    entrypoints.extend(DEFAULT_EVALUATORS.values())

    for entrypoint in entrypoints:
        assert callable(load_source_ref(entrypoint, "Default plugin"))


async def test_create_default_importers() -> None:
    """Create package-backed importers from the Kitaru distribution."""
    repository = FakePluginRepository()
    owner_id = uuid.uuid4()

    await ensure_default_importers(repository, owner_id)

    for name, (provider, _, entrypoint) in DEFAULT_IMPORTERS.items():
        importer = await repository.get_by_name(PluginKind.IMPORTER, name)
        importer_version = await repository.get_version(importer.id, 1)
        assert importer.owner_id == owner_id
        assert importer.provider == provider
        assert importer.metadata == {"built_in": True}
        assert importer.latest_version == 1
        assert importer_version.display_version == KITARU_VERSION
        assert importer_version.source == PackagePluginSource(
            requirement=f"kitaru=={KITARU_VERSION}", entrypoint=entrypoint
        )


async def test_preserve_existing_importer() -> None:
    """Leave an existing importer unchanged during local startup."""
    repository = FakePluginRepository()
    existing = await repository.create(
        Plugin(
            owner_id=uuid.uuid4(),
            kind=PluginKind.IMPORTER,
            name="langfuse",
            provider="langfuse",
        )
    )

    await ensure_default_importers(repository, uuid.uuid4())

    importer = await repository.get(existing.id)
    assert importer == existing
    assert importer.latest_version == 0


async def test_create_default_evaluators() -> None:
    """Create package-backed starting-point evaluators for a local account."""
    repository = FakePluginRepository()
    owner_id = uuid.uuid4()

    await ensure_default_evaluators(repository, owner_id)

    for name, entrypoint in DEFAULT_EVALUATORS.items():
        evaluator = await repository.get_by_name(PluginKind.EVALUATOR, name)
        evaluator_version = await repository.get_version(evaluator.id, 1)
        assert evaluator.owner_id == owner_id
        assert evaluator.metadata == {"built_in": True}
        assert evaluator_version.source == PackagePluginSource(
            requirement=f"kitaru=={KITARU_VERSION}", entrypoint=entrypoint
        )
