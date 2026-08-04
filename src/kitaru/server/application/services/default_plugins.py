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
"""Default plugins for local Kitaru servers."""

import uuid
from importlib.metadata import version

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginNotFound,
)

KITARU_VERSION = version("kitaru")
DEFAULT_IMPORTERS = {
    "braintrust": (
        "braintrust",
        "Import Braintrust project-log and UI JSON exports.",
        "kitaru.importers.braintrust:parse",
    ),
    "langfuse": (
        "langfuse",
        "Import Langfuse JSON and JSONL trace exports.",
        "kitaru.importers.langfuse:parse",
    ),
    "otlp": (
        "opentelemetry",
        "Import OpenTelemetry OTLP JSON, JSONL, and NDJSON exports.",
        "kitaru.importers.otlp:parse",
    ),
}
DEFAULT_EVALUATORS = {
    "cost": "kitaru.evaluators.basic:cost",
    "latency": "kitaru.evaluators.basic:latency",
    "tool-call-patterns": "kitaru.evaluators.basic:tool_call_patterns",
}


async def _plugin_exists(
    repository: PluginRepository, kind: PluginKind, name: str
) -> bool:
    """Check whether a named plugin already exists.

    Args:
        repository: Plugin registry persistence.
        kind: Plugin kind to query.
        name: Plugin name to query.

    Returns:
        Whether the plugin exists.
    """
    try:
        await repository.get_by_name(kind, name)
    except PluginNotFound:
        return False
    return True


async def ensure_default_importers(
    repository: PluginRepository, owner_id: uuid.UUID
) -> None:
    """Create the default importers for a local account when absent.

    Args:
        repository: Plugin registry persistence.
        owner_id: Local account that owns the defaults.
    """
    for name, (provider, description, entrypoint) in DEFAULT_IMPORTERS.items():
        if await _plugin_exists(repository, PluginKind.IMPORTER, name):
            continue
        importer = await repository.create(
            Plugin(
                owner_id=owner_id,
                kind=PluginKind.IMPORTER,
                name=name,
                description=description,
                provider=provider,
                metadata={"built_in": True},
            )
        )
        await repository.create_version(
            importer.id,
            PackagePluginSource(
                requirement=f"kitaru=={KITARU_VERSION}",
                entrypoint=entrypoint,
            ),
            KITARU_VERSION,
        )


async def ensure_default_evaluators(
    repository: PluginRepository, owner_id: uuid.UUID
) -> None:
    """Create low-cost starting-point evaluators when absent.

    Args:
        repository: Plugin registry persistence.
        owner_id: Local account that owns the defaults.
    """
    for name, entrypoint in DEFAULT_EVALUATORS.items():
        if await _plugin_exists(repository, PluginKind.EVALUATOR, name):
            continue
        evaluator = await repository.create(
            Plugin(
                owner_id=owner_id,
                kind=PluginKind.EVALUATOR,
                name=name,
                description="Compute a deterministic signal for session review.",
                metadata={"built_in": True},
            )
        )
        await repository.create_version(
            evaluator.id,
            PackagePluginSource(
                requirement=f"kitaru=={KITARU_VERSION}",
                entrypoint=entrypoint,
            ),
            KITARU_VERSION,
        )


async def ensure_default_plugins(
    repository: PluginRepository, owner_id: uuid.UUID
) -> None:
    """Create all default plugins for a local account.

    Args:
        repository: Plugin registry persistence.
        owner_id: Local account that owns the defaults.
    """
    await ensure_default_importers(repository, owner_id)
    await ensure_default_evaluators(repository, owner_id)
