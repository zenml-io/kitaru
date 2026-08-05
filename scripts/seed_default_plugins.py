#!/usr/bin/env python3
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
"""Seed development importers and evaluators through the public Kitaru API."""

import argparse
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterVersionCreateRequest,
)
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.client import KitaruAPIClient

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_SEED_MARKER = "scripts/seed_default_plugins.py"


@dataclass(frozen=True, slots=True)
class PluginDefinition:
    """One development plugin registered by this script."""

    kind: Literal["importer", "evaluator"]
    name: str
    path: Path
    entrypoint: str
    description: str
    provider: str | None = None


PLUGIN_DEFINITIONS = (
    PluginDefinition(
        kind="importer",
        name="braintrust",
        path=_REPOSITORY_ROOT / "src/kitaru/importers/braintrust.py",
        entrypoint="parse",
        description="Import Braintrust project-log and UI JSON exports.",
        provider="braintrust",
    ),
    PluginDefinition(
        kind="importer",
        name="langfuse",
        path=_REPOSITORY_ROOT / "src/kitaru/importers/langfuse.py",
        entrypoint="parse",
        description="Import Langfuse JSON and JSONL trace exports.",
        provider="langfuse",
    ),
    PluginDefinition(
        kind="importer",
        name="otlp",
        path=_REPOSITORY_ROOT / "src/kitaru/importers/otlp.py",
        entrypoint="parse",
        description="Import OpenTelemetry OTLP JSON, JSONL, and NDJSON exports.",
        provider="opentelemetry",
    ),
    PluginDefinition(
        kind="evaluator",
        name="cost",
        path=_REPOSITORY_ROOT / "src/kitaru/evaluators/basic.py",
        entrypoint="cost",
        description="Report the total recorded session cost.",
    ),
    PluginDefinition(
        kind="evaluator",
        name="latency",
        path=_REPOSITORY_ROOT / "src/kitaru/evaluators/basic.py",
        entrypoint="latency",
        description="Measure session wall-clock duration.",
    ),
    PluginDefinition(
        kind="evaluator",
        name="tool-call-patterns",
        path=_REPOSITORY_ROOT / "src/kitaru/evaluators/basic.py",
        entrypoint="tool_call_patterns",
        description="Count repeated calls to the same tool.",
    ),
)


def _get_digest(content: bytes) -> str:
    """Return the SHA-256 digest of plugin source bytes."""
    return hashlib.sha256(content).hexdigest()


async def _get_parent(
    resource: Any, kind: Literal["importer", "evaluator"], name: str
) -> Any | None:
    """Return an exact named plugin parent when it exists."""
    params_type = ImporterListParams if kind == "importer" else EvaluatorListParams
    page = await resource.list(
        params_type(
            size=2,
            filter=FilterCondition(field="name", op=FilterOp.EQ, value=name),
        )
    )
    matches = page.items
    if len(matches) > 1:
        raise RuntimeError(f"More than one plugin is named '{name}'")
    return matches[0] if matches else None


def _is_seeded_parent(parent: Any) -> bool:
    """Return whether an existing parent is managed by a Kitaru seed path."""
    return bool(
        parent.metadata.get("built_in")
        or parent.metadata.get("seeded_by") == _SEED_MARKER
    )


async def _latest_matches(
    client: KitaruAPIClient,
    resource: Any,
    parent: Any,
    definition: PluginDefinition,
    digest: str,
) -> tuple[bool, bool]:
    """Return whether the latest source matches and whether it is package-backed."""
    if parent.latest_version < 1:
        return False, False
    latest = await resource.get_version(parent.id, parent.latest_version)
    if isinstance(latest.source, PackagePluginSource):
        return False, True
    if not isinstance(latest.source, ScriptPluginSource):
        return False, False
    blob = await client.blobs.get(latest.source.blob_id)
    return (
        blob.sha256 == digest and latest.source.entrypoint == definition.entrypoint,
        False,
    )


async def _create_parent(resource: Any, definition: PluginDefinition) -> Any:
    """Create one marked importer or evaluator parent."""
    metadata = {"built_in": True, "seeded_by": _SEED_MARKER}
    if definition.kind == "importer":
        return await resource.create(
            ImporterCreateRequest(
                name=definition.name,
                description=definition.description,
                provider=definition.provider,
                metadata=metadata,
            )
        )
    return await resource.create(
        EvaluatorCreateRequest(
            name=definition.name,
            description=definition.description,
            metadata=metadata,
        )
    )


async def _create_version(
    client: KitaruAPIClient,
    resource: Any,
    parent: Any,
    definition: PluginDefinition,
    content: bytes,
    display_version: str,
) -> Any:
    """Upload source and create one immutable script plugin version."""
    blob = await client.blobs.upload(
        content,
        media_type="text/x-python",
        filename=definition.path.name,
    )
    source = ScriptPluginSource(blob_id=blob.id, entrypoint=definition.entrypoint)
    if definition.kind == "importer":
        request = ImporterVersionCreateRequest(
            source=source, display_version=display_version
        )
    else:
        request = EvaluatorVersionCreateRequest(
            source=source, display_version=display_version
        )
    return await resource.create_version(parent.id, request)


async def seed_plugin(
    client: KitaruAPIClient,
    definition: PluginDefinition,
    display_version: str | None = None,
) -> dict[str, Any]:
    """Seed one plugin and return its action summary."""
    content = definition.path.read_bytes()
    digest = _get_digest(content)
    version_label = display_version or f"inline-{digest[:12]}"
    resource = client.importers if definition.kind == "importer" else client.evaluators
    parent = await _get_parent(resource, definition.kind, definition.name)
    if parent is None:
        parent = await _create_parent(resource, definition)
    elif not _is_seeded_parent(parent):
        raise RuntimeError(
            f"Refusing to modify unmarked {definition.kind} '{definition.name}'"
        )
    matches, package_backed = await _latest_matches(
        client, resource, parent, definition, digest
    )
    if package_backed:
        return {
            "kind": definition.kind,
            "name": definition.name,
            "action": "skipped_package_version",
            "version": parent.latest_version,
        }
    if matches:
        return {
            "kind": definition.kind,
            "name": definition.name,
            "action": "unchanged",
            "version": parent.latest_version,
        }
    version = await _create_version(
        client, resource, parent, definition, content, version_label
    )
    return {
        "kind": definition.kind,
        "name": definition.name,
        "action": "created_version",
        "version": version.version,
        "display_version": version.display_version,
        "digest": digest,
    }


async def seed_defaults(display_version: str | None = None) -> list[dict[str, Any]]:
    """Seed every development plugin into the configured Kitaru workspace."""
    async with KitaruAPIClient() as client:
        return [
            await seed_plugin(client, definition, display_version)
            for definition in PLUGIN_DEFINITIONS
        ]


def main() -> None:
    """Parse arguments, seed the selected workspace, and print JSON results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--display-version",
        default=os.environ.get("KITARU_PLUGIN_DISPLAY_VERSION"),
        help="Label applied to newly created versions, such as a Git commit SHA.",
    )
    args = parser.parse_args()
    print(json.dumps(asyncio.run(seed_defaults(args.display_version)), indent=2))


if __name__ == "__main__":
    main()
