#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""One shared local operation for exporting a frozen experiment cohort."""

import asyncio
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from kitaru.exports.config import ExportFormat, ExportRequest
from kitaru.exports.formats.harbor import render_harbor
from kitaru.exports.formats.verifiers_v1 import render_verifiers_v1
from kitaru.exports.models import ExportError, ExportManifest, RewardSelector
from kitaru.exports.resolve import resolve_export
from kitaru.exports.source import inventory_source
from kitaru.exports.writer import publish_bundle


class ExportReceipt(BaseModel):
    """Bounded receipt for a completed preflight or published bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    format: ExportFormat
    dry_run: bool
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    session_count: int
    evaluator_count: int
    source_digest: str
    destination: str
    archive_path: str | None = None
    bundle_digest: str | None = None
    validation_level: str | None = None


def _preflight_destination(request: ExportRequest) -> None:
    destination = request.destination.expanduser().absolute()
    if not destination.parent.is_dir():
        raise ExportError(
            "invalid_destination", "Destination parent must already be a directory."
        )
    if destination.exists():
        raise ExportError("destination_conflict", f"Destination exists: {destination}")
    archive_path = destination.with_name(f"{destination.name}.zip")
    if request.archive and archive_path.exists():
        raise ExportError("archive_conflict", f"Archive exists: {archive_path}")


async def export_experiment(client: Any, request: ExportRequest) -> ExportReceipt:
    """Resolve an export and optionally publish its deterministic local artifact."""
    _preflight_destination(request)
    source = await asyncio.to_thread(
        inventory_source,
        request.source_root,
        destination=request.destination,
    )
    resolved = await resolve_export(
        client,
        experiment_id=request.experiment_id,
        cohort_version_id=request.cohort_version_id,
        agent_version_id=request.agent_version_id,
        reward=RewardSelector.parse(request.primary_reward),
        source=source,
    )
    if request.dry_run:
        return ExportReceipt(
            format=request.format,
            dry_run=True,
            experiment_id=resolved.experiment.id,
            cohort_version_id=resolved.cohort_version.id,
            agent_version_id=resolved.agent_version.id,
            session_count=len(resolved.sessions),
            evaluator_count=len(resolved.evaluators),
            source_digest=source.digest,
            destination=str(request.destination.expanduser().absolute()),
        )

    manifest: ExportManifest | None = None

    def render(root: Path) -> None:
        nonlocal manifest
        if request.format == "harbor":
            assert request.trace_format is not None
            assert request.trace_path is not None
            manifest = render_harbor(
                resolved,
                root,
                trace_format=request.trace_format,
                trace_path=request.trace_path,
                required_environment_names=request.required_environment_names,
            )
        else:
            manifest = render_verifiers_v1(
                resolved,
                root,
                required_environment_names=request.required_environment_names,
            )

    published = await asyncio.to_thread(
        publish_bundle,
        request.destination,
        render,
        archive=request.archive,
    )
    assert manifest is not None
    return ExportReceipt(
        format=request.format,
        dry_run=False,
        experiment_id=resolved.experiment.id,
        cohort_version_id=resolved.cohort_version.id,
        agent_version_id=resolved.agent_version.id,
        session_count=len(resolved.sessions),
        evaluator_count=len(resolved.evaluators),
        source_digest=source.digest,
        destination=str(request.destination.expanduser().absolute()),
        archive_path=str(published.archive_path) if published.archive_path else None,
        bundle_digest=published.digest,
        validation_level=manifest.validation.level,
    )
