#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""One shared local operation for exporting a frozen experiment cohort."""

import asyncio
import os
import uuid
from enum import StrEnum
from pathlib import Path
from threading import Event, Lock
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from kitaru.exports.config import EXPORT_TARGET_VERSIONS, ExportFormat, ExportRequest
from kitaru.exports.formats.harbor import render_harbor
from kitaru.exports.formats.verifiers_v1 import render_verifiers_v1
from kitaru.exports.models import (
    ArtifactProvenance,
    BoundedPathSummary,
    ContentPolicy,
    DependencyReceipt,
    EnvironmentPolicy,
    ExportAssurance,
    ExportError,
    ExportManifest,
    ExportWarning,
    RewardSelector,
    RuntimeRequirements,
    SourcePolicy,
    TaskProvenance,
)
from kitaru.exports.resolve import finalize_remote_export, resolve_remote_export
from kitaru.exports.source import inventory_source
from kitaru.exports.writer import (
    commit_staged_bundle,
    discard_staged_bundle,
    stage_bundle,
)


class ExportReceipt(BaseModel):
    """Bounded receipt for a completed preflight or published bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    format: ExportFormat
    dry_run: bool
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    session_count: int
    task_count: int
    evaluator_count: int
    source_digest: str
    destination: str
    archive_path: str | None = None
    bundle_digest: str | None = None
    validation_level: str | None = None
    target_version: str | None = None
    content_policy: ContentPolicy = Field(default_factory=ContentPolicy)
    environment_policy: EnvironmentPolicy = Field(default_factory=EnvironmentPolicy)
    source_policy: SourcePolicy = Field(default_factory=SourcePolicy)
    warnings: tuple[ExportWarning, ...] = ()
    source_exclusions: BoundedPathSummary = Field(
        default_factory=lambda: BoundedPathSummary(
            samples=(), total_count=0, truncated=False
        )
    )
    assurance: ExportAssurance | None = None
    dependencies: DependencyReceipt = Field(default_factory=DependencyReceipt)
    provenance: ArtifactProvenance | None = None
    runtime_requirements: RuntimeRequirements = Field(
        default_factory=RuntimeRequirements
    )
    task_provenance: tuple[TaskProvenance, ...] = ()


class ExportOperationState(StrEnum):
    """Represent one synchronized export operation state."""

    STAGING = "staging"
    REVOCATION_REQUESTED = "revocation_requested"
    COMMITTING = "committing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ExportOperationRevoked(Exception):
    """Stop exporter-owned work after revocation wins commit authority."""


_ALLOWED_STATE_TRANSITIONS: dict[
    ExportOperationState, frozenset[ExportOperationState]
] = {
    ExportOperationState.STAGING: frozenset(
        {
            ExportOperationState.REVOCATION_REQUESTED,
            ExportOperationState.COMMITTING,
            ExportOperationState.COMPLETED,
            ExportOperationState.FAILED,
        }
    ),
    ExportOperationState.REVOCATION_REQUESTED: frozenset(
        {ExportOperationState.CANCELLED, ExportOperationState.FAILED}
    ),
    ExportOperationState.COMMITTING: frozenset(
        {ExportOperationState.COMPLETED, ExportOperationState.FAILED}
    ),
    ExportOperationState.COMPLETED: frozenset(),
    ExportOperationState.CANCELLED: frozenset(),
    ExportOperationState.FAILED: frozenset(),
}


class ExportOperationStateMachine:
    """Synchronize revocation, commit authority, and terminal export states."""

    def __init__(self) -> None:
        """Initialize an operation in its private staging phase."""
        self._state = ExportOperationState.STAGING
        self._lock = Lock()
        self._revocation_requested = Event()

    @property
    def state(self) -> ExportOperationState:
        """Return the current synchronized operation state."""
        with self._lock:
            return self._state

    def _transition(self, target: ExportOperationState) -> None:
        if target not in _ALLOWED_STATE_TRANSITIONS[self._state]:
            raise ExportError(
                "invalid_operation_state",
                "Invalid export state transition: "
                f"{self._state.value} -> {target.value}.",
            )
        self._state = target

    def request_revocation(self) -> bool:
        """Request revocation before the worker acquires commit authority."""
        with self._lock:
            if self._state is not ExportOperationState.STAGING:
                return False
            self._transition(ExportOperationState.REVOCATION_REQUESTED)
            self._revocation_requested.set()
            return True

    def wait_for_revocation(self, timeout: float | None = None) -> bool:
        """Block until revocation is requested, for worker synchronization."""
        return self._revocation_requested.wait(timeout)

    def checkpoint(self) -> None:
        """Stop staging work after revocation has won commit authority."""
        with self._lock:
            if self._state is ExportOperationState.REVOCATION_REQUESTED:
                raise ExportOperationRevoked
            if self._state is not ExportOperationState.STAGING:
                raise ExportError(
                    "invalid_operation_state",
                    f"Cannot stage while export state is {self._state.value}.",
                )

    def try_start_commit(self) -> bool:
        """Acquire commit authority unless revocation already won the gate."""
        with self._lock:
            if self._state is not ExportOperationState.STAGING:
                return False
            self._transition(ExportOperationState.COMMITTING)
            return True

    def mark_cancelled(self) -> None:
        """Acknowledge revocation after cleanup has stopped publication."""
        with self._lock:
            self._transition(ExportOperationState.CANCELLED)

    def mark_completed(self) -> None:
        """Record successful publication or dry-run completion."""
        with self._lock:
            self._transition(ExportOperationState.COMPLETED)

    def mark_failed(self) -> None:
        """Record a handled failure from any nonterminal working state."""
        with self._lock:
            self._transition(ExportOperationState.FAILED)


def _preflight_destination(request: ExportRequest) -> None:
    destination = request.destination.expanduser().absolute()
    if not destination.parent.is_dir():
        raise ExportError(
            "invalid_destination", "Destination parent must already be a directory."
        )
    if os.path.lexists(destination):
        raise ExportError("destination_conflict", f"Destination exists: {destination}")
    archive_path = destination.with_name(f"{destination.name}.zip")
    if request.archive and os.path.lexists(archive_path):
        raise ExportError("archive_conflict", f"Archive exists: {archive_path}")


async def export_experiment(
    client: Any,
    request: ExportRequest,
    *,
    operation: ExportOperationStateMachine | None = None,
) -> ExportReceipt:
    """Resolve an export and optionally publish its deterministic local artifact."""
    state = operation or ExportOperationStateMachine()
    try:
        state.checkpoint()
        _preflight_destination(request)
        remote = await resolve_remote_export(
            client,
            experiment_id=request.experiment_id,
            cohort_version_id=request.cohort_version_id,
            agent_version_id=request.agent_version_id,
            reward=RewardSelector.parse(request.primary_reward),
            environment_policy=request.environment_policy,
        )
        state.checkpoint()
        archive_path = (
            request.destination.with_name(f"{request.destination.name}.zip")
            if request.archive
            else None
        )
        source = await asyncio.to_thread(
            inventory_source,
            request.source_root,
            source_policy=request.source_policy,
            destination=request.destination,
            archive_path=archive_path,
            cancellation_checkpoint=state.checkpoint,
        )
        state.checkpoint()
        resolved = finalize_remote_export(remote, source=source)
        state.checkpoint()
        if request.dry_run:
            target_version = EXPORT_TARGET_VERSIONS[request.format]
            receipt = ExportReceipt(
                format=request.format,
                dry_run=True,
                experiment_id=resolved.experiment.id,
                cohort_version_id=resolved.cohort_version.id,
                agent_version_id=resolved.agent_version.id,
                session_count=len(resolved.sessions),
                task_count=len(resolved.sessions),
                evaluator_count=len(resolved.evaluators),
                source_digest=source.digest,
                destination=str(request.destination.expanduser().absolute()),
                target_version=target_version,
                content_policy=request.content_policy,
                environment_policy=request.environment_policy,
                source_policy=request.source_policy,
                warnings=request.policy_warnings,
                source_exclusions=BoundedPathSummary.from_paths(source.excluded),
                assurance=ExportAssurance.preflight_only(target_version),
            )
            state.mark_completed()
            return receipt

        manifest: ExportManifest | None = None

        def render(root: Path) -> None:
            nonlocal manifest
            state.checkpoint()
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
            state.checkpoint()

        staged = await asyncio.to_thread(
            stage_bundle,
            request.destination,
            render,
            archive=request.archive,
            cancellation_checkpoint=state.checkpoint,
        )
        if not state.try_start_commit():
            await asyncio.to_thread(discard_staged_bundle, staged)
            state.mark_cancelled()
            raise ExportOperationRevoked
        published = await asyncio.to_thread(commit_staged_bundle, staged)
        state.mark_completed()
        assert manifest is not None
        return ExportReceipt(
            format=request.format,
            dry_run=False,
            experiment_id=resolved.experiment.id,
            cohort_version_id=resolved.cohort_version.id,
            agent_version_id=resolved.agent_version.id,
            session_count=len(resolved.sessions),
            task_count=len(resolved.sessions),
            evaluator_count=len(resolved.evaluators),
            source_digest=source.digest,
            destination=str(request.destination.expanduser().absolute()),
            archive_path=(
                str(published.archive_path) if published.archive_path else None
            ),
            bundle_digest=published.digest,
            validation_level=manifest.validation.level,
            target_version=manifest.target_version,
            content_policy=request.content_policy,
            environment_policy=request.environment_policy,
            source_policy=request.source_policy,
            warnings=tuple(
                warning
                for _, warning in sorted(
                    {
                        warning.code: warning
                        for warning in (*manifest.warnings, *request.policy_warnings)
                    }.items()
                )
            ),
            source_exclusions=BoundedPathSummary.from_paths(source.excluded),
            assurance=manifest.assurance,
            dependencies=manifest.dependencies,
            provenance=manifest.provenance,
            runtime_requirements=manifest.runtime_requirements,
            task_provenance=manifest.task_provenance,
        )
    except ExportOperationRevoked:
        if state.state is ExportOperationState.REVOCATION_REQUESTED:
            state.mark_cancelled()
        raise
    except BaseException:
        if state.state in {
            ExportOperationState.STAGING,
            ExportOperationState.REVOCATION_REQUESTED,
            ExportOperationState.COMMITTING,
        }:
            state.mark_failed()
        raise
