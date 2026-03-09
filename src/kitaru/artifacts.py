"""Artifact helpers for explicit named artifacts.

``kitaru.save()`` persists a named artifact inside a checkpoint.
``kitaru.load()`` retrieves a named artifact from a previous execution.

Both are valid only inside a checkpoint.

Example::

    from kitaru import checkpoint

    @checkpoint
    def research(topic: str) -> str:
        context = gather_sources(topic)
        kitaru.save("sources", context, type="context")
        return summarize(context)

    # In a later execution:
    @checkpoint
    def refine(exec_id: str) -> str:
        old_sources = kitaru.load(exec_id, "sources")
        return improve(old_sources)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactSaveType, ArtifactType
from zenml.models import PipelineRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru.errors import (
    KitaruContextError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.runtime import (
    _get_current_checkpoint_id,
    _get_current_execution_id,
    _is_inside_checkpoint,
)

_ALLOWED_ARTIFACT_TYPES = {
    "prompt",
    "response",
    "context",
    "input",
    "output",
    "blob",
}
_CHECKPOINT_SOURCE_ALIAS_PREFIX = "__kitaru_checkpoint_source_"


@dataclass(frozen=True)
class _ArtifactMatch:
    """Artifact match candidate during `kitaru.load()` lookup."""

    step_name: str
    artifact: ArtifactVersionResponse


def _parse_scope_uuid(scope_id: str, *, scope_name: str, api_name: str) -> UUID:
    """Parse a runtime scope identifier as a UUID.

    Args:
        scope_id: Raw scope identifier from runtime context.
        scope_name: Human-readable scope name for error messages.
        api_name: Calling Kitaru API name.

    Returns:
        Parsed UUID.

    Raises:
        KitaruStateError: If the scope identifier is not a valid UUID.
    """
    try:
        return UUID(scope_id)
    except ValueError as exc:
        raise KitaruStateError(
            f"kitaru.{api_name}() found an invalid {scope_name} ID in runtime "
            f"scope: {scope_id!r}."
        ) from exc


def _require_checkpoint_scope(api_name: str) -> tuple[UUID, UUID]:
    """Validate that an API call happens inside a checkpoint scope.

    Args:
        api_name: Calling Kitaru API name.

    Returns:
        Tuple of current execution UUID and checkpoint UUID.

    Raises:
        KitaruContextError: If called outside checkpoint scope.
        KitaruStateError: If scope IDs are missing or invalid.
    """
    if not _is_inside_checkpoint():
        raise KitaruContextError(
            f"kitaru.{api_name}() can only be called inside a @checkpoint."
        )

    execution_id = _get_current_execution_id()
    if execution_id is None:
        raise KitaruStateError(
            f"kitaru.{api_name}() requires an active execution ID inside @checkpoint."
        )

    checkpoint_id = _get_current_checkpoint_id()
    if checkpoint_id is None:
        raise KitaruStateError(
            f"kitaru.{api_name}() requires an active checkpoint ID inside @checkpoint."
        )

    return (
        _parse_scope_uuid(execution_id, scope_name="execution", api_name=api_name),
        _parse_scope_uuid(checkpoint_id, scope_name="checkpoint", api_name=api_name),
    )


def _normalize_artifact_type(artifact_type: str) -> str:
    """Validate and normalize Kitaru artifact type labels.

    Args:
        artifact_type: Artifact type supplied by the user.

    Returns:
        Normalized artifact type.

    Raises:
        KitaruUsageError: If the type is unsupported.
    """
    normalized = artifact_type.strip().lower()
    if normalized not in _ALLOWED_ARTIFACT_TYPES:
        allowed = ", ".join(sorted(_ALLOWED_ARTIFACT_TYPES))
        raise KitaruUsageError(
            "Unsupported Kitaru artifact type "
            f"{artifact_type!r}. Expected one of: {allowed}."
        )
    return normalized


def _normalize_step_name(step_name: str) -> str:
    """Normalize ZenML step names back to user-facing checkpoint names."""
    if step_name.startswith(_CHECKPOINT_SOURCE_ALIAS_PREFIX):
        return step_name.removeprefix(_CHECKPOINT_SOURCE_ALIAS_PREFIX)
    return step_name


def _matches_requested_name(
    *,
    step_name: str,
    artifact: ArtifactVersionResponse,
    requested_name: str,
) -> bool:
    """Check whether an artifact matches a Kitaru `load()` name lookup."""
    normalized_step_name = _normalize_step_name(step_name)

    if artifact.save_type == ArtifactSaveType.MANUAL:
        return artifact.name == requested_name

    if artifact.save_type == ArtifactSaveType.STEP_OUTPUT:
        return (
            artifact.name == requested_name
            or step_name == requested_name
            or normalized_step_name == requested_name
        )

    return False


def _collect_named_artifact_matches(
    *,
    run: PipelineRunResponse,
    requested_name: str,
) -> list[_ArtifactMatch]:
    """Collect candidate artifacts in a run for a given Kitaru lookup name."""
    matches: list[_ArtifactMatch] = []
    seen_artifact_ids: set[UUID] = set()

    for step_name, step in run.steps.items():
        for output_artifacts in step.outputs.values():
            for artifact in output_artifacts:
                if not _matches_requested_name(
                    step_name=step_name,
                    artifact=artifact,
                    requested_name=requested_name,
                ):
                    continue

                if artifact.id in seen_artifact_ids:
                    continue

                seen_artifact_ids.add(artifact.id)
                matches.append(_ArtifactMatch(step_name=step_name, artifact=artifact))

    return matches


def _format_match(match: _ArtifactMatch) -> str:
    """Format a match for ambiguity error messages."""
    step_name = _normalize_step_name(match.step_name)
    return (
        f"step='{step_name}', artifact='{match.artifact.name}', "
        f"save_type='{match.artifact.save_type.value}'"
    )


def save(
    name: str,
    value: Any,
    *,
    type: str = "output",
    tags: list[str] | None = None,
) -> None:
    """Persist a named artifact inside the current checkpoint.

    Args:
        name: Artifact name (unique within the checkpoint).
        value: The value to persist. Must be serializable.
        type: Artifact type for categorization (one of ``"prompt"``,
            ``"response"``, ``"context"``, ``"input"``, ``"output"``,
            ``"blob"``).
        tags: Optional tags for filtering and discovery.

    Raises:
        KitaruContextError: If called outside a checkpoint.
        KitaruStateError: If runtime scope IDs are missing or invalid.
        KitaruUsageError: If `type` is unsupported.
    """
    _require_checkpoint_scope("save")
    artifact_type = _normalize_artifact_type(type)

    save_artifact(
        data=value,
        name=name,
        artifact_type=ArtifactType.DATA,
        tags=tags,
        user_metadata={"kitaru_artifact_type": artifact_type},
    )


def load(exec_id: str, name: str) -> Any:
    """Load a named artifact from a previous execution.

    Args:
        exec_id: The execution ID to load from.
        name: The artifact name to retrieve.

    Returns:
        The materialized artifact value.

    Raises:
        KitaruContextError: If called outside a checkpoint.
        KitaruStateError: If runtime scope is invalid.
        KitaruRuntimeError: If lookup is not found or ambiguous.
        KitaruUsageError: If `exec_id` is not a valid UUID.
    """
    _require_checkpoint_scope("load")

    try:
        target_execution_id = UUID(exec_id)
    except ValueError as exc:
        raise KitaruUsageError(
            f"kitaru.load() expected `exec_id` to be a UUID, got {exec_id!r}."
        ) from exc

    client = Client()
    run = client.get_pipeline_run(
        target_execution_id,
        allow_name_prefix_match=False,
    )
    hydrated_run = run.get_hydrated_version()
    matches = _collect_named_artifact_matches(
        run=hydrated_run,
        requested_name=name,
    )

    if not matches:
        raise KitaruRuntimeError(
            f"No artifact named {name!r} was found in execution {target_execution_id}."
        )

    if len(matches) > 1:
        details = ", ".join(_format_match(match) for match in matches)
        raise KitaruRuntimeError(
            f"Multiple artifacts named {name!r} were found in execution "
            f"{target_execution_id}. Please disambiguate by choosing a unique "
            f"artifact name. Matches: {details}."
        )

    selected = matches[0].artifact
    selected_hydrated = client.get_artifact_version(selected.id, hydrate=True)
    return selected_hydrated.load()
