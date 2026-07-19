"""Immutable experiment target membership persistence."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru._experiments._models import (
    _INLINE_TARGET_LIMIT,
    ArtifactTargetMembership,
    ExperimentRecord,
    InlineTargetMembership,
    TargetMembership,
    _canonical_json,
    _required_string,
    _sha256,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruStateError,
    KitaruUsageError,
)


def target_manifest_payload(execution_ids: Sequence[str]) -> tuple[str, str]:
    """Return canonical ordered target JSON and its SHA-256."""
    ids = [
        _required_string(item, field_name="Target execution ID")
        for item in execution_ids
    ]
    if len(ids) != len(set(ids)):
        raise KitaruUsageError("Target execution IDs must be unique.")
    payload = _canonical_json(ids)
    return payload, _sha256(payload)


def load_target_membership(
    membership: TargetMembership,
    *,
    project_id: str,
    client: Any | None = None,
) -> list[str]:
    """Load and verify the immutable ordered target membership."""
    if isinstance(membership, InlineTargetMembership):
        return list(membership.execution_ids)

    resolved_client = client or Client()
    try:
        artifact = resolved_client.get_artifact_version(
            name_id_or_prefix=membership.artifact_version_id,
            project=project_id,
            hydrate=True,
        )
        loaded = artifact.load()
    except Exception as exc:
        raise KitaruBackendError(
            "Unable to load the experiment target manifest."
        ) from exc

    if not isinstance(loaded, list) or not all(
        isinstance(item, str) and item.strip() for item in loaded
    ):
        raise KitaruMetadataConflictError(
            "The target manifest did not load as an ordered ID list."
        )
    execution_ids = [cast(str, item) for item in loaded]
    _, actual_hash = target_manifest_payload(execution_ids)
    if len(execution_ids) != membership.count or actual_hash != membership.sha256:
        raise KitaruMetadataConflictError(
            "The target manifest count or SHA-256 does not match Project metadata."
        )
    return execution_ids


def experiment_targets_execution(
    record: ExperimentRecord,
    execution_id: str,
    *,
    client: Any | None = None,
) -> bool:
    """Return verified target membership for one execution."""
    normalized_id = _required_string(execution_id, field_name="Execution ID")
    return normalized_id in load_target_membership(
        record.spec.target_membership,
        project_id=record.spec.candidate_project_id,
        client=client,
    )


def persist_target_membership(
    *,
    experiment_id: str,
    project_id: str,
    execution_ids: Sequence[str],
    client: Any | None = None,
    save_artifact_fn: Callable[..., Any] = save_artifact,
) -> TargetMembership:
    """Inline small memberships or publish and verify one immutable manifest."""
    ids = list(execution_ids)
    if not ids:
        raise KitaruUsageError("Pass at least one target execution ID.")
    _payload, content_hash = target_manifest_payload(ids)
    if len(ids) <= _INLINE_TARGET_LIMIT:
        return InlineTargetMembership(execution_ids=ids, count=len(ids))

    resolved_client = client or Client()
    active_project_id = str(
        getattr(getattr(resolved_client, "active_project", None), "id", "")
    ).strip()
    if active_project_id != project_id:
        raise KitaruStateError(
            "Target manifests must be saved while the Agent Project is active."
        )

    name = f"kitaru-experiment-targets-{experiment_id}"
    artifact = _find_target_manifest(
        resolved_client,
        name=name,
        project_id=project_id,
    )
    if artifact is None:
        try:
            artifact = save_artifact_fn(
                data=ids,
                name=name,
                version="1",
                artifact_type=ArtifactType.DATA,
                save_type=ArtifactSaveType.MANUAL,
                has_custom_name=True,
                extract_metadata=False,
                include_visualizations=False,
                user_metadata={
                    "kitaru_experiment_id": experiment_id,
                    "kitaru_target_count": len(ids),
                    "kitaru_target_sha256": content_hash,
                },
            )
        except Exception as exc:
            artifact = _find_target_manifest(
                resolved_client,
                name=name,
                project_id=project_id,
            )
            if artifact is None:
                raise KitaruBackendError(
                    "Unable to save the experiment target manifest."
                ) from exc

    artifact_id = str(getattr(artifact, "id", "")).strip()
    if not artifact_id:
        raise KitaruStateError("The target manifest has no artifact-version ID.")
    hydrated = resolved_client.get_artifact_version(
        name_id_or_prefix=artifact_id,
        project=project_id,
        hydrate=True,
    )
    loaded = hydrated.load()
    loaded_ids = [str(item) for item in loaded] if isinstance(loaded, list) else None
    if loaded_ids is None:
        raise KitaruMetadataConflictError(
            "The target manifest did not load as an ordered ID list."
        )
    _, actual_hash = target_manifest_payload(loaded_ids)
    if loaded_ids != ids or len(loaded_ids) != len(ids) or actual_hash != content_hash:
        raise KitaruMetadataConflictError(
            "The existing target manifest conflicts with this idempotent request."
        )
    return ArtifactTargetMembership(
        artifact_version_id=artifact_id,
        count=len(ids),
        sha256=content_hash,
    )


def _find_target_manifest(
    client: Any,
    *,
    name: str,
    project_id: str,
) -> Any | None:
    try:
        page = client.list_artifact_versions(
            name=f"equals:{name}",
            version="1",
            project=project_id,
            hydrate=True,
            size=2,
        )
    except Exception as exc:
        raise KitaruBackendError(
            "Unable to resolve the experiment target manifest."
        ) from exc
    items = list(getattr(page, "items", page))
    exact = [item for item in items if str(getattr(item, "name", "")) == name]
    if len(exact) > 1:
        raise KitaruMetadataConflictError(
            "Multiple artifact versions match the immutable target manifest."
        )
    return exact[0] if exact else None
