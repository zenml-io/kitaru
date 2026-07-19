"""Execution evidence freezing for score attempts."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import NAMESPACE_URL, uuid5

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)
from zenml.artifacts.utils import save_artifact
from zenml.client import Client
from zenml.enums import ArtifactSaveType, ArtifactType

from kitaru._run_identity import extract_run_project_identity
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruStateError,
)
from kitaru.scoring._contracts import (
    canonical_json,
    require_string,
    sha256_json,
    validate_sha256,
)

_INLINE_EVIDENCE_MANIFEST_LIMIT = 100
_INLINE_EVIDENCE_MANIFEST_MAX_BYTES = 64 * 1024
_RESERVED_EVIDENCE_TAG = "kitaru-evidence-manifest-v1"


class ArtifactContentReference(BaseModel):
    """Hashable reference to stored content used as score evidence."""

    artifact_version_id: str
    name: str | None = None
    content_hash: str | None = None
    materializer: str | None = None
    data_type: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("artifact_version_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return require_string(value, field_name="Artifact version ID")


class ExecutionEvidence(BaseModel):
    """Frozen adapter-neutral evidence for one stored execution."""

    schema_version: Literal[1] = 1
    execution_id: str
    project_id: str
    source: Literal["zenml_pipeline_run"] = "zenml_pipeline_run"
    status: str
    checkpoint_ids: list[str] = Field(default_factory=list)
    inputs: dict[str, JsonValue] = Field(default_factory=dict)
    outputs: dict[str, JsonValue] = Field(default_factory=dict)
    messages: list[JsonValue] = Field(default_factory=list)
    tool_calls: list[JsonValue] = Field(default_factory=list)
    artifact_references: list[ArtifactContentReference] = Field(default_factory=list)
    usage: dict[str, JsonValue] = Field(default_factory=dict)
    cost: dict[str, JsonValue] = Field(default_factory=dict)
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("execution_id", "project_id", "status")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return require_string(value, field_name="Execution evidence field")

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_content_hash(self) -> ExecutionEvidence:
        payload = self.model_dump(mode="json", exclude={"content_hash"})
        expected = sha256_json(payload)
        if self.content_hash != expected:
            raise ValueError("Execution evidence content_hash does not match payload.")
        return self

    @classmethod
    def from_run(cls, run: Any) -> ExecutionEvidence:
        """Freeze evidence from a hydrated ZenML run without loading artifacts."""
        execution_id = require_string(
            str(getattr(run, "id", "")), field_name="Execution ID"
        )
        project_identity = extract_run_project_identity(run)
        project_id = require_string(
            str(project_identity.project_id or ""), field_name="Project ID"
        )
        raw_status = getattr(run, "status", None)
        status = str(getattr(raw_status, "value", raw_status) or "unknown").lower()
        steps = getattr(run, "steps", {}) or {}
        checkpoint_ids: list[str] = []
        artifact_refs: list[ArtifactContentReference] = []
        for step_name, step in sorted(steps.items(), key=lambda item: str(item[0])):
            checkpoint_ids.append(str(getattr(step, "id", step_name)))
            outputs = getattr(step, "outputs", {}) or {}
            for output_name, output_artifacts in sorted(
                outputs.items(), key=lambda item: str(item[0])
            ):
                for artifact in output_artifacts or []:
                    artifact_id = str(getattr(artifact, "id", "")).strip()
                    if not artifact_id:
                        continue
                    artifact_refs.append(
                        ArtifactContentReference(
                            artifact_version_id=artifact_id,
                            name=str(
                                getattr(artifact, "name", output_name) or output_name
                            ),
                            content_hash=getattr(artifact, "content_hash", None),
                            materializer=_source_string(
                                getattr(artifact, "materializer", None)
                            ),
                            data_type=_source_string(
                                getattr(artifact, "data_type", None)
                            ),
                        )
                    )
        inputs = _json_object(
            getattr(getattr(run, "config", None), "parameters", {}) or {}
        )
        metadata = _flatten_run_metadata(getattr(run, "run_metadata", {}) or {})
        usage = {
            key: value for key, value in metadata.items() if key.startswith("usage_")
        }
        cost = {
            key: value for key, value in metadata.items() if key.startswith("cost_")
        }
        payload = {
            "schema_version": 1,
            "execution_id": execution_id,
            "project_id": project_id,
            "source": "zenml_pipeline_run",
            "status": status,
            "checkpoint_ids": checkpoint_ids,
            "inputs": inputs,
            "outputs": {},
            "messages": [],
            "tool_calls": [],
            "artifact_references": [
                item.model_dump(mode="json") for item in artifact_refs
            ],
            "usage": usage,
            "cost": cost,
        }
        return cls(
            execution_id=execution_id,
            project_id=project_id,
            status=status,
            checkpoint_ids=checkpoint_ids,
            inputs=inputs,
            outputs={},
            messages=[],
            tool_calls=[],
            artifact_references=artifact_refs,
            usage=usage,
            cost=cost,
            content_hash=sha256_json(payload),
        )


class ComparisonEvidence(BaseModel):
    """Frozen comparative evidence for candidate versus immediate original."""

    schema_version: Literal[1] = 1
    candidate: ExecutionEvidence
    original: ExecutionEvidence
    relationship: Literal["immediate_original"] = "immediate_original"
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="after")
    def _validate_content_hash(self) -> ComparisonEvidence:
        payload = self.model_dump(mode="json", exclude={"content_hash"})
        expected = sha256_json(payload)
        if self.content_hash != expected:
            raise ValueError("Comparison evidence content_hash does not match payload.")
        return self

    @classmethod
    def from_pair(
        cls,
        *,
        candidate: ExecutionEvidence,
        original: ExecutionEvidence,
    ) -> ComparisonEvidence:
        payload = {
            "schema_version": 1,
            "candidate": candidate.model_dump(mode="json"),
            "original": original.model_dump(mode="json"),
            "relationship": "immediate_original",
        }
        return cls(
            candidate=candidate, original=original, content_hash=sha256_json(payload)
        )


class EvidenceManifestEntry(BaseModel):
    """One ordered target entry in a frozen evidence manifest."""

    target_execution_id: str
    evidence: ExecutionEvidence
    original_evidence: ExecutionEvidence | None = None
    comparison: ComparisonEvidence | None = None
    availability: Literal["available", "missing_original"] = "available"
    reason: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("target_execution_id")
    @classmethod
    def _validate_target(cls, value: str) -> str:
        return require_string(value, field_name="Target execution ID")

    @model_validator(mode="after")
    def _validate_shape(self) -> EvidenceManifestEntry:
        if self.comparison is not None and self.original_evidence is None:
            raise ValueError("Comparative evidence requires original evidence.")
        if self.comparison is not None:
            if self.comparison.candidate != self.evidence:
                raise ValueError(
                    "Comparison candidate evidence must match entry evidence."
                )
            if self.comparison.original != self.original_evidence:
                raise ValueError(
                    "Comparison original evidence must match entry original evidence."
                )
        if self.comparison is None and self.original_evidence is not None:
            raise ValueError("Original evidence requires a comparison payload.")
        if self.availability == "missing_original" and not self.reason:
            raise ValueError("Missing comparative originals require a reason.")
        if self.availability == "missing_original" and self.comparison is not None:
            raise ValueError(
                "Missing comparative originals cannot include comparison evidence."
            )
        return self


class EvidenceManifest(BaseModel):
    """Immutable ordered evidence manifest for a score attempt."""

    schema_version: Literal[1] = 1
    manifest_id: str
    project_id: str
    created_at: str
    entries: list[EvidenceManifestEntry]
    content_hash: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("manifest_id", "project_id", "created_at")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return require_string(value, field_name="Evidence manifest field")

    @field_validator("content_hash")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_manifest(self) -> EvidenceManifest:
        ids = [entry.target_execution_id for entry in self.entries]
        if not ids:
            raise ValueError("Evidence manifests require at least one entry.")
        if len(ids) != len(set(ids)):
            raise ValueError("Evidence manifest target IDs must be unique.")
        if any(entry.evidence.project_id != self.project_id for entry in self.entries):
            raise ValueError("Evidence entries must belong to the manifest project.")
        expected = sha256_json(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("Evidence manifest content_hash does not match payload.")
        return self

    @classmethod
    def create(
        cls,
        *,
        project_id: str,
        entries: Sequence[EvidenceManifestEntry],
        created_at: str | None = None,
        manifest_id: str | None = None,
    ) -> EvidenceManifest:
        timestamp = created_at or datetime.now(UTC).isoformat()
        normalized_project = require_string(project_id, field_name="Project ID")
        if manifest_id is not None:
            normalized_id = manifest_id
        else:
            id_payload = [
                normalized_project,
                timestamp,
                [entry.target_execution_id for entry in entries],
            ]
            normalized_id = (
                f"evm-{uuid5(NAMESPACE_URL, canonical_json(id_payload)).hex}"
            )
        payload = {
            "schema_version": 1,
            "manifest_id": normalized_id,
            "project_id": normalized_project,
            "created_at": timestamp,
            "entries": [entry.model_dump(mode="json") for entry in entries],
        }
        return cls(**payload, content_hash=sha256_json(payload))


class InlineEvidenceManifestReference(BaseModel):
    """Bounded evidence manifest stored directly in experiment metadata."""

    schema_version: Literal[1] = 1
    storage: Literal["inline"] = "inline"
    manifest: EvidenceManifest
    count: int = Field(ge=1, le=_INLINE_EVIDENCE_MANIFEST_LIMIT)
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_reference(self) -> InlineEvidenceManifestReference:
        if self.count != len(self.manifest.entries):
            raise ValueError("Evidence manifest reference count must match entries.")
        if self.sha256 != self.manifest.content_hash:
            raise ValueError("Evidence manifest reference hash must match manifest.")
        return self


class ArtifactEvidenceManifestReference(BaseModel):
    """Reference to an artifact-backed immutable evidence manifest."""

    schema_version: Literal[1] = 1
    storage: Literal["artifact"] = "artifact"
    artifact_version_id: str
    count: int = Field(ge=1)
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("artifact_version_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return require_string(value, field_name="Artifact version ID")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)


EvidenceManifestReference = Annotated[
    InlineEvidenceManifestReference | ArtifactEvidenceManifestReference,
    Field(discriminator="storage"),
]


def freeze_execution_evidence_manifest(
    runs: Sequence[Any],
    *,
    project_id: str,
    comparative: bool = False,
    client: Any | None = None,
    run_loader: Callable[[str], Any] | None = None,
    created_at: str | None = None,
) -> EvidenceManifest:
    """Resolve stored run evidence and optional immediate-original evidence."""
    cache = {str(getattr(run, "id", "")): run for run in runs}
    resolved_client = client or Client()
    entries: list[EvidenceManifestEntry] = []
    for run in runs:
        evidence = ExecutionEvidence.from_run(run)
        if evidence.project_id != project_id:
            raise KitaruStateError("Score evidence must stay within one Agent Project.")
        original_evidence: ExecutionEvidence | None = None
        comparison: ComparisonEvidence | None = None
        availability: Literal["available", "missing_original"] = "available"
        reason: str | None = None
        if comparative:
            original = getattr(run, "original_run", None)
            original_id = (
                str(getattr(original, "id", "")).strip() if original is not None else ""
            )
            if not original_id:
                availability = "missing_original"
                reason = "Execution has no immediate original replay lineage."
            else:
                original_run = cache.get(original_id) or (
                    run_loader(original_id)
                    if run_loader is not None
                    else _load_run(resolved_client, original_id)
                )
                cache[original_id] = original_run
                original_evidence = ExecutionEvidence.from_run(original_run)
                if original_evidence.project_id != project_id:
                    raise KitaruStateError(
                        "Comparative evidence cannot cross Agent Projects."
                    )
                comparison = ComparisonEvidence.from_pair(
                    candidate=evidence,
                    original=original_evidence,
                )
        entries.append(
            EvidenceManifestEntry(
                target_execution_id=evidence.execution_id,
                evidence=evidence,
                original_evidence=original_evidence,
                comparison=comparison,
                availability=availability,
                reason=reason,
            )
        )
    return EvidenceManifest.create(
        project_id=project_id, entries=entries, created_at=created_at
    )


def persist_evidence_manifest(
    manifest: EvidenceManifest,
    *,
    project_id: str,
    client: Any | None = None,
    save_artifact_fn: Callable[..., Any] = save_artifact,
) -> EvidenceManifestReference:
    """Inline small manifests or persist large manifests as immutable artifacts."""
    if manifest.project_id != project_id:
        raise KitaruStateError(
            "Evidence manifests must be saved in their Agent Project."
        )
    serialized_size = len(
        canonical_json(manifest.model_dump(mode="json")).encode("utf-8")
    )
    if (
        len(manifest.entries) <= _INLINE_EVIDENCE_MANIFEST_LIMIT
        and serialized_size <= _INLINE_EVIDENCE_MANIFEST_MAX_BYTES
    ):
        return InlineEvidenceManifestReference(
            manifest=manifest,
            count=len(manifest.entries),
            sha256=manifest.content_hash,
        )
    resolved_client = client or Client()
    active_project_id = str(
        getattr(getattr(resolved_client, "active_project", None), "id", "")
    ).strip()
    if active_project_id != project_id:
        raise KitaruStateError(
            "Evidence manifests require the Agent Project to be active."
        )
    name = f"kitaru-evidence-manifest-{manifest.manifest_id}"
    artifact = _find_evidence_manifest(
        resolved_client,
        name=name,
        project_id=project_id,
    )
    if artifact is None:
        try:
            artifact = save_artifact_fn(
                data=manifest.model_dump(mode="json"),
                name=name,
                version="1",
                artifact_type=ArtifactType.DATA,
                save_type=ArtifactSaveType.MANUAL,
                has_custom_name=True,
                tags=[_RESERVED_EVIDENCE_TAG],
                extract_metadata=False,
                include_visualizations=False,
                user_metadata={
                    "kitaru_project_id": project_id,
                    "kitaru_evidence_manifest_id": manifest.manifest_id,
                    "kitaru_evidence_manifest_sha256": manifest.content_hash,
                    "kitaru_evidence_manifest_count": len(manifest.entries),
                },
            )
        except Exception as exc:
            artifact = _find_evidence_manifest(
                resolved_client,
                name=name,
                project_id=project_id,
            )
            if artifact is None:
                raise KitaruBackendError(
                    "Unable to save the evidence manifest."
                ) from exc
    artifact_id = str(getattr(artifact, "id", "")).strip()
    if not artifact_id:
        raise KitaruStateError("The evidence manifest has no artifact-version ID.")
    loaded = resolved_client.get_artifact_version(
        name_id_or_prefix=artifact_id,
        project=project_id,
        hydrate=True,
    ).load()
    loaded_manifest = EvidenceManifest.model_validate(loaded)
    if loaded_manifest != manifest:
        raise KitaruMetadataConflictError(
            "The existing evidence manifest conflicts with this idempotent request."
        )
    return ArtifactEvidenceManifestReference(
        artifact_version_id=artifact_id,
        count=len(manifest.entries),
        sha256=manifest.content_hash,
    )


def _find_evidence_manifest(
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
            tags=_RESERVED_EVIDENCE_TAG,
            hydrate=True,
            size=2,
        )
    except Exception as exc:
        raise KitaruBackendError("Unable to resolve the evidence manifest.") from exc
    items = list(getattr(page, "items", page))
    exact = [item for item in items if str(getattr(item, "name", "")) == name]
    if len(exact) > 1:
        raise KitaruMetadataConflictError(
            "Multiple artifact versions match the immutable evidence manifest."
        )
    return exact[0] if exact else None


def load_evidence_manifest(
    reference: EvidenceManifestReference,
    *,
    project_id: str,
    client: Any | None = None,
) -> EvidenceManifest:
    """Load and hash-verify a persisted evidence manifest reference."""
    if isinstance(reference, InlineEvidenceManifestReference):
        return reference.manifest
    resolved_client = client or Client()
    try:
        artifact = resolved_client.get_artifact_version(
            name_id_or_prefix=reference.artifact_version_id,
            project=project_id,
            hydrate=True,
        )
        loaded = artifact.load()
    except Exception as exc:
        raise KitaruBackendError("Unable to load the evidence manifest.") from exc
    manifest = EvidenceManifest.model_validate(loaded)
    if manifest.project_id != project_id or manifest.content_hash != reference.sha256:
        raise KitaruMetadataConflictError("Evidence manifest project or hash mismatch.")
    return manifest


def _load_run(client: Any, execution_id: str) -> Any:
    try:
        return client.get_pipeline_run(
            name_id_or_prefix=execution_id,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to load execution '{execution_id}' for evidence."
        ) from exc


def _source_string(value: Any) -> str | None:
    if value is None:
        return None
    module = getattr(value, "module", None)
    attribute = getattr(value, "attribute", None)
    if module and attribute:
        return f"{module}.{attribute}"
    return str(value)


def _json_object(value: Any) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        return {}
    try:
        canonical_json(value)
    except Exception:
        return {key: str(item) for key, item in value.items()}
    return cast(dict[str, JsonValue], value)


def _flatten_run_metadata(metadata: Any) -> dict[str, JsonValue]:
    if not isinstance(metadata, dict):
        return {}
    flattened: dict[str, JsonValue] = {}
    for key, value in metadata.items():
        raw_value = getattr(value, "value", value)
        if isinstance(raw_value, str | int | float | bool) or raw_value is None:
            flattened[str(key)] = raw_value
    return flattened
