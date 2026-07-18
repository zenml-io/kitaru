"""Project-backed Agent metadata models and reconciliation helpers."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    computed_field,
    field_validator,
    model_validator,
)
from zenml.client import Client

from kitaru._config import _projects as project_ops
from kitaru._config._projects import (
    _active_project_model,
    _get_project_by_exact_selector,
    _project_info_from_model,
)
from kitaru._experiments import (
    ExperimentRecord,
    validate_experiment_record_transition,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruMetadataReconciliationError,
    KitaruStateError,
    KitaruUsageError,
)

_KITARU_METADATA_KEY = "kitaru"
_KITARU_METADATA_SCHEMA_VERSION = 1
_PROJECT_METADATA_RECONCILIATION_MAX_ATTEMPTS = 3
_PROJECT_METADATA_RECONCILIATION_LOCK = threading.RLock()
_OWNED_ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "agent",
        "agent_version_order",
        "agent_version_aliases",
        "agent_versions",
        "experiments",
        "experiment_idempotency_index",
    }
)


def _non_empty_string(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


class _AgentExecutable(BaseModel):
    """Executable entrypoint stored with an Agent."""

    kind: Literal["entrypoint"]
    entrypoint: str
    repo_root_marker: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("entrypoint", "repo_root_marker")
    @classmethod
    def _validate_non_empty(cls, value: str) -> str:
        return _non_empty_string(value, field_name="Executable field")


class _AgentMetadata(BaseModel):
    """Agent identity stored inside the Kitaru metadata envelope."""

    agent_id: str
    name: str
    default_agent_version_id: str | None = None
    default_executable: _AgentExecutable | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("agent_id", "name")
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        return _non_empty_string(value, field_name="Agent field")

    @field_validator("default_agent_version_id")
    @classmethod
    def _validate_optional_string(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _non_empty_string(value, field_name="Agent field")


class _AgentVersionManifest(BaseModel):
    """Immutable manifest for one Pipeline-backed AgentVersion."""

    schema_version: Literal[1]
    agent_version_id: str
    pipeline_id: str
    pipeline_name: str
    fingerprint: str
    git_sha: str
    git_dirty: bool
    working_tree_hash: str | None
    configuration_hash: str
    worldview_hash: str
    entrypoint: str
    registered_at: str
    source: Literal["registration"]

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator(
        "agent_version_id",
        "pipeline_id",
        "pipeline_name",
        "fingerprint",
        "git_sha",
        "configuration_hash",
        "worldview_hash",
        "entrypoint",
        "registered_at",
    )
    @classmethod
    def _validate_required_string(cls, value: str) -> str:
        return _non_empty_string(value, field_name="AgentVersion field")

    @field_validator("working_tree_hash")
    @classmethod
    def _validate_optional_hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _non_empty_string(value, field_name="working_tree_hash")

    @field_validator("registered_at")
    @classmethod
    def _validate_registered_at(cls, value: str) -> str:
        normalized = _non_empty_string(value, field_name="registered_at")
        try:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("registered_at must be an ISO 8601 timestamp.") from exc
        if parsed.tzinfo is None:
            raise ValueError("registered_at must include a timezone.")
        return normalized

    @model_validator(mode="after")
    def _validate_pipeline_identity(self) -> _AgentVersionManifest:
        if self.agent_version_id != self.pipeline_id:
            raise ValueError("AgentVersion ID and Pipeline ID must be identical.")
        return self


class _AgentMetadataEnvelope(BaseModel):
    """Typed Kitaru data stored in project_metadata['kitaru']."""

    schema_version: Literal[1]
    agent: _AgentMetadata
    agent_version_order: list[str] | None = None
    agent_version_aliases: dict[str, str] = Field(default_factory=dict)
    agent_versions: dict[str, _AgentVersionManifest] = Field(default_factory=dict)
    experiments: dict[str, ExperimentRecord] = Field(default_factory=dict)
    experiment_idempotency_index: dict[str, str] = Field(default_factory=dict)

    # Unknown keys remain opaque so newer metadata survives older clients.
    model_config = ConfigDict(extra="allow", strict=True)

    @field_validator("agent_version_order")
    @classmethod
    def _validate_order(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized = [
            _non_empty_string(item, field_name="AgentVersion order entry")
            for item in value
        ]
        if len(normalized) != len(set(normalized)):
            raise ValueError("AgentVersion order entries must be unique.")
        return normalized

    @field_validator("agent_version_aliases")
    @classmethod
    def _validate_aliases(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for alias, version_id in value.items():
            normalized_alias = _non_empty_string(alias, field_name="AgentVersion alias")
            if normalized_alias in normalized:
                raise ValueError("AgentVersion aliases must be unique.")
            normalized[normalized_alias] = _non_empty_string(
                version_id, field_name="AgentVersion alias target"
            )
        return normalized

    @model_validator(mode="after")
    def _validate_references(self) -> _AgentMetadataEnvelope:
        version_ids = set(self.agent_versions)
        fingerprints: set[str] = set()
        for map_key, manifest in self.agent_versions.items():
            if map_key != manifest.agent_version_id or map_key != manifest.pipeline_id:
                raise ValueError(
                    "AgentVersion map keys, AgentVersion IDs, and Pipeline IDs "
                    "must be identical."
                )
            if manifest.fingerprint in fingerprints:
                raise ValueError(
                    "AgentVersion fingerprints must identify exactly one Pipeline."
                )
            fingerprints.add(manifest.fingerprint)

        dangling_aliases = set(self.agent_version_aliases.values()) - version_ids
        if dangling_aliases:
            raise ValueError("AgentVersion aliases must target existing versions.")

        if self.agent_version_order is not None:
            dangling_order = set(self.agent_version_order) - version_ids
            if dangling_order:
                raise ValueError(
                    "AgentVersion order entries must target existing versions."
                )

        default_version_id = self.agent.default_agent_version_id
        default_executable = self.agent.default_executable
        if (default_version_id is None) != (default_executable is None):
            raise ValueError(
                "The default AgentVersion ID and executable must be set together."
            )
        if default_version_id is not None and default_version_id not in version_ids:
            raise ValueError(
                "The default AgentVersion ID must target an existing version."
            )
        if (
            default_version_id is not None
            and default_executable is not None
            and default_executable.entrypoint
            != self.agent_versions[default_version_id].entrypoint
        ):
            raise ValueError(
                "The default executable must match the default AgentVersion entrypoint."
            )

        expected_index: dict[str, str] = {}
        for experiment_id, record in self.experiments.items():
            spec = record.spec
            if experiment_id != spec.experiment_id:
                raise ValueError(
                    "Experiment map keys must match immutable experiment IDs."
                )
            if spec.candidate_project_id != self.agent.agent_id:
                raise ValueError(
                    "Experiment candidates must belong to the Agent Project."
                )
            if spec.kind == "replay":
                if spec.candidate_agent_version_id not in version_ids:
                    raise ValueError(
                        "Experiment candidates must target a registered AgentVersion."
                    )
                manifest = self.agent_versions[spec.candidate_agent_version_id]
                if spec.candidate_pipeline_id != manifest.pipeline_id:
                    raise ValueError(
                        "Experiment candidate Pipeline IDs must match AgentVersion IDs."
                    )
                if spec.executable.entrypoint != manifest.entrypoint:
                    raise ValueError(
                        "Experiment executables must match the AgentVersion manifest."
                    )
            key = spec.idempotency_key
            if key in expected_index and expected_index[key] != experiment_id:
                raise ValueError(
                    "Experiment idempotency keys must identify exactly one attempt."
                )
            expected_index[key] = experiment_id

        if self.experiment_idempotency_index != expected_index:
            raise ValueError(
                "The experiment idempotency index must exactly match the catalog."
            )
        return self


class AgentVersionInfo(_AgentVersionManifest):
    """Public projection of one immutable AgentVersion manifest."""

    aliases: list[str] = Field(default_factory=list)


class AgentInfo(BaseModel):
    """Public Agent projection backed by one hydrated Project."""

    agent_id: str
    name: str
    display_name: str | None
    description: str | None
    is_active: bool
    default_agent_version_id: str | None
    default_executable: _AgentExecutable | None
    agent_version_aliases: dict[str, str]
    agent_versions: list[AgentVersionInfo]
    experiments: list[ExperimentRecord] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @computed_field(return_type=int)
    @property
    def version_count(self) -> int:
        """Return the number of versions in the canonical projection."""
        return len(self.agent_versions)

    @computed_field(return_type=int)
    @property
    def experiment_count(self) -> int:
        """Return the number of durable experiment attempts."""
        return len(self.experiments)

    def list_experiments(self) -> list[ExperimentRecord]:
        """Return experiment records in deterministic newest-first order."""
        return list(self.experiments)

    def get_experiment(self, name_or_id: str) -> ExperimentRecord:
        """Resolve an exact attempt ID or an unambiguous suite/name."""
        selector = _non_empty_string(
            name_or_id,
            field_name="Experiment selector",
        )
        by_id = [
            record
            for record in self.experiments
            if record.spec.experiment_id == selector
        ]
        if by_id:
            return by_id[0]

        matches = [
            record
            for record in self.experiments
            if record.spec.suite_key == selector or record.spec.name == selector
        ]
        if not matches:
            raise KitaruStateError(f"Experiment '{selector}' was not found.")
        if len(matches) > 1:
            raise KitaruUsageError(
                f"Experiment selector '{selector}' is ambiguous. "
                "Select the attempt by experiment ID."
            )
        return matches[0]


class AgentRegistrationResult(BaseModel):
    """Typed result returned by Agent registration."""

    agent: AgentInfo
    agent_version: AgentVersionInfo
    label: str | None
    created: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class AgentCreateResult(BaseModel):
    """Structured result for Agent creation operations."""

    agent: AgentInfo
    previous_active_agent: str | None
    activated: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class AgentDeleteResult(BaseModel):
    """Structured result for Agent deletion operations."""

    deleted_agent: AgentInfo

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


_AgentMetadataMutation = Callable[
    [_AgentMetadataEnvelope | None], _AgentMetadataEnvelope
]
_AgentMetadataVerification = Callable[[_AgentMetadataEnvelope], bool]


def _complete_project_metadata(project_model: Any) -> dict[str, Any]:
    """Return a defensive copy of a hydrated Project's complete metadata."""
    try:
        raw_metadata = project_model.project_metadata
    except AttributeError:
        return {}
    except Exception as exc:
        raise KitaruStateError(
            "Unable to read Agent metadata from the configured runtime."
        ) from exc

    if raw_metadata is None:
        return {}
    if not isinstance(raw_metadata, Mapping):
        raise KitaruStateError(
            "Project metadata from the configured runtime is malformed."
        )
    return deepcopy(dict(raw_metadata))


def _parse_agent_metadata(
    project_id: str,
    project_metadata: Mapping[str, Any],
) -> _AgentMetadataEnvelope | None:
    """Parse and validate only Kitaru's Project metadata namespace."""
    if _KITARU_METADATA_KEY not in project_metadata:
        return None

    raw_namespace = project_metadata[_KITARU_METADATA_KEY]
    if not isinstance(raw_namespace, Mapping):
        raise KitaruStateError("Kitaru Agent metadata is malformed.")
    if not _OWNED_ENVELOPE_KEYS.intersection(raw_namespace):
        return None

    try:
        envelope = _AgentMetadataEnvelope.model_validate(dict(raw_namespace))
    except ValidationError as exc:
        raise KitaruStateError(
            "Kitaru Agent metadata is malformed or uses an unsupported schema."
        ) from exc

    if envelope.agent.agent_id != project_id:
        raise KitaruStateError(
            "Kitaru Agent metadata does not match its backing Project."
        )
    return envelope


def _ordered_version_ids(envelope: _AgentMetadataEnvelope) -> list[str]:
    """Return deterministic AgentVersion order without inferring from runs."""
    ordered_ids = list(envelope.agent_version_order or [])
    remaining_ids = set(envelope.agent_versions) - set(ordered_ids)
    ordered_ids.extend(
        sorted(
            remaining_ids,
            key=lambda version_id: (
                datetime.fromisoformat(
                    envelope.agent_versions[version_id].registered_at.replace(
                        "Z", "+00:00"
                    )
                ),
                version_id,
            ),
        )
    )
    return ordered_ids


def _ordered_experiment_records(
    envelope: _AgentMetadataEnvelope,
) -> list[ExperimentRecord]:
    """Return durable experiment records in deterministic newest-first order."""
    return sorted(
        envelope.experiments.values(),
        key=lambda record: (
            datetime.fromisoformat(record.spec.created_at.replace("Z", "+00:00")),
            record.spec.experiment_id,
        ),
        reverse=True,
    )


def _manifest_for_fingerprint(
    envelope: _AgentMetadataEnvelope | None,
    fingerprint: str,
) -> _AgentVersionManifest | None:
    """Resolve the unique manifest for a registration fingerprint."""
    if envelope is None:
        return None
    candidates = [
        manifest
        for manifest in envelope.agent_versions.values()
        if manifest.fingerprint == fingerprint
    ]
    if len(candidates) > 1:
        raise KitaruMetadataConflictError(
            "The Agent fingerprint maps to multiple Pipeline UUIDs."
        )
    return candidates[0] if candidates else None


def _version_info(
    manifest: _AgentVersionManifest,
    *,
    aliases: list[str],
) -> AgentVersionInfo:
    """Build a public version projection from an immutable manifest."""
    return AgentVersionInfo(
        **manifest.model_dump(),
        aliases=sorted(aliases),
    )


def _agent_info_from_project_model(
    project_model: Any,
    *,
    active_project_id: str | None,
) -> AgentInfo | None:
    """Project one hydrated Project into an Agent, or None if uninitialized."""
    project = _project_info_from_model(
        project_model,
        active_project_id=active_project_id,
    )
    project_metadata = _complete_project_metadata(project_model)
    envelope = _parse_agent_metadata(project.id, project_metadata)
    if envelope is None:
        return None

    aliases_by_version: dict[str, list[str]] = {
        version_id: [] for version_id in envelope.agent_versions
    }
    for alias, version_id in envelope.agent_version_aliases.items():
        aliases_by_version[version_id].append(alias)

    versions = [
        _version_info(
            envelope.agent_versions[version_id],
            aliases=aliases_by_version[version_id],
        )
        for version_id in _ordered_version_ids(envelope)
    ]
    logical_name = envelope.agent.name
    display_name = (
        project.display_name if project.name == logical_name else logical_name
    )
    return AgentInfo(
        agent_id=project.id,
        name=logical_name,
        display_name=display_name,
        description=project.description,
        is_active=project.is_active,
        default_agent_version_id=envelope.agent.default_agent_version_id,
        default_executable=envelope.agent.default_executable,
        agent_version_aliases=dict(sorted(envelope.agent_version_aliases.items())),
        agent_versions=versions,
        experiments=_ordered_experiment_records(envelope),
    )


def _validate_exact_project(project_model: Any, *, project_id: str) -> None:
    """Require an exact ID lookup rather than accepting a Project name."""
    actual_id = str(getattr(project_model, "id", "")).strip()
    if not actual_id or actual_id != project_id:
        raise KitaruStateError(
            "Project metadata reconciliation requires an exact Project ID."
        )


def _validate_monotonic_mutation(
    previous: _AgentMetadataEnvelope | None,
    desired: _AgentMetadataEnvelope,
) -> None:
    """Reject changes to existing immutable manifests or aliases."""
    if previous is None:
        return

    if desired.agent.name != previous.agent.name:
        raise KitaruMetadataConflictError(
            "Agent metadata reconciliation cannot change the immutable Agent name."
        )

    for version_id, manifest in previous.agent_versions.items():
        if desired.agent_versions.get(version_id) != manifest:
            raise KitaruMetadataConflictError(
                "Agent metadata reconciliation cannot replace or remove an "
                "existing immutable AgentVersion manifest."
            )
    for alias, version_id in previous.agent_version_aliases.items():
        if desired.agent_version_aliases.get(alias) != version_id:
            raise KitaruMetadataConflictError(
                "Agent metadata reconciliation cannot move or remove an "
                "existing AgentVersion alias."
            )
    for experiment_id, record in previous.experiments.items():
        updated = desired.experiments.get(experiment_id)
        if updated is None:
            raise KitaruMetadataConflictError(
                "Agent metadata reconciliation cannot remove an experiment."
            )
        validate_experiment_record_transition(record, updated)
    for key, experiment_id in previous.experiment_idempotency_index.items():
        if desired.experiment_idempotency_index.get(key) != experiment_id:
            raise KitaruMetadataConflictError(
                "Agent metadata reconciliation cannot move or remove an "
                "experiment idempotency key."
            )


def _validated_mutation_result(
    project_id: str,
    previous: _AgentMetadataEnvelope | None,
    mutation: _AgentMetadataMutation,
) -> _AgentMetadataEnvelope:
    """Apply a mutation to a copy and validate the complete desired envelope."""
    mutation_input = previous.model_copy(deep=True) if previous is not None else None
    desired_raw = mutation(mutation_input)
    if not isinstance(desired_raw, _AgentMetadataEnvelope):
        raise KitaruUsageError(
            "Agent metadata mutations must return a typed metadata envelope."
        )
    try:
        desired = _AgentMetadataEnvelope.model_validate(desired_raw.model_dump())
    except ValidationError as exc:
        raise KitaruStateError(
            "The requested Agent metadata mutation is invalid."
        ) from exc
    if desired.agent.agent_id != project_id:
        raise KitaruMetadataConflictError(
            "Agent metadata reconciliation cannot change the backing Project ID."
        )
    _validate_monotonic_mutation(previous, desired)
    return desired


def _merged_project_metadata(
    current_metadata: Mapping[str, Any],
    desired: _AgentMetadataEnvelope,
) -> dict[str, Any]:
    """Replace Kitaru-owned fields while retaining opaque metadata."""
    merged = deepcopy(dict(current_metadata))
    raw_namespace = current_metadata.get(_KITARU_METADATA_KEY, {})
    if raw_namespace and not isinstance(raw_namespace, Mapping):
        raise KitaruStateError("Kitaru Agent metadata is malformed.")
    merged_namespace = deepcopy(dict(raw_namespace))
    desired_payload = desired.model_dump(mode="json")
    for key in _OWNED_ENVELOPE_KEYS:
        merged_namespace[key] = desired_payload[key]
    for key, value in desired_payload.items():
        if key not in _OWNED_ENVELOPE_KEYS:
            merged_namespace[key] = value
    merged[_KITARU_METADATA_KEY] = merged_namespace
    return merged


def _preserved_attempt_state(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> bool:
    """Check that foreign metadata observed before the write still exists."""
    for key, value in before.items():
        if key == _KITARU_METADATA_KEY:
            continue
        if key not in after or after[key] != value:
            return False

    before_namespace = before.get(_KITARU_METADATA_KEY)
    after_namespace = after.get(_KITARU_METADATA_KEY)
    if isinstance(before_namespace, Mapping):
        if not isinstance(after_namespace, Mapping):
            return False
        for key, value in before_namespace.items():
            if key in _OWNED_ENVELOPE_KEYS:
                continue
            if key not in after_namespace or after_namespace[key] != value:
                return False
    return True


def _preserves_existing_owned_state(
    previous: _AgentMetadataEnvelope | None,
    actual: _AgentMetadataEnvelope,
) -> bool:
    """Check that records present before an attempt still exist unchanged."""
    if previous is None:
        return True
    for version_id, manifest in previous.agent_versions.items():
        if actual.agent_versions.get(version_id) != manifest:
            return False
    for alias, version_id in previous.agent_version_aliases.items():
        if actual.agent_version_aliases.get(alias) != version_id:
            return False
    return True


def _contains_desired_owned_state(
    desired: _AgentMetadataEnvelope,
    actual: _AgentMetadataEnvelope,
) -> bool:
    """Check the intended owned records without rejecting concurrent additions."""
    if actual.agent != desired.agent:
        return False
    if actual.agent_version_order != desired.agent_version_order:
        return False
    for version_id, manifest in desired.agent_versions.items():
        if actual.agent_versions.get(version_id) != manifest:
            return False
    for alias, version_id in desired.agent_version_aliases.items():
        if actual.agent_version_aliases.get(alias) != version_id:
            return False
    for experiment_id, record in desired.experiments.items():
        if actual.experiments.get(experiment_id) != record:
            return False
    for key, experiment_id in desired.experiment_idempotency_index.items():
        if actual.experiment_idempotency_index.get(key) != experiment_id:
            return False
    return True


def reconcile_kitaru_metadata(
    project_id: str,
    mutation: _AgentMetadataMutation,
    verify: _AgentMetadataVerification,
    *,
    client_factory: Callable[[], Any] = Client,
) -> _AgentMetadataEnvelope:
    """Reconcile and verify Kitaru metadata for one exact Project ID.

    ZenML replaces the complete Project metadata dictionary and has no revision
    or conditional update. The module lock prevents in-process Kitaru writers
    from interleaving, but it does not make concurrent multi-process writes safe.
    """
    normalized_project_id = project_id.strip()
    if not normalized_project_id:
        raise KitaruUsageError("Project ID cannot be empty.")

    client = client_factory()
    with _PROJECT_METADATA_RECONCILIATION_LOCK:
        for _attempt in range(_PROJECT_METADATA_RECONCILIATION_MAX_ATTEMPTS):
            try:
                before_project = _get_project_by_exact_selector(
                    client, normalized_project_id
                )
            except Exception:
                continue

            _validate_exact_project(before_project, project_id=normalized_project_id)
            before_metadata = _complete_project_metadata(before_project)
            previous = _parse_agent_metadata(normalized_project_id, before_metadata)
            desired = _validated_mutation_result(
                normalized_project_id, previous, mutation
            )
            published_metadata = _merged_project_metadata(before_metadata, desired)

            if published_metadata != before_metadata:
                try:
                    client.update_project(
                        normalized_project_id,
                        project_metadata=published_metadata,
                    )
                except Exception:
                    # An ambiguous update failure may still have committed.
                    continue

            try:
                after_project = _get_project_by_exact_selector(
                    client, normalized_project_id
                )
            except Exception:
                continue

            _validate_exact_project(after_project, project_id=normalized_project_id)
            after_metadata = _complete_project_metadata(after_project)
            actual = _parse_agent_metadata(normalized_project_id, after_metadata)
            if actual is None:
                continue
            if not _preserves_existing_owned_state(previous, actual):
                continue
            if not _preserved_attempt_state(before_metadata, after_metadata):
                continue
            if not _contains_desired_owned_state(desired, actual):
                continue
            if not verify(actual):
                continue
            return actual

    raise KitaruMetadataReconciliationError(
        "Unable to verify the Project metadata update after "
        f"{_PROJECT_METADATA_RECONCILIATION_MAX_ATTEMPTS} attempts."
    ) from None


def _reconcile_project_agent_metadata(
    project_id: str,
    mutation: _AgentMetadataMutation,
    verify: _AgentMetadataVerification,
    *,
    client_factory: Callable[[], Any] = Client,
) -> _AgentMetadataEnvelope:
    """Backward-compatible delegate to the shared Kitaru reconciler."""
    return reconcile_kitaru_metadata(
        project_id,
        mutation,
        verify,
        client_factory=client_factory,
    )


def _reconcile_agent_version_registration(
    *,
    project_id: str,
    agent_name: str,
    manifest: _AgentVersionManifest,
    label: str | None,
    client_factory: Callable[[], Any] = Client,
) -> _AgentMetadataEnvelope:
    """Persist one immutable manifest and optional alias through reconciliation."""

    executable = _AgentExecutable(
        kind="entrypoint",
        entrypoint=manifest.entrypoint,
        repo_root_marker=".kitaru",
    )
    normalized_agent_name = _non_empty_string(agent_name, field_name="Agent name")
    normalized_label = (
        _non_empty_string(label, field_name="AgentVersion label")
        if label is not None
        else None
    )

    def add_manifest(
        current: _AgentMetadataEnvelope | None,
    ) -> _AgentMetadataEnvelope:
        if current is None:
            current = _AgentMetadataEnvelope(
                schema_version=1,
                agent=_AgentMetadata(
                    agent_id=project_id,
                    name=normalized_agent_name,
                ),
            )
        elif current.agent.name != normalized_agent_name:
            raise KitaruMetadataConflictError(
                "The backing Project is already registered to Agent "
                f"{current.agent.name!r}, not {normalized_agent_name!r}."
            )

        fingerprint_manifest = _manifest_for_fingerprint(current, manifest.fingerprint)
        if fingerprint_manifest is not None and fingerprint_manifest != manifest:
            raise KitaruMetadataConflictError(
                "The Agent fingerprint conflicts with an immutable manifest."
            )

        versions = dict(current.agent_versions)
        existing = versions.get(manifest.pipeline_id)
        if existing is not None and existing != manifest:
            raise KitaruMetadataConflictError(
                "The Pipeline UUID already has a different immutable manifest."
            )
        versions[manifest.pipeline_id] = manifest

        order = list(current.agent_version_order or [])
        if manifest.pipeline_id not in order:
            order.append(manifest.pipeline_id)

        agent = current.agent
        if agent.default_agent_version_id is None:
            agent = agent.model_copy(
                update={
                    "default_agent_version_id": manifest.pipeline_id,
                    "default_executable": executable,
                }
            )

        aliases = dict(current.agent_version_aliases)
        if normalized_label is not None:
            existing_target = aliases.get(normalized_label)
            if existing_target is not None and existing_target != manifest.pipeline_id:
                raise KitaruMetadataConflictError(
                    "The AgentVersion alias already points to a different version."
                )
            aliases[normalized_label] = manifest.pipeline_id

        return current.model_copy(
            update={
                "agent": agent,
                "agent_versions": versions,
                "agent_version_order": order,
                "agent_version_aliases": aliases,
            },
            deep=True,
        )

    return reconcile_kitaru_metadata(
        project_id,
        add_manifest,
        lambda actual: (
            actual.agent.name == normalized_agent_name
            and actual.agent_versions.get(manifest.pipeline_id) == manifest
            and (
                normalized_label is None
                or actual.agent_version_aliases.get(normalized_label)
                == manifest.pipeline_id
            )
        ),
        client_factory=client_factory,
    )


def _reconcile_active_project_agent_metadata(
    mutation: _AgentMetadataMutation,
    verify: _AgentMetadataVerification,
    *,
    client_factory: Callable[[], Any] = Client,
) -> _AgentMetadataEnvelope:
    """Initialize or update the active default Project without lifecycle guards."""
    client = client_factory()
    active_project = _active_project_model(client)
    project_id = str(getattr(active_project, "id", "")).strip()
    if not project_id:
        raise KitaruStateError(
            "Unable to resolve the active Project for Agent metadata."
        )
    return reconcile_kitaru_metadata(
        project_id,
        mutation,
        verify,
        client_factory=lambda: client,
    )


def _initialized_agent_from_project(
    project_model: Any,
    *,
    active_project_id: str | None,
) -> AgentInfo:
    """Return an Agent projection or reject an uninitialized backing Project."""
    agent = _agent_info_from_project_model(
        project_model,
        active_project_id=active_project_id,
    )
    if agent is None:
        project_name = str(getattr(project_model, "name", "")).strip()
        identifier = project_name or str(getattr(project_model, "id", "")).strip()
        raise KitaruStateError(
            f"Kitaru Agent '{identifier}' is not initialized. Register the Agent first."
        )
    return agent


def current_agent(*, client_factory: Callable[[], Any] = Client) -> AgentInfo:
    """Return the initialized Agent backed by the active Project."""
    client = client_factory()
    project_model = _active_project_model(client)
    project_id = str(getattr(project_model, "id", "")).strip()
    return _initialized_agent_from_project(
        project_model,
        active_project_id=project_id,
    )


def list_agents(
    *,
    page: int | None = None,
    size: int | None = None,
    client_factory: Callable[[], Any] = Client,
) -> list[AgentInfo]:
    """List initialized Agents visible to the current user."""
    if (page is None) != (size is None):
        raise KitaruUsageError("Agent pagination requires both page and size.")
    for field_name, value in (("page", page), ("size", size)):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
        ):
            raise KitaruUsageError(
                f"Agent pagination {field_name} must be a positive integer."
            )

    client = client_factory()
    active_project_id = project_ops._active_project_id(client)
    start = (page - 1) * size if page is not None and size is not None else 0
    end = start + size if size is not None else None
    agents: list[AgentInfo] = []

    first_page = client.list_projects(hydrate=True)
    total_pages_raw = getattr(first_page, "total_pages", 1)
    page_size_raw = getattr(first_page, "max_size", 1)
    try:
        total_pages = int(total_pages_raw)
    except (TypeError, ValueError):
        total_pages = 1
    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 1

    for page_number in range(1, total_pages + 1):
        page_result = (
            first_page
            if page_number == 1
            else client.list_projects(
                page=page_number,
                size=page_size,
                hydrate=True,
            )
        )
        for project_model in project_ops._project_models_from_page(page_result):
            agent = _agent_info_from_project_model(
                project_model,
                active_project_id=active_project_id,
            )
            if agent is not None:
                agents.append(agent)
                if end is not None and len(agents) >= end:
                    return agents[start:end]

    return agents[start:end]


def _is_uuid_selector(selector: str) -> bool:
    """Return whether a selector is a canonical UUID."""
    try:
        return str(UUID(selector)) == selector.lower()
    except ValueError:
        return False


def _resolve_agent_project(client: Any, selector: str) -> Any:
    """Resolve an Agent UUID directly or its exact unique logical name."""
    if _is_uuid_selector(selector):
        return _get_project_by_exact_selector(client, selector)

    try:
        exact_project = _get_project_by_exact_selector(client, selector)
    except Exception:
        exact_project = None
    if exact_project is not None and (
        str(getattr(exact_project, "id", "")).strip() == selector
    ):
        return exact_project

    matches: list[Any] = []
    first_page = client.list_projects(hydrate=True)
    try:
        total_pages = int(getattr(first_page, "total_pages", 1))
    except (TypeError, ValueError):
        total_pages = 1
    try:
        page_size = int(getattr(first_page, "max_size", 1))
    except (TypeError, ValueError):
        page_size = 1

    for page_number in range(1, total_pages + 1):
        page_result = (
            first_page
            if page_number == 1
            else client.list_projects(
                page=page_number,
                size=page_size,
                hydrate=True,
            )
        )
        for project_model in project_ops._project_models_from_page(page_result):
            project_id = str(getattr(project_model, "id", "")).strip()
            envelope = _parse_agent_metadata(
                project_id, _complete_project_metadata(project_model)
            )
            if envelope is not None and envelope.agent.name == selector:
                matches.append(project_model)

    if not matches:
        raise KitaruStateError(f"Kitaru Agent '{selector}' was not found.")
    if len(matches) > 1:
        raise KitaruStateError(
            f"Multiple initialized Kitaru Agents have the name '{selector}'. "
            "Select the Agent by UUID."
        )
    return matches[0]


def get_agent(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> AgentInfo:
    """Return an initialized Agent by logical name or backing Project UUID."""
    selector = project_ops._normalize_project_selector(name_or_id)
    client = client_factory()
    try:
        project_model = _resolve_agent_project(client, selector)
    except KitaruStateError:
        raise
    except Exception as exc:
        raise KitaruBackendError(f"Failed to load Agent '{selector}': {exc}") from exc
    return _initialized_agent_from_project(
        project_model,
        active_project_id=project_ops._active_project_id(client),
    )


def _recoverable_uninitialized_project(
    client: Any,
    *,
    name: str,
    description: str,
    display_name: str | None,
) -> Any | None:
    """Return an exact request-matching Project from an interrupted creation."""
    normalized_name = project_ops._normalize_project_selector(name, field_name="name")
    normalized_description = (
        project_ops._normalize_optional_project_string(description) or ""
    )
    normalized_display_name = project_ops._normalize_optional_project_string(
        display_name
    )
    try:
        project_model = _get_project_by_exact_selector(client, normalized_name)
    except Exception:
        return None

    project_id = str(getattr(project_model, "id", "")).strip()
    project_name = str(getattr(project_model, "name", "")).strip()
    project_description = (
        project_ops._normalize_optional_project_string(
            getattr(project_model, "description", None)
        )
        or ""
    )
    project_display_name = project_ops._normalize_optional_project_string(
        getattr(project_model, "display_name", None)
    )
    if (
        not project_id
        or project_name != normalized_name
        or project_description != normalized_description
        or project_display_name != normalized_display_name
    ):
        return None
    envelope = _parse_agent_metadata(
        project_id, _complete_project_metadata(project_model)
    )
    if envelope is not None and envelope.agent.agent_id != project_id:
        return None
    return project_model


def create_agent(
    name: str,
    *,
    description: str = "",
    display_name: str | None = None,
    activate: bool = True,
    client_factory: Callable[[], Any] = Client,
) -> AgentCreateResult:
    """Create and initialize an Agent through the guarded Project lifecycle."""
    client = client_factory()
    try:
        project_result = project_ops.create_project(
            name,
            description=description,
            display_name=display_name,
            activate=False,
            client_factory=lambda: client,
        )
        project = project_result.project
        previous_active_agent = project_result.previous_active_project
    except KitaruBackendError:
        project_model = _recoverable_uninitialized_project(
            client,
            name=name,
            description=description,
            display_name=display_name,
        )
        if project_model is None:
            raise
        project = _project_info_from_model(
            project_model,
            active_project_id=project_ops._active_project_id(client),
        )
        try:
            previous_active_agent = project_ops.current_project(
                client_factory=lambda: client
            ).name
        except (KitaruBackendError, KitaruStateError):
            previous_active_agent = None

    def initialize(
        current: _AgentMetadataEnvelope | None,
    ) -> _AgentMetadataEnvelope:
        if current is not None:
            if (
                current.agent.agent_id != project.id
                or current.agent.name != project.name
            ):
                raise KitaruMetadataConflictError(
                    "The backing Project is already initialized as a different Agent."
                )
            return current
        return _AgentMetadataEnvelope(
            schema_version=_KITARU_METADATA_SCHEMA_VERSION,
            agent=_AgentMetadata(
                agent_id=project.id,
                name=project.name,
            ),
        )

    reconcile_kitaru_metadata(
        project.id,
        initialize,
        lambda actual: (
            actual.agent.agent_id == project.id and actual.agent.name == project.name
        ),
        client_factory=lambda: client,
    )
    if activate:
        project_ops.use_project(project.id, client_factory=lambda: client)

    project_model = _get_project_by_exact_selector(client, project.id)
    agent = _initialized_agent_from_project(
        project_model,
        active_project_id=project_ops._active_project_id(client),
    )
    return AgentCreateResult(
        agent=agent,
        previous_active_agent=previous_active_agent,
        activated=activate,
    )


def use_agent(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> AgentInfo:
    """Activate an initialized Agent through the guarded Project lifecycle."""
    client = client_factory()
    agent = get_agent(name_or_id, client_factory=lambda: client)
    project = project_ops.use_project(agent.agent_id, client_factory=lambda: client)
    project_model = _get_project_by_exact_selector(client, project.id)
    return _initialized_agent_from_project(
        project_model,
        active_project_id=project.id,
    )


def delete_agent(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> AgentDeleteResult:
    """Delete an initialized Agent through the guarded Project lifecycle."""
    client = client_factory()
    agent = get_agent(name_or_id, client_factory=lambda: client)
    project_ops.delete_project(agent.agent_id, client_factory=lambda: client)
    return AgentDeleteResult(deleted_agent=agent)
