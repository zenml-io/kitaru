"""Fail-closed loading for immutable imported replay evidence."""

from collections.abc import Mapping
from typing import Any, Never, cast

from pydantic import ValidationError
from zenml.client import Client

from kitaru._agent_registration import (
    resolve_registered_agent_version,
    verify_hydrated_submitted_run_binding,
)
from kitaru._import_contract import (
    IMPORT_AGENT_NAME_KEY,
    IMPORT_ATTRIBUTION_KEY,
    IMPORT_INTEGRITY_KEY,
    IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY,
    IMPORT_RAW_EVIDENCE_DIGEST_KEY,
    IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY,
    IMPORT_REPLAY_BUNDLE_DIGEST_KEY,
    IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY,
    IMPORT_REPLAY_PROFILE_VERSION_KEY,
    IMPORT_REPLAY_READINESS_KEY,
    IMPORT_SCHEMA_VERSION_KEY,
    IMPORT_SNAPSHOT_KIND_KEY,
    IMPORT_SOURCE_AGENT_VERSION_ID_KEY,
    IMPORT_SOURCE_FINGERPRINT_KEY,
    IMPORT_SOURCE_PIPELINE_ID_KEY,
    IMPORT_SOURCE_PROJECT_ID_KEY,
    IMPORT_SOURCE_PROVIDER_KEY,
    IMPORT_SOURCE_TRACE_ID_KEY,
    IMPORT_STATUS_KEY,
    IMPORTED_EXECUTION_ENVIRONMENT_KEY,
)
from kitaru._run_identity import extract_run_project_identity
from kitaru.errors import KitaruStateError
from kitaru.imports._models import TraceIntegrity
from kitaru.imports._pydantic_ai_replay import (
    _PYDANTIC_AI_REPLAY_PROFILE_VERSION,
    PreparedImportedReplayEvidence,
    PydanticAIReplayBundle,
    _readiness,
)
from kitaru.imports._replay_evidence import (
    EvidenceRedactionStatus,
    ImportedEvidenceArtifactIdentity,
    ImportedReplayEvidenceIdentity,
    ImportedReplayUnsupportedReason,
    RawImportedEvidence,
    ReplayReadinessSummary,
    SourceAttribution,
    SourceAttributionStatus,
    classify_source_attribution,
    contains_redaction,
)

_IMPORT_SCHEMA_VERSION = 5
_RAW_EVIDENCE_SCHEMA_VERSION = 1
_REPLAY_BUNDLE_SCHEMA_VERSION = 1
_IMPORTED_SNAPSHOT_KIND = "imported_observed"


class ImportedReplayEvidenceError(KitaruStateError):
    """Typed failure raised before a candidate is resolved or executed."""

    def __init__(
        self,
        reason: ImportedReplayUnsupportedReason,
        message: str,
        *,
        execution_id: str,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.execution_id = execution_id


def _fail(
    reason: ImportedReplayUnsupportedReason,
    message: str,
    *,
    execution_id: str,
) -> Never:
    raise ImportedReplayEvidenceError(reason, message, execution_id=execution_id)


def _required_string(
    values: Mapping[str, Any],
    key: str,
    *,
    reason: ImportedReplayUnsupportedReason,
    execution_id: str,
) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value.strip():
        _fail(
            reason,
            f"Imported execution metadata is missing required field {key!r}.",
            execution_id=execution_id,
        )
    return cast(str, value).strip()


def _required_schema_version(
    metadata: Mapping[str, Any],
    key: str,
    expected: int,
    *,
    execution_id: str,
) -> int:
    value = metadata.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value != expected:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_SCHEMA_MISMATCH,
            f"Imported evidence field {key!r} must be schema version {expected}.",
            execution_id=execution_id,
        )
    return cast(int, value)


def _artifact_payload(
    client: Any,
    *,
    execution_id: str,
    project_id: str,
    artifact_id: str,
    role: str,
    unavailable_reason: ImportedReplayUnsupportedReason,
) -> Mapping[str, Any]:
    try:
        artifact = client.get_artifact_version(
            name_id_or_prefix=artifact_id,
            project=project_id,
            hydrate=True,
        )
    except Exception as exc:
        raise ImportedReplayEvidenceError(
            unavailable_reason,
            f"Imported execution references unavailable {role.replace('_', ' ')}.",
            execution_id=execution_id,
        ) from exc

    if str(getattr(artifact, "id", "")).strip() != artifact_id:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_ROLE_MISMATCH,
            f"The {role.replace('_', ' ')} lookup resolved to a different artifact.",
            execution_id=execution_id,
        )
    if str(getattr(artifact, "project_id", "")).strip() != project_id:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_SCOPE_MISMATCH,
            f"The {role.replace('_', ' ')} belongs to a different Agent Project.",
            execution_id=execution_id,
        )
    expected_name = f"kitaru-import-{execution_id}::{role}"
    if str(getattr(artifact, "name", "")).strip() != expected_name:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_ROLE_MISMATCH,
            f"The {role.replace('_', ' ')} has a different immutable role.",
            execution_id=execution_id,
        )
    try:
        payload = artifact.load()
    except Exception as exc:
        raise ImportedReplayEvidenceError(
            unavailable_reason,
            f"Imported execution references unreadable {role.replace('_', ' ')}.",
            execution_id=execution_id,
        ) from exc
    if not isinstance(payload, Mapping):
        _fail(
            ImportedReplayUnsupportedReason.EVIDENCE_INVALID,
            f"The {role.replace('_', ' ')} payload is not a mapping.",
            execution_id=execution_id,
        )
    return payload


def load_imported_replay_evidence(
    execution_id: str,
    *,
    client: Any | None = None,
) -> PreparedImportedReplayEvidence:
    """Load and revalidate immutable evidence before candidate resolution."""
    normalized_execution_id = execution_id.strip()
    if not normalized_execution_id:
        raise KitaruStateError("Imported replay execution ID cannot be empty.")
    resolved_client = client or Client()
    try:
        run = resolved_client.get_pipeline_run(
            name_id_or_prefix=normalized_execution_id,
            allow_name_prefix_match=False,
            hydrate=True,
        )
    except Exception as exc:
        raise ImportedReplayEvidenceError(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "Unable to load the imported source execution.",
            execution_id=normalized_execution_id,
        ) from exc
    if str(getattr(run, "id", "")).strip() != normalized_execution_id:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "The imported source lookup resolved to a different execution.",
            execution_id=normalized_execution_id,
        )

    environment = getattr(run, "orchestrator_environment", {}) or {}
    metadata = getattr(run, "run_metadata", {}) or {}
    if not isinstance(environment, Mapping) or not isinstance(metadata, Mapping):
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "The imported source execution has malformed stored metadata.",
            execution_id=normalized_execution_id,
        )
    if environment.get(IMPORTED_EXECUTION_ENVIRONMENT_KEY) is not True:
        _fail(
            ImportedReplayUnsupportedReason.NOT_IMPORTED,
            "The selected execution is not an immutable imported execution.",
            execution_id=normalized_execution_id,
        )
    schema_version = environment.get(
        IMPORT_SCHEMA_VERSION_KEY, metadata.get(IMPORT_SCHEMA_VERSION_KEY)
    )
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        _fail(
            ImportedReplayUnsupportedReason.LEGACY_IMPORT,
            "The imported execution has no supported schema version.",
            execution_id=normalized_execution_id,
        )
    if schema_version < _IMPORT_SCHEMA_VERSION:
        _fail(
            ImportedReplayUnsupportedReason.LEGACY_IMPORT,
            "Legacy imported executions cannot be prepared for replay.",
            execution_id=normalized_execution_id,
        )
    if schema_version != _IMPORT_SCHEMA_VERSION:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "The imported execution uses an unknown schema version.",
            execution_id=normalized_execution_id,
        )
    if (
        metadata.get(IMPORT_STATUS_KEY) != "complete"
        or metadata.get(IMPORT_SNAPSHOT_KIND_KEY) != _IMPORTED_SNAPSHOT_KIND
        or metadata.get(IMPORT_INTEGRITY_KEY) != TraceIntegrity.COMPLETE.value
    ):
        _fail(
            ImportedReplayUnsupportedReason.IMPORT_INCOMPLETE,
            "The imported execution is incomplete or has ambiguous source ordering.",
            execution_id=normalized_execution_id,
        )

    project_id = extract_run_project_identity(run).project_id
    if project_id is None:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "The imported execution has no verifiable Agent Project.",
            execution_id=normalized_execution_id,
        )
    source_agent_version_id = _required_string(
        environment,
        IMPORT_SOURCE_AGENT_VERSION_ID_KEY,
        reason=ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
        execution_id=normalized_execution_id,
    )
    source_pipeline_id = _required_string(
        environment,
        IMPORT_SOURCE_PIPELINE_ID_KEY,
        reason=ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
        execution_id=normalized_execution_id,
    )
    source_fingerprint = _required_string(
        environment,
        IMPORT_SOURCE_FINGERPRINT_KEY,
        reason=ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
        execution_id=normalized_execution_id,
    )
    if source_agent_version_id != source_pipeline_id:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
            "The imported source AgentVersion and Pipeline IDs differ.",
            execution_id=normalized_execution_id,
        )
    try:
        attribution = SourceAttribution.model_validate(
            metadata.get(IMPORT_ATTRIBUTION_KEY), strict=False
        )
    except (TypeError, ValidationError) as exc:
        raise ImportedReplayEvidenceError(
            ImportedReplayUnsupportedReason.SOURCE_ATTRIBUTION_UNVERIFIED,
            "The imported source attribution is missing or malformed.",
            execution_id=normalized_execution_id,
        ) from exc
    if attribution.status is not SourceAttributionStatus.SOURCE_VERIFIED:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_ATTRIBUTION_UNVERIFIED,
            "The imported source AgentVersion is not verified by provider evidence.",
            execution_id=normalized_execution_id,
        )

    try:
        binding = resolve_registered_agent_version(
            resolved_client,
            agent=project_id,
            version=source_agent_version_id,
        )
        verify_hydrated_submitted_run_binding(run, binding=binding)
    except Exception as exc:
        raise ImportedReplayEvidenceError(
            ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
            "The imported source AgentVersion or Pipeline binding is no longer valid.",
            execution_id=normalized_execution_id,
        ) from exc
    if (
        binding.project_id != project_id
        or binding.pipeline_id != source_pipeline_id
        or binding.fingerprint != source_fingerprint
        or binding.agent_name
        != _required_string(
            environment,
            IMPORT_AGENT_NAME_KEY,
            reason=ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
            execution_id=normalized_execution_id,
        )
    ):
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_BINDING_INVALID,
            "The imported source identity differs from the registered AgentVersion.",
            execution_id=normalized_execution_id,
        )
    verified_attribution = classify_source_attribution(
        attribution.stamps,
        git_sha=binding.manifest.git_sha,
        aliases=binding.aliases,
    )
    if verified_attribution.status is not SourceAttributionStatus.SOURCE_VERIFIED:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_ATTRIBUTION_UNVERIFIED,
            "Provider evidence no longer verifies the registered source AgentVersion.",
            execution_id=normalized_execution_id,
        )

    raw_artifact_id = _required_string(
        metadata,
        IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY,
        reason=ImportedReplayUnsupportedReason.RAW_EVIDENCE_UNAVAILABLE,
        execution_id=normalized_execution_id,
    )
    replay_artifact_id = _required_string(
        metadata,
        IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY,
        reason=ImportedReplayUnsupportedReason.REPLAY_BUNDLE_UNAVAILABLE,
        execution_id=normalized_execution_id,
    )
    raw_schema_version = _required_schema_version(
        metadata,
        IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY,
        _RAW_EVIDENCE_SCHEMA_VERSION,
        execution_id=normalized_execution_id,
    )
    replay_schema_version = _required_schema_version(
        metadata,
        IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY,
        _REPLAY_BUNDLE_SCHEMA_VERSION,
        execution_id=normalized_execution_id,
    )
    raw_digest = _required_string(
        environment,
        IMPORT_RAW_EVIDENCE_DIGEST_KEY,
        reason=ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
        execution_id=normalized_execution_id,
    )
    replay_digest = _required_string(
        environment,
        IMPORT_REPLAY_BUNDLE_DIGEST_KEY,
        reason=ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
        execution_id=normalized_execution_id,
    )
    profile_version = _required_string(
        metadata,
        IMPORT_REPLAY_PROFILE_VERSION_KEY,
        reason=ImportedReplayUnsupportedReason.REPLAY_PROFILE_UNSUPPORTED,
        execution_id=normalized_execution_id,
    )
    if profile_version != _PYDANTIC_AI_REPLAY_PROFILE_VERSION:
        _fail(
            ImportedReplayUnsupportedReason.REPLAY_PROFILE_UNSUPPORTED,
            "The imported replay profile is not supported.",
            execution_id=normalized_execution_id,
        )

    raw_payload = _artifact_payload(
        resolved_client,
        execution_id=normalized_execution_id,
        project_id=project_id,
        artifact_id=raw_artifact_id,
        role="raw_evidence",
        unavailable_reason=ImportedReplayUnsupportedReason.RAW_EVIDENCE_UNAVAILABLE,
    )
    replay_payload = _artifact_payload(
        resolved_client,
        execution_id=normalized_execution_id,
        project_id=project_id,
        artifact_id=replay_artifact_id,
        role="replay_bundle",
        unavailable_reason=ImportedReplayUnsupportedReason.REPLAY_BUNDLE_UNAVAILABLE,
    )
    if raw_payload.get("schema_version") != raw_schema_version:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_SCHEMA_MISMATCH,
            "Raw evidence schema differs from its immutable reference.",
            execution_id=normalized_execution_id,
        )
    if replay_payload.get("schema_version") != replay_schema_version:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_SCHEMA_MISMATCH,
            "Replay bundle schema differs from its immutable reference.",
            execution_id=normalized_execution_id,
        )
    if replay_payload.get("profile_version") != profile_version:
        _fail(
            ImportedReplayUnsupportedReason.REPLAY_PROFILE_UNSUPPORTED,
            "Replay bundle profile differs from its immutable reference.",
            execution_id=normalized_execution_id,
        )
    if raw_payload.get("raw_content_sha256") != raw_digest:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
            "Raw evidence digest differs from its immutable reference.",
            execution_id=normalized_execution_id,
        )
    if replay_payload.get("bundle_digest") != replay_digest:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
            "Replay bundle digest differs from its immutable reference.",
            execution_id=normalized_execution_id,
        )
    try:
        raw_evidence = RawImportedEvidence.model_validate(raw_payload, strict=False)
        replay_bundle = PydanticAIReplayBundle.model_validate(
            replay_payload, strict=False
        )
    except (TypeError, ValidationError) as exc:
        raise ImportedReplayEvidenceError(
            ImportedReplayUnsupportedReason.EVIDENCE_INVALID,
            "Imported replay evidence failed immutable content validation.",
            execution_id=normalized_execution_id,
        ) from exc
    if replay_bundle.raw_evidence_digest != raw_evidence.raw_content_sha256:
        _fail(
            ImportedReplayUnsupportedReason.ARTIFACT_HASH_MISMATCH,
            "Replay bundle does not bind the loaded raw evidence.",
            execution_id=normalized_execution_id,
        )
    if raw_evidence.redaction_status is not EvidenceRedactionStatus.NOT_REDACTED:
        _fail(
            ImportedReplayUnsupportedReason.EVIDENCE_REDACTED,
            "Redacted imported evidence cannot be prepared for replay.",
            execution_id=normalized_execution_id,
        )
    if not replay_bundle.root_input_present or contains_redaction(
        replay_bundle.root_input
    ):
        _fail(
            ImportedReplayUnsupportedReason.ROOT_INPUT_MISSING,
            "Imported replay preparation requires a complete unredacted root input.",
            execution_id=normalized_execution_id,
        )

    source = raw_evidence.source
    expected_source = (
        _required_string(
            environment,
            IMPORT_SOURCE_PROVIDER_KEY,
            reason=ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            execution_id=normalized_execution_id,
        ),
        _required_string(
            environment,
            IMPORT_SOURCE_PROJECT_ID_KEY,
            reason=ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            execution_id=normalized_execution_id,
        ),
        _required_string(
            environment,
            IMPORT_SOURCE_TRACE_ID_KEY,
            reason=ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            execution_id=normalized_execution_id,
        ),
    )
    if source.identity != expected_source:
        _fail(
            ImportedReplayUnsupportedReason.SOURCE_EXECUTION_INVALID,
            "Imported evidence source identity differs from its execution.",
            execution_id=normalized_execution_id,
        )
    try:
        readiness = ReplayReadinessSummary.model_validate(
            metadata.get(IMPORT_REPLAY_READINESS_KEY), strict=False
        )
    except (TypeError, ValidationError) as exc:
        raise ImportedReplayEvidenceError(
            ImportedReplayUnsupportedReason.READINESS_MISMATCH,
            "Imported replay readiness is missing or malformed.",
            execution_id=normalized_execution_id,
        ) from exc
    parts = tuple(
        part for observation in replay_bundle.observations for part in observation.parts
    )
    expected_readiness = _readiness(
        diagnostics=replay_bundle.diagnostics,
        parts=parts,
        root_input_usable=True,
    )
    if readiness != expected_readiness:
        _fail(
            ImportedReplayUnsupportedReason.READINESS_MISMATCH,
            "Imported replay readiness differs from the loaded evidence.",
            execution_id=normalized_execution_id,
        )

    identity = ImportedReplayEvidenceIdentity(
        execution_id=normalized_execution_id,
        project_id=project_id,
        source_agent_version_id=source_agent_version_id,
        source_pipeline_id=source_pipeline_id,
        source_fingerprint=source_fingerprint,
        source_provider=source.provider,
        source_project_id=source.project_id,
        source_trace_id=source.trace_id,
        raw_evidence=ImportedEvidenceArtifactIdentity(
            artifact_id=raw_artifact_id,
            schema_version=raw_schema_version,
            sha256=raw_digest,
        ),
        replay_bundle=ImportedEvidenceArtifactIdentity(
            artifact_id=replay_artifact_id,
            schema_version=replay_schema_version,
            sha256=replay_digest,
        ),
        replay_profile_version=profile_version,
    )
    return PreparedImportedReplayEvidence(
        identity=identity,
        raw_evidence=raw_evidence,
        replay_bundle=replay_bundle,
        readiness=readiness,
    )
