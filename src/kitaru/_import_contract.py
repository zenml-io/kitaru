"""Shared metadata contract for synthetic imported executions."""

from zenml.models import PipelineRunResponse

from kitaru.errors import KitaruStateError

IMPORTED_EXECUTION_ENVIRONMENT_KEY = "kitaru_synthetic_import"
IMPORTED_OBSERVATION_ID_METADATA_KEY = "kitaru_import_observation_id_v1"
IMPORTED_PARENT_OBSERVATION_ID_METADATA_KEY = "kitaru_import_parent_observation_id_v1"

IMPORT_SCHEMA_VERSION_KEY = "kitaru_import_schema_version"
IMPORT_SNAPSHOT_KIND_KEY = "kitaru_snapshot_kind_v1"
IMPORT_SOURCE_CONTENT_DIGEST_KEY = "kitaru_import_content_digest_v1"
IMPORT_SOURCE_PROVIDER_KEY = "kitaru_import_source_provider_v1"
IMPORT_SOURCE_PROJECT_ID_KEY = "kitaru_import_source_project_id_v1"
IMPORT_SOURCE_TRACE_ID_KEY = "kitaru_import_source_trace_id_v1"
IMPORT_AGENT_NAME_KEY = "kitaru_import_agent_name_v1"
IMPORT_STACK_ID_KEY = "kitaru_import_stack_id_v1"
IMPORT_STATUS_KEY = "kitaru_import_status_v1"
IMPORT_SOURCE_AGENT_VERSION_ID_KEY = "kitaru_import_source_agent_version_id_v1"
IMPORT_SOURCE_AGENT_VERSION_LABEL_KEY = "kitaru_import_source_agent_version_label_v1"
IMPORT_SOURCE_PIPELINE_ID_KEY = "kitaru_import_source_pipeline_id_v1"
IMPORT_SOURCE_FINGERPRINT_KEY = "kitaru_import_source_fingerprint_v1"
IMPORT_RAW_EVIDENCE_DIGEST_KEY = "kitaru_import_raw_evidence_sha256_v1"
IMPORT_REPLAY_BUNDLE_DIGEST_KEY = "kitaru_import_replay_bundle_sha256_v1"
IMPORT_COHORT_TAG_KEY = "kitaru_import_cohort_tag_v1"
IMPORT_RAW_EVIDENCE_ARTIFACT_ID_KEY = "kitaru_import_raw_evidence_artifact_id_v1"
IMPORT_REPLAY_BUNDLE_ARTIFACT_ID_KEY = "kitaru_import_replay_bundle_artifact_id_v1"
IMPORT_ATTRIBUTION_KEY = "kitaru_import_attribution_v1"
IMPORT_INTEGRITY_KEY = "kitaru_import_integrity_v1"
IMPORT_OBSERVATION_COUNT_KEY = "kitaru_import_observation_count_v1"
IMPORT_RAW_EVIDENCE_SCHEMA_VERSION_KEY = "kitaru_import_raw_evidence_schema_version_v1"
IMPORT_REPLAY_BUNDLE_SCHEMA_VERSION_KEY = (
    "kitaru_import_replay_bundle_schema_version_v1"
)
IMPORT_REPLAY_PROFILE_VERSION_KEY = "kitaru_import_replay_profile_version_v1"
IMPORT_REPLAY_READINESS_KEY = "kitaru_import_replay_readiness_v1"


def raise_if_imported_execution(run: PipelineRunResponse, operation: str) -> None:
    """Refuse lifecycle operations on synthetic imported executions.

    Imported executions never ran real flow code: their steps resolve to a
    placeholder that raises if executed, so retrying, resuming, replaying,
    or cancelling them can only fail after mutating run state.

    Args:
        run: The pipeline run backing the execution.
        operation: Past-tense operation name for the error message, for
            example "retried".

    Raises:
        KitaruStateError: If the run is a synthetic imported execution.
    """
    if run.orchestrator_environment.get(IMPORTED_EXECUTION_ENVIRONMENT_KEY):
        raise KitaruStateError(
            f"Execution '{run.id}' was imported from an external trace and "
            f"never executed real flow code, so it cannot be {operation}. "
            "Imported executions are read-only records."
        )
