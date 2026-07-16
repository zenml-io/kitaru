"""Shared metadata contract for synthetic imported executions."""

from zenml.models import PipelineRunResponse

from kitaru.errors import KitaruStateError

IMPORTED_EXECUTION_ENVIRONMENT_KEY = "kitaru_synthetic_import"
IMPORTED_OBSERVATION_ID_METADATA_KEY = "kitaru_import_observation_id_v1"
IMPORTED_PARENT_OBSERVATION_ID_METADATA_KEY = "kitaru_import_parent_observation_id_v1"


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
