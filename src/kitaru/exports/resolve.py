"""Read-only resolution and preflight for experiment exports."""

import asyncio
import hashlib
import json
import re
import uuid
from typing import Any

from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.api_models.v1.session import SessionListParams
from kitaru.exports.models import (
    ExportError,
    MaterializedEvaluator,
    ResolvedExport,
    RewardSelector,
    SourceInventory,
)

_MAX_TASK_INPUT_BYTES = 32 * 1024
_MAX_SESSION_FETCHES = 20
_EXACT_REQUIREMENT = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*"
    r"(?:\[[A-Za-z0-9._-]+(?:,[A-Za-z0-9._-]+)*\])?"
    r"==[A-Za-z0-9][A-Za-z0-9.!+_-]*$"
)


def _require_matching_agent(actual: uuid.UUID, expected: uuid.UUID, label: str) -> None:
    if actual != expected:
        raise ExportError("agent_mismatch", f"{label} belongs to a different agent.")


def _validate_tool_policy(policy: Any) -> None:
    configs = [policy.default, *policy.tools.values()]
    if any(config.type != "passthrough" for config in configs):
        raise ExportError(
            "unsupported_tool_policy",
            "Export v1 supports passthrough tool policies only.",
        )


def _validate_package_requirement(value: str) -> str:
    if _EXACT_REQUIREMENT.fullmatch(value) is None:
        raise ExportError(
            "unsupported_evaluator_source",
            "Evaluator package requirements must be a package name and one exact "
            "== version, without URLs or markers.",
        )
    return value


async def _resolve_evaluator(client: Any, config: Any) -> MaterializedEvaluator:
    if config.version is None:
        raise ExportError(
            "unpinned_evaluator",
            f"Evaluator {config.evaluator!r} must select an exact version.",
        )
    params = EvaluatorListParams(
        filter=FilterCondition(field="name", op=FilterOp.EQ, value=config.evaluator)
    )
    matches = [
        evaluator
        async for evaluator in client.evaluators.iter(params)
        if evaluator.name == config.evaluator
    ]
    if len(matches) != 1:
        raise ExportError(
            "evaluator_not_found",
            f"Expected one evaluator named {config.evaluator!r}, found {len(matches)}.",
        )
    evaluator = matches[0]
    version = await client.evaluators.get_version(evaluator.id, config.version)
    if version.evaluator_id != evaluator.id or version.version != config.version:
        raise ExportError(
            "evaluator_version_mismatch",
            f"Evaluator {config.evaluator!r} returned the wrong version.",
        )

    source = version.source
    script: bytes | None = None
    if isinstance(source, ScriptPluginSource) or source.type == "script":
        script = await client.blobs.download(source.blob_id)
        source_digest = hashlib.sha256(script).hexdigest()
    elif isinstance(source, PackagePluginSource) or source.type == "package":
        requirement = _validate_package_requirement(source.requirement)
        source_digest = hashlib.sha256(requirement.encode("utf-8")).hexdigest()
    else:
        raise ExportError(
            "unsupported_evaluator_source",
            f"Evaluator {config.evaluator!r} has an unsupported source.",
        )
    return MaterializedEvaluator(
        name=config.evaluator,
        version=version,
        params=dict(config.params),
        script=script,
        source_sha256=source_digest,
    )


async def resolve_export(
    client: Any,
    *,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    reward: RewardSelector,
    source: SourceInventory,
) -> ResolvedExport:
    """Resolve and validate all remote inputs without changing Kitaru state.

    Args:
        client: Existing Kitaru API client.
        experiment_id: Exact experiment id.
        cohort_version_id: Exact immutable cohort version id.
        agent_version_id: Exact agent version id.
        reward: Explicit primary reward selection.
        source: Validated local source inventory.

    Raises:
        ExportError: The selected objects cannot be exported faithfully.

    Returns:
        Frozen renderer input.
    """
    experiment, cohort_version, agent_version = await asyncio.gather(
        client.experiments.get(experiment_id),
        client.cohort_versions.get(cohort_version_id),
        client.agent_versions.get(agent_version_id),
    )
    cohort = await client.cohorts.get(cohort_version.cohort_id)
    _require_matching_agent(cohort.agent_id, experiment.agent_id, "Cohort")
    _require_matching_agent(
        agent_version.agent_id, experiment.agent_id, "Agent version"
    )
    if agent_version.run_spec is None:
        raise ExportError("missing_run_spec", "Agent version has no run specification.")
    if experiment.override is not None:
        raise ExportError(
            "unsupported_override", "Export v1 does not support replay overrides."
        )
    _validate_tool_policy(experiment.tool_policy)

    working_dir = agent_version.run_spec.working_dir
    if working_dir:
        candidate = (source.root / working_dir).resolve(strict=False)
        try:
            candidate.relative_to(source.root)
        except ValueError as error:
            raise ExportError(
                "invalid_working_directory",
                "Agent working directory escapes the source root.",
            ) from error
        if not candidate.is_dir():
            raise ExportError(
                "invalid_working_directory",
                "Agent working directory does not exist in the source root.",
            )

    params = SessionListParams(
        filter=FilterCondition(
            field="cohort_version_id", op=FilterOp.EQ, value=str(cohort_version_id)
        ),
        sort="id:asc",
    )
    summaries = [session async for session in client.sessions.iter(params)]
    session_ids = sorted({session.id for session in summaries}, key=str)
    if len(session_ids) != cohort_version.session_count:
        raise ExportError(
            "cohort_count_mismatch",
            f"Cohort version records {cohort_version.session_count} sessions but "
            f"{len(session_ids)} unique members were returned.",
        )
    session_slots = asyncio.Semaphore(_MAX_SESSION_FETCHES)

    async def get_session(session_id: uuid.UUID) -> Any:
        async with session_slots:
            return await client.sessions.get_with_nodes(session_id)

    sessions = list(
        await asyncio.gather(*(get_session(session_id) for session_id in session_ids))
    )
    sessions.sort(key=lambda full: str(full.session.id))
    if [full.session.id for full in sessions] != session_ids:
        raise ExportError(
            "cohort_session_mismatch",
            "Full session responses did not match the cohort membership.",
        )
    for full in sessions:
        session = full.session
        _require_matching_agent(session.agent_id, experiment.agent_id, "Session")
        try:
            input_size = len(json.dumps(session.inputs).encode("utf-8"))
        except (TypeError, ValueError) as error:
            raise ExportError(
                "invalid_task_inputs", f"Session {session.id} inputs are not JSON."
            ) from error
        if input_size > _MAX_TASK_INPUT_BYTES:
            raise ExportError(
                "inputs_too_large",
                f"Session {session.id} inputs exceed {_MAX_TASK_INPUT_BYTES} bytes.",
            )

    evaluator_configs = list(experiment.evaluators)
    if sum(config.evaluator == reward.evaluator for config in evaluator_configs) != 1:
        raise ExportError(
            "invalid_reward_selector",
            f"Primary reward evaluator {reward.evaluator!r} is not selected "
            "exactly once.",
        )
    evaluators = tuple(
        await asyncio.gather(
            *(_resolve_evaluator(client, config) for config in evaluator_configs)
        )
    )
    return ResolvedExport(
        experiment=experiment,
        cohort_version=cohort_version,
        agent_version=agent_version,
        sessions=tuple(sessions),
        evaluators=evaluators,
        reward=reward,
        source=source,
    )
