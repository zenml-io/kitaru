"""Read-only resolution and preflight for experiment exports."""

import asyncio
import hashlib
import json
import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Literal

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from pydantic import BaseModel

from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.api_models.v1.session import SessionListParams
from kitaru.client.exceptions import ResponseTooLargeError
from kitaru.exports._dependencies import classify_dependencies
from kitaru.exports._runtime import parse_command_argv, reject_protected_source
from kitaru.exports._sanitize import EphemeralSanitizer
from kitaru.exports.models import (
    V1_EXPORT_BUDGETS,
    ContentPolicy,
    DependencyPlan,
    EnvironmentPolicy,
    ExportError,
    MaterializedEvaluator,
    ResolvedExport,
    RewardSelector,
    RuntimeEnvironmentRequirement,
    SourceInventory,
    SourcePolicy,
)

_MAX_TASK_INPUT_BYTES = 32 * 1024
_MAX_SESSION_FETCHES = 20
_MAX_RESOURCE_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_SECRET_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_SESSION_PAGE_BYTES = V1_EXPORT_BUDGETS.max_session_bytes
_EXACT_REQUIREMENT = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*"
    r"(?:\[[A-Za-z0-9._-]+(?:,[A-Za-z0-9._-]+)*\])?"
    r"==[A-Za-z0-9][A-Za-z0-9.!+_-]*$"
)
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RESERVED_ENVIRONMENT_NAMES = frozenset(
    {
        "KITARU_EXTERNAL_EVALUATION",
        "KITARU_MCP_URLS",
        "KITARU_TASK_INPUTS",
        "KITARU_TRACE_PATH",
        "PYTHONPATH",
    }
)


@dataclass(frozen=True)
class RemoteExportResolution:
    """Hold sanitized remote state until local source preflight completes."""

    experiment: Any
    cohort_version: Any
    agent_version: Any
    sessions: tuple[Any, ...]
    evaluators: tuple[MaterializedEvaluator, ...]
    reward: RewardSelector
    command_argv: tuple[str, ...]
    required_environment_names: tuple[str, ...]
    runtime_environment: tuple[RuntimeEnvironmentRequirement, ...]
    content_policy: ContentPolicy
    environment_policy: EnvironmentPolicy
    _sanitizer: EphemeralSanitizer


async def _call_bounded(
    method: Any,
    *args: Any,
    max_bytes: int,
    code: str,
    message: str,
    **kwargs: Any,
) -> Any:
    """Call one SDK resource with a response cap and stable export error."""
    try:
        return await method(*args, **kwargs, max_bytes=max_bytes)
    except ResponseTooLargeError as error:
        raise ExportError(code, message) from error


async def _iterate_bounded(
    method: Any,
    *args: Any,
    max_bytes: int,
    code: str,
    message: str,
    **kwargs: Any,
) -> AsyncIterator[Any]:
    """Iterate SDK pages with a response cap and stable export error."""
    try:
        iterator = method(*args, **kwargs, max_bytes=max_bytes)
        async for item in iterator:
            yield item
    except ResponseTooLargeError as error:
        raise ExportError(code, message) from error


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


def _json_ready(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, SimpleNamespace):
        return {key: _json_ready(item) for key, item in vars(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _serialized_size(value: Any, *, code: str, message: str) -> int:
    try:
        return len(
            json.dumps(
                _json_ready(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        )
    except (TypeError, ValueError) as error:
        raise ExportError(code, message) from error


def _secret_text(value: Any) -> str:
    getter = getattr(value, "get_secret_value", None)
    if not callable(getter):
        raise ExportError(
            "secret_resolution_failed",
            "An attached secret could not be resolved safely.",
        )
    resolved = getter()
    if not isinstance(resolved, str):
        raise ExportError(
            "secret_resolution_failed",
            "An attached secret could not be resolved safely.",
        )
    return resolved


def _validate_environment_name(name: str) -> None:
    if _ENVIRONMENT_NAME.fullmatch(name) is None:
        raise ExportError(
            "invalid_environment_name",
            "Attached and registered environment names must be valid identifiers.",
        )
    if name in _RESERVED_ENVIRONMENT_NAMES:
        raise ExportError(
            "reserved_environment_name",
            "A registered runtime value uses a target-reserved environment name.",
        )


async def _resolve_attached_secrets(
    client: Any, secret_ids: list[uuid.UUID]
) -> tuple[EphemeralSanitizer, tuple[str, ...]]:
    if len(secret_ids) > V1_EXPORT_BUDGETS.max_attached_secrets:
        raise ExportError(
            "too_many_attached_secrets",
            "An agent version may attach at most 100 secrets for export.",
        )
    ordered_unique_ids = tuple(dict.fromkeys(secret_ids))
    values: list[str] = []
    total_value_bytes = 0
    environment_names: set[str] = set()
    for secret_id in ordered_unique_ids:
        try:
            secret = await _call_bounded(
                client.secrets.get,
                secret_id,
                include_values=True,
                max_bytes=_MAX_SECRET_RESPONSE_BYTES,
                code="protected_values_too_large",
                message="An attached secret response exceeds the export limit.",
            )
            secret_values = secret.values
            if not isinstance(secret_values, dict):
                raise TypeError("secret values are not a mapping")
            for name, secret_value in secret_values.items():
                if not isinstance(name, str):
                    raise TypeError("secret name is not text")
                _validate_environment_name(name)
                environment_names.add(name)
                value = _secret_text(secret_value)
                total_value_bytes += len(value.encode("utf-8"))
                if total_value_bytes > V1_EXPORT_BUDGETS.max_protected_value_bytes:
                    raise ExportError(
                        "protected_values_too_large",
                        "Attached secret values exceed the 1 MiB export limit.",
                    )
                values.append(value)
        except ExportError:
            raise
        except Exception:
            raise ExportError(
                "secret_resolution_failed",
                "An attached secret could not be authorized and resolved safely.",
            ) from None
    return EphemeralSanitizer(values), tuple(sorted(environment_names))


def _replace_run_spec(agent_version: Any, run_spec: Any) -> Any:
    if isinstance(agent_version, BaseModel):
        return agent_version.model_copy(update={"run_spec": run_spec}, deep=True)
    copied = SimpleNamespace(**vars(agent_version))
    copied.run_spec = run_spec
    return copied


def _replace_run_environment(run_spec: Any, environment: dict[str, str]) -> Any:
    updates = {"env": environment, "secret_ids": []}
    if isinstance(run_spec, BaseModel):
        return run_spec.model_copy(update=updates, deep=True)
    copied = SimpleNamespace(**vars(run_spec))
    copied.env = dict(environment)
    copied.secret_ids = []
    return copied


def _sanitize_agent_version(
    agent_version: Any,
    *,
    sanitizer: EphemeralSanitizer,
    environment_policy: EnvironmentPolicy,
    secret_environment_names: tuple[str, ...],
) -> tuple[Any, tuple[RuntimeEnvironmentRequirement, ...]]:
    sanitized_agent = sanitizer.sanitize(agent_version)
    run_spec = sanitized_agent.run_spec
    registered = dict(run_spec.env)
    for name, value in registered.items():
        _validate_environment_name(name)
        sanitizer.reject_text(
            value,
            code="protected_value_in_environment",
            message=(
                "Protected runtime material appears in a registered environment "
                "value; export cannot infer safe ownership."
            ),
        )
    secret_names = set(secret_environment_names)
    if environment_policy.mode == "include":
        included = {
            name: value
            for name, value in registered.items()
            if name not in secret_names
        }
    else:
        included = {}

    sources: dict[str, Literal["attached_secret", "registered_environment"]] = {
        name: "attached_secret" for name in secret_environment_names
    }
    if environment_policy.mode == "runtime_only":
        for name in registered:
            sources.setdefault(name, "registered_environment")
    requirements = tuple(
        RuntimeEnvironmentRequirement(
            name=name,
            owner="agent",
            source=source,
        )
        for name, source in sorted(sources.items())
    )
    sanitized_run_spec = _replace_run_environment(run_spec, included)
    return _replace_run_spec(sanitized_agent, sanitized_run_spec), requirements


async def _resolve_evaluator(
    client: Any,
    config: Any,
    *,
    sanitizer: EphemeralSanitizer,
) -> MaterializedEvaluator:
    sanitizer.reject_text(
        config.evaluator,
        code="protected_value_in_evaluator",
        message="Protected runtime material appears in an evaluator identifier.",
    )
    if config.version is None:
        raise ExportError(
            "unpinned_evaluator",
            f"Evaluator {config.evaluator!r} must select an exact version.",
        )
    params = EvaluatorListParams(
        filter=FilterCondition(field="name", op=FilterOp.EQ, value=config.evaluator),
        size=2,
    )
    matches = [
        evaluator
        async for evaluator in _iterate_bounded(
            client.evaluators.iter,
            params,
            max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
            code="evaluator_response_too_large",
            message="Evaluator metadata exceeds the export response limit.",
        )
        if evaluator.name == config.evaluator
    ]
    if len(matches) != 1:
        raise ExportError(
            "evaluator_not_found",
            f"Expected one evaluator named {config.evaluator!r}, found {len(matches)}.",
        )
    evaluator = matches[0]
    version = await _call_bounded(
        client.evaluators.get_version,
        evaluator.id,
        config.version,
        max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
        code="evaluator_response_too_large",
        message="Evaluator version metadata exceeds the export response limit.",
    )
    if version.evaluator_id != evaluator.id or version.version != config.version:
        raise ExportError(
            "evaluator_version_mismatch",
            f"Evaluator {config.evaluator!r} returned the wrong version.",
        )

    source = version.source
    script: bytes | None = None
    if isinstance(source, ScriptPluginSource) or source.type == "script":
        script = await _call_bounded(
            client.blobs.download,
            source.blob_id,
            max_bytes=V1_EXPORT_BUDGETS.max_evaluator_bytes,
            code="evaluator_too_large",
            message="An evaluator source blob exceeds the 10 MiB export limit.",
        )
        if len(script) > V1_EXPORT_BUDGETS.max_evaluator_bytes:
            raise ExportError(
                "evaluator_too_large",
                "An evaluator source blob exceeds the 10 MiB export limit.",
            )
        sanitizer.reject_bytes(
            script,
            code="protected_value_in_evaluator",
            message=(
                "Protected runtime material appears in evaluator source; export "
                "cannot rewrite executable material safely."
            ),
        )
        source_digest = hashlib.sha256(script).hexdigest()
    elif isinstance(source, PackagePluginSource) or source.type == "package":
        sanitizer.reject_text(
            source.requirement,
            code="protected_value_in_dependency",
            message="Protected runtime material appears in evaluator dependencies.",
        )
        requirement = _validate_package_requirement(source.requirement)
        source_digest = hashlib.sha256(requirement.encode("utf-8")).hexdigest()
    else:
        raise ExportError(
            "unsupported_evaluator_source",
            f"Evaluator {config.evaluator!r} has an unsupported source.",
        )
    return MaterializedEvaluator(
        name=config.evaluator,
        version=sanitizer.sanitize(version),
        params=sanitizer.sanitize(dict(config.params)),
        script=script,
        source_sha256=source_digest,
    )


def _replace_fields(value: Any, updates: dict[str, Any]) -> Any:
    """Return one copied API value with schema-valid content omissions."""
    if isinstance(value, BaseModel):
        return value.model_copy(update=updates, deep=True)
    copied = SimpleNamespace(**vars(value))
    for name, replacement in updates.items():
        setattr(copied, name, replacement)
    return copied


def _apply_content_policy(value: Any, policy: ContentPolicy) -> Any:
    """Remove only the optional evidence categories selected by the caller."""
    session_updates: dict[str, Any] = {}
    if not policy.is_included("session_outputs"):
        session_updates["outputs"] = None
    if not policy.is_included("metadata"):
        session_updates["metadata"] = {}
    if not policy.is_included("diagnostic_details"):
        session_updates.update(error=None, external_id=None)
    if not policy.is_included("usage_and_cost"):
        session_updates.update(cost=None, tokens=None)
    session = _replace_fields(value.session, session_updates)

    payload_categories = {
        "llm_call": "model_payloads",
        "tool_call": "tool_payloads",
        "subagent_call": "subagent_payloads",
        "span": "span_payloads",
    }
    nodes: list[Any] = []
    for node in value.nodes:
        updates: dict[str, Any] = {}
        node_type = str(node.node_type)
        category = payload_categories.get(node_type)
        if category is not None and not policy.is_included(category):
            updates.update(
                inputs=None,
                outputs=None,
                input_text_selector=None,
                output_text_selector=None,
            )
            if node_type == "llm_call":
                updates.update(system_prompt_selector=None, model_params=None)
            if node_type == "span":
                updates["attributes"] = None
        if not policy.is_included("visible_reasoning"):
            updates["reasoning"] = None
        if not policy.is_included("metadata"):
            updates["metadata"] = {}
        if not policy.is_included("diagnostic_details"):
            updates.update(error=None, external_id=None, trace_id=None, cache_key=None)
        if not policy.is_included("usage_and_cost"):
            updates.update(cost=None, tokens=None)
        nodes.append(_replace_fields(node, updates))
    return _replace_fields(value, {"session": session, "nodes": nodes})


async def _resolve_sessions(
    client: Any,
    *,
    experiment: Any,
    cohort_version: Any,
    cohort_version_id: uuid.UUID,
    sanitizer: EphemeralSanitizer,
    content_policy: ContentPolicy,
) -> tuple[Any, ...]:
    if cohort_version.session_count > V1_EXPORT_BUDGETS.max_sessions:
        raise ExportError(
            "too_many_sessions",
            "A cohort export may contain at most 1,000 sessions.",
        )
    params = SessionListParams(
        filter=FilterCondition(
            field="cohort_version_id", op=FilterOp.EQ, value=str(cohort_version_id)
        ),
        sort="id:asc",
    )
    member_ids: set[uuid.UUID] = set()
    async for summary in _iterate_bounded(
        client.sessions.iter,
        params,
        max_bytes=_MAX_SESSION_PAGE_BYTES,
        code="sessions_too_large",
        message="A session list page exceeds the 16 MiB export limit.",
    ):
        member_ids.add(summary.id)
        if len(member_ids) > V1_EXPORT_BUDGETS.max_sessions:
            raise ExportError(
                "too_many_sessions",
                "A cohort export may contain at most 1,000 sessions.",
            )
    session_ids = sorted(member_ids, key=str)
    if len(session_ids) != cohort_version.session_count:
        raise ExportError(
            "cohort_count_mismatch",
            f"Cohort version records {cohort_version.session_count} sessions but "
            f"{len(session_ids)} unique members were returned.",
        )
    session_slots = asyncio.Semaphore(_MAX_SESSION_FETCHES)

    async def get_session(session_id: uuid.UUID) -> Any:
        async with session_slots:
            return await _call_bounded(
                client.sessions.get_with_nodes,
                session_id,
                max_bytes=V1_EXPORT_BUDGETS.max_session_bytes,
                code="session_too_large",
                message="A full session response exceeds the 16 MiB export limit.",
            )

    sessions = list(
        await asyncio.gather(*(get_session(session_id) for session_id in session_ids))
    )
    sessions.sort(key=lambda full: str(full.session.id))
    if [full.session.id for full in sessions] != session_ids:
        raise ExportError(
            "cohort_session_mismatch",
            "Full session responses did not match the cohort membership.",
        )
    total_bytes = 0
    sanitized_sessions: list[Any] = []
    for full in sessions:
        session = full.session
        _require_matching_agent(session.agent_id, experiment.agent_id, "Session")
        input_size = _serialized_size(
            session.inputs,
            code="invalid_task_inputs",
            message=f"Session {session.id} inputs are not JSON.",
        )
        if input_size > _MAX_TASK_INPUT_BYTES:
            raise ExportError(
                "inputs_too_large",
                f"Session {session.id} inputs exceed {_MAX_TASK_INPUT_BYTES} bytes.",
            )
        session_bytes = _serialized_size(
            full,
            code="invalid_session",
            message=f"Session {session.id} is not serializable.",
        )
        if session_bytes > V1_EXPORT_BUDGETS.max_session_bytes:
            raise ExportError(
                "session_too_large",
                "A serialized session exceeds the 16 MiB export limit.",
            )
        total_bytes += session_bytes
        if total_bytes > V1_EXPORT_BUDGETS.max_total_session_bytes:
            raise ExportError(
                "sessions_too_large",
                "Serialized sessions exceed the 256 MiB aggregate export limit.",
            )
        sanitized = sanitizer.sanitize(full)
        sanitized_sessions.append(_apply_content_policy(sanitized, content_policy))
    return tuple(sanitized_sessions)


async def resolve_remote_export(
    client: Any,
    *,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    reward: RewardSelector,
    content_policy: ContentPolicy | None = None,
    environment_policy: EnvironmentPolicy | None = None,
) -> RemoteExportResolution:
    """Authorize, sanitize, and freeze remote state before local source access."""
    selected_content = content_policy or ContentPolicy()
    selected_environment = environment_policy or EnvironmentPolicy()
    experiment, cohort_version, agent_version = await asyncio.gather(
        _call_bounded(
            client.experiments.get,
            experiment_id,
            max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
            code="remote_response_too_large",
            message="Experiment metadata exceeds the export response limit.",
        ),
        _call_bounded(
            client.cohort_versions.get,
            cohort_version_id,
            max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
            code="remote_response_too_large",
            message="Cohort version metadata exceeds the export response limit.",
        ),
        _call_bounded(
            client.agent_versions.get,
            agent_version_id,
            max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
            code="remote_response_too_large",
            message="Agent version metadata exceeds the export response limit.",
        ),
    )
    cohort = await _call_bounded(
        client.cohorts.get,
        cohort_version.cohort_id,
        max_bytes=_MAX_RESOURCE_RESPONSE_BYTES,
        code="remote_response_too_large",
        message="Cohort metadata exceeds the export response limit.",
    )
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

    sanitizer, secret_names = await _resolve_attached_secrets(
        client, list(agent_version.run_spec.secret_ids)
    )
    command_argv = parse_command_argv(
        agent_version.run_spec.command,
        sanitizer=sanitizer,
    )
    sanitized_agent_version, runtime_environment = _sanitize_agent_version(
        agent_version,
        sanitizer=sanitizer,
        environment_policy=selected_environment,
        secret_environment_names=secret_names,
    )
    sessions = await _resolve_sessions(
        client,
        experiment=experiment,
        cohort_version=cohort_version,
        cohort_version_id=cohort_version_id,
        sanitizer=sanitizer,
        content_policy=selected_content,
    )

    evaluator_configs = list(experiment.evaluators)
    evaluator_names = [config.evaluator for config in evaluator_configs]
    if len(set(evaluator_names)) != len(evaluator_names):
        raise ExportError(
            "duplicate_evaluator_name",
            "Export v1 requires unique evaluator names.",
        )
    if evaluator_names.count(reward.evaluator) != 1:
        raise ExportError(
            "invalid_reward_selector",
            f"Primary reward evaluator {reward.evaluator!r} is not selected "
            "exactly once.",
        )
    evaluators: list[MaterializedEvaluator] = []
    total_evaluator_bytes = 0
    for config in evaluator_configs:
        evaluator = await _resolve_evaluator(
            client,
            config,
            sanitizer=sanitizer,
        )
        if evaluator.script is not None:
            total_evaluator_bytes += len(evaluator.script)
            if total_evaluator_bytes > V1_EXPORT_BUDGETS.max_total_evaluator_bytes:
                raise ExportError(
                    "evaluators_too_large",
                    "Evaluator source blobs exceed the 100 MiB aggregate export limit.",
                )
        evaluators.append(evaluator)

    return RemoteExportResolution(
        experiment=sanitizer.sanitize(experiment),
        cohort_version=sanitizer.sanitize(cohort_version),
        agent_version=sanitized_agent_version,
        sessions=sessions,
        evaluators=tuple(evaluators),
        reward=reward,
        command_argv=command_argv,
        required_environment_names=tuple(
            requirement.name for requirement in runtime_environment
        ),
        runtime_environment=runtime_environment,
        content_policy=selected_content,
        environment_policy=selected_environment,
        _sanitizer=sanitizer,
    )


def _validate_dependency_conflicts(
    remote: RemoteExportResolution, plan: DependencyPlan
) -> None:
    requirements: dict[str, str] = {
        item.project: " ".join(item.requirement.split()) for item in plan.requirements
    }
    for evaluator in remote.evaluators:
        source = evaluator.version.source
        if not (isinstance(source, PackagePluginSource) or source.type == "package"):
            continue
        requirement = _validate_package_requirement(source.requirement)
        project = canonicalize_name(Requirement(requirement).name)
        normalized = " ".join(requirement.split())
        previous = requirements.get(project)
        if previous is not None and previous != normalized:
            raise ExportError(
                "dependency_conflict",
                "Agent and evaluator environments declare conflicting requirements.",
            )
        requirements[project] = normalized


def finalize_remote_export(
    remote: RemoteExportResolution,
    *,
    source: SourceInventory,
    source_policy: SourcePolicy | None = None,
) -> ResolvedExport:
    """Combine sanitized remote state with one validated local source snapshot."""
    dependency_plan = classify_dependencies(source, sanitizer=remote._sanitizer)
    reject_protected_source(source, sanitizer=remote._sanitizer)
    _validate_dependency_conflicts(remote, dependency_plan)

    working_dir = remote.agent_version.run_spec.working_dir
    if working_dir:
        remote._sanitizer.reject_text(
            working_dir,
            code="protected_value_in_path",
            message="Protected runtime material appears in the agent working path.",
        )
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

    return ResolvedExport(
        experiment=remote.experiment,
        cohort_version=remote.cohort_version,
        agent_version=remote.agent_version,
        sessions=remote.sessions,
        evaluators=remote.evaluators,
        reward=remote.reward,
        source=source,
        command_argv=remote.command_argv,
        required_environment_names=remote.required_environment_names,
        runtime_environment=remote.runtime_environment,
        dependency_plan=dependency_plan,
        content_policy=remote.content_policy,
        environment_policy=remote.environment_policy,
        source_policy=source_policy or SourcePolicy(),
    )


async def resolve_export(
    client: Any,
    *,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    reward: RewardSelector,
    source: SourceInventory,
    content_policy: ContentPolicy | None = None,
    environment_policy: EnvironmentPolicy | None = None,
    source_policy: SourcePolicy | None = None,
) -> ResolvedExport:
    """Resolve remote inputs first, then validate one local source inventory.

    This compatibility wrapper leaves existing callers unchanged. Callers that
    must prove no local source access before authorization can call
    :func:`resolve_remote_export`, acquire the source snapshot, and then call
    :func:`finalize_remote_export`.

    Raises:
        ExportError: The selected objects cannot be exported faithfully.

    Returns:
        Frozen renderer input containing no attached-secret values.
    """
    remote = await resolve_remote_export(
        client,
        experiment_id=experiment_id,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
        reward=reward,
        content_policy=content_policy,
        environment_policy=environment_policy,
    )
    return finalize_remote_export(remote, source=source, source_policy=source_policy)
