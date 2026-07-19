"""Run an imported Langfuse trace through a bounded regression journey."""

import importlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import click
from reference_agent.config import (
    IMPORTED_SOURCE_VARIANT,
    IMPORTED_SOURCE_VERSION,
)

from kitaru import KitaruClient, RegressionLimits
from kitaru.adapters.pydantic_ai import (
    ImportedReplayFallbackPolicy,
    ImportedReplayPreparationError,
    prepare_imported_replay_history,
)
from kitaru.errors import KitaruUsageError
from kitaru.imports import (
    ImportedReplayBoundary,
    ImportedReplayBoundaryKind,
    ImportedReplayMode,
    ReplayPartKind,
    load_imported_replay_evidence,
)
from kitaru.replay import EXPERIMENT_ID_METADATA_KEY

AGENT_NAME = "support-agent"
SOURCE_VARIANT = IMPORTED_SOURCE_VARIANT
SOURCE_VERSION = IMPORTED_SOURCE_VERSION
DEFAULT_CANDIDATE_VARIANT = os.getenv(
    "SUPPORT_AGENT_VARIANT", "nano_trimmed_permissions"
)
DEFAULT_CANDIDATE_VERSION = os.getenv("SUPPORT_AGENT_VERSION", "v2.2-counterfactual")
DEFAULT_EXPERIMENT = "support-agent-permissions-v2"
DEFAULT_BOUNDARY_KIND = "tool-result"
DEFAULT_BOUNDARY_INDEX = 1
_INSPECTION_DEFAULT_PAGE_SIZE = 25
_INSPECTION_MAX_PAGE_SIZE = 100
_INSPECTION_MAX_SCORES = 100


def _json_value(value: Any) -> Any:
    """Recursively convert SDK and model values to JSON-compatible data."""
    if hasattr(value, "to_json"):
        return _json_value(value.to_json())
    if hasattr(value, "model_dump"):
        return _json_value(value.model_dump(mode="json"))
    if is_dataclass(value) and not isinstance(value, type):
        return _json_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    if isinstance(value, Enum):
        return _json_value(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (Path, UUID)):
        return str(value)
    return value


def _json(value: Any) -> str:
    """Serialize an SDK result for the tutorial CLI."""
    return json.dumps(_json_value(value), indent=2, sort_keys=True)


def _registration_module() -> Any:
    """Load the PydanticAI agent only for registration or execution commands."""
    return importlib.import_module("evals.register")


def _registered_agent(
    *, variant: str, version: str, model: Any | None = None
) -> tuple[Any, Any]:
    """Build and register one explicit implementation variant."""
    if version == SOURCE_VERSION and variant != SOURCE_VARIANT:
        raise KitaruUsageError(
            f"Source version {SOURCE_VERSION!r} is frozen to variant "
            f"{SOURCE_VARIANT!r}."
        )
    registration_module = _registration_module()
    agent = registration_module.configure_agent(variant, model=model)
    agent.register(
        label=version,
        entrypoint=registration_module.entrypoint_for_variant(variant),
    )
    return agent, registration_module.support_resolution_objective


def _import_traces(
    source: str,
    *,
    source_project_id: str | None,
    trace_ids: list[str],
    limit: int | None,
    dry_run: bool,
) -> Any:
    """Plan or execute a Langfuse import under the declared source version."""
    client = KitaruClient()
    return client.imports.langfuse(
        source,
        source_project_id=source_project_id,
        agent=AGENT_NAME,
        version=SOURCE_VERSION,
        trace_ids=trace_ids or None,
        limit=limit,
        dry_run=dry_run,
        confirm_data_storage=not dry_run,
    )


def _validated_message_history_boundaries(
    evidence: Any,
) -> list[ImportedReplayBoundary]:
    """Return complete source messages that the adapter can reconstruct."""
    candidates: list[ImportedReplayBoundary] = []
    for observation in evidence.replay_bundle.observations:
        groups: dict[int, list[Any]] = {}
        for part in observation.parts:
            groups.setdefault(part.message_index, []).append(part)
        for parts in groups.values():
            kinds = {part.kind for part in parts}
            if kinds <= {ReplayPartKind.MODEL_TEXT, ReplayPartKind.TOOL_CALL}:
                boundary_kind = ImportedReplayBoundaryKind.MODEL_MESSAGE
            elif kinds == {ReplayPartKind.TOOL_RESULT}:
                boundary_kind = ImportedReplayBoundaryKind.TOOL_RESULT
            else:
                continue
            final_part = parts[-1]
            boundary = ImportedReplayBoundary(
                kind=boundary_kind,
                observation_id=final_part.observation_id,
                sequence=final_part.sequence,
                occurrence=final_part.occurrence,
                call_id=(
                    final_part.call_id
                    if boundary_kind is ImportedReplayBoundaryKind.TOOL_RESULT
                    else None
                ),
            )
            try:
                prepare_imported_replay_history(
                    evidence,
                    boundary=boundary,
                    fallback_policy=ImportedReplayFallbackPolicy.BLOCK,
                )
            except ImportedReplayPreparationError:
                continue
            candidates.append(boundary)
    return candidates


def _message_history_boundary(
    execution_id: str,
    *,
    kind: Literal["model-message", "tool-result"],
    index: int,
) -> ImportedReplayBoundary:
    """Select a validated complete source-message boundary."""
    if index < 0:
        raise KitaruUsageError("Imported replay boundary index must be non-negative.")
    expected_kind = (
        ImportedReplayBoundaryKind.MODEL_MESSAGE
        if kind == "model-message"
        else ImportedReplayBoundaryKind.TOOL_RESULT
    )
    boundaries = [
        boundary
        for boundary in _validated_message_history_boundaries(
            load_imported_replay_evidence(execution_id)
        )
        if boundary.kind is expected_kind
    ]
    try:
        return boundaries[index]
    except IndexError as exc:
        raise KitaruUsageError(
            f"Imported execution {execution_id!r} has no {kind} boundary at "
            f"index {index}; found {len(boundaries)}."
        ) from exc


def _replay_cases(
    execution_ids: list[str],
    *,
    name: str,
    idempotency_key: str,
    repeats: int,
    candidate_variant: str,
    candidate_version: str,
    model: Any | None = None,
) -> Any:
    """Replay ordered imported roots as a scored, named candidate suite."""
    agent, objective = _registered_agent(
        variant=candidate_variant,
        version=candidate_version,
        model=model,
    )
    return agent.replay(
        execution_ids,
        imported_mode=ImportedReplayMode.ROOT_INPUT,
        on_error="collect",
        idempotency_key=idempotency_key,
        repeats=repeats,
        wait=True,
        name=name,
        suite_key=name,
        scorers=[objective],
        objective_minimum_mean=1.0,
    )


def _resume_case(
    execution_id: str,
    *,
    boundary_kind: Literal["model-message", "tool-result"],
    boundary_index: int,
    name: str,
    idempotency_key: str,
    candidate_variant: str,
    candidate_version: str,
    model: Any | None = None,
) -> Any:
    """Resume one imported case from an inspected complete history boundary."""
    boundary = _message_history_boundary(
        execution_id,
        kind=boundary_kind,
        index=boundary_index,
    )
    agent, objective = _registered_agent(
        variant=candidate_variant,
        version=candidate_version,
        model=model,
    )
    return agent.replay(
        execution_id,
        imported_mode=ImportedReplayMode.MESSAGE_HISTORY,
        imported_boundary=boundary,
        on_error="collect",
        idempotency_key=idempotency_key,
        repeats=1,
        wait=True,
        name=name,
        suite_key=name,
        scorers=[objective],
        objective_minimum_mean=1.0,
    )


def _rerun_suite(
    suite: str,
    *,
    idempotency_key: str,
    limits: RegressionLimits,
    candidate_variant: str,
    candidate_version: str,
    model: Any | None = None,
) -> Any:
    """Rerun a protected suite against the registered candidate and assert PASS."""
    agent, objective = _registered_agent(
        variant=candidate_variant,
        version=candidate_version,
        model=model,
    )
    result = agent.replay(
        experiment=suite,
        idempotency_key=idempotency_key,
        repeats=1,
        scorers=[objective],
        limits=limits,
    )
    result.assert_pass()
    return result


def _attempt_reference(attempt: Any) -> dict[str, Any]:
    """Return bounded attempt identity without serializing its full record."""
    verdict = attempt.verdict
    return {
        "experiment_id": attempt.experiment_id,
        "suite_key": getattr(attempt.spec, "suite_key", None),
        "verdict": None if verdict is None else verdict.verdict,
    }


def _inspect_execution(
    execution_id: str,
    *,
    client: KitaruClient | None = None,
    attempts: tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    """Return bounded durable import, lineage, score, and attempt evidence."""
    client = client or KitaruClient()
    execution = client.executions.get(execution_id)
    if attempts is None:
        experiment_id = execution.metadata.get(EXPERIMENT_ID_METADATA_KEY)
        attempts = (
            (
                client.agents.experiments.get_attempt(
                    experiment_id,
                    agent=AGENT_NAME,
                ),
            )
            if isinstance(experiment_id, str) and experiment_id
            else ()
        )
    imported_member = next(
        (
            member
            for attempt in attempts
            for member in attempt.record.imported_replay_members
            if member.child_execution_id == execution.exec_id
        ),
        None,
    )
    immediate_parent_id = execution.original_exec_id
    root_execution_id = execution.root_exec_id
    if imported_member is not None:
        immediate_parent_id = imported_member.parent_execution_id
        root_execution_id = imported_member.root_execution_id
    scores = execution.scores.list()
    payload: dict[str, Any] = {
        "execution_id": execution.exec_id,
        "status": execution.status,
        "project_id": execution.project_id,
        "immediate_parent_id": immediate_parent_id,
        "root_execution_id": root_execution_id,
        "import": execution.import_info,
        "cost": execution.llm_usage_summary,
        "scores": scores[:_INSPECTION_MAX_SCORES],
        "scores_truncated": len(scores) > _INSPECTION_MAX_SCORES,
        "experiments": [_attempt_reference(attempt) for attempt in attempts],
    }
    if execution.import_info is not None:
        evidence = load_imported_replay_evidence(execution_id)
        payload["replay_readiness"] = evidence.readiness
        payload["available_boundaries"] = [
            {
                "kind": boundary.kind,
                "observation_id": boundary.observation_id,
                "sequence": boundary.sequence,
                "occurrence": boundary.occurrence,
                "call_id": boundary.call_id,
            }
            for boundary in _validated_message_history_boundaries(evidence)
        ]
    return payload


def _inspect_experiment(
    experiment_id_or_suite: str,
    *,
    page: int = 1,
    page_size: int = _INSPECTION_DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Return one bounded member page and serialize the full attempt once."""
    if isinstance(page, bool) or page < 1:
        raise KitaruUsageError("Inspection page must be >= 1.")
    if (
        isinstance(page_size, bool)
        or page_size < 1
        or page_size > _INSPECTION_MAX_PAGE_SIZE
    ):
        raise KitaruUsageError(
            f"Inspection page size must be between 1 and {_INSPECTION_MAX_PAGE_SIZE}."
        )
    client = KitaruClient()
    attempt = client.agents.experiments.resolve_source(
        experiment_id_or_suite,
        agent=AGENT_NAME,
    )
    member_page = attempt.runs.list(page=page, size=page_size)
    members = list(member_page.items)
    attempt_payload = attempt.to_json()
    attempt_payload["score_aggregate_data"] = attempt.score_aggregate
    total_pages = member_page.total_pages
    return {
        "attempt": attempt_payload,
        "member_page": {
            "page": page,
            "page_size": page_size,
            "returned": len(members),
            "total": getattr(member_page, "total", None),
            "total_pages": total_pages,
            "has_more": page < total_pages,
        },
        "members": [
            _inspect_execution(
                str(member.id),
                client=client,
                attempts=(attempt,),
            )
            for member in members
        ],
    }


@click.group()
def cli() -> None:
    """Imported Langfuse-to-regression replay example."""


@cli.command("register")
@click.option(
    "--role",
    type=click.Choice(["source", "candidate"]),
    default="candidate",
    show_default=True,
)
@click.option("--variant")
@click.option("--version")
def register_cmd(role: str, variant: str | None, version: str | None) -> None:
    """Register an explicit source or candidate AgentVersion."""
    if role == "source":
        if variant not in {None, SOURCE_VARIANT}:
            raise click.UsageError(
                f"The source fixture is immutable: --variant must be "
                f"{SOURCE_VARIANT!r}."
            )
        if version not in {None, SOURCE_VERSION}:
            raise click.UsageError(
                f"The source fixture is immutable: --version must be "
                f"{SOURCE_VERSION!r}."
            )
        selected_variant = SOURCE_VARIANT
        selected_version = SOURCE_VERSION
    else:
        selected_variant = variant or DEFAULT_CANDIDATE_VARIANT
        selected_version = version or DEFAULT_CANDIDATE_VERSION
    _agent, _objective = _registered_agent(
        variant=selected_variant,
        version=selected_version,
    )
    click.echo(
        _json(
            {
                "agent": AGENT_NAME,
                "role": role,
                "variant": selected_variant,
                "version": selected_version,
            }
        )
    )


@cli.command("import-traces")
@click.argument("source")
@click.option("--source-project-id")
@click.option("--trace-id", "trace_ids", multiple=True)
@click.option("--limit", type=click.IntRange(min=1))
@click.option(
    "--commit",
    is_flag=True,
    help="Persist imported observations. The default is a read-only dry run.",
)
def import_traces_cmd(
    source: str,
    source_project_id: str | None,
    trace_ids: tuple[str, ...],
    limit: int | None,
    commit: bool,
) -> None:
    """Plan or import a JSONL export or langfuse://trace/<id> URI."""
    click.echo(
        _json(
            _import_traces(
                source,
                source_project_id=source_project_id,
                trace_ids=list(trace_ids),
                limit=limit,
                dry_run=not commit,
            )
        )
    )


@cli.command("replay")
@click.argument("exec_id")
@click.option("--name")
@click.option("--idempotency-key", required=True)
@click.option("--repeats", type=click.IntRange(min=1), default=1, show_default=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
def replay_cmd(
    exec_id: str,
    name: str | None,
    idempotency_key: str,
    repeats: int,
    candidate_variant: str,
    candidate_version: str,
) -> None:
    """Replay one imported root as a scored candidate experiment."""
    click.echo(
        _json(
            _replay_cases(
                [exec_id],
                name=name or f"case-{exec_id}",
                idempotency_key=idempotency_key,
                repeats=repeats,
                candidate_variant=candidate_variant,
                candidate_version=candidate_version,
            )
        )
    )


@cli.command("resume")
@click.argument("exec_id")
@click.option(
    "--boundary-kind",
    type=click.Choice(["model-message", "tool-result"]),
    default=DEFAULT_BOUNDARY_KIND,
    show_default=True,
)
@click.option(
    "--boundary-index",
    type=click.IntRange(min=0),
    default=DEFAULT_BOUNDARY_INDEX,
    show_default=True,
)
@click.option("--name")
@click.option("--idempotency-key", required=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
def resume_cmd(
    exec_id: str,
    boundary_kind: Literal["model-message", "tool-result"],
    boundary_index: int,
    name: str | None,
    idempotency_key: str,
    candidate_variant: str,
    candidate_version: str,
) -> None:
    """Resume one imported case from a complete persisted history boundary."""
    click.echo(
        _json(
            _resume_case(
                exec_id,
                boundary_kind=boundary_kind,
                boundary_index=boundary_index,
                name=name or f"resume-{exec_id}",
                idempotency_key=idempotency_key,
                candidate_variant=candidate_variant,
                candidate_version=candidate_version,
            )
        )
    )


@cli.command("experiment")
@click.argument("exec_ids", nargs=-1, required=True)
@click.option("--name", default=DEFAULT_EXPERIMENT, show_default=True)
@click.option("--idempotency-key", required=True)
@click.option("--repeats", type=click.IntRange(min=1), default=3, show_default=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
def experiment_cmd(
    exec_ids: tuple[str, ...],
    name: str,
    idempotency_key: str,
    repeats: int,
    candidate_variant: str,
    candidate_version: str,
) -> None:
    """Replay an explicit ordered imported set as one named suite."""
    click.echo(
        _json(
            _replay_cases(
                list(exec_ids),
                name=name,
                idempotency_key=idempotency_key,
                repeats=repeats,
                candidate_variant=candidate_variant,
                candidate_version=candidate_version,
            )
        )
    )


@cli.command("rerun")
@click.argument("suite")
@click.option("--idempotency-key", required=True)
@click.option("--max-trials", type=int, default=3, show_default=True)
@click.option("--max-cost-usd", type=float, default=1.0, show_default=True)
@click.option("--max-incurred-tokens", type=int, default=100_000, show_default=True)
@click.option("--max-duration-seconds", type=float, default=300.0, show_default=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
def rerun_cmd(
    suite: str,
    idempotency_key: str,
    max_trials: int,
    max_cost_usd: float,
    max_incurred_tokens: int,
    max_duration_seconds: float,
    candidate_variant: str,
    candidate_version: str,
) -> None:
    """Rerun a named protected suite as a bounded regression gate."""
    click.echo(
        _json(
            _rerun_suite(
                suite,
                idempotency_key=idempotency_key,
                limits=RegressionLimits(
                    max_trials=max_trials,
                    max_cost_usd=max_cost_usd,
                    max_incurred_tokens=max_incurred_tokens,
                    max_duration_seconds=max_duration_seconds,
                ),
                candidate_variant=candidate_variant,
                candidate_version=candidate_version,
            )
        )
    )


@cli.command("inspect-execution")
@click.argument("exec_id")
def inspect_execution_cmd(exec_id: str) -> None:
    """Inspect import attribution, readiness, lineage, scores, and cost."""
    click.echo(_json(_inspect_execution(exec_id)))


@cli.command("inspect-experiment")
@click.argument("experiment_id_or_suite")
@click.option("--page", type=click.IntRange(min=1), default=1, show_default=True)
@click.option(
    "--page-size",
    type=click.IntRange(min=1, max=_INSPECTION_MAX_PAGE_SIZE),
    default=_INSPECTION_DEFAULT_PAGE_SIZE,
    show_default=True,
)
def inspect_experiment_cmd(
    experiment_id_or_suite: str,
    page: int,
    page_size: int,
) -> None:
    """Inspect one bounded attempt-member page, scores, limits, and verdict."""
    click.echo(
        _json(
            _inspect_experiment(
                experiment_id_or_suite,
                page=page,
                page_size=page_size,
            )
        )
    )


if __name__ == "__main__":
    cli()
