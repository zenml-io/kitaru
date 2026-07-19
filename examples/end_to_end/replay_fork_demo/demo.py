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
_OUTPUT_TYPE = click.Choice(["text", "json"])
_output_option = click.option(
    "--output", "output", type=_OUTPUT_TYPE, default="text", show_default=True
)


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


def _value(value: Any) -> str:
    """Return a readable scalar value for tutorial output."""
    if value is None:
        return "not available"
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _echo_rows(title: str, rows: list[tuple[str, Any]]) -> None:
    """Print a compact vertical summary without truncating values."""
    click.echo(title)
    if not rows:
        return
    width = max(len(label) for label, _ in rows)
    for label, value in rows:
        click.echo(f"  {label:<{width}}  {_value(value)}")


def _emit_registration(*, role: str, variant: str, version: str, output: str) -> None:
    payload = {
        "agent": AGENT_NAME,
        "role": role,
        "variant": variant,
        "version": version,
    }
    if output == "json":
        click.echo(_json(payload))
        return
    _echo_rows(
        "Registered AgentVersion",
        [
            ("Agent", AGENT_NAME),
            ("Role", role),
            ("Variant", variant),
            ("Version", version),
        ],
    )


def _emit_import_result(result: Any, *, output: str) -> None:
    if output == "json":
        click.echo(_json(result))
        return
    action = "Previewed" if result.dry_run else "Imported"
    _echo_rows(
        f"{action} {result.selected_trace_count} trace(s)",
        [
            ("Agent", f"{result.agent_name} @ {result.requested_version}"),
            ("Project", result.source_project_id),
            ("Storage", result.artifact_store_type),
        ],
    )
    for outcome in result.outcomes:
        rows = [
            ("Status", outcome.status),
            ("Integrity", outcome.integrity),
            ("Observations", outcome.observation_count),
        ]
        if outcome.execution_id is not None:
            rows.append(("Execution ID", outcome.execution_id))
        if outcome.existing_execution_id is not None:
            rows.append(("Existing execution", outcome.existing_execution_id))
        if outcome.reason is not None:
            rows.append(("Problem", outcome.reason))
        if outcome.resolution is not None:
            rows.append(("Next action", outcome.resolution))
        click.echo()
        _echo_rows(f"Trace: {outcome.trace_id}", rows)


def _imported_replay_evidence_rows(evidence: Any) -> list[tuple[str, Any]]:
    """Return the shared compact imported-replay evidence rows."""

    def field(name: str, default: Any = None) -> Any:
        if isinstance(evidence, Mapping):
            return evidence.get(name, default)
        return getattr(evidence, name, default)

    return [
        ("Comparability", _value(field("comparability"))),
        (
            "Recorded replies",
            f"{field('recorded_response_hits', 0)}/"
            f"{field('eligible_recorded_responses', 0)} served, "
            f"{field('recorded_response_misses', 0)} missed",
        ),
        ("Blocked calls", field("blocked_calls", 0)),
        ("Path divergences", field("path_divergences", 0)),
    ]


def _replay_description(record: Any) -> str:
    descriptions: list[str] = []
    for row in record.spec.planning_rows:
        if row.disposition != "imported":
            continue
        plan = row.replay_plan
        if plan is None:
            continue
        description = f"{plan.mode.value} from {plan.boundary.kind.value}"
        if description not in descriptions:
            descriptions.append(description)
    return ", ".join(descriptions) or record.spec.at


def _experiment_rows(result: Any, *, candidate_version: str) -> list[tuple[str, Any]]:
    record = result.record
    spec = record.spec
    counts = record.counts
    rows: list[tuple[str, Any]] = [
        ("Attempt", spec.experiment_id),
        ("Candidate", f"{AGENT_NAME} @ {candidate_version}"),
        ("Status", record.status),
        ("Trials", f"{counts.verified}/{counts.intended} verified"),
        ("Replay", _replay_description(record)),
    ]
    evidence = record.imported_replay_evidence
    if evidence is not None:
        rows.extend(_imported_replay_evidence_rows(evidence))
    members = record.imported_replay_members
    if members:
        displayed_ids = [member.child_execution_id for member in members[:5]]
        suffix = (
            ""
            if len(members) <= len(displayed_ids)
            else f" · +{len(members) - len(displayed_ids)} more"
        )
        rows.append(("Child executions", ", ".join(displayed_ids) + suffix))
    verdict = record.verdict
    if verdict is not None:
        if verdict.objective is not None:
            objective = verdict.objective
            rows.append(
                (
                    "Objective",
                    f"{objective.scorer.name}: {_value(objective.mean)} "
                    f"(required {objective.minimum_mean}) · "
                    f"{'passed' if objective.passed else 'not passed'}",
                )
            )
        failed_protections = [
            fact.protection_id
            for fact in verdict.protections
            if fact.passed is not True
        ]
        rows.append(
            (
                "Protections",
                (
                    "all passed"
                    if not failed_protections
                    else "not passed: " + ", ".join(failed_protections)
                ),
            )
        )
        rows.append(("Why", verdict.message))
        if verdict.reason_codes:
            rows.append(
                ("Reason codes", ", ".join(code.value for code in verdict.reason_codes))
            )
    limit = record.operational_limit
    if limit is not None:
        facts = limit.facts
        rows.append(
            (
                "Usage",
                f"${facts.incurred_cost_usd:.4f}, {facts.incurred_tokens} tokens, "
                f"{facts.duration_seconds:.1f}s",
            )
        )
        if limit.stopped or not limit.verified:
            rows.append(("Limit", limit.reason_code or "usage could not be verified"))
    if result.submission.compare_url:
        rows.append(("Compare", result.submission.compare_url))
    rows.append(
        (
            "Inspect",
            f"kitaru agents experiments {AGENT_NAME} {spec.suite_key}",
        )
    )
    return rows


def _emit_experiment_result(
    result: Any, *, candidate_version: str, output: str
) -> None:
    if output == "json":
        click.echo(_json(result))
        return
    verdict = result.record.verdict
    verdict_label = "NOT GRADED" if verdict is None else verdict.verdict.value.upper()
    _echo_rows(
        f"{verdict_label}  {result.record.spec.suite_key}",
        _experiment_rows(result, candidate_version=candidate_version),
    )


def _emit_execution_inspection(payload: dict[str, Any], *, output: str) -> None:
    if output == "json":
        click.echo(_json(payload))
        return
    rows: list[tuple[str, Any]] = [
        ("Execution", payload["execution_id"]),
        ("Status", payload["status"]),
        ("Parent", payload["immediate_parent_id"]),
        ("Root", payload["root_execution_id"]),
    ]
    import_info = _json_value(payload.get("import"))
    if isinstance(import_info, Mapping):
        attribution = import_info.get("attribution")
        if isinstance(attribution, Mapping):
            attribution = attribution.get("status")
        rows.extend(
            [
                ("Imported from", import_info.get("provider")),
                ("Source trace", import_info.get("source_trace_id")),
                ("Attribution", attribution),
            ]
        )
    cost = _json_value(payload.get("cost"))
    if isinstance(cost, Mapping) and cost:
        display_cost = cost.get("display_cost_usd")
        if isinstance(display_cost, (int, float)) and not isinstance(
            display_cost, bool
        ):
            rows.append(("Cost", f"${display_cost:.6f}"))
        rows.append(("Tokens", cost.get("total_tokens")))
    if payload.get("scores_omitted"):
        rows.append(("Scores", "use --output json for score records"))
    else:
        rows.append(("Scores", len(payload["scores"])))
    if payload["scores_truncated"]:
        rows.append(("Score note", f"showing first {_INSPECTION_MAX_SCORES}"))
    _echo_rows("Kitaru execution evidence", rows)
    readiness = _json_value(payload.get("replay_readiness"))
    if isinstance(readiness, Mapping):
        readiness_rows: list[tuple[str, Any]] = []
        for capability, details in readiness.items():
            if not isinstance(details, Mapping):
                continue
            readiness_rows.append(
                (capability.replace("_", " ").capitalize(), details.get("status"))
            )
        if readiness_rows:
            click.echo()
            _echo_rows("Replay readiness", readiness_rows)
    boundaries = payload.get("available_boundaries", [])
    if boundaries:
        click.echo()
        _echo_rows(
            "Replay boundaries",
            [
                (
                    f"[{index}] {boundary['kind']}",
                    f"observation {boundary['observation_id']} · sequence "
                    f"{boundary['sequence']} · occurrence {boundary['occurrence']}"
                    + (
                        ""
                        if boundary["call_id"] is None
                        else f" · call {boundary['call_id']}"
                    ),
                )
                for index, boundary in enumerate(boundaries)
            ],
        )
        boundary_count = payload.get("available_boundary_count")
        if isinstance(boundary_count, int) and boundary_count > len(boundaries):
            click.echo(
                f"  Showing {len(boundaries)} of {boundary_count} boundaries; "
                "use --output json for all."
            )


def _emit_experiment_inspection(payload: dict[str, Any], *, output: str) -> None:
    if output == "json":
        click.echo(_json(payload))
        return
    attempt = payload["attempt"]
    verdict = attempt.get("verdict")
    verdict_label = "NOT GRADED" if verdict is None else verdict["verdict"].upper()
    counts = attempt.get("counts", {})
    evidence = attempt.get("imported_replay_evidence")
    rows: list[tuple[str, Any]] = [
        ("Attempt", attempt.get("experiment_id")),
        ("Suite", attempt.get("suite_key")),
        ("Status", attempt.get("status")),
        (
            "Trials",
            f"{counts.get('verified', 0)}/{counts.get('intended', 0)} verified",
        ),
    ]
    if evidence is not None:
        rows.extend(_imported_replay_evidence_rows(evidence))
    if verdict is not None:
        rows.append(("Why", verdict["message"]))
    page = payload["member_page"]
    rows.append(
        (
            "Members",
            f"{page['returned']} shown · {page['total']} total · page {page['page']}/"
            f"{page['total_pages']}",
        )
    )
    _echo_rows(f"{verdict_label}  {attempt.get('suite_key')}", rows)
    exceptional = [
        member for member in payload["members"] if member.get("status") != "completed"
    ]
    if exceptional:
        click.echo()
        _echo_rows(
            "Members needing attention",
            [(member["execution_id"], member.get("status")) for member in exceptional],
        )


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
    assert_pass: bool = True,
) -> Any:
    """Rerun a protected suite against the registered candidate."""
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
    if assert_pass:
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
    include_scores: bool = True,
    boundary_limit: int | None = None,
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
    scores = execution.scores.list() if include_scores else []
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
    if not include_scores:
        payload["scores_omitted"] = True
    if execution.import_info is not None:
        evidence = load_imported_replay_evidence(execution_id)
        payload["replay_readiness"] = evidence.readiness
        boundaries = _validated_message_history_boundaries(evidence)
        displayed_boundaries = (
            boundaries if boundary_limit is None else boundaries[:boundary_limit]
        )
        payload["available_boundaries"] = [
            {
                "kind": boundary.kind,
                "observation_id": boundary.observation_id,
                "sequence": boundary.sequence,
                "occurrence": boundary.occurrence,
                "call_id": boundary.call_id,
            }
            for boundary in displayed_boundaries
        ]
        if boundary_limit is not None:
            payload["available_boundary_count"] = len(boundaries)
    return payload


def _inspect_experiment(
    experiment_id_or_suite: str,
    *,
    page: int = 1,
    page_size: int = _INSPECTION_DEFAULT_PAGE_SIZE,
    detailed: bool = True,
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
    if detailed:
        attempt_payload = attempt.to_json()
        attempt_payload["score_aggregate_data"] = attempt.score_aggregate
    else:
        record = attempt.record
        attempt_payload = {
            "experiment_id": record.spec.experiment_id,
            "suite_key": record.spec.suite_key,
            "status": record.status,
            "counts": record.counts.model_dump(mode="json"),
            "imported_replay_evidence": (
                None
                if record.imported_replay_evidence is None
                else record.imported_replay_evidence.model_dump(mode="json")
            ),
            "verdict": (
                None
                if record.verdict is None
                else record.verdict.model_dump(mode="json")
            ),
        }
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
        "members": (
            [
                _inspect_execution(
                    str(member.id),
                    client=client,
                    attempts=(attempt,),
                )
                for member in members
            ]
            if detailed
            else []
        ),
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
@_output_option
def register_cmd(
    role: str, variant: str | None, version: str | None, output: str
) -> None:
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
    _emit_registration(
        role=role,
        variant=selected_variant,
        version=selected_version,
        output=output,
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
@_output_option
def import_traces_cmd(
    source: str,
    source_project_id: str | None,
    trace_ids: tuple[str, ...],
    limit: int | None,
    commit: bool,
    output: str,
) -> None:
    """Plan or import a JSONL export or langfuse://trace/<id> URI."""
    result = _import_traces(
        source,
        source_project_id=source_project_id,
        trace_ids=list(trace_ids),
        limit=limit,
        dry_run=not commit,
    )
    _emit_import_result(result, output=output)


@cli.command("replay")
@click.argument("exec_id")
@click.option("--name")
@click.option("--idempotency-key", required=True)
@click.option("--repeats", type=click.IntRange(min=1), default=1, show_default=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
@_output_option
def replay_cmd(
    exec_id: str,
    name: str | None,
    idempotency_key: str,
    repeats: int,
    candidate_variant: str,
    candidate_version: str,
    output: str,
) -> None:
    """Replay one imported root as a scored candidate experiment."""
    result = _replay_cases(
        [exec_id],
        name=name or f"case-{exec_id}",
        idempotency_key=idempotency_key,
        repeats=repeats,
        candidate_variant=candidate_variant,
        candidate_version=candidate_version,
    )
    _emit_experiment_result(
        result,
        candidate_version=candidate_version,
        output=output,
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
@_output_option
def resume_cmd(
    exec_id: str,
    boundary_kind: Literal["model-message", "tool-result"],
    boundary_index: int,
    name: str | None,
    idempotency_key: str,
    candidate_variant: str,
    candidate_version: str,
    output: str,
) -> None:
    """Resume one imported case from a complete persisted history boundary."""
    result = _resume_case(
        exec_id,
        boundary_kind=boundary_kind,
        boundary_index=boundary_index,
        name=name or f"resume-{exec_id}",
        idempotency_key=idempotency_key,
        candidate_variant=candidate_variant,
        candidate_version=candidate_version,
    )
    _emit_experiment_result(
        result,
        candidate_version=candidate_version,
        output=output,
    )


@cli.command("experiment")
@click.argument("exec_ids", nargs=-1, required=True)
@click.option("--name", default=DEFAULT_EXPERIMENT, show_default=True)
@click.option("--idempotency-key", required=True)
@click.option("--repeats", type=click.IntRange(min=1), default=3, show_default=True)
@click.option("--candidate-variant", default=DEFAULT_CANDIDATE_VARIANT)
@click.option("--candidate-version", default=DEFAULT_CANDIDATE_VERSION)
@_output_option
def experiment_cmd(
    exec_ids: tuple[str, ...],
    name: str,
    idempotency_key: str,
    repeats: int,
    candidate_variant: str,
    candidate_version: str,
    output: str,
) -> None:
    """Replay an explicit ordered imported set as one named suite."""
    result = _replay_cases(
        list(exec_ids),
        name=name,
        idempotency_key=idempotency_key,
        repeats=repeats,
        candidate_variant=candidate_variant,
        candidate_version=candidate_version,
    )
    _emit_experiment_result(
        result,
        candidate_version=candidate_version,
        output=output,
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
@_output_option
def rerun_cmd(
    suite: str,
    idempotency_key: str,
    max_trials: int,
    max_cost_usd: float,
    max_incurred_tokens: int,
    max_duration_seconds: float,
    candidate_variant: str,
    candidate_version: str,
    output: str,
) -> None:
    """Rerun a named protected suite as a bounded regression gate."""
    result = _rerun_suite(
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
        assert_pass=False,
    )
    _emit_experiment_result(
        result,
        candidate_version=candidate_version,
        output=output,
    )
    try:
        result.assert_pass()
    except AssertionError:
        raise click.exceptions.Exit(1) from None


@cli.command("inspect-execution")
@click.argument("exec_id")
@_output_option
def inspect_execution_cmd(exec_id: str, output: str) -> None:
    """Inspect import attribution, readiness, lineage, scores, and cost."""
    payload = _inspect_execution(
        exec_id,
        include_scores=output == "json",
        boundary_limit=None if output == "json" else 10,
    )
    _emit_execution_inspection(payload, output=output)


@cli.command("inspect-experiment")
@click.argument("experiment_id_or_suite")
@click.option("--page", type=click.IntRange(min=1), default=1, show_default=True)
@click.option(
    "--page-size",
    type=click.IntRange(min=1, max=_INSPECTION_MAX_PAGE_SIZE),
    default=_INSPECTION_DEFAULT_PAGE_SIZE,
    show_default=True,
)
@_output_option
def inspect_experiment_cmd(
    experiment_id_or_suite: str,
    page: int,
    page_size: int,
    output: str,
) -> None:
    """Inspect one bounded attempt-member page, scores, limits, and verdict."""
    payload = _inspect_experiment(
        experiment_id_or_suite,
        page=page,
        page_size=page_size,
        detailed=output == "json",
    )
    _emit_experiment_inspection(payload, output=output)


if __name__ == "__main__":
    cli()
