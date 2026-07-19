"""Canonical Agent CLI commands."""

from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import KITARU_PROJECT_ENV, AgentInfo
from kitaru.experiments import (
    ExperimentRecord,
    ExperimentSpec,
    FrozenImportedReplayPlan,
)
from kitaru.inspection import serialize_agent, serialize_experiment

from . import agents_app
from ._dependencies import cli_dependencies
from ._helpers import (
    DEFAULT_LIST_PAGE,
    DEFAULT_LIST_SIZE,
    OutputFormatOption,
    PaginationPageOption,
    PaginationSizeOption,
    _emit_json_item,
    _emit_json_items,
    _emit_pagination_note,
    _emit_snapshot,
    _exit_with_error,
    _print_success,
    _print_warning,
    _resolve_output_format,
    _validate_pagination,
)


def _agent_list_rows(agents: list[AgentInfo]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru agents list`."""
    if not agents:
        return [("Agents", "none found")]

    return [
        (
            agent.name,
            (
                f"{agent.agent_id}"
                f"{' (active)' if agent.is_active else ''}"
                f" · {agent.version_count} version"
                f"{'' if agent.version_count == 1 else 's'}"
            ),
        )
        for agent in agents
    ]


def _current_agent_rows(agent: AgentInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru agents current`."""
    return [
        ("Agent", agent.name),
        ("Agent ID", agent.agent_id),
        ("Versions", str(agent.version_count)),
        ("Default version", agent.default_agent_version_id or "not registered"),
    ]


def _experiment_rows(record: ExperimentRecord) -> list[tuple[str, str]]:
    """Build truthful read-only experiment rows."""
    spec = record.spec
    evidence = record.imported_replay_evidence
    verdict = record.verdict
    replay_spec = spec if isinstance(spec, ExperimentSpec) else None
    imported_plan = next(
        (
            row.replay_plan
            for row in ([] if replay_spec is None else replay_spec.planning_rows)
            if isinstance(row.replay_plan, FrozenImportedReplayPlan)
        ),
        None,
    )
    mode = "native" if imported_plan is None else imported_plan.mode.value
    if imported_plan is not None:
        boundary = imported_plan.boundary.kind.value
    elif replay_spec is not None:
        boundary = replay_spec.at
    else:
        boundary = "not applicable"
    rows = [
        ("Experiment ID", spec.experiment_id),
        ("Suite", spec.suite_key),
        ("Status", record.status),
        (
            "Trials",
            f"{record.counts.verified}/{record.counts.intended} verified"
            + (
                ""
                if not (
                    record.counts.failed
                    or record.counts.skipped
                    or record.counts.unverified
                )
                else (
                    f" · {record.counts.failed} failed, "
                    f"{record.counts.skipped} skipped, "
                    f"{record.counts.unverified} unverified"
                )
            ),
        ),
        ("Replay mode", mode),
        ("Replay boundary", boundary),
        (
            "Comparability",
            "not applicable" if evidence is None else evidence.comparability.value,
        ),
        (
            "Recorded responses",
            "not applicable"
            if evidence is None
            else (
                f"{evidence.recorded_response_hits}/"
                f"{evidence.eligible_recorded_responses} hits, "
                f"{evidence.recorded_response_misses} misses"
            ),
        ),
        (
            "Blocked calls",
            "not applicable" if evidence is None else str(evidence.blocked_calls),
        ),
        (
            "Path divergences",
            "not applicable" if evidence is None else str(evidence.path_divergences),
        ),
        ("Verdict", "not graded" if verdict is None else verdict.verdict.value),
    ]
    if verdict is not None:
        if verdict.objective is not None:
            objective = verdict.objective
            rows.append(
                (
                    "Objective",
                    f"{objective.scorer.name}: {objective.mean} "
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
    members = record.imported_replay_members
    if members:
        displayed_ids = [member.child_execution_id for member in members[:5]]
        suffix = (
            ""
            if len(members) <= len(displayed_ids)
            else f" · +{len(members) - len(displayed_ids)} more"
        )
        rows.append(("Child executions", ", ".join(displayed_ids) + suffix))
    if record.operational_limit is not None:
        limit = record.operational_limit
        facts = limit.facts
        rows.append(
            (
                "Usage",
                f"${facts.incurred_cost_usd:.4f}, {facts.incurred_tokens} tokens, "
                f"{facts.duration_seconds:.1f}s",
            )
        )
        if limit.stopped or not limit.verified:
            rows.append(
                (
                    "Limit",
                    (
                        limit.reason_code.value
                        if limit.reason_code is not None
                        else "usage could not be verified"
                    ),
                )
            )
    return rows


def _agent_show_rows(agent: AgentInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru agents show`."""
    return [
        ("Name", agent.name),
        ("ID", agent.agent_id),
        ("Display name", agent.display_name or "not set"),
        ("Description", agent.description or "not set"),
        ("Active", "yes" if agent.is_active else "no"),
        ("Versions", str(agent.version_count)),
        ("Default version", agent.default_agent_version_id or "not registered"),
    ]


@agents_app.command
def list_(
    *,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List initialized Kitaru Agents visible to the current user."""
    command = "agents.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )
    agents = run_with_cli_error_boundary(
        lambda: cli_dependencies().list_agents(page=page, size=size),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_agent(agent) for agent in agents],
            output=output_format,
        )
        return

    if page > DEFAULT_LIST_PAGE and not agents:
        rows: list[tuple[str, str]] = [("Agents", f"no items on page {page}")]
    else:
        rows = _agent_list_rows(agents)
    _emit_snapshot("Kitaru Agents", rows)
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(agents),
        output=output_format,
    )


@agents_app.command
def current(output: OutputFormatOption = "text") -> None:
    """Show the active initialized Kitaru Agent."""
    command = "agents.current"
    output_format = _resolve_output_format(output)
    agent = run_with_cli_error_boundary(
        cli_dependencies().current_agent,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_agent(agent), output=output_format)
        return

    _emit_snapshot("Kitaru Agent", _current_agent_rows(agent))


@agents_app.command
def show(
    name_or_id: Annotated[
        str,
        Parameter(help="Agent name or ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Show an initialized Kitaru Agent by name or ID."""
    command = "agents.show"
    output_format = _resolve_output_format(output)
    agent = run_with_cli_error_boundary(
        lambda: cli_dependencies().get_agent(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_agent(agent), output=output_format)
        return

    _emit_snapshot("Kitaru Agent", _agent_show_rows(agent))


@agents_app.command
def experiments(
    name_or_id: Annotated[
        str,
        Parameter(help="Agent name or ID."),
    ],
    experiment: Annotated[
        str | None,
        Parameter(help="Optional exact experiment, suite, or display name."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Inspect durable experiment attempts without mutating them."""
    command = "agents.experiments"
    output_format = _resolve_output_format(output)
    agent = run_with_cli_error_boundary(
        lambda: cli_dependencies().get_agent(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    records = (
        [agent.get_experiment(experiment)]
        if experiment is not None
        else agent.list_experiments()
    )
    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_experiment(record) for record in records],
            output=output_format,
        )
        return
    if not records:
        _emit_snapshot("Kitaru Experiments", [("Experiments", "none found")])
        return
    for index, record in enumerate(records):
        if index:
            print()
        _emit_snapshot("Kitaru Experiment", _experiment_rows(record))


@agents_app.command
def create(
    name: Annotated[
        str,
        Parameter(help="Agent name."),
    ],
    *,
    no_activate: Annotated[
        bool | None,
        Parameter(help="Create without activating the Agent."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Create a Kitaru Agent on Pro/Cloud, activating it by default."""
    command = "agents.create"
    output_format = _resolve_output_format(output)
    result = run_with_cli_error_boundary(
        lambda: cli_dependencies().create_agent(
            name,
            activate=not no_activate,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_agent(result.agent)
        payload["previous_active_agent"] = result.previous_active_agent
        payload["activated"] = result.activated
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(f"Created Agent: {result.agent.name}")
    if result.activated and result.agent.is_active:
        if result.previous_active_agent is not None:
            print(
                f"Activated Agent: {result.previous_active_agent} → {result.agent.name}"
            )
        else:
            print(f"Activated Agent: {result.agent.name}")
    elif result.activated:
        _print_warning(
            "Agent activation is still overridden by the environment.",
            f"Unset or update {KITARU_PROJECT_ENV} to use {result.agent.name}.",
        )


@agents_app.command
def use(
    name_or_id: Annotated[
        str,
        Parameter(help="Agent name or ID to activate."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Use a Kitaru Agent on Pro/Cloud as the active default."""
    command = "agents.use"
    output_format = _resolve_output_format(output)
    agent = run_with_cli_error_boundary(
        lambda: cli_dependencies().use_agent(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_agent(agent), output=output_format)
        return

    _print_success(
        f"Activated Agent: {agent.name}",
        detail=f"Agent ID: {agent.agent_id}",
    )


@agents_app.command
def delete(
    name_or_id: Annotated[
        str,
        Parameter(help="Agent name or ID to delete."),
    ],
    *,
    yes: Annotated[
        bool,
        Parameter(help="Confirm Agent deletion."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Delete a Kitaru Agent on Pro/Cloud by name or ID."""
    command = "agents.delete"
    output_format = _resolve_output_format(output)
    if not yes:
        _exit_with_error(
            command,
            f"Kitaru will not delete Agent '{name_or_id}' without explicit "
            "confirmation. Re-run with --yes if you want to delete it.",
            output=output_format,
        )

    result = run_with_cli_error_boundary(
        lambda: cli_dependencies().delete_agent(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_agent(result.deleted_agent),
            output=output_format,
        )
        return

    _print_success(f"Deleted Agent: {result.deleted_agent.name}")
