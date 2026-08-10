#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""CLI boundary for portable experiment exports."""

from pathlib import Path
from typing import Any, Literal

from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import get_agent_version, resolve_asset
from kitaru.cli.session_selection import get_cohort_version
from kitaru.exports.config import ExportRequest, TraceFormat
from kitaru.exports.models import ExportError, get_export_error_kind
from kitaru.exports.operation import export_experiment


def _map_export_error(error: ExportError) -> CLIError:
    return CLIError(
        get_export_error_kind(error),
        error.message,
        details={"export_code": error.code},
    )


async def export_experiment_command(
    client: Any,
    experiment: str,
    *,
    cohort_version: str,
    agent: str,
    format: Literal["harbor", "verifiers-v1"],
    source_root: Path,
    destination: Path,
    primary_reward: str,
    required_env: list[str] | None,
    trace_format: TraceFormat | None,
    trace_path: str | None,
    archive: bool,
    dry_run: bool,
) -> CommandResult:
    """Resolve friendly CLI references and run one shared export operation."""
    resolved_experiment = await resolve_asset(
        client.experiments, experiment, "Experiment"
    )
    _, resolved_cohort_version = await get_cohort_version(client, cohort_version)
    _, resolved_agent_version = await get_agent_version(client, agent)
    try:
        receipt = await export_experiment(
            client,
            ExportRequest(
                experiment_id=resolved_experiment.id,
                cohort_version_id=resolved_cohort_version.id,
                agent_version_id=resolved_agent_version.id,
                format=format,
                source_root=source_root,
                destination=destination,
                primary_reward=primary_reward,
                required_environment_names=tuple(required_env or ()),
                trace_format=trace_format,
                trace_path=trace_path,
                archive=archive,
                dry_run=dry_run,
            ),
        )
    except ExportError as error:
        raise _map_export_error(error) from error
    item = receipt.model_dump(mode="json")
    if dry_run:
        next_actions = ["Run the same command without --dry-run to write the bundle."]
    elif format == "harbor":
        next_actions = [f"Follow the Harbor commands in {destination / 'README.md'}."]
    else:
        next_actions = [
            f"Follow the Verifiers commands in {destination / 'README.md'}."
        ]
    return CommandResult(item=item, next_actions=next_actions)
