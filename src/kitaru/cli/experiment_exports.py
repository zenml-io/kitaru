#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""CLI boundary for portable experiment exports."""

from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import get_agent_version, resolve_asset
from kitaru.cli.session_selection import get_cohort_version
from kitaru.exports.config import ExportRequest, TraceFormat
from kitaru.exports.models import (
    ContentCategory,
    ContentPolicy,
    EnvironmentPolicy,
    ExportError,
    SourcePolicy,
    get_export_error_kind,
)
from kitaru.exports.operation import ExportReceipt, export_experiment
from kitaru.exports.plugin import resolve_exporter


def _map_export_error(error: ExportError) -> CLIError:
    hint = {
        "destination_conflict": "Choose a new destination path.",
        "archive_conflict": "Choose a destination whose ZIP path does not exist.",
        "missing_source_include": "Check --include-source against the source root.",
        "exporter_not_installed": (
            "Install the named package in the Python environment running this "
            "command, then retry."
        ),
        "exporter_ambiguous": "Remove the duplicate exporter package, then retry.",
        "exporter_incompatible": (
            "Upgrade Kitaru and the named exporter package together, then retry."
        ),
        "exporter_load_failed": (
            "Reinstall or upgrade the named exporter package, then retry."
        ),
    }.get(error.code)
    return CLIError(
        get_export_error_kind(error),
        error.message,
        details={"export_code": error.code},
        hint=hint,
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
    omit_content: list[ContentCategory] | None,
    environment_mode: Literal["include", "runtime_only"],
    include_source: list[str] | None,
    exclude_source: list[str] | None,
    trace_format: TraceFormat | None,
    trace_path: str | None,
    archive: bool,
    dry_run: bool,
) -> CommandResult:
    """Resolve friendly CLI references and run one shared export operation."""
    try:
        exporter = resolve_exporter(format)
    except ExportError as error:
        raise _map_export_error(error) from error
    resolved_experiment = await resolve_asset(
        client.experiments, experiment, "Experiment"
    )
    _, resolved_cohort_version = await get_cohort_version(client, cohort_version)
    _, resolved_agent_version = await get_agent_version(client, agent)
    try:
        request = ExportRequest(
            experiment_id=resolved_experiment.id,
            cohort_version_id=resolved_cohort_version.id,
            agent_version_id=resolved_agent_version.id,
            format=format,
            source_root=source_root,
            destination=destination,
            primary_reward=primary_reward,
            required_environment_names=tuple(required_env or ()),
            content_policy=ContentPolicy(omit=tuple(omit_content or ())),
            environment_policy=EnvironmentPolicy(mode=environment_mode),
            source_policy=SourcePolicy(
                include=tuple(include_source or ()),
                exclude=tuple(exclude_source or ()),
            ),
            trace_format=trace_format,
            trace_path=trace_path,
            archive=archive,
            dry_run=dry_run,
        )
    except ValidationError as error:
        issue = error.errors(include_input=False, include_url=False)[0]
        location = ".".join(str(part) for part in issue["loc"])
        message = str(issue["msg"]).removeprefix("Value error, ")
        prefix = f"Invalid {location}: " if location else "Invalid export request: "
        raise CLIError("invalid_arguments", f"{prefix}{message}.") from error

    try:
        receipt = await export_experiment(
            client,
            request,
            exporter=exporter,
        )
    except ExportError as error:
        raise _map_export_error(error) from error
    try:
        validated_receipt = ExportReceipt.model_validate(receipt)
    except ValidationError as error:
        raise CLIError(
            "internal_error", "The export produced an invalid result."
        ) from error
    item = validated_receipt.model_dump(mode="json")
    published_destination = Path(validated_receipt.destination)
    if dry_run:
        next_actions = ["Run the same command without --dry-run to write the bundle."]
    elif format == "harbor":
        next_actions = [
            f"Follow the Harbor commands in {published_destination / 'README.md'}."
        ]
    else:
        next_actions = [
            f"Follow the Verifiers commands in {published_destination / 'README.md'}."
        ]
    return CommandResult(
        item=item,
        warnings=[warning.message for warning in validated_receipt.warnings],
        next_actions=next_actions,
    )
