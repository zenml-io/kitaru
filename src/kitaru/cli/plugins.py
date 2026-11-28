#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Shared importer, evaluator, and analyzer CLI operations."""

import uuid
from pathlib import Path
from typing import Any, Literal

from kitaru.api_models.v1.analyzer import AnalyzerUpdateRequest
from kitaru.api_models.v1.evaluator import EvaluatorUpdateRequest
from kitaru.api_models.v1.importer import ImporterUpdateRequest
from kitaru.cli import invocation_context, registration
from kitaru.cli.command_registry import IDEMPOTENCY_KEY_PARAMETER
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.schema import ParameterSpec

PLUGIN_SOURCE_PARAMETERS = (
    ParameterSpec("--script", "path", "option", False, "Python script to upload."),
    ParameterSpec(
        "--package", "requirement", "option", False, "Pinned package requirement."
    ),
    ParameterSpec(
        "--entrypoint",
        "string",
        "option",
        True,
        "Script attribute or module reference.",
    ),
    ParameterSpec(
        "--display-version", "string", "option", False, "Human-readable version."
    ),
)


def build_plugin_register_parameters(kind: str) -> tuple[ParameterSpec, ...]:
    """Build metadata for one plugin register command."""
    parent = [
        ParameterSpec("NAME", "string", "argument", True, f"New {kind} name."),
        ParameterSpec(
            "--description",
            "string",
            "option",
            False,
            "Parent description. For --script, falls back to top-level "
            "DESCRIPTION or the module docstring.",
        ),
        ParameterSpec("--metadata", "JSON object", "option", False, "Parent metadata."),
    ]
    if kind in {"importer", "analyzer", "evaluator"}:
        parent.append(
            ParameterSpec("--provider", "string", "option", False, "Source provider.")
        )
    if kind in {"importer", "analyzer", "evaluator"}:
        parent.append(
            ParameterSpec(
                "--connection-schema",
                "path",
                "option",
                False,
                "JSON Schema file describing the connection values.",
            )
        )
    if kind == "evaluator":
        parent.append(
            ParameterSpec("--agent-id", "UUID", "option", False, "Scoping agent.")
        )
    return (*parent, *PLUGIN_SOURCE_PARAMETERS)


def build_plugin_update_parameters(kind: str) -> tuple[ParameterSpec, ...]:
    """Build metadata for one plugin update command."""
    return (
        ParameterSpec(
            kind.upper(), "reference", "argument", True, f"{kind.title()} UUID or name."
        ),
        ParameterSpec("--description", "string", "option", False, "New description."),
        ParameterSpec(
            "--clear-description", "boolean", "option", False, "Clear the description."
        ),
        ParameterSpec("--logo-url", "string", "option", False, "New logo URL."),
        ParameterSpec(
            "--clear-logo-url", "boolean", "option", False, "Clear the logo URL."
        ),
        ParameterSpec(
            "--metadata",
            "JSON object",
            "option",
            False,
            "Replacement metadata; {} clears it.",
        ),
        ParameterSpec(
            "--connection-schema",
            "path",
            "option",
            False,
            "JSON Schema file describing the connection values.",
        ),
        ParameterSpec(
            "--clear-connection-schema",
            "boolean",
            "option",
            False,
            "Clear the connection schema.",
        ),
    )


def build_plugin_version_register_parameters(kind: str) -> tuple[ParameterSpec, ...]:
    """Build metadata for one plugin version register command."""
    return (
        ParameterSpec(
            kind.upper(), "reference", "argument", True, f"{kind.title()} UUID or name."
        ),
        *PLUGIN_SOURCE_PARAMETERS,
        ParameterSpec(
            "--description",
            "string",
            "option",
            False,
            f"Update the parent {kind} description.",
        ),
        IDEMPOTENCY_KEY_PARAMETER,
    )


def build_plugin_update_request(
    kind: str,
    *,
    description: str | None,
    clear_description: bool,
    logo_url: str | None,
    clear_logo_url: bool,
    metadata: str | None,
    connection_schema: Path | None,
    clear_connection_schema: bool,
) -> ImporterUpdateRequest | EvaluatorUpdateRequest | AnalyzerUpdateRequest:
    """Validate selected plugin update options and construct the request model."""
    if description is not None and clear_description:
        raise CLIError(
            "invalid_arguments",
            "--description and --clear-description cannot be used together.",
        )
    if logo_url is not None and clear_logo_url:
        raise CLIError(
            "invalid_arguments",
            "--logo-url and --clear-logo-url cannot be used together.",
        )
    if connection_schema is not None and clear_connection_schema:
        raise CLIError(
            "invalid_arguments",
            "--connection-schema and --clear-connection-schema cannot be used "
            "together.",
        )
    fields: dict[str, Any] = {}
    if description is not None:
        fields["description"] = description
    elif clear_description:
        fields["description"] = None
    if logo_url is not None:
        fields["logo_url"] = logo_url
    elif clear_logo_url:
        fields["logo_url"] = None
    if metadata is not None:
        fields["metadata"] = registration.parse_json_object(
            metadata, option="--metadata"
        )
    if connection_schema is not None:
        fields["connection_schema"] = registration.read_json_object(
            connection_schema, option="--connection-schema"
        )
    elif clear_connection_schema:
        fields["connection_schema"] = None
    if not fields:
        raise CLIError("invalid_arguments", f"Select at least one {kind} update.")
    return registration.get_plugin_update_request_type(kind)(**fields)


async def update_plugin(
    client: Any,
    *,
    kind: str,
    reference: str,
    description: str | None,
    clear_description: bool,
    logo_url: str | None,
    clear_logo_url: bool,
    metadata: str | None,
    connection_schema: Path | None,
    clear_connection_schema: bool,
) -> CommandResult:
    """Update explicitly selected fields on one exact plugin parent."""
    request = build_plugin_update_request(
        kind,
        description=description,
        clear_description=clear_description,
        logo_url=logo_url,
        clear_logo_url=clear_logo_url,
        metadata=metadata,
        connection_schema=connection_schema,
        clear_connection_schema=clear_connection_schema,
    )
    resource = registration.get_plugin_resource(client, kind)
    parent = await registration.resolve_asset(resource, reference, kind.title())
    registration.reject_builtin_plugin(parent, kind)
    updated = await resource.update(parent.id, request)
    return CommandResult(item=updated.model_dump(mode="json"))


async def register_plugin_command(
    kind: str,
    name: str,
    *,
    script: Path | None,
    package: str | None,
    entrypoint: str | None,
    description: str | None,
    provider: str | None,
    metadata: str | None,
    agent_id: uuid.UUID | None,
    display_version: str | None,
    connection_schema: Path | None = None,
) -> CommandResult:
    """Run one kind-specific parent-plus-version registration."""
    source = registration.prepare_plugin_source(
        script=script, package=package, entrypoint=entrypoint
    )
    resolved_description = (
        description
        if description is not None
        else source.description
        if isinstance(source, registration.ScriptSource)
        else None
    )
    parent = registration.plugin_parent_request(
        kind,
        name,
        description=resolved_description,
        provider=provider,
        metadata=metadata,
        agent_id=agent_id,
        connection_schema=connection_schema,
    )
    async with invocation_context.open_asset_client() as client:
        return await registration.register_plugin(
            client,
            kind=kind,
            parent_request=parent,
            source=source,
            display_version=display_version,
        )


async def update_plugin_command(
    kind: str,
    reference: str,
    *,
    description: str | None,
    clear_description: bool,
    logo_url: str | None,
    clear_logo_url: bool,
    metadata: str | None,
    connection_schema: Path | None,
    clear_connection_schema: bool,
) -> CommandResult:
    """Update explicitly selected fields on one plugin parent."""
    async with invocation_context.open_asset_client() as client:
        return await update_plugin(
            client,
            kind=kind,
            reference=reference,
            description=description,
            clear_description=clear_description,
            logo_url=logo_url,
            clear_logo_url=clear_logo_url,
            metadata=metadata,
            connection_schema=connection_schema,
            clear_connection_schema=clear_connection_schema,
        )


async def register_plugin_version_command(
    kind: str,
    reference: str,
    *,
    script: Path | None,
    package: str | None,
    entrypoint: str | None,
    display_version: str | None,
    description: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Run one kind-specific version registration."""
    source = registration.prepare_plugin_source(
        script=script, package=package, entrypoint=entrypoint
    )
    async with invocation_context.open_asset_client() as client:
        return await registration.register_plugin_version(
            client,
            kind=kind,
            reference=reference,
            source=source,
            display_version=display_version,
            description=description,
            idempotency_key=idempotency_key,
        )


async def list_plugin_command(
    kind: Literal["importer", "evaluator", "analyzer"],
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page for a plugin kind."""
    params = registration.list_params(
        kind, size=size, cursor=cursor, sort=sort, filter=filter
    )
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        return registration.page_result(await resource.list(params), size=size)


async def get_plugin_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer, evaluator, or analyzer."""
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        item = await registration.resolve_asset(resource, reference, kind.title())
        return CommandResult(item=item.model_dump(mode="json"))


async def list_plugin_versions_command(
    kind: str,
    reference: str,
    *,
    size: int,
    cursor: str | None,
    sort: str,
) -> CommandResult:
    """List one server page of importer, evaluator, or analyzer versions."""
    params = registration.version_list_params(size=size, cursor=cursor, sort=sort)
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        parent = await registration.resolve_asset(resource, reference, kind.title())
        return registration.page_result(
            await resource.list_versions(parent.id, params), size=size
        )


async def get_plugin_version_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer, evaluator, or analyzer version."""
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        _, item = await registration.get_plugin_version(
            resource, reference, kind.title()
        )
        return CommandResult(item=item.model_dump(mode="json"))
