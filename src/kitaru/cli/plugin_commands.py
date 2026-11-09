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
"""Importer, evaluator, and analyzer plugin commands."""

import uuid
from pathlib import Path
from typing import Literal

from kitaru.cli import invocation_context, registration, scaffold
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    LIST_PARAMETERS,
    VERSION_LIST_PARAMETERS,
    analyzer_app,
    analyzer_version_app,
    build_spec,
    evaluator_app,
    evaluator_version_app,
    importer_app,
    importer_version_app,
    register_command,
)
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec

_PLUGIN_SOURCE_PARAMETERS = (
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


def _plugin_register_parameters(kind: str) -> tuple[ParameterSpec, ...]:
    """Build metadata for one plugin register command."""
    parent = [
        ParameterSpec("NAME", "string", "argument", True, f"New {kind} name."),
        ParameterSpec(
            "--description", "string", "option", False, "Parent description."
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
    return (*parent, *_PLUGIN_SOURCE_PARAMETERS)


async def _register_plugin_command(
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
    parent = registration.plugin_parent_request(
        kind,
        name,
        description=description,
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


async def _register_plugin_version_command(
    kind: str,
    reference: str,
    *,
    script: Path | None,
    package: str | None,
    entrypoint: str | None,
    display_version: str | None,
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
            idempotency_key=idempotency_key,
        )


async def _list_plugin_command(
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


async def _get_plugin_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer, evaluator, or analyzer."""
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        item = await registration.resolve_asset(resource, reference, kind.title())
        return CommandResult(item=item.model_dump(mode="json"))


async def _list_plugin_versions_command(
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


async def _get_plugin_version_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer, evaluator, or analyzer version."""
    async with invocation_context.open_asset_client() as client:
        resource = registration.get_plugin_resource(client, kind)
        _, item = await registration.get_plugin_version(
            resource, reference, kind.title()
        )
        return CommandResult(item=item.model_dump(mode="json"))


@register_command(
    importer_app,
    build_spec(
        ("importer", "scaffold"),
        "Create a minimal importer Parser script.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "Scaffold name."),
            ParameterSpec("--path", "path", "option", False, "Exact target .py file."),
            ParameterSpec(
                "--force", "boolean", "option", False, "Overwrite the target file."
            ),
        ),
        read_only=False,
        side_effects=("writes_local_file",),
        idempotency="conflict_unless_force",
        errors=("invalid_arguments", "conflict", "internal_error"),
        offline=True,
    ),
)
def importer_scaffold(
    name: str, /, *, path: Path | None = None, force: bool = False
) -> CommandResult:
    """Create one importer scaffold file."""
    return scaffold.scaffold_asset("importer", name, path=path, force=force)


@register_command(
    importer_app,
    build_spec(
        ("importer", "test"),
        "Load and optionally invoke an importer Parser in a bounded child process.",
        parameters=(
            ParameterSpec("PATH", "path", "argument", True, "Importer script."),
            ParameterSpec(
                "--entrypoint", "attribute", "option", True, "Parser attribute."
            ),
            ParameterSpec("--payload", "path", "option", False, "Payload to parse."),
            ParameterSpec(
                "--params", "JSON object", "option", False, "Parser parameters."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Child timeout."
            ),
        ),
        side_effects=("reads_local_file", "executes_local_code"),
        errors=("invalid_arguments", "timeout", "internal_error"),
        offline=True,
    ),
)
def importer_test(
    path: Path,
    /,
    *,
    entrypoint: str,
    payload: Path | None = None,
    params: str | None = None,
    timeout: float = 10.0,
) -> CommandResult:
    """Validate an importer locally without contacting a server."""
    return scaffold.test_importer(
        path,
        entrypoint=entrypoint,
        payload=payload,
        params=params,
        timeout=timeout,
    )


@register_command(
    importer_app,
    build_spec(
        ("importer", "register"),
        "Create an importer and its first version.",
        parameters=_plugin_register_parameters("importer"),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def importer_register(
    name: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    description: str | None = None,
    provider: str | None = None,
    connection_schema: Path | None = None,
    metadata: str | None = None,
    display_version: str | None = None,
) -> CommandResult:
    """Create an importer parent, source, and initial version."""
    return await _register_plugin_command(
        "importer",
        name,
        script=script,
        package=package,
        entrypoint=entrypoint,
        description=description,
        provider=provider,
        metadata=metadata,
        agent_id=None,
        display_version=display_version,
        connection_schema=connection_schema,
    )


@register_command(
    importer_app,
    build_spec(
        ("importer", "list"),
        "List importers.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def importer_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of importers."""
    return await _list_plugin_command(
        "importer", size=size, cursor=cursor, sort=sort, filter=filter
    )


@register_command(
    importer_app,
    build_spec(
        ("importer", "get"),
        "Get an importer by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def importer_get(importer: str, /) -> CommandResult:
    """Get one exact importer."""
    return await _get_plugin_command("importer", importer)


@register_command(
    importer_version_app,
    build_spec(
        ("importer", "version", "register"),
        "Create the next version of an existing importer.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
            *_PLUGIN_SOURCE_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def importer_version_register(
    importer: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next importer version."""
    return await _register_plugin_version_command(
        "importer",
        importer,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        idempotency_key=idempotency_key,
    )


@register_command(
    importer_version_app,
    build_spec(
        ("importer", "version", "list"),
        "List versions of an exact importer.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
            *VERSION_LIST_PARAMETERS,
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def importer_version_list(
    importer: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of importer versions."""
    return await _list_plugin_versions_command(
        "importer", importer, size=size, cursor=cursor, sort=sort
    )


@register_command(
    importer_version_app,
    build_spec(
        ("importer", "version", "get"),
        "Get an importer version by exact PARENT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "IMPORTER@VERSION",
                "reference",
                "argument",
                True,
                "Exact version reference.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def importer_version_get(importer_version: str, /) -> CommandResult:
    """Get one exact importer version, accepting @latest for reads."""
    return await _get_plugin_version_command("importer", importer_version)


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "scaffold"),
        "Create a minimal evaluator script.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "Scaffold name."),
            ParameterSpec("--path", "path", "option", False, "Exact target .py file."),
            ParameterSpec(
                "--force", "boolean", "option", False, "Overwrite the target file."
            ),
        ),
        read_only=False,
        side_effects=("writes_local_file",),
        idempotency="conflict_unless_force",
        errors=("invalid_arguments", "conflict", "internal_error"),
        offline=True,
    ),
)
def evaluator_scaffold(
    name: str, /, *, path: Path | None = None, force: bool = False
) -> CommandResult:
    """Create one evaluator scaffold file."""
    return scaffold.scaffold_asset("evaluator", name, path=path, force=force)


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "test"),
        "Load an evaluator and validate its signature in a bounded child process.",
        parameters=(
            ParameterSpec("PATH", "path", "argument", True, "Evaluator script."),
            ParameterSpec(
                "--entrypoint", "attribute", "option", True, "Evaluator attribute."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Child timeout."
            ),
        ),
        side_effects=("reads_local_file", "executes_local_code"),
        errors=("invalid_arguments", "timeout", "internal_error"),
        offline=True,
    ),
)
def evaluator_test(
    path: Path, /, *, entrypoint: str, timeout: float = 10.0
) -> CommandResult:
    """Validate an evaluator locally without contacting a server."""
    return scaffold.test_evaluator(path, entrypoint=entrypoint, timeout=timeout)


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "register"),
        "Create an evaluator and its first version.",
        parameters=_plugin_register_parameters("evaluator"),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def evaluator_register(
    name: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    description: str | None = None,
    metadata: str | None = None,
    provider: str | None = None,
    connection_schema: Path | None = None,
    agent_id: uuid.UUID | None = None,
    display_version: str | None = None,
) -> CommandResult:
    """Create an evaluator parent, source, and initial version."""
    return await _register_plugin_command(
        "evaluator",
        name,
        script=script,
        package=package,
        entrypoint=entrypoint,
        description=description,
        provider=provider,
        metadata=metadata,
        agent_id=agent_id,
        display_version=display_version,
        connection_schema=connection_schema,
    )


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "list"),
        "List evaluators.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def evaluator_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of evaluators."""
    return await _list_plugin_command(
        "evaluator", size=size, cursor=cursor, sort=sort, filter=filter
    )


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "get"),
        "Get an evaluator by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def evaluator_get(evaluator: str, /) -> CommandResult:
    """Get one exact evaluator."""
    return await _get_plugin_command("evaluator", evaluator)


@register_command(
    evaluator_version_app,
    build_spec(
        ("evaluator", "version", "register"),
        "Create the next version of an existing evaluator.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
            *_PLUGIN_SOURCE_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def evaluator_version_register(
    evaluator: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next evaluator version."""
    return await _register_plugin_version_command(
        "evaluator",
        evaluator,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        idempotency_key=idempotency_key,
    )


@register_command(
    evaluator_version_app,
    build_spec(
        ("evaluator", "version", "list"),
        "List versions of an exact evaluator.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
            *VERSION_LIST_PARAMETERS,
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def evaluator_version_list(
    evaluator: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of evaluator versions."""
    return await _list_plugin_versions_command(
        "evaluator", evaluator, size=size, cursor=cursor, sort=sort
    )


@register_command(
    evaluator_version_app,
    build_spec(
        ("evaluator", "version", "get"),
        "Get an evaluator version by exact PARENT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "EVALUATOR@VERSION",
                "reference",
                "argument",
                True,
                "Exact version reference.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def evaluator_version_get(evaluator_version: str, /) -> CommandResult:
    """Get one exact evaluator version, accepting @latest for reads."""
    return await _get_plugin_version_command("evaluator", evaluator_version)


@register_command(
    analyzer_app,
    build_spec(
        ("analyzer", "register"),
        "Create an analyzer and its first version.",
        parameters=_plugin_register_parameters("analyzer"),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def analyzer_register(
    name: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    description: str | None = None,
    provider: str | None = None,
    connection_schema: Path | None = None,
    metadata: str | None = None,
    display_version: str | None = None,
) -> CommandResult:
    """Create an analyzer parent, source, and initial version."""
    return await _register_plugin_command(
        "analyzer",
        name,
        script=script,
        package=package,
        entrypoint=entrypoint,
        description=description,
        provider=provider,
        metadata=metadata,
        agent_id=None,
        display_version=display_version,
        connection_schema=connection_schema,
    )


@register_command(
    analyzer_app,
    build_spec(
        ("analyzer", "list"),
        "List analyzers.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def analyzer_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of analyzers."""
    return await _list_plugin_command(
        "analyzer", size=size, cursor=cursor, sort=sort, filter=filter
    )


@register_command(
    analyzer_app,
    build_spec(
        ("analyzer", "get"),
        "Get an analyzer by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "ANALYZER", "reference", "argument", True, "Analyzer UUID or name."
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def analyzer_get(analyzer: str, /) -> CommandResult:
    """Get one exact analyzer."""
    return await _get_plugin_command("analyzer", analyzer)


@register_command(
    analyzer_version_app,
    build_spec(
        ("analyzer", "version", "register"),
        "Create the next version of an existing analyzer.",
        parameters=(
            ParameterSpec(
                "ANALYZER", "reference", "argument", True, "Analyzer UUID or name."
            ),
            *_PLUGIN_SOURCE_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def analyzer_version_register(
    analyzer: str,
    /,
    *,
    script: Path | None = None,
    package: str | None = None,
    entrypoint: str | None = None,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next analyzer version."""
    return await _register_plugin_version_command(
        "analyzer",
        analyzer,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        idempotency_key=idempotency_key,
    )


@register_command(
    analyzer_version_app,
    build_spec(
        ("analyzer", "version", "list"),
        "List versions of an exact analyzer.",
        parameters=(
            ParameterSpec(
                "ANALYZER", "reference", "argument", True, "Analyzer UUID or name."
            ),
            *VERSION_LIST_PARAMETERS,
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def analyzer_version_list(
    analyzer: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of analyzer versions."""
    return await _list_plugin_versions_command(
        "analyzer", analyzer, size=size, cursor=cursor, sort=sort
    )


@register_command(
    analyzer_version_app,
    build_spec(
        ("analyzer", "version", "get"),
        "Get an analyzer version by exact PARENT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "ANALYZER@VERSION",
                "reference",
                "argument",
                True,
                "Exact version reference.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def analyzer_version_get(analyzer_version: str, /) -> CommandResult:
    """Get one exact analyzer version, accepting @latest for reads."""
    return await _get_plugin_version_command("analyzer", analyzer_version)
