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

from kitaru.cli import plugins, scaffold
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
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
        parameters=plugins.build_plugin_register_parameters("importer"),
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
    return await plugins.register_plugin_command(
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
    return await plugins.list_plugin_command(
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
    return await plugins.get_plugin_command("importer", importer)


@register_command(
    importer_app,
    build_spec(
        ("importer", "update"),
        "Update selected fields on an exact importer.",
        parameters=plugins.build_plugin_update_parameters("importer"),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def importer_update(
    importer: str,
    /,
    *,
    description: str | None = None,
    clear_description: bool = False,
    logo_url: str | None = None,
    clear_logo_url: bool = False,
    metadata: str | None = None,
    connection_schema: Path | None = None,
    clear_connection_schema: bool = False,
) -> CommandResult:
    """Update selected fields on one exact importer."""
    return await plugins.update_plugin_command(
        "importer",
        importer,
        description=description,
        clear_description=clear_description,
        logo_url=logo_url,
        clear_logo_url=clear_logo_url,
        metadata=metadata,
        connection_schema=connection_schema,
        clear_connection_schema=clear_connection_schema,
    )


@register_command(
    importer_version_app,
    build_spec(
        ("importer", "version", "register"),
        "Create the next version of an existing importer.",
        parameters=plugins.build_plugin_version_register_parameters("importer"),
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
    description: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next importer version."""
    return await plugins.register_plugin_version_command(
        "importer",
        importer,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        description=description,
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
    return await plugins.list_plugin_versions_command(
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
    return await plugins.get_plugin_version_command("importer", importer_version)


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
        parameters=plugins.build_plugin_register_parameters("evaluator"),
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
    return await plugins.register_plugin_command(
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
    return await plugins.list_plugin_command(
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
    return await plugins.get_plugin_command("evaluator", evaluator)


@register_command(
    evaluator_app,
    build_spec(
        ("evaluator", "update"),
        "Update selected fields on an exact evaluator.",
        parameters=plugins.build_plugin_update_parameters("evaluator"),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def evaluator_update(
    evaluator: str,
    /,
    *,
    description: str | None = None,
    clear_description: bool = False,
    logo_url: str | None = None,
    clear_logo_url: bool = False,
    metadata: str | None = None,
    connection_schema: Path | None = None,
    clear_connection_schema: bool = False,
) -> CommandResult:
    """Update selected fields on one exact evaluator."""
    return await plugins.update_plugin_command(
        "evaluator",
        evaluator,
        description=description,
        clear_description=clear_description,
        logo_url=logo_url,
        clear_logo_url=clear_logo_url,
        metadata=metadata,
        connection_schema=connection_schema,
        clear_connection_schema=clear_connection_schema,
    )


@register_command(
    evaluator_version_app,
    build_spec(
        ("evaluator", "version", "register"),
        "Create the next version of an existing evaluator.",
        parameters=plugins.build_plugin_version_register_parameters("evaluator"),
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
    description: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next evaluator version."""
    return await plugins.register_plugin_version_command(
        "evaluator",
        evaluator,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        description=description,
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
    return await plugins.list_plugin_versions_command(
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
    return await plugins.get_plugin_version_command("evaluator", evaluator_version)


@register_command(
    analyzer_app,
    build_spec(
        ("analyzer", "register"),
        "Create an analyzer and its first version.",
        parameters=plugins.build_plugin_register_parameters("analyzer"),
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
    return await plugins.register_plugin_command(
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
    return await plugins.list_plugin_command(
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
    return await plugins.get_plugin_command("analyzer", analyzer)


@register_command(
    analyzer_app,
    build_spec(
        ("analyzer", "update"),
        "Update selected fields on an exact analyzer.",
        parameters=plugins.build_plugin_update_parameters("analyzer"),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def analyzer_update(
    analyzer: str,
    /,
    *,
    description: str | None = None,
    clear_description: bool = False,
    logo_url: str | None = None,
    clear_logo_url: bool = False,
    metadata: str | None = None,
    connection_schema: Path | None = None,
    clear_connection_schema: bool = False,
) -> CommandResult:
    """Update selected fields on one exact analyzer."""
    return await plugins.update_plugin_command(
        "analyzer",
        analyzer,
        description=description,
        clear_description=clear_description,
        logo_url=logo_url,
        clear_logo_url=clear_logo_url,
        metadata=metadata,
        connection_schema=connection_schema,
        clear_connection_schema=clear_connection_schema,
    )


@register_command(
    analyzer_version_app,
    build_spec(
        ("analyzer", "version", "register"),
        "Create the next version of an existing analyzer.",
        parameters=plugins.build_plugin_version_register_parameters("analyzer"),
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
    description: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next analyzer version."""
    return await plugins.register_plugin_version_command(
        "analyzer",
        analyzer,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
        description=description,
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
    return await plugins.list_plugin_versions_command(
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
    return await plugins.get_plugin_version_command("analyzer", analyzer_version)
