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
"""Connection commands."""

from typing import Annotated

from cyclopts import Parameter

from kitaru.cli import connections, invocation_context
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    LIST_PARAMETERS,
    build_spec,
    connection_app,
    register_command,
)
from kitaru.cli.invocation_context import get_invocation
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec

_CONNECTION_REFERENCE_PARAMETER = ParameterSpec(
    "CONNECTION", "reference", "argument", True, "Connection UUID or name."
)
_CONNECTION_VALUE_PARAMETERS = (
    ParameterSpec(
        "--set", "KEY=VALUE[]", "option", False, "Non-secret environment value."
    ),
    ParameterSpec(
        "--set-secret", "KEY=VALUE[]", "option", False, "Sensitive environment value."
    ),
)

_CONNECTION_UPDATE_VALUE_PARAMETERS = (
    ParameterSpec(
        "--set",
        "KEY=VALUE[]",
        "option",
        False,
        "Non-secret environment value. Keeps the other stored values.",
    ),
    ParameterSpec(
        "--set-secret",
        "KEY=VALUE[]",
        "option",
        False,
        "Sensitive environment value. Replaces all stored secret values.",
    ),
)


@register_command(
    connection_app,
    build_spec(
        ("connection", "create"),
        "Create a connection from a plugin schema or from direct values.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New connection name."),
            ParameterSpec(
                "--importer",
                "reference",
                "option",
                False,
                "Importer whose connection schema drives the prompts.",
            ),
            ParameterSpec(
                "--analyzer",
                "reference",
                "option",
                False,
                "Analyzer whose connection schema drives the prompts.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference",
                "option",
                False,
                "Evaluator whose connection schema drives the prompts.",
            ),
            ParameterSpec(
                "--provider", "string", "option", False, "Provider addressed directly."
            ),
            *_CONNECTION_VALUE_PARAMETERS,
            ParameterSpec(
                "--default",
                "boolean",
                "option",
                False,
                "Make this the provider's default connection.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        interaction="visible and hidden schema value prompts",
        errors=(*ASSET_WRITE_ERRORS, "interaction_required"),
    ),
)
async def connection_create(
    name: str,
    /,
    *,
    importer: str | None = None,
    analyzer: str | None = None,
    evaluator: str | None = None,
    provider: str | None = None,
    set: list[str] | None = None,
    set_secret: list[str] | None = None,
    default: bool = False,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one connection, prompting for a plugin's schema values."""
    invocation = get_invocation()
    async with invocation_context.open_asset_client() as client:
        return await connections.create_connection(
            client,
            name,
            importer=importer,
            analyzer=analyzer,
            evaluator=evaluator,
            provider=provider,
            values=set,
            secret_values=set_secret,
            default=default,
            non_interactive=invocation.non_interactive,
            idempotency_key=idempotency_key,
        )


@register_command(
    connection_app,
    build_spec(
        ("connection", "list"),
        "List connections.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def connection_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of connections."""
    async with invocation_context.open_asset_client() as client:
        return await connections.list_connections(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    connection_app,
    build_spec(
        ("connection", "get"),
        "Get a connection by exact UUID or case-sensitive name.",
        parameters=(_CONNECTION_REFERENCE_PARAMETER,),
        errors=ASSET_READ_ERRORS,
    ),
)
async def connection_get(connection: str, /) -> CommandResult:
    """Get one exact connection."""
    async with invocation_context.open_asset_client() as client:
        return await connections.get_connection(client, connection)


@register_command(
    connection_app,
    build_spec(
        ("connection", "update"),
        "Update values on an exact connection.",
        parameters=(
            _CONNECTION_REFERENCE_PARAMETER,
            *_CONNECTION_UPDATE_VALUE_PARAMETERS,
            ParameterSpec(
                "--default/--no-default",
                "boolean",
                "option",
                False,
                "Set or clear the provider's default connection.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def connection_update(
    connection: str,
    /,
    *,
    set: list[str] | None = None,
    set_secret: list[str] | None = None,
    default: Annotated[
        bool | None, Parameter(name="--default", negative="--no-default")
    ] = None,
) -> CommandResult:
    """Update only explicitly selected connection fields."""
    async with invocation_context.open_asset_client() as client:
        return await connections.update_connection(
            client,
            connection,
            values=set,
            secret_values=set_secret,
            default=default,
        )


@register_command(
    connection_app,
    build_spec(
        ("connection", "set-default"),
        "Make a connection the default for its provider.",
        parameters=(_CONNECTION_REFERENCE_PARAMETER,),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def connection_set_default(connection: str, /) -> CommandResult:
    """Make one connection the default for its provider."""
    async with invocation_context.open_asset_client() as client:
        return await connections.set_default_connection(client, connection)


@register_command(
    connection_app,
    build_spec(
        ("connection", "delete"),
        "Delete a connection and the secret holding its values.",
        parameters=(
            _CONNECTION_REFERENCE_PARAMETER,
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def connection_delete(
    connection: str, /, *, force: bool = False
) -> CommandResult:
    """Delete one connection and the secret holding its values."""
    async with invocation_context.open_asset_client() as client:
        return await connections.delete_connection(client, connection, force=force)
