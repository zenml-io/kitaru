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
"""Provider connection CLI commands."""

import getpass
from collections.abc import Callable
from typing import Any

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionUpdateRequest,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    build_list_params,
    page_result,
    parse_env,
    resolve_asset,
)


async def create_connection(
    client: Any,
    name: str,
    *,
    importer: str | None,
    provider: str | None,
    values: list[str] | None,
    secret_values: list[str] | None,
    default: bool,
    non_interactive: bool,
    idempotency_key: str | None = None,
    value_prompt: Callable[[str], str] = input,
    secret_prompt: Callable[[str], str] = getpass.getpass,
) -> CommandResult:
    """Create a connection from a schema prompt or from direct values."""
    if (importer is None) == (provider is None):
        raise CLIError(
            "invalid_arguments", "Provide exactly one of --importer or --provider."
        )
    env = parse_env(values or [])
    secrets = parse_env(secret_values or [])
    if importer is None:
        assert provider is not None
        resolved_provider = provider
    else:
        parent = await resolve_asset(client.importers, importer, "Importer")
        if parent.provider is None:
            raise CLIError(
                "invalid_arguments", f"Importer {parent.name!r} has no provider."
            )
        resolved_provider = parent.provider
        _collect_schema_values(
            parent.connection_schema,
            env,
            secrets,
            importer_name=parent.name,
            non_interactive=non_interactive,
            value_prompt=value_prompt,
            secret_prompt=secret_prompt,
        )
    created = await client.connections.create(
        ConnectionCreateRequest(
            name=name,
            provider=resolved_provider,
            env=env,
            secrets=secrets,
            default=default,
        ),
        idempotency_key=idempotency_key,
    )
    return CommandResult(item=created.model_dump(mode="json"))


async def list_connections(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of connections."""
    params = build_list_params(
        ConnectionListParams, size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.connections.list(params), size=size)


async def get_connection(client: Any, reference: str) -> CommandResult:
    """Get one connection by exact UUID or case-sensitive name."""
    connection = await resolve_asset(client.connections, reference, "Connection")
    return CommandResult(item=connection.model_dump(mode="json"))


async def update_connection(
    client: Any,
    reference: str,
    *,
    values: list[str] | None,
    secret_values: list[str] | None,
    default: bool | None,
) -> CommandResult:
    """Update only explicitly selected connection fields."""
    fields: dict[str, Any] = {}
    if values is not None:
        fields["env"] = parse_env(values)
    if secret_values is not None:
        fields["secrets"] = parse_env(secret_values)
    if default is not None:
        fields["default"] = default
    if not fields:
        raise CLIError("invalid_arguments", "Select at least one connection update.")
    connection = await resolve_asset(client.connections, reference, "Connection")
    # Send the full env because the server replaces the map, while --set names
    # only the keys to change.
    if "env" in fields:
        fields["env"] = {**connection.env, **fields["env"]}
    updated = await client.connections.update(
        connection.id, ConnectionUpdateRequest(**fields)
    )
    return CommandResult(item=updated.model_dump(mode="json"))


async def set_default_connection(client: Any, reference: str) -> CommandResult:
    """Make one connection the default for its provider."""
    connection = await resolve_asset(client.connections, reference, "Connection")
    updated = await client.connections.update(
        connection.id, ConnectionUpdateRequest(default=True)
    )
    return CommandResult(item=updated.model_dump(mode="json"))


async def delete_connection(
    client: Any, reference: str, *, force: bool
) -> CommandResult:
    """Delete one connection and the secret holding its values."""
    if not force:
        raise CLIError(
            "invalid_arguments",
            "Deleting a connection and its stored secret requires --force.",
        )
    connection = await resolve_asset(client.connections, reference, "Connection")
    await client.connections.delete(connection.id)
    return CommandResult(item={"id": str(connection.id), "deleted": True})


def _collect_schema_values(
    schema: dict[str, Any] | None,
    env: dict[str, str],
    secrets: dict[str, str],
    *,
    importer_name: str,
    non_interactive: bool,
    value_prompt: Callable[[str], str],
    secret_prompt: Callable[[str], str],
) -> None:
    """Fill env and secrets from an importer connection schema."""
    properties = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(properties, dict):
        if not env and not secrets:
            raise CLIError(
                "invalid_arguments",
                f"Importer {importer_name!r} has no connection schema. "
                "Use --set or --set-secret.",
            )
        return
    required = schema.get("required") if isinstance(schema, dict) else None
    required_keys = set(required) if isinstance(required, list) else set()
    if non_interactive:
        missing = sorted(required_keys - set(env) - set(secrets))
        if missing:
            raise CLIError(
                "interaction_required",
                "Non-interactive creation requires --set or --set-secret for "
                f"{', '.join(missing)}.",
            )
        return
    for key, definition in properties.items():
        if key in env or key in secrets:
            continue
        if not isinstance(definition, dict):
            definition = {}
        secret = definition.get("writeOnly") is True
        default = definition.get("default")
        default_text = None if default is None else str(default)
        prompt = secret_prompt if secret else value_prompt
        value = prompt(_prompt_label(key, definition, default_text)).strip()
        if not value:
            value = default_text or ""
        if not value:
            if key in required_keys:
                raise CLIError("invalid_arguments", f"{key} is required.")
            continue
        if secret:
            secrets[key] = value
        else:
            env[key] = value


def _prompt_label(key: str, definition: dict[str, Any], default: str | None) -> str:
    """Build one prompt line from a schema property."""
    label = key
    description = definition.get("description")
    if isinstance(description, str) and description:
        label = f"{label} ({description})"
    if default is not None:
        label = f"{label} [{default}]"
    return f"{label}: "
