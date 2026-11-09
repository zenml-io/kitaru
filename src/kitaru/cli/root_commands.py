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
"""Top-level account, diagnostics, schema, and config commands."""

from typing import Annotated

from cyclopts import Parameter

from kitaru.cli import auth as auth_commands
from kitaru.cli import diagnostics, local_runtime
from kitaru.cli import setup as setup_commands
from kitaru.cli.command_registry import (
    app,
    build_spec,
    config_app,
    local_app,
    register_command,
)
from kitaru.cli.config import CONFIG_KEYS, read_config, write_config
from kitaru.cli.invocation_context import get_invocation
from kitaru.cli.output import CLIError, CommandResult, emit_event, get_output_context
from kitaru.cli.schema import ParameterSpec, describe_schema
from kitaru.client.config import get_config_path

_MACHINE_TRUE = {"1", "true", "yes", "on"}
_MACHINE_FALSE = {"0", "false", "no", "off"}


@register_command(
    app,
    build_spec(
        ("login",),
        "Authenticate with managed cloud (14-day trial, no credit card required) "
        "or a Kitaru server.",
        parameters=(
            ParameterSpec(
                "SERVER",
                "URL",
                "argument",
                False,
                "Managed or self-hosted instance URL; omit for managed cloud.",
            ),
            ParameterSpec(
                "--local",
                "boolean",
                "option",
                False,
                "Provision and use a local container deployment.",
            ),
            ParameterSpec(
                "--port",
                "integer",
                "option",
                False,
                "Host port for --local; overrides KITARU_LOCAL_PORT.",
            ),
            ParameterSpec(
                "--upgrade",
                "boolean",
                "option",
                False,
                "Upgrade the CLI-owned local deployment.",
            ),
            ParameterSpec(
                "--username", "string", "option", False, "Local account name."
            ),
            ParameterSpec(
                "--password-stdin",
                "boolean",
                "option",
                False,
                "Read password from stdin.",
            ),
            ParameterSpec(
                "--api-key-stdin",
                "boolean",
                "option",
                False,
                "Read API key from stdin.",
            ),
            ParameterSpec(
                "--refresh",
                "boolean",
                "option",
                False,
                "Skip stored credentials and force a new device login.",
            ),
        ),
        read_only=False,
        side_effects=(
            "writes_local_config",
            "writes_local_file",
            "executes_local_code",
        ),
        idempotency="local_reconciliation_or_credential_replacement",
        interaction=("local dashboard launch, device flow, or hidden password prompt"),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "authentication_failed",
            "interaction_required",
            "network_error",
            "timeout",
            "internal_error",
        ),
    ),
)
async def login(
    server: str | None = None,
    /,
    *,
    local: bool = False,
    port: int | None = None,
    upgrade: bool = False,
    username: str | None = None,
    password_stdin: bool = False,
    api_key_stdin: bool = False,
    refresh: bool = False,
) -> CommandResult:
    """Authenticate with managed cloud or a server and store its credential."""
    invocation = get_invocation()
    chosen_server = server or invocation.server
    if chosen_server and not local:
        invocation.resolve_target(server)
    return await auth_commands.login(
        server=chosen_server,
        local=local,
        port=port,
        upgrade=upgrade,
        refresh=refresh,
        username=username,
        password_stdin=password_stdin,
        api_key_stdin=api_key_stdin,
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
        non_interactive=invocation.non_interactive,
        no_browser=invocation.no_browser,
        stdin=invocation.stdin,
        package_version=diagnostics.package_version(),
    )


@register_command(
    app,
    build_spec(
        ("logout",),
        "Disconnect from a server and stop an owned local deployment.",
        parameters=(
            ParameterSpec(
                "SERVER", "URL", "argument", False, "Server to log out from."
            ),
            ParameterSpec(
                "--all", "boolean", "option", False, "Remove every stored credential."
            ),
            ParameterSpec(
                "--volumes/-v",
                "boolean",
                "option",
                False,
                "Delete data for the CLI-owned local deployment.",
            ),
        ),
        read_only=False,
        side_effects=(
            "writes_local_config",
            "writes_local_file",
            "executes_local_code",
        ),
        idempotency="idempotent",
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
async def logout(
    server: str | None = None,
    /,
    *,
    all: bool = False,
    volumes: Annotated[
        bool, Parameter(name=("--volumes", "-v"), negative=False)
    ] = False,
) -> CommandResult:
    """Remove one or all credentials from the local credential store."""
    invocation = get_invocation()
    if all:
        if server or invocation.server:
            raise CLIError(
                "invalid_arguments",
                "SERVER or --server cannot be used with --all.",
            )
        if volumes:
            raise CLIError(
                "invalid_arguments", "--all and --volumes cannot be combined."
            )
        server_url = None
    else:
        server_url = invocation.resolve_target(server).server_url
    return await auth_commands.logout(
        server_url=server_url,
        all_servers=all,
        delete_volumes=volumes,
        allow_orphan_cleanup=volumes and server is None and invocation.server is None,
        credential_store=invocation.credential_store,
    )


@register_command(
    local_app,
    build_spec(
        ("local", "logs"),
        "Read logs from the CLI-owned local deployment.",
        parameters=(
            ParameterSpec(
                "--service",
                "server|db",
                "option",
                False,
                "Limit output to one Compose service.",
            ),
            ParameterSpec(
                "--tail", "integer", "option", False, "Number of recent lines."
            ),
            ParameterSpec(
                "--follow", "boolean", "option", False, "Follow new log lines."
            ),
        ),
        side_effects=("executes_local_code",),
        errors=("invalid_configuration", "timeout", "internal_error"),
        streams=True,
    ),
)
async def local_logs(
    *,
    service: str | None = None,
    tail: int = 100,
    follow: bool = False,
) -> CommandResult:
    """Read or follow logs from the local container deployment."""
    if follow and get_output_context().mode == "json":
        raise CLIError(
            "invalid_arguments",
            "--follow requires text or JSONL output.",
            hint="Pass `--output jsonl` for structured streaming logs.",
        )
    result = await local_runtime.get_local_logs(
        service=service,
        tail=tail,
        follow=follow,
    )
    if follow:
        assert not isinstance(result, list)
        count = 0
        async for line in result:
            emit_event("log", {"line": line})
            count += 1
        return CommandResult(item={"lines": count}, event="complete")
    assert isinstance(result, list)
    return CommandResult(items=[{"line": line} for line in result])


@register_command(
    app,
    build_spec(
        ("status",),
        "Show connection, authentication, compatibility, and live-worker status.",
        side_effects=("reads_local_file",),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "authentication_failed",
            "network_error",
            "internal_error",
        ),
    ),
)
async def status() -> CommandResult:
    """Show the selected server's quick operational status."""
    invocation = get_invocation()
    return await diagnostics.status(
        target=invocation.resolve_target(),
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
    )


@register_command(
    app,
    build_spec(
        ("info",),
        "Show local runtime details and resolved server information.",
        side_effects=("reads_local_file",),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "network_error",
            "internal_error",
        ),
    ),
)
async def info() -> CommandResult:
    """Show local package, Python, platform, and remote server details."""
    invocation = get_invocation()
    return await diagnostics.info(
        target=invocation.resolve_target(),
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
    )


@register_command(
    app,
    build_spec(
        ("doctor",),
        "Run independent local, server, authentication, and tooling checks.",
        side_effects=("reads_local_file",),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "authentication_failed",
            "network_error",
            "internal_error",
        ),
    ),
)
async def doctor() -> CommandResult:
    """Run every diagnostic and report failures in a fixed order."""
    invocation = get_invocation()
    return await diagnostics.doctor(
        credential_store=invocation.credential_store,
        explicit_server=invocation.server,
        timeout=invocation.request_timeout,
    )


@register_command(
    app,
    build_spec(
        ("setup",),
        "Install the agent skills and register the MCP server with every "
        "detected coding agent. Re-run after installing a new one. The global "
        "--server picks the server the MCP server targets.",
        parameters=(
            ParameterSpec(
                "--mode",
                "string",
                "option",
                False,
                "MCP capability mode: read-only, standard (default), or destructive.",
            ),
            ParameterSpec(
                "--no-skills", "boolean", "option", False, "Skip installing the skills."
            ),
            ParameterSpec(
                "--no-mcp",
                "boolean",
                "option",
                False,
                "Skip registering the MCP server.",
            ),
        ),
        read_only=False,
        side_effects=("writes_local_file", "executes_local_code"),
        idempotency="idempotent",
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "network_error",
            "internal_error",
        ),
    ),
)
async def setup(
    *,
    mode: Annotated[str, Parameter(name="--mode")] = "standard",
    no_skills: Annotated[bool, Parameter(name="--no-skills")] = False,
    no_mcp: Annotated[bool, Parameter(name="--no-mcp")] = False,
) -> CommandResult:
    """Wire skills and the MCP server into installed coding agents."""
    invocation = get_invocation()
    return await setup_commands.setup(
        server=invocation.server,
        mode=mode,
        install_skills=not no_skills,
        register_mcp=not no_mcp,
    )


@register_command(
    app,
    build_spec(
        ("version",),
        "Show the installed Kitaru version.",
        offline=True,
    ),
)
def version() -> CommandResult:
    """Show the installed package version without local or network access."""
    return CommandResult(item={"version": diagnostics.package_version()})


@register_command(
    app,
    build_spec(
        ("schema",),
        "Describe the CLI command tree from offline registration metadata.",
        parameters=(
            ParameterSpec(
                "COMMAND", "string[]", "argument", False, "Command or group path."
            ),
        ),
        errors=("invalid_arguments", "internal_error"),
        offline=True,
    ),
)
def schema(*command: str) -> CommandResult:
    """Describe top-level commands or one bounded command subtree."""
    return CommandResult(items=describe_schema(tuple(command)))


@register_command(
    config_app,
    build_spec(
        ("config", "list"),
        "List allowlisted CLI preferences with effective values and sources.",
        side_effects=("reads_local_file",),
        errors=("invalid_configuration", "internal_error"),
    ),
)
def config_list() -> CommandResult:
    """List the only Stage 1 persisted CLI preference."""
    config = read_config()
    source = "persisted" if get_config_path().exists() else "default"
    return CommandResult(
        items=[
            {
                "key": "cli.machine_mode",
                "value": config.cli.machine_mode,
                "source": source,
            }
        ]
    )


@register_command(
    config_app,
    build_spec(
        ("config", "get"),
        "Get one allowlisted CLI preference.",
        parameters=(
            ParameterSpec(
                "KEY", "string", "argument", True, "Allowlisted preference key."
            ),
        ),
        side_effects=("reads_local_file",),
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
def config_get(key: str, /) -> CommandResult:
    """Get an allowlisted preference and its provenance."""
    _validate_config_key(key)
    config = read_config()
    return CommandResult(
        item={
            "key": key,
            "value": config.cli.machine_mode,
            "source": "persisted" if get_config_path().exists() else "default",
        }
    )


@register_command(
    config_app,
    build_spec(
        ("config", "set"),
        "Set one allowlisted CLI preference.",
        parameters=(
            ParameterSpec(
                "KEY", "string", "argument", True, "Allowlisted preference key."
            ),
            ParameterSpec(
                "VALUE", "boolean", "argument", True, "Boolean preference value."
            ),
        ),
        read_only=False,
        side_effects=("reads_local_file", "writes_local_config"),
        idempotency="idempotent",
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
def config_set(key: str, value: str, /) -> CommandResult:
    """Set an allowlisted preference after strict value parsing."""
    _validate_config_key(key)
    parsed = _parse_bool(value)
    config = read_config()
    config.cli.machine_mode = parsed
    write_config(config)
    return CommandResult(item={"key": key, "value": parsed, "source": "persisted"})


@register_command(
    config_app,
    build_spec(
        ("config", "path"),
        "Show the global CLI config path without reading its contents.",
        side_effects=("reads_local_file",),
        offline=True,
    ),
)
def config_path() -> CommandResult:
    """Show the config path even when the document is missing or malformed."""
    path = get_config_path()
    return CommandResult(item={"path": str(path), "exists": path.exists()})


def _parse_bool(value: str) -> bool:
    """Parse a persisted boolean without accepting surprising spellings."""
    normalized = value.strip().lower()
    if normalized in _MACHINE_TRUE:
        return True
    if normalized in _MACHINE_FALSE:
        return False
    raise CLIError(
        "invalid_arguments",
        "Boolean values must be true/false, yes/no, on/off, or 1/0.",
    )


def _validate_config_key(key: str) -> None:
    """Reject attempts to use config as a generic key-value store."""
    if key not in CONFIG_KEYS:
        raise CLIError(
            "invalid_arguments",
            f"Unknown config key {key!r}. Allowed keys: {', '.join(CONFIG_KEYS)}.",
        )
