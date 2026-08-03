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
"""Cyclopts command application and shared invocation boundary."""

import asyncio
import inspect
import math
import sys
import uuid
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, TextIO, TypeVar

import httpx
from cyclopts import App, Parameter
from cyclopts.exceptions import CycloptsError
from pydantic import ValidationError as PydanticValidationError

from kitaru.api_models.v1.task import TaskKind
from kitaru.cli import auth as auth_commands
from kitaru.cli import (
    cohorts,
    diagnostics,
    evaluations,
    experiment_runs,
    experiments,
    jobs,
    registration,
    scaffold,
    sessions,
    workers,
)
from kitaru.cli.config import (
    CONFIG_KEYS,
    ConfigStore,
    ResolvedTarget,
    get_config_path,
    resolve_target,
    validate_context_name,
    validate_server_url,
)
from kitaru.cli.output import (
    CLIError,
    CommandResult,
    OutputContext,
    OutputMode,
    emit_error,
    emit_result,
    reset_output_context,
    resolve_output_mode,
    set_output_context,
)
from kitaru.cli.schema import (
    GROUP_DESCRIPTIONS,
    CommandSpec,
    ParameterSpec,
    SideEffect,
    describe_schema,
    is_command_group,
    is_offline,
    register_spec,
)
from kitaru.client.control_plane import ControlPlaneLoginError
from kitaru.client.credential_store import CredentialStore
from kitaru.client.device_grant import DeviceLoginError
from kitaru.client.exceptions import APIError

F = TypeVar("F", bound=Callable[..., Any])
_MACHINE_TRUE = {"1", "true", "yes", "on"}
_MACHINE_FALSE = {"0", "false", "no", "off"}

app = App(
    name="kitaru",
    help="Connect to, inspect, and configure Kitaru.",
    version=diagnostics.package_version,
    result_action="return_value",
    print_error=False,
    help_on_error=False,
    default_parameter=Parameter(negative=False),
)
config_app = App(
    name="config",
    help=GROUP_DESCRIPTIONS["config"],
    default_parameter=Parameter(negative=False),
)
context_app = App(
    name="context",
    help=GROUP_DESCRIPTIONS["context"],
    default_parameter=Parameter(negative=False),
)
agent_app = App(
    name="agent",
    help=GROUP_DESCRIPTIONS["agent"],
    default_parameter=Parameter(negative=False),
)
agent_version_app = App(
    name="version",
    help="Register and inspect agent versions.",
    default_parameter=Parameter(negative=False),
)
cohort_app = App(
    name="cohort",
    help=GROUP_DESCRIPTIONS["cohort"],
    default_parameter=Parameter(negative=False),
)
cohort_version_app = App(
    name="version",
    help="Create and manage immutable cohort membership versions.",
    default_parameter=Parameter(negative=False),
)
experiment_app = App(
    name="experiment",
    help=GROUP_DESCRIPTIONS["experiment"],
    default_parameter=Parameter(negative=False),
)
experiment_run_app = App(
    name="run",
    help="Start, inspect, watch, cancel, and delete experiment runs.",
    default_parameter=Parameter(negative=False),
)
importer_app = App(
    name="importer",
    help=GROUP_DESCRIPTIONS["importer"],
    default_parameter=Parameter(negative=False),
)
importer_version_app = App(
    name="version",
    help="Register and inspect importer versions.",
    default_parameter=Parameter(negative=False),
)
evaluator_app = App(
    name="evaluator",
    help=GROUP_DESCRIPTIONS["evaluator"],
    default_parameter=Parameter(negative=False),
)
evaluator_version_app = App(
    name="version",
    help="Register and inspect evaluator versions.",
    default_parameter=Parameter(negative=False),
)
session_app = App(
    name="session",
    help=GROUP_DESCRIPTIONS["session"],
    default_parameter=Parameter(negative=False),
)
evaluation_app = App(
    name="evaluation",
    help=GROUP_DESCRIPTIONS["evaluation"],
    default_parameter=Parameter(negative=False),
)
worker_app = App(
    name="worker",
    help=GROUP_DESCRIPTIONS["worker"],
    default_parameter=Parameter(negative=False),
)
job_app = App(
    name="job",
    help=GROUP_DESCRIPTIONS["job"],
    default_parameter=Parameter(negative=False),
)
agent_app.command(agent_version_app, name="version")
cohort_app.command(cohort_version_app, name="version")
experiment_app.command(experiment_run_app, name="run")
importer_app.command(importer_version_app, name="version")
evaluator_app.command(evaluator_version_app, name="version")
app.command(config_app, name="config")
app.command(context_app, name="context")
app.command(agent_app, name="agent")
app.command(cohort_app, name="cohort")
app.command(experiment_app, name="experiment")
app.command(importer_app, name="importer")
app.command(evaluator_app, name="evaluator")
app.command(session_app, name="session")
app.command(evaluation_app, name="evaluation")
app.command(worker_app, name="worker")
app.command(job_app, name="job")


@dataclass(slots=True)
class Invocation:
    """Resolved non-secret settings and lazy stores for one invocation."""

    server: str | None
    context_name: str | None
    request_timeout: float
    non_interactive: bool
    no_browser: bool
    stdin: TextIO
    _config_store: ConfigStore | None = None
    _credential_store: CredentialStore | None = None

    @property
    def config_store(self) -> ConfigStore:
        """Return the lazily constructed local config store."""
        if self._config_store is None:
            self._config_store = ConfigStore()
        return self._config_store

    @property
    def credential_store(self) -> CredentialStore:
        """Return the lazily constructed credential store."""
        if self._credential_store is None:
            self._credential_store = CredentialStore()
        return self._credential_store

    def resolve_target(self, explicit_server: str | None = None) -> ResolvedTarget:
        """Resolve a server from command and global inputs."""
        if explicit_server and self.server:
            command_url = validate_server_url(explicit_server)
            global_url = validate_server_url(self.server)
            if command_url != global_url:
                raise CLIError(
                    "invalid_arguments",
                    "The command SERVER and global --server identify "
                    "different servers.",
                )
        return resolve_target(
            self.config_store,
            explicit_server=explicit_server or self.server,
            context_name=self.context_name,
        )


_INVOCATION: ContextVar[Invocation | None] = ContextVar(
    "kitaru_cli_invocation", default=None
)
_FUNCTION_SPECS: dict[Callable[..., Any], CommandSpec] = {}


def _invocation() -> Invocation:
    """Return the current command invocation."""
    value = _INVOCATION.get()
    if value is None:
        raise RuntimeError("No CLI invocation is active")
    return value


@app.meta.default
async def _launch(
    *tokens: Annotated[
        str,
        Parameter(show=False, allow_leading_hyphen=True),
    ],
    output: Annotated[
        OutputMode,
        Parameter(
            name=("--output", "-o"),
            help="Serialization mode; JSONL is streaming-only.",
        ),
    ] = "auto",
    machine: Annotated[
        bool | None,
        Parameter(
            name="--machine",
            negative="--no-machine",
            env_var="KITARU_MACHINE_MODE",
            help="Use plain rendering on a terminal.",
        ),
    ] = None,
    server: Annotated[
        str | None,
        Parameter(name="--server", help="Server URL override."),
    ] = None,
    context: Annotated[
        str | None,
        Parameter(name="--context", help="Named context override."),
    ] = None,
    non_interactive: Annotated[
        bool,
        Parameter(
            name="--non-interactive",
            negative=False,
            env_var="KITARU_NON_INTERACTIVE",
            help="Disable prompts and browser actions.",
        ),
    ] = False,
    no_browser: Annotated[
        bool,
        Parameter(name="--no-browser", negative=False, help="Never open a browser."),
    ] = False,
    request_timeout: Annotated[
        float,
        Parameter(name="--request-timeout", help="HTTP timeout in seconds."),
    ] = 30.0,
    debug: Annotated[
        bool,
        Parameter(
            name="--debug", negative=False, help="Enable redacted debug diagnostics."
        ),
    ] = False,
    traceback: Annotated[
        bool,
        Parameter(
            name="--traceback", negative=False, help="Show a redacted traceback."
        ),
    ] = False,
) -> int:
    """Resolve global behavior, invoke one command, and emit one result."""
    if not math.isfinite(request_timeout) or request_timeout <= 0:
        return _emit_early_error(
            CLIError("invalid_arguments", "--request-timeout must be positive."),
            tokens=tokens,
            output=output,
            machine=bool(machine),
            non_interactive=non_interactive,
            debug=debug,
            traceback=traceback,
        )
    command: Callable[..., Any]
    try:
        command, bound, _ = app.parse_args(tokens)
    except CycloptsError as error:
        return _emit_early_error(
            CLIError("invalid_arguments", str(error)),
            tokens=tokens,
            output=output,
            machine=bool(machine),
            non_interactive=non_interactive,
            debug=debug,
            traceback=traceback,
            exception=error,
        )
    spec = _FUNCTION_SPECS.get(command)
    if not isinstance(spec, CommandSpec):
        return _emit_early_error(
            CLIError("internal_error", "Command metadata is missing."),
            tokens=tokens,
            output=output,
            machine=bool(machine),
            non_interactive=non_interactive,
            debug=debug,
            traceback=traceback,
        )
    config_store: ConfigStore | None = None
    try:
        mode = resolve_output_mode(
            output, is_tty=sys.stdout.isatty(), streaming=spec.streams
        )
        if mode in {"json", "jsonl"}:
            non_interactive = True
        if machine is None and not is_offline(spec.path) and spec.path != ("doctor",):
            config_store = ConfigStore()
            machine = config_store.load().cli.machine_mode
        machine = bool(machine) or mode != "text" or not sys.stdout.isatty()
        output_context = OutputContext(
            command=spec.command,
            mode=mode,
            machine=machine,
            non_interactive=non_interactive,
            debug=debug,
            traceback=traceback or debug,
            stdout=sys.stdout,
            stderr=sys.stderr,
            rich=mode == "text" and sys.stdout.isatty() and not machine,
        )
    except CLIError as error:
        return _emit_early_error(
            error,
            tokens=tokens,
            output=output,
            machine=bool(machine),
            non_interactive=non_interactive,
            debug=debug,
            traceback=traceback,
        )
    output_token = set_output_context(output_context)
    invocation = Invocation(
        server=server,
        context_name=context,
        request_timeout=request_timeout,
        non_interactive=non_interactive,
        no_browser=no_browser or non_interactive,
        stdin=sys.stdin,
        _config_store=config_store,
    )
    invocation_token = _INVOCATION.set(invocation)
    try:
        result = command(*bound.args, **bound.kwargs)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, CommandResult):
            raise TypeError(
                f"Command returned unsupported result {type(result).__name__}"
            )
        return emit_result(result)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except BrokenPipeError:
        return 0
    except asyncio.CancelledError:
        raise
    except BaseException as exception:
        error = _convert_error(exception)
        return emit_error(
            error,
            exception=exception,
            traceback=exception.__traceback__,
        )
    finally:
        _INVOCATION.reset(invocation_token)
        reset_output_context(output_token)


_GLOBAL_PARAMETERS = (
    ParameterSpec(
        "--output/-o", "auto|text|json|jsonl", "option", False, "Serialization mode."
    ),
    ParameterSpec(
        "--machine/--no-machine", "boolean", "option", False, "Terminal rendering mode."
    ),
    ParameterSpec("--server", "URL", "option", False, "Server URL override."),
    ParameterSpec("--context", "string", "option", False, "Named context override."),
    ParameterSpec(
        "--non-interactive", "boolean", "option", False, "Disable interaction."
    ),
    ParameterSpec(
        "--no-browser", "boolean", "option", False, "Disable browser launch."
    ),
    ParameterSpec(
        "--request-timeout", "positive float", "option", False, "HTTP timeout."
    ),
    ParameterSpec("--debug", "boolean", "option", False, "Redacted debug diagnostics."),
    ParameterSpec("--traceback", "boolean", "option", False, "Redacted traceback."),
)


def _spec(
    path: tuple[str, ...],
    description: str,
    *,
    parameters: tuple[ParameterSpec, ...] = (),
    read_only: bool = True,
    side_effects: tuple[SideEffect, ...] = (),
    idempotency: str = "read_only",
    interaction: str = "none",
    errors: tuple[str, ...] = ("invalid_arguments", "internal_error"),
    offline: bool = False,
    streams: bool = False,
) -> CommandSpec:
    """Create one leaf contract including shared global options."""
    if "invalid_arguments" not in errors:
        errors = ("invalid_arguments", *errors)
    if not offline and "reads_local_file" not in side_effects:
        side_effects = ("reads_local_file", *side_effects)
    return CommandSpec(
        path=path,
        description=description,
        parameters=_GLOBAL_PARAMETERS + parameters,
        read_only=read_only,
        side_effects=side_effects,
        idempotency=idempotency,
        interaction=interaction,
        streams=streams,
        error_kinds=errors,
        output_modes=("auto", "text", "json", "jsonl")
        if streams
        else ("auto", "text", "json"),
        offline=offline,
    )


def _register(target: App, spec: CommandSpec) -> Callable[[F], F]:
    """Register a command and the metadata that powers offline schema."""
    register_spec(spec)

    def decorator(function: F) -> F:
        _FUNCTION_SPECS[function] = spec
        target.command(function, name=spec.path[-1], help=spec.description)
        return function

    return decorator


@_register(
    app,
    _spec(
        ("login",),
        "Authenticate with a Kitaru server.",
        parameters=(
            ParameterSpec(
                "SERVER",
                "URL",
                "argument",
                False,
                "Managed or self-hosted instance URL.",
            ),
            ParameterSpec(
                "--local", "boolean", "option", False, "Use http://localhost:8000."
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
        ),
        read_only=False,
        side_effects=("writes_local_config",),
        idempotency="credential_replacement_after_validation",
        interaction=(
            "device flow or hidden password prompt unless explicit stdin input is used"
        ),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "authentication_failed",
            "interaction_required",
            "network_error",
            "partial_failure",
            "internal_error",
        ),
    ),
)
async def login(
    server: str | None = None,
    /,
    *,
    local: bool = False,
    username: str | None = None,
    password_stdin: bool = False,
    api_key_stdin: bool = False,
) -> CommandResult:
    """Authenticate with a server and optionally save the global context."""
    invocation = _invocation()
    chosen_server = server or invocation.server
    if server and invocation.server:
        invocation.resolve_target(server)
    return await auth_commands.login(
        server=chosen_server,
        local=local,
        context_name=invocation.context_name,
        username=username,
        password_stdin=password_stdin,
        api_key_stdin=api_key_stdin,
        config_store=invocation.config_store,
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
        non_interactive=invocation.non_interactive,
        no_browser=invocation.no_browser,
        stdin=invocation.stdin,
    )


@_register(
    app,
    _spec(
        ("logout",),
        "Remove locally stored credentials without changing contexts.",
        parameters=(
            ParameterSpec(
                "SERVER", "URL", "argument", False, "Server to log out from."
            ),
            ParameterSpec(
                "--all", "boolean", "option", False, "Remove every stored credential."
            ),
        ),
        read_only=False,
        side_effects=("writes_local_config",),
        idempotency="idempotent",
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
def logout(server: str | None = None, /, *, all: bool = False) -> CommandResult:
    """Remove one or all credentials from the local credential store."""
    invocation = _invocation()
    if all:
        if server or invocation.server or invocation.context_name:
            raise CLIError(
                "invalid_arguments",
                "SERVER, --server, or --context cannot be used with --all.",
            )
        server_url = None
    else:
        server_url = invocation.resolve_target(server).server_url
    return auth_commands.logout(
        server_url=server_url,
        all_servers=all,
        credential_store=invocation.credential_store,
    )


@_register(
    app,
    _spec(
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
    invocation = _invocation()
    return await diagnostics.status(
        target=invocation.resolve_target(),
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
    )


@_register(
    app,
    _spec(
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
    invocation = _invocation()
    return await diagnostics.info(
        target=invocation.resolve_target(),
        credential_store=invocation.credential_store,
        timeout=invocation.request_timeout,
    )


@_register(
    app,
    _spec(
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
    invocation = _invocation()
    return await diagnostics.doctor(
        config_store=invocation.config_store,
        credential_store=invocation.credential_store,
        explicit_server=invocation.server,
        context_name=invocation.context_name,
        timeout=invocation.request_timeout,
    )


@_register(
    app,
    _spec(
        ("version",),
        "Show the installed Kitaru version.",
        offline=True,
    ),
)
def version() -> CommandResult:
    """Show the installed package version without local or network access."""
    return CommandResult(item={"version": diagnostics.package_version()})


@_register(
    app,
    _spec(
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


@_register(
    config_app,
    _spec(
        ("config", "list"),
        "List allowlisted CLI preferences with effective values and sources.",
        side_effects=("reads_local_file",),
        errors=("invalid_configuration", "internal_error"),
    ),
)
def config_list() -> CommandResult:
    """List the only Stage 1 persisted CLI preference."""
    store = _invocation().config_store
    config = store.load()
    source = "persisted" if store.path.exists() else "default"
    return CommandResult(
        items=[
            {
                "key": "cli.machine_mode",
                "value": config.cli.machine_mode,
                "source": source,
            }
        ]
    )


@_register(
    config_app,
    _spec(
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
    store = _invocation().config_store
    config = store.load()
    return CommandResult(
        item={
            "key": key,
            "value": config.cli.machine_mode,
            "source": "persisted" if store.path.exists() else "default",
        }
    )


@_register(
    config_app,
    _spec(
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
    config = _invocation().config_store.set_machine_mode(parsed)
    return CommandResult(
        item={"key": key, "value": config.cli.machine_mode, "source": "persisted"}
    )


@_register(
    config_app,
    _spec(
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


@_register(
    context_app,
    _spec(
        ("context", "add"),
        "Add or update a named server context without activating it.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "Context name."),
            ParameterSpec("SERVER", "URL", "argument", True, "Server URL."),
        ),
        read_only=False,
        side_effects=("reads_local_file", "writes_local_config"),
        idempotency="idempotent replacement",
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
def context_add(name: str, server: str, /) -> CommandResult:
    """Add or replace one named context."""
    config = _invocation().config_store.add_context(name, server)
    return CommandResult(
        item={
            "name": name,
            "server_url": config.contexts[name].server_url,
            "active": config.active_context == name,
        }
    )


@_register(
    context_app,
    _spec(
        ("context", "list"),
        "List named contexts and the active selection.",
        side_effects=("reads_local_file",),
        errors=("invalid_configuration", "internal_error"),
    ),
)
def context_list() -> CommandResult:
    """List contexts in stable name order."""
    config = _invocation().config_store.load()
    return CommandResult(
        items=[
            {
                "name": name,
                "server_url": context.server_url,
                "active": config.active_context == name,
            }
            for name, context in sorted(config.contexts.items())
        ]
    )


@_register(
    context_app,
    _spec(
        ("context", "get"),
        "Get a named context, defaulting to the active context.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", False, "Context name."),
        ),
        side_effects=("reads_local_file",),
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "not_found",
            "internal_error",
        ),
    ),
)
def context_get(name: str | None = None, /) -> CommandResult:
    """Get an exact named context or the active context."""
    config = _invocation().config_store.load()
    if name is not None:
        validate_context_name(name)
    selected = name or config.active_context
    if selected is None:
        raise CLIError("invalid_configuration", "No active context is selected.")
    context = config.contexts.get(selected)
    if context is None:
        raise CLIError("not_found", f"Context {selected!r} does not exist.")
    return CommandResult(
        item={
            "name": selected,
            "server_url": context.server_url,
            "active": config.active_context == selected,
        }
    )


@_register(
    context_app,
    _spec(
        ("context", "use"),
        "Select an existing context.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "Context name."),
        ),
        read_only=False,
        side_effects=("reads_local_file", "writes_local_config"),
        idempotency="idempotent",
        errors=("invalid_arguments", "invalid_configuration", "internal_error"),
    ),
)
def context_use(name: str, /) -> CommandResult:
    """Select an exact existing context."""
    config = _invocation().config_store.use_context(name)
    return CommandResult(
        item={
            "name": name,
            "server_url": config.contexts[name].server_url,
            "active": True,
        }
    )


@_register(
    context_app,
    _spec(
        ("context", "remove"),
        "Remove a context without deleting its credentials.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "Context name."),
            ParameterSpec(
                "--force", "boolean", "option", False, "Remove the active context."
            ),
        ),
        read_only=False,
        side_effects=("reads_local_file", "writes_local_config"),
        idempotency="not_found after first removal",
        errors=(
            "invalid_arguments",
            "invalid_configuration",
            "not_found",
            "conflict",
            "internal_error",
        ),
    ),
)
def context_remove(name: str, /, *, force: bool = False) -> CommandResult:
    """Remove one exact context and optionally clear the active pointer."""
    config = _invocation().config_store.remove_context(name, force=force)
    return CommandResult(
        item={
            "name": name,
            "removed": True,
            "active_context": config.active_context,
            "credentials_changed": False,
        }
    )


_COLLECTION_READ_ERRORS = (
    "invalid_arguments",
    "invalid_configuration",
    "authentication_failed",
    "network_error",
    "internal_error",
)
_UUID_READ_ERRORS = (*_COLLECTION_READ_ERRORS, "not_found")
_ASSET_READ_ERRORS = (*_UUID_READ_ERRORS, "conflict")
_ASSET_WRITE_ERRORS = (*_ASSET_READ_ERRORS, "partial_failure")
_JOB_WAIT_ERRORS = ("timeout", "remote_failed", "remote_canceled")
_LIST_PARAMETERS = (
    ParameterSpec("--size", "integer", "option", False, "Items per page."),
    ParameterSpec("--cursor", "string", "option", False, "Page cursor."),
    ParameterSpec("--sort", "field:direction", "option", False, "Server sort order."),
    ParameterSpec("--filter", "JSON", "option", False, "Server filter expression."),
)
_VERSION_LIST_PARAMETERS = _LIST_PARAMETERS[:-1]
_WAIT_PARAMETERS = (
    ParameterSpec(
        "--wait", "boolean", "option", False, "Wait for remote work settlement."
    ),
    ParameterSpec(
        "--interval",
        "positive float",
        "option",
        False,
        "Polling interval; requires --wait.",
    ),
    ParameterSpec(
        "--timeout",
        "positive float",
        "option",
        False,
        "Local wait timeout; requires --wait.",
    ),
)
_AGENT_SOURCE_PARAMETERS = (
    ParameterSpec("--spec", "path", "option", False, "YAML or JSON spec document."),
    ParameterSpec(
        "--entrypoint",
        "MODULE:ATTRIBUTE",
        "option",
        False,
        "Zero-argument Python wrapper.",
    ),
    ParameterSpec(
        "--command", "string", "option", False, "Shell command stored as supplied."
    ),
    ParameterSpec(
        "--display-version", "string", "option", False, "Human-readable version."
    ),
    ParameterSpec(
        "--working-dir", "path", "option", False, "Worker process directory."
    ),
    ParameterSpec(
        "--env", "KEY=VALUE[]", "option", False, "Non-secret process environment."
    ),
    ParameterSpec("--secret-id", "UUID[]", "option", False, "Server secret IDs."),
    ParameterSpec(
        "--timeout-seconds", "positive integer", "option", False, "Process timeout."
    ),
    ParameterSpec("--tool", "string[]", "option", False, "Declared tools."),
    ParameterSpec("--mcp-server", "string[]", "option", False, "Declared MCP servers."),
    ParameterSpec("--skill", "string[]", "option", False, "Declared skills."),
)
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


def _open_asset_client():
    """Open an SDK client for the invocation's resolved target."""
    invocation = _invocation()
    target = invocation.resolve_target()
    return registration.open_client(
        target.server_url, invocation.credential_store, invocation.request_timeout
    )


def _agent_direct_values(
    *values: Any,
    env: list[str] | None,
    secret_id: list[uuid.UUID] | None,
    tool: list[str] | None,
    mcp_server: list[str] | None,
    skill: list[str] | None,
) -> bool:
    """Return whether any direct agent option was supplied alongside a spec."""
    return any(value is not None for value in values) or any(
        value is not None for value in (env, secret_id, tool, mcp_server, skill)
    )


@_register(
    agent_app,
    _spec(
        ("agent", "register"),
        "Create an agent and its first mutable version.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New agent name."),
            ParameterSpec(
                "--description", "string", "option", False, "Parent description."
            ),
            ParameterSpec(
                "--version-description",
                "string",
                "option",
                False,
                "Version description.",
            ),
            *_AGENT_SOURCE_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def agent_register(
    name: str,
    /,
    *,
    spec: Path | None = None,
    entrypoint: str | None = None,
    command: str | None = None,
    description: str | None = None,
    version_description: str | None = None,
    display_version: str | None = None,
    working_dir: str | None = None,
    env: list[str] | None = None,
    secret_id: list[uuid.UUID] | None = None,
    timeout_seconds: int | None = None,
    tool: list[str] | None = None,
    mcp_server: list[str] | None = None,
    skill: list[str] | None = None,
) -> CommandResult:
    """Create a new agent parent and initial version."""
    if spec is not None:
        if _agent_direct_values(
            entrypoint,
            command,
            description,
            version_description,
            display_version,
            working_dir,
            timeout_seconds,
            env=env,
            secret_id=secret_id,
            tool=tool,
            mcp_server=mcp_server,
            skill=skill,
        ):
            raise CLIError(
                "invalid_arguments", "--spec conflicts with direct agent options."
            )
        parent_request, version_request = registration.load_agent_register_spec(
            name, spec
        )
    else:
        parent_request, version_request = registration.build_agent_requests(
            name,
            command=command,
            entrypoint=entrypoint,
            description=description,
            version_description=version_description,
            display_version=display_version,
            working_dir=working_dir,
            env=env,
            secret_ids=secret_id,
            timeout_seconds=timeout_seconds,
            tools=tool,
            mcp_servers=mcp_server,
            skills=skill,
        )
    async with _open_asset_client() as client:
        return await registration.register_agent(
            client, parent_request, version_request
        )


@_register(
    agent_app,
    _spec(
        ("agent", "list"),
        "List agents.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
    ),
)
async def agent_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of agents."""
    params = registration.list_params(
        "agent", size=size, cursor=cursor, sort=sort, filter=filter
    )
    async with _open_asset_client() as client:
        return registration.page_result(await client.agents.list(params), size=size)


@_register(
    agent_app,
    _spec(
        ("agent", "get"),
        "Get an agent by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def agent_get(agent: str, /) -> CommandResult:
    """Get one exact agent."""
    async with _open_asset_client() as client:
        item = await registration.resolve_asset(client.agents, agent, "Agent")
        return CommandResult(item=item.model_dump(mode="json"))


@_register(
    agent_version_app,
    _spec(
        ("agent", "version", "register"),
        "Create the next mutable version of an existing agent.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Version description."
            ),
            *_AGENT_SOURCE_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def agent_version_register(
    agent: str,
    /,
    *,
    spec: Path | None = None,
    entrypoint: str | None = None,
    command: str | None = None,
    description: str | None = None,
    display_version: str | None = None,
    working_dir: str | None = None,
    env: list[str] | None = None,
    secret_id: list[uuid.UUID] | None = None,
    timeout_seconds: int | None = None,
    tool: list[str] | None = None,
    mcp_server: list[str] | None = None,
    skill: list[str] | None = None,
) -> CommandResult:
    """Create the next server-assigned version of an exact agent."""
    if spec is not None:
        if _agent_direct_values(
            entrypoint,
            command,
            description,
            display_version,
            working_dir,
            timeout_seconds,
            env=env,
            secret_id=secret_id,
            tool=tool,
            mcp_server=mcp_server,
            skill=skill,
        ):
            raise CLIError(
                "invalid_arguments", "--spec conflicts with direct agent options."
            )
        request = registration.load_agent_version_spec(spec)
    else:
        request = registration.build_agent_version_request(
            command=command,
            entrypoint=entrypoint,
            description=description,
            display_version=display_version,
            working_dir=working_dir,
            env=env,
            secret_ids=secret_id,
            timeout_seconds=timeout_seconds,
            tools=tool,
            mcp_servers=mcp_server,
            skills=skill,
        )
    async with _open_asset_client() as client:
        return await registration.register_agent_version(client, agent, request)


@_register(
    agent_version_app,
    _spec(
        ("agent", "version", "list"),
        "List versions of an exact agent.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
            *_VERSION_LIST_PARAMETERS,
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def agent_version_list(
    agent: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of agent versions."""
    params = registration.version_list_params(size=size, cursor=cursor, sort=sort)
    async with _open_asset_client() as client:
        parent = await registration.resolve_asset(client.agents, agent, "Agent")
        page = await client.agents.list_versions(parent.id, params)
        return registration.page_result(page, size=size)


@_register(
    agent_version_app,
    _spec(
        ("agent", "version", "get"),
        "Get an agent version by exact PARENT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "AGENT@VERSION",
                "reference",
                "argument",
                True,
                "Exact version reference.",
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def agent_version_get(agent_version: str, /) -> CommandResult:
    """Get one exact agent version, accepting @latest for reads."""
    async with _open_asset_client() as client:
        _, version = await registration.get_agent_version(client, agent_version)
        return CommandResult(item=version.model_dump(mode="json"))


@_register(
    cohort_app,
    _spec(
        ("cohort", "create"),
        "Create a cohort bound to one exact agent.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New cohort name."),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Exact agent UUID or case-sensitive name.",
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Cohort description."
            ),
            ParameterSpec(
                "--metadata", "JSON object", "option", False, "Cohort metadata."
            ),
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_create(
    name: str,
    /,
    *,
    agent: str,
    description: str | None = None,
    metadata: str | None = None,
) -> CommandResult:
    """Create a cohort bound to one exact agent."""
    async with _open_asset_client() as client:
        return await cohorts.create_cohort(
            client,
            name,
            agent=agent,
            description=description,
            metadata=metadata,
        )


@_register(
    cohort_app,
    _spec(
        ("cohort", "list"),
        "List cohorts.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
    ),
)
async def cohort_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of cohorts."""
    async with _open_asset_client() as client:
        return await cohorts.list_cohorts(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    cohort_app,
    _spec(
        ("cohort", "get"),
        "Get a cohort by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def cohort_get(cohort: str, /) -> CommandResult:
    """Get one exact cohort."""
    async with _open_asset_client() as client:
        return await cohorts.get_cohort(client, cohort)


@_register(
    cohort_app,
    _spec(
        ("cohort", "update"),
        "Update selected fields on an exact cohort.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec("--name", "string", "option", False, "New cohort name."),
            ParameterSpec(
                "--description", "string", "option", False, "New description."
            ),
            ParameterSpec(
                "--clear-description",
                "boolean",
                "option",
                False,
                "Clear the description.",
            ),
            ParameterSpec(
                "--metadata",
                "JSON object",
                "option",
                False,
                "Replacement metadata; {} clears it.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_update(
    cohort: str,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
    metadata: str | None = None,
) -> CommandResult:
    """Update selected fields on one exact cohort."""
    async with _open_asset_client() as client:
        return await cohorts.update_cohort(
            client,
            cohort,
            name=name,
            description=description,
            clear_description=clear_description,
            metadata=metadata,
        )


@_register(
    cohort_app,
    _spec(
        ("cohort", "delete"),
        "Delete a cohort and all of its versions.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec(
                "--force",
                "boolean",
                "option",
                False,
                "Confirm cascading remote deletion.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_delete(cohort: str, /, *, force: bool = False) -> CommandResult:
    """Delete one cohort and all of its versions."""
    async with _open_asset_client() as client:
        return await cohorts.delete_cohort(client, cohort, force=force)


@_register(
    cohort_version_app,
    _spec(
        ("cohort", "version", "create"),
        "Create the next immutable version from a membership delta.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec(
                "--add-session",
                "UUID[]",
                "option",
                False,
                "Ordered session IDs to add.",
            ),
            ParameterSpec(
                "--remove-session",
                "UUID[]",
                "option",
                False,
                "Ordered session IDs to remove.",
            ),
            ParameterSpec(
                "--display-version",
                "string",
                "option",
                False,
                "Human-readable version.",
            ),
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_server_assigned_version",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_version_create(
    cohort: str,
    /,
    *,
    add_session: list[uuid.UUID] | None = None,
    remove_session: list[uuid.UUID] | None = None,
    display_version: str | None = None,
) -> CommandResult:
    """Create an immutable version from an ordered membership delta."""
    async with _open_asset_client() as client:
        return await cohorts.create_cohort_version(
            client,
            cohort,
            add_session_ids=add_session,
            remove_session_ids=remove_session,
            display_version=display_version,
        )


@_register(
    cohort_version_app,
    _spec(
        ("cohort", "version", "list"),
        "List immutable versions of an exact cohort.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            *_VERSION_LIST_PARAMETERS,
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def cohort_version_list(
    cohort: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of immutable cohort versions."""
    async with _open_asset_client() as client:
        return await cohorts.list_cohort_versions(
            client, cohort, size=size, cursor=cursor, sort=sort
        )


@_register(
    cohort_version_app,
    _spec(
        ("cohort", "version", "get"),
        "Get a version by UUID or exact COHORT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def cohort_version_get(version: str, /) -> CommandResult:
    """Get one exact immutable cohort version."""
    async with _open_asset_client() as client:
        return await cohorts.get_cohort_version_result(client, version)


@_register(
    cohort_version_app,
    _spec(
        ("cohort", "version", "update"),
        "Update a cohort version's display version.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
            ),
            ParameterSpec(
                "--display-version",
                "string",
                "option",
                False,
                "New human-readable version.",
            ),
            ParameterSpec(
                "--clear-display-version",
                "boolean",
                "option",
                False,
                "Clear the human-readable version.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_version_update(
    version: str,
    /,
    *,
    display_version: str | None = None,
    clear_display_version: bool = False,
) -> CommandResult:
    """Update one cohort version's display version."""
    async with _open_asset_client() as client:
        return await cohorts.update_cohort_version(
            client,
            version,
            display_version=display_version,
            clear_display_version=clear_display_version,
        )


@_register(
    cohort_version_app,
    _spec(
        ("cohort", "version", "delete"),
        "Delete one immutable cohort version without reusing its number.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
            ),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def cohort_version_delete(
    version: str, /, *, force: bool = False
) -> CommandResult:
    """Delete one exact immutable cohort version."""
    async with _open_asset_client() as client:
        return await cohorts.delete_cohort_version(client, version, force=force)


@_register(
    experiment_app,
    _spec(
        ("experiment", "create"),
        "Create an experiment with exact evaluator versions.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New experiment name."),
            ParameterSpec(
                "--description", "string", "option", False, "Experiment description."
            ),
            ParameterSpec(
                "--override",
                "JSON object",
                "option",
                False,
                "Replay override applied to every run.",
            ),
            ParameterSpec(
                "--tool-policy",
                "JSON object",
                "option",
                False,
                "Tool policy applied to every run; omitted uses the server default.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                True,
                "Exact EVALUATOR@VERSION references.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def experiment_create(
    name: str,
    /,
    *,
    evaluator: list[str],
    description: str | None = None,
    override: str | None = None,
    tool_policy: str | None = None,
    evaluator_params: list[str] | None = None,
) -> CommandResult:
    """Create an experiment with exact evaluator versions."""
    async with _open_asset_client() as client:
        return await experiments.create_experiment(
            client,
            name,
            description=description,
            override=override,
            tool_policy=tool_policy,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
        )


@_register(
    experiment_app,
    _spec(
        ("experiment", "list"),
        "List experiments.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
    ),
)
async def experiment_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of experiments."""
    async with _open_asset_client() as client:
        return await experiments.list_experiments(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    experiment_app,
    _spec(
        ("experiment", "get"),
        "Get an experiment by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def experiment_get(experiment: str, /) -> CommandResult:
    """Get one exact experiment."""
    async with _open_asset_client() as client:
        return await experiments.get_experiment(client, experiment)


@_register(
    experiment_app,
    _spec(
        ("experiment", "update"),
        "Update selected fields on an exact experiment.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec("--name", "string", "option", False, "New experiment name."),
            ParameterSpec(
                "--description", "string", "option", False, "New description."
            ),
            ParameterSpec(
                "--clear-description",
                "boolean",
                "option",
                False,
                "Clear the description.",
            ),
            ParameterSpec(
                "--override",
                "JSON object",
                "option",
                False,
                "Replacement replay override.",
            ),
            ParameterSpec(
                "--clear-override",
                "boolean",
                "option",
                False,
                "Clear the replay override.",
            ),
            ParameterSpec(
                "--tool-policy",
                "JSON object",
                "option",
                False,
                "Replacement tool policy; cannot be cleared.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                False,
                "Exact evaluator versions replacing the complete list.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def experiment_update(
    experiment: str,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
    override: str | None = None,
    clear_override: bool = False,
    tool_policy: str | None = None,
    evaluator: list[str] | None = None,
    evaluator_params: list[str] | None = None,
) -> CommandResult:
    """Update selected fields on one exact experiment."""
    async with _open_asset_client() as client:
        return await experiments.update_experiment(
            client,
            experiment,
            name=name,
            description=description,
            clear_description=clear_description,
            override=override,
            clear_override=clear_override,
            tool_policy=tool_policy,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
        )


@_register(
    experiment_app,
    _spec(
        ("experiment", "delete"),
        "Delete an experiment and preserve server conflict behavior.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def experiment_delete(
    experiment: str, /, *, force: bool = False
) -> CommandResult:
    """Delete one exact experiment."""
    async with _open_asset_client() as client:
        return await experiments.delete_experiment(client, experiment, force=force)


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "start"),
        "Start an experiment run and optionally wait for terminal settlement.",
        parameters=(
            ParameterSpec(
                "EXPERIMENT",
                "reference",
                "argument",
                True,
                "Experiment UUID or name.",
            ),
            ParameterSpec(
                "--cohort-version",
                "UUID",
                "option",
                True,
                "Exact cohort-version ID.",
            ),
            ParameterSpec(
                "--agent",
                "AGENT@VERSION",
                "option",
                True,
                "Exact agent version reference.",
            ),
            ParameterSpec(
                "--evaluate-baselines",
                "boolean",
                "option",
                False,
                "Also score each baseline session.",
            ),
            *_WAIT_PARAMETERS,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_run_created_per_request",
        errors=(*_ASSET_WRITE_ERRORS, *_JOB_WAIT_ERRORS),
        streams=True,
    ),
)
async def experiment_run_start(
    experiment: str,
    /,
    *,
    cohort_version: uuid.UUID,
    agent: str,
    evaluate_baselines: bool = False,
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
) -> CommandResult:
    """Start one exact experiment run and optionally wait for it."""
    async with _open_asset_client() as client:
        return await experiment_runs.start_run(
            client,
            experiment,
            cohort_version_id=cohort_version,
            agent_reference=agent,
            evaluate_baselines=evaluate_baselines,
            wait=wait,
            interval=interval,
            timeout=timeout,
        )


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "list"),
        "List experiment runs without deriving state from their jobs.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
    ),
)
async def experiment_run_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of experiment runs."""
    async with _open_asset_client() as client:
        return await experiment_runs.list_runs(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "get"),
        "Get an experiment run and its aggregate progress by exact UUID.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
        ),
        errors=_UUID_READ_ERRORS,
    ),
)
async def experiment_run_get(run: uuid.UUID, /) -> CommandResult:
    """Get one experiment run without mapping terminal status to an error."""
    async with _open_asset_client() as client:
        return await experiment_runs.get_run(client, run)


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "jobs"),
        "List one page of replay jobs backing an experiment run.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            *_LIST_PARAMETERS,
        ),
        errors=_UUID_READ_ERRORS,
    ),
)
async def experiment_run_jobs(
    run: uuid.UUID,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of replay jobs for a run."""
    async with _open_asset_client() as client:
        return await experiment_runs.list_run_jobs(
            client,
            run,
            size=size,
            cursor=cursor,
            sort=sort,
            filter=filter,
        )


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "watch"),
        "Poll a run until it completes, fails, or is canceled without changing it.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            ParameterSpec(
                "--interval", "positive float", "option", False, "Run polling interval."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Local wait timeout."
            ),
        ),
        errors=(*_ASSET_READ_ERRORS, *_JOB_WAIT_ERRORS),
        streams=True,
    ),
)
async def experiment_run_watch(
    run: uuid.UUID,
    /,
    *,
    interval: float = 2.0,
    timeout: float | None = None,
) -> CommandResult:
    """Watch one run locally without changing remote work."""
    async with _open_asset_client() as client:
        return await experiment_runs.watch_run(
            client, run, interval=interval, timeout=timeout
        )


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "cancel"),
        "Request run cancellation once without waiting for settlement.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="server_rejects_settled_runs",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def experiment_run_cancel(run: uuid.UUID, /) -> CommandResult:
    """Request run cancellation and return after server acceptance."""
    async with _open_asset_client() as client:
        return await experiment_runs.cancel_run(client, run)


@_register(
    experiment_run_app,
    _spec(
        ("experiment", "run", "delete"),
        "Delete a run and immediately delete all of its replay jobs and tasks.",
        parameters=(
            ParameterSpec("RUN", "UUID", "argument", True, "Experiment-run ID."),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=_ASSET_WRITE_ERRORS,
    ),
)
async def experiment_run_delete(
    run: uuid.UUID, /, *, force: bool = False
) -> CommandResult:
    """Delete one run and its replay jobs and tasks immediately."""
    async with _open_asset_client() as client:
        return await experiment_runs.delete_run(client, run, force=force)


def _plugin_register_parameters(kind: str) -> tuple[ParameterSpec, ...]:
    """Build metadata for one plugin register command."""
    parent = [
        ParameterSpec("NAME", "string", "argument", True, f"New {kind} name."),
        ParameterSpec(
            "--description", "string", "option", False, "Parent description."
        ),
        ParameterSpec("--metadata", "JSON object", "option", False, "Parent metadata."),
    ]
    if kind == "importer":
        parent.append(
            ParameterSpec("--provider", "string", "option", False, "Source provider.")
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
    display_version: str | None,
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
    )
    async with _open_asset_client() as client:
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
) -> CommandResult:
    """Run one kind-specific version registration."""
    source = registration.prepare_plugin_source(
        script=script, package=package, entrypoint=entrypoint
    )
    async with _open_asset_client() as client:
        return await registration.register_plugin_version(
            client,
            kind=kind,
            reference=reference,
            source=source,
            display_version=display_version,
        )


async def _list_plugin_command(
    kind: Literal["importer", "evaluator"],
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
    async with _open_asset_client() as client:
        resource = client.importers if kind == "importer" else client.evaluators
        return registration.page_result(await resource.list(params), size=size)


async def _get_plugin_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer or evaluator."""
    async with _open_asset_client() as client:
        resource = client.importers if kind == "importer" else client.evaluators
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
    """List one server page of importer or evaluator versions."""
    params = registration.version_list_params(size=size, cursor=cursor, sort=sort)
    async with _open_asset_client() as client:
        resource = client.importers if kind == "importer" else client.evaluators
        parent = await registration.resolve_asset(resource, reference, kind.title())
        return registration.page_result(
            await resource.list_versions(parent.id, params), size=size
        )


async def _get_plugin_version_command(kind: str, reference: str) -> CommandResult:
    """Get one exact importer or evaluator version."""
    async with _open_asset_client() as client:
        resource = client.importers if kind == "importer" else client.evaluators
        _, item = await registration.get_plugin_version(
            resource, reference, kind.title()
        )
        return CommandResult(item=item.model_dump(mode="json"))


@_register(
    importer_app,
    _spec(
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


@_register(
    importer_app,
    _spec(
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


@_register(
    importer_app,
    _spec(
        ("importer", "register"),
        "Create an importer and its first version.",
        parameters=_plugin_register_parameters("importer"),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=_ASSET_WRITE_ERRORS,
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
        display_version=display_version,
    )


@_register(
    importer_app,
    _spec(
        ("importer", "list"),
        "List importers.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
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


@_register(
    importer_app,
    _spec(
        ("importer", "get"),
        "Get an importer by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def importer_get(importer: str, /) -> CommandResult:
    """Get one exact importer."""
    return await _get_plugin_command("importer", importer)


@_register(
    importer_version_app,
    _spec(
        ("importer", "version", "register"),
        "Create the next version of an existing importer.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
            *_PLUGIN_SOURCE_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=_ASSET_WRITE_ERRORS,
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
) -> CommandResult:
    """Create the next importer version."""
    return await _register_plugin_version_command(
        "importer",
        importer,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
    )


@_register(
    importer_version_app,
    _spec(
        ("importer", "version", "list"),
        "List versions of an exact importer.",
        parameters=(
            ParameterSpec(
                "IMPORTER", "reference", "argument", True, "Importer UUID or name."
            ),
            *_VERSION_LIST_PARAMETERS,
        ),
        errors=_ASSET_READ_ERRORS,
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


@_register(
    importer_version_app,
    _spec(
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
        errors=_ASSET_READ_ERRORS,
    ),
)
async def importer_version_get(importer_version: str, /) -> CommandResult:
    """Get one exact importer version, accepting @latest for reads."""
    return await _get_plugin_version_command("importer", importer_version)


@_register(
    evaluator_app,
    _spec(
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


@_register(
    evaluator_app,
    _spec(
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


@_register(
    evaluator_app,
    _spec(
        ("evaluator", "register"),
        "Create an evaluator and its first version.",
        parameters=_plugin_register_parameters("evaluator"),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=_ASSET_WRITE_ERRORS,
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
        provider=None,
        metadata=metadata,
        display_version=display_version,
    )


@_register(
    evaluator_app,
    _spec(
        ("evaluator", "list"),
        "List evaluators.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
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


@_register(
    evaluator_app,
    _spec(
        ("evaluator", "get"),
        "Get an evaluator by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def evaluator_get(evaluator: str, /) -> CommandResult:
    """Get one exact evaluator."""
    return await _get_plugin_command("evaluator", evaluator)


@_register(
    evaluator_version_app,
    _spec(
        ("evaluator", "version", "register"),
        "Create the next version of an existing evaluator.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
            *_PLUGIN_SOURCE_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "uploads_data", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=_ASSET_WRITE_ERRORS,
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
) -> CommandResult:
    """Create the next evaluator version."""
    return await _register_plugin_version_command(
        "evaluator",
        evaluator,
        script=script,
        package=package,
        entrypoint=entrypoint,
        display_version=display_version,
    )


@_register(
    evaluator_version_app,
    _spec(
        ("evaluator", "version", "list"),
        "List versions of an exact evaluator.",
        parameters=(
            ParameterSpec(
                "EVALUATOR", "reference", "argument", True, "Evaluator UUID or name."
            ),
            *_VERSION_LIST_PARAMETERS,
        ),
        errors=_ASSET_READ_ERRORS,
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


@_register(
    evaluator_version_app,
    _spec(
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
        errors=_ASSET_READ_ERRORS,
    ),
)
async def evaluator_version_get(evaluator_version: str, /) -> CommandResult:
    """Get one exact evaluator version, accepting @latest for reads."""
    return await _get_plugin_version_command("evaluator", evaluator_version)


@_register(
    session_app,
    _spec(
        ("session", "import"),
        "Upload a local payload and create an import job.",
        parameters=(
            ParameterSpec("FILE", "path", "argument", True, "Local payload file."),
            ParameterSpec(
                "--importer",
                "reference",
                "option",
                True,
                "Exact IMPORTER@VERSION reference.",
            ),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Exact AGENT@VERSION reference.",
            ),
            ParameterSpec(
                "--params", "JSON object", "option", False, "Importer parameters."
            ),
            ParameterSpec(
                "--media-type",
                "string",
                "option",
                False,
                "Payload media type.",
            ),
            *_WAIT_PARAMETERS,
        ),
        read_only=False,
        side_effects=("uploads_data", "creates_remote_state"),
        idempotency="blob_content_deduped_import_job_created_per_request",
        errors=(
            *_ASSET_WRITE_ERRORS,
            *_JOB_WAIT_ERRORS,
        ),
        streams=True,
    ),
)
async def session_import(
    file: Path,
    /,
    *,
    importer: str,
    agent: str,
    params: str | None = None,
    media_type: str = "application/octet-stream",
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
) -> CommandResult:
    """Upload a local payload and create one import job."""
    async with _open_asset_client() as client:
        return await sessions.import_sessions(
            client,
            file,
            importer=importer,
            agent=agent,
            params=params,
            media_type=media_type,
            wait=wait,
            interval=interval,
            timeout=timeout,
        )


@_register(
    session_app,
    _spec(
        ("session", "list"),
        "List sessions.",
        parameters=_LIST_PARAMETERS,
        errors=_COLLECTION_READ_ERRORS,
    ),
)
async def session_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of sessions."""
    async with _open_asset_client() as client:
        return await sessions.list_sessions(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    session_app,
    _spec(
        ("session", "get"),
        "Get a session by exact UUID.",
        parameters=(ParameterSpec("SESSION", "UUID", "argument", True, "Session ID."),),
        errors=_UUID_READ_ERRORS,
    ),
)
async def session_get(session: uuid.UUID, /) -> CommandResult:
    """Get one session by exact UUID."""
    async with _open_asset_client() as client:
        return await sessions.get_session(client, session)


@_register(
    session_app,
    _spec(
        ("session", "nodes"),
        "List a session's nodes in index order.",
        parameters=(
            ParameterSpec("SESSION", "UUID", "argument", True, "Session ID."),
            ParameterSpec("--size", "integer", "option", False, "Items per page."),
            ParameterSpec("--cursor", "string", "option", False, "Page cursor."),
            ParameterSpec(
                "--include-payloads",
                "boolean",
                "option",
                False,
                "Include node inputs, outputs, and attributes.",
            ),
        ),
        errors=_UUID_READ_ERRORS,
    ),
)
async def session_nodes(
    session: uuid.UUID,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    include_payloads: bool = False,
) -> CommandResult:
    """List one server page of a session's nodes."""
    async with _open_asset_client() as client:
        return await sessions.list_session_nodes(
            client,
            session,
            size=size,
            cursor=cursor,
            include_payloads=include_payloads,
        )


@_register(
    session_app,
    _spec(
        ("session", "evaluate"),
        "Evaluate explicit sessions with exact evaluator versions.",
        parameters=(
            ParameterSpec(
                "SESSION", "UUID[]", "argument", False, "Explicit session IDs."
            ),
            ParameterSpec(
                "--sessions-file",
                "path",
                "option",
                False,
                "UTF-8 file with one session UUID per nonblank line.",
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                True,
                "Exact EVALUATOR@VERSION references.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            *_WAIT_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_job_created_per_request",
        errors=(
            *_ASSET_READ_ERRORS,
            *_JOB_WAIT_ERRORS,
        ),
        streams=True,
    ),
)
async def session_evaluate(
    session: list[str] | None = None,
    /,
    *,
    sessions_file: Path | None = None,
    evaluator: list[str],
    evaluator_params: list[str] | None = None,
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
) -> CommandResult:
    """Evaluate explicit sessions with exact evaluator versions."""
    async with _open_asset_client() as client:
        return await evaluations.evaluate_sessions(
            client,
            session,
            sessions_file=sessions_file,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            wait=wait,
            interval=interval,
            timeout=timeout,
        )


@_register(
    evaluation_app,
    _spec(
        ("evaluation", "list"),
        "List stored evaluations.",
        parameters=_LIST_PARAMETERS,
        errors=_COLLECTION_READ_ERRORS,
    ),
)
async def evaluation_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of stored evaluations."""
    async with _open_asset_client() as client:
        return await evaluations.list_evaluations(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    evaluation_app,
    _spec(
        ("evaluation", "get"),
        "Get a stored evaluation by exact UUID.",
        parameters=(
            ParameterSpec("EVALUATION", "UUID", "argument", True, "Evaluation ID."),
        ),
        errors=_UUID_READ_ERRORS,
    ),
)
async def evaluation_get(evaluation: uuid.UUID, /) -> CommandResult:
    """Get one stored evaluation by exact UUID."""
    async with _open_asset_client() as client:
        return await evaluations.get_evaluation(client, evaluation)


_WORKER_START_PARAMETERS = (
    ParameterSpec("--name", "string", "option", False, "Ephemeral worker name."),
    ParameterSpec(
        "--kinds", "agent|evaluator|importer[]", "option", False, "Task kinds to claim."
    ),
    ParameterSpec(
        "--selector",
        "KEY=VALUE[,VALUE][]|JSON[]",
        "option",
        False,
        "Task label selectors, combined by conjunction.",
    ),
    ParameterSpec("--job-id", "UUID", "option", False, "Restrict claims to one job."),
    ParameterSpec(
        "--concurrency", "positive integer", "option", False, "Maximum held tasks."
    ),
    ParameterSpec(
        "--claim-batch-size",
        "positive integer",
        "option",
        False,
        "Maximum tasks per claim.",
    ),
    ParameterSpec(
        "--poll-interval", "positive float", "option", False, "Idle polling interval."
    ),
    ParameterSpec(
        "--heartbeat-interval", "positive float", "option", False, "Heartbeat interval."
    ),
    ParameterSpec(
        "--timeout", "positive float", "option", False, "Worker lifetime in seconds."
    ),
    ParameterSpec(
        "--blob-cache-root", "path", "option", False, "Blob cache directory."
    ),
    ParameterSpec(
        "--payload-cache-root", "path", "option", False, "Payload cache directory."
    ),
    ParameterSpec(
        "--metadata", "KEY=VALUE[]", "option", False, "Worker registration metadata."
    ),
)


@_register(
    worker_app,
    _spec(
        ("worker", "start"),
        "Run a generic local worker in the foreground without durable provider "
        "binding. The first SIGINT drains held tasks; a second exits immediately "
        "and may leave child processes running.",
        parameters=_WORKER_START_PARAMETERS,
        read_only=False,
        side_effects=(
            "reads_local_file",
            "creates_remote_state",
            "executes_local_code",
        ),
        idempotency="runtime_registration_upsert_by_name",
        errors=_ASSET_READ_ERRORS,
        streams=True,
    ),
)
async def worker_start(
    *,
    name: str | None = None,
    kinds: list[TaskKind] | None = None,
    selectors: Annotated[
        list[str] | None,
        Parameter(name="--selector", help="KEY=VALUE[,VALUE] or selector JSON."),
    ] = None,
    job_id: uuid.UUID | None = None,
    concurrency: int | None = None,
    claim_batch_size: int | None = None,
    poll_interval: float | None = None,
    heartbeat_interval: float | None = None,
    timeout: float | None = None,
    blob_cache_root: Path | None = None,
    payload_cache_root: Path | None = None,
    metadata: list[str] | None = None,
) -> CommandResult:
    """Launch the existing generic worker as one foreground process."""
    invocation = _invocation()
    target = invocation.resolve_target()
    return await workers.start_worker(
        target,
        invocation.credential_store,
        name=name,
        kinds=kinds,
        selectors=selectors,
        job_id=job_id,
        concurrency=concurrency,
        claim_batch_size=claim_batch_size,
        poll_interval=poll_interval,
        heartbeat_interval=heartbeat_interval,
        request_timeout=invocation.request_timeout,
        timeout=timeout,
        blob_cache_root=blob_cache_root,
        payload_cache_root=payload_cache_root,
        metadata=metadata,
    )


@_register(
    worker_app,
    _spec(
        ("worker", "list"),
        "List workers, describing each server record as live or stale.",
        parameters=_LIST_PARAMETERS,
        errors=_ASSET_READ_ERRORS,
    ),
)
async def worker_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "last_seen_at:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of worker records."""
    async with _open_asset_client() as client:
        return await workers.list_workers(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@_register(
    worker_app,
    _spec(
        ("worker", "get"),
        "Get a worker by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "WORKER", "reference", "argument", True, "Worker UUID or exact name."
            ),
        ),
        errors=_ASSET_READ_ERRORS,
    ),
)
async def worker_get(worker: str, /) -> CommandResult:
    """Get one exact worker record."""
    async with _open_asset_client() as client:
        return await workers.get_worker(client, worker)


_JOB_ERRORS = (
    "invalid_arguments",
    "invalid_configuration",
    "authentication_failed",
    "not_found",
    "conflict",
    "network_error",
    "internal_error",
)


@_register(
    job_app,
    _spec(
        ("job", "get"),
        "Get a job and optionally include a complete task snapshot.",
        parameters=(
            ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),
            ParameterSpec(
                "--tasks", "boolean", "option", False, "Include all job tasks."
            ),
        ),
        errors=_JOB_ERRORS,
    ),
)
async def job_get(job: uuid.UUID, /, *, tasks: bool = False) -> CommandResult:
    """Get one job and optionally paginate through all of its tasks."""
    async with _open_asset_client() as client:
        return await jobs.get_job(client, job, tasks=tasks)


@_register(
    job_app,
    _spec(
        ("job", "watch"),
        "Poll a job until it completes, fails, or is canceled without changing it.",
        parameters=(
            ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),
            ParameterSpec(
                "--interval", "positive float", "option", False, "Polling interval."
            ),
            ParameterSpec(
                "--timeout", "positive float", "option", False, "Local wait timeout."
            ),
        ),
        errors=(*_JOB_ERRORS, "timeout", "remote_failed", "remote_canceled"),
        streams=True,
    ),
)
async def job_watch(
    job: uuid.UUID,
    /,
    *,
    interval: float = 2.0,
    timeout: float | None = None,
) -> CommandResult:
    """Wait locally for a terminal job state without canceling remote work."""
    async with _open_asset_client() as client:
        return await jobs.watch_job(client, job, interval=interval, timeout=timeout)


@_register(
    job_app,
    _spec(
        ("job", "cancel"),
        "Request job cancellation once without waiting for settlement.",
        parameters=(ParameterSpec("JOB", "UUID", "argument", True, "Job ID."),),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="server_rejects_settled_jobs",
        errors=_JOB_ERRORS,
    ),
)
async def job_cancel(job: uuid.UUID, /) -> CommandResult:
    """Request cancellation and return immediately after server acceptance."""
    async with _open_asset_client() as client:
        return await jobs.cancel_job(client, job)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI with an explicit argument sequence.

    Args:
        argv: Arguments to parse. Defaults to the process argument vector.

    Returns:
        Process exit code.
    """
    tokens = list(sys.argv[1:] if argv is None else argv)
    try:
        result = app.meta(
            tokens,
            exit_on_error=False,
            print_error=False,
            result_action="return_value",
            backend="asyncio",
        )
        return int(result or 0)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except BrokenPipeError:
        return 0
    except CycloptsError as exception:
        requested = _extract_output(tokens)
        return _emit_early_error(
            CLIError("invalid_arguments", str(exception)),
            tokens=tokens,
            output=requested,
            machine="--machine" in tokens,
            non_interactive="--non-interactive" in tokens,
            debug="--debug" in tokens,
            traceback="--traceback" in tokens,
            exception=exception,
        )


def _emit_early_error(
    error: CLIError,
    *,
    tokens: Sequence[str],
    output: OutputMode,
    machine: bool,
    non_interactive: bool,
    debug: bool,
    traceback: bool,
    exception: BaseException | None = None,
) -> int:
    """Emit a global-option or parsing error before invocation bootstrap."""
    try:
        mode = resolve_output_mode(output, is_tty=sys.stdout.isatty())
    except CLIError:
        mode = "text" if sys.stdout.isatty() else "json"
    command = _guess_command(tokens)
    context = OutputContext(
        command=command,
        mode=mode,
        machine=machine or mode != "text" or not sys.stdout.isatty(),
        non_interactive=non_interactive or mode != "text",
        debug=debug,
        traceback=traceback or debug,
        stdout=sys.stdout,
        stderr=sys.stderr,
        rich=False,
    )
    token = set_output_context(context)
    try:
        return emit_error(
            error,
            exception=exception,
            traceback=exception.__traceback__ if exception else None,
        )
    finally:
        reset_output_context(token)


def _convert_error(exception: BaseException) -> CLIError:
    """Map SDK, validation, and transport failures to stable CLI errors."""
    if isinstance(exception, CLIError):
        return exception
    if isinstance(exception, (CycloptsError, PydanticValidationError, ValueError)):
        return CLIError("invalid_arguments", str(exception))
    if isinstance(exception, (DeviceLoginError, ControlPlaneLoginError)):
        return CLIError("authentication_failed", str(exception))
    if isinstance(exception, APIError):
        details = {"status_code": exception.status_code}
        if exception.status_code in {401, 403}:
            return CLIError("authentication_failed", exception.detail, details=details)
        if exception.status_code == 404:
            return CLIError("not_found", exception.detail, details=details)
        if exception.status_code == 409:
            return CLIError("conflict", exception.detail, details=details)
        if exception.status_code in {400, 413, 422}:
            return CLIError("invalid_arguments", exception.detail, details=details)
        if exception.status_code >= 500:
            return CLIError(
                "network_error", exception.detail, retryable=True, details=details
            )
        return CLIError("internal_error", str(exception), details=details)
    if isinstance(exception, httpx.TimeoutException):
        return CLIError(
            "network_error", "The server request timed out.", retryable=True
        )
    if isinstance(exception, httpx.TransportError):
        return CLIError(
            "network_error", f"The server is unavailable: {exception}", retryable=True
        )
    return CLIError("internal_error", str(exception) or type(exception).__name__)


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


def _extract_output(tokens: Sequence[str]) -> OutputMode:
    """Best-effort extraction of serialization mode for parser failures."""
    for index, token in enumerate(tokens):
        if token.startswith("--output="):
            value = token.partition("=")[2]
        elif token in {"--output", "-o"} and index + 1 < len(tokens):
            value = tokens[index + 1]
        else:
            continue
        if value in {"auto", "text", "json", "jsonl"}:
            return value  # type: ignore[return-value]
    return "auto"


def _guess_command(tokens: Sequence[str]) -> str:
    """Best-effort dotted command name for failures before command parsing."""
    names: list[str] = []
    skip_value = False
    options_with_values = {
        "--output",
        "-o",
        "--server",
        "--context",
        "--request-timeout",
    }
    for token in tokens:
        if skip_value:
            skip_value = False
            continue
        if token in options_with_values:
            skip_value = True
            continue
        if token.startswith("-"):
            continue
        names.append(token)
        if is_command_group(tuple(names)):
            continue
        break
    return ".".join(names) if names else "cli"
