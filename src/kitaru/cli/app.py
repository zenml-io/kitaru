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
"""Cyclopts entry point that runs one command and emits its result."""

import asyncio
import inspect
import math
import sys
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Annotated, Any

import httpx
from cyclopts import Parameter
from cyclopts.exceptions import CycloptsError
from pydantic import ValidationError as PydanticValidationError

# Importing each command module runs its decorators, which register the
# commands on `app`.
from kitaru.cli import (  # noqa: F401
    agent_commands,
    cohort_commands,
    connection_commands,
    experiment_commands,
    investigation_commands,
    plugin_commands,
    root_commands,
    session_commands,
    worker_commands,
)
from kitaru.cli.command_registry import FUNCTION_SPECS, app
from kitaru.cli.config import read_config
from kitaru.cli.invocation_context import INVOCATION, Invocation
from kitaru.cli.output import (
    CLIError,
    CommandResult,
    OutputContext,
    OutputMode,
    describe_validation_error,
    emit_error,
    emit_result,
    reset_output_context,
    resolve_output_mode,
    set_output_context,
)
from kitaru.cli.schema import CommandSpec, get_spec, is_command_group, is_offline
from kitaru.cli.skill_discovery import (
    INSTALL_COMMAND,
    SKILLS_URL,
    get_kitaru_skill_status,
)
from kitaru.client.control_plane import ControlPlaneLoginError
from kitaru.client.device_grant import DeviceLoginError
from kitaru.client.exceptions import (
    APIError,
    InvalidServerResponseError,
    parse_mastra_replay_refusal,
)


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
    if command == app.help_print:
        if not tokens:
            return _emit_root_result(
                lambda: command(*bound.args, **bound.kwargs),
                output=output,
                machine=bool(machine),
                debug=debug,
                traceback=traceback,
            )
        with suppress(BrokenPipeError):
            command(*bound.args, **bound.kwargs)
        return 0
    spec = FUNCTION_SPECS.get(command)
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
    try:
        mode = resolve_output_mode(
            output, is_tty=sys.stdout.isatty(), streaming=spec.streams
        )
        if mode in {"json", "jsonl"}:
            non_interactive = True
        if machine is None and not is_offline(spec.path) and spec.path != ("doctor",):
            machine = read_config().cli.machine_mode
        machine = bool(machine) or mode != "text" or not sys.stdout.isatty()
        output_context = OutputContext(
            command=spec.command,
            mode=mode,
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
        request_timeout=request_timeout,
        non_interactive=non_interactive,
        no_browser=no_browser or non_interactive,
        stdin=sys.stdin,
    )
    invocation_token = INVOCATION.set(invocation)
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
        return emit_error(CLIError("interrupted", "Interrupted."))
    except BrokenPipeError:
        return 0
    except asyncio.CancelledError:
        raise
    except BaseException as exception:
        error = _convert_error(
            exception,
            server_url=invocation.resolved_server_url,
        )
        return emit_error(
            error,
            exception=exception,
            traceback=exception.__traceback__,
        )
    finally:
        INVOCATION.reset(invocation_token)
        reset_output_context(output_token)


def _emit_root_result(
    render_help: Callable[[], Any],
    *,
    output: OutputMode,
    machine: bool,
    debug: bool,
    traceback: bool,
) -> int:
    """Render root help with agent-skill onboarding."""
    try:
        mode = resolve_output_mode(output, is_tty=sys.stdout.isatty())
    except CLIError as error:
        return _emit_early_error(
            error,
            tokens=(),
            output=output,
            machine=machine,
            non_interactive=False,
            debug=debug,
            traceback=traceback,
        )
    machine = machine or mode != "text" or not sys.stdout.isatty()
    context = OutputContext(
        command="kitaru",
        mode=mode,
        debug=debug,
        traceback=traceback or debug,
        stdout=sys.stdout,
        stderr=sys.stderr,
        rich=mode == "text" and sys.stdout.isatty() and not machine,
    )
    output_token = set_output_context(context)
    try:
        if mode == "text":
            render_help()
        skill_status = get_kitaru_skill_status()
        return emit_result(
            CommandResult(
                item={"skills": skill_status},
                links={"skills": SKILLS_URL},
                next_actions=[] if skill_status["installed"] else [INSTALL_COMMAND],
            )
        )
    finally:
        reset_output_context(output_token)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI with an explicit argument sequence.

    Args:
        argv: Arguments to parse. Defaults to the process argument vector.

    Returns:
        Process exit code.
    """
    tokens = list(sys.argv[1:] if argv is None else argv)
    try:
        if _is_root_help_invocation(tokens):
            output_value = _extract_output_value(tokens)
            if output_value is not None and output_value not in {
                "auto",
                "text",
                "json",
                "jsonl",
            }:
                return _emit_unparsed_error(
                    CLIError(
                        "invalid_arguments",
                        f"Invalid output mode {output_value!r}.",
                    ),
                    tokens=tokens,
                )
            return _emit_root_result(
                lambda: app.help_print([]),
                output="text" if output_value is None else _extract_output(tokens),
                machine="--machine" in tokens,
                debug="--debug" in tokens,
                traceback="--traceback" in tokens,
            )
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
            return _emit_unparsed_error(
                CLIError("interrupted", "Interrupted."),
                tokens=tokens,
                streaming=True,
            )
        except CycloptsError as exception:
            return _emit_unparsed_error(
                CLIError("invalid_arguments", str(exception)),
                tokens=tokens,
                exception=exception,
            )
    except BrokenPipeError:
        return 0


def _emit_unparsed_error(
    error: CLIError,
    *,
    tokens: Sequence[str],
    exception: BaseException | None = None,
    streaming: bool = False,
) -> int:
    """Emit an error using global options extracted before command parsing."""
    output = _extract_output(tokens)
    return _emit_early_error(
        error,
        tokens=tokens,
        output=output,
        machine="--machine" in tokens,
        non_interactive="--non-interactive" in tokens,
        debug="--debug" in tokens,
        traceback="--traceback" in tokens,
        exception=exception,
        streaming=streaming and output == "jsonl",
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
    streaming: bool = False,
) -> int:
    """Emit a global-option or parsing error before invocation bootstrap."""
    try:
        mode = resolve_output_mode(
            output, is_tty=sys.stdout.isatty(), streaming=streaming
        )
    except CLIError:
        mode = "text" if sys.stdout.isatty() else "json"
    command = _guess_command(tokens)
    context = OutputContext(
        command=command,
        mode=mode,
        debug=debug,
        traceback=traceback or debug,
        stdout=sys.stdout,
        stderr=sys.stderr,
        rich=(mode == "text" and sys.stderr.isatty() and not machine),
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


def _convert_error(
    exception: BaseException, *, server_url: str | None = None
) -> CLIError:
    """Map SDK, validation, and transport failures to stable CLI errors."""
    if isinstance(exception, CLIError):
        return exception
    if isinstance(exception, PydanticValidationError):
        location, message = describe_validation_error(exception)
        return CLIError("invalid_arguments", f"Invalid {location}: {message}.")
    if isinstance(exception, CycloptsError):
        return CLIError("invalid_arguments", str(exception))
    if isinstance(exception, (DeviceLoginError, ControlPlaneLoginError)):
        return CLIError("authentication_failed", str(exception))
    if isinstance(exception, InvalidServerResponseError):
        return CLIError(
            "invalid_configuration",
            str(exception),
            hint="Check the server URL and that the client and server versions match.",
        )
    if isinstance(exception, APIError):
        details = {"status_code": exception.status_code}
        # Proxies and edge servers often answer with an empty body, which
        # would otherwise render as a title with nothing under it.
        detail = exception.detail.strip() or (
            f"The server returned HTTP {exception.status_code} with no detail."
        )
        if exception.status_code in {401, 403}:
            return CLIError("authentication_failed", detail, details=details)
        if exception.status_code == 404:
            return CLIError("not_found", detail, details=details)
        if exception.status_code == 409:
            refusal = parse_mastra_replay_refusal(detail)
            if refusal is not None:
                details.update(refusal)
            return CLIError("conflict", detail, details=details)
        if exception.status_code in {400, 413, 422}:
            return CLIError("invalid_arguments", detail, details=details)
        if exception.status_code >= 500:
            return CLIError("network_error", detail, retryable=True, details=details)
        return CLIError("internal_error", str(exception), details=details)
    if isinstance(exception, httpx.TransportError):
        server_url = _get_transport_server_url(exception, server_url)
    if isinstance(exception, httpx.TimeoutException):
        message = (
            f"The request to {server_url} timed out."
            if server_url
            else "The server request timed out."
        )
        return CLIError(
            "network_error",
            message,
            retryable=True,
            details={"server_url": server_url} if server_url else None,
            hint="Check the selected server with `kitaru status`, then retry.",
        )
    if isinstance(exception, httpx.TransportError):
        location = f" at {server_url}" if server_url else ""
        return CLIError(
            "network_error",
            f"The server{location} is unavailable: {exception}",
            retryable=True,
            details={"server_url": server_url} if server_url else None,
            hint="Check the selected server with `kitaru status`, then retry.",
        )
    return CLIError("internal_error", str(exception) or type(exception).__name__)


def _get_transport_server_url(
    exception: httpx.TransportError,
    selected_server_url: str | None = None,
) -> str | None:
    """Return the selected server or a secret-safe failed request origin."""
    try:
        request_url = exception.request.url
    except RuntimeError:
        request_url = None
    if selected_server_url:
        selected_url = httpx.URL(selected_server_url)
        if request_url is None or (
            request_url.scheme,
            request_url.host,
            request_url.port,
        ) == (
            selected_url.scheme,
            selected_url.host,
            selected_url.port,
        ):
            return str(
                selected_url.copy_with(
                    username=None,
                    password=None,
                    query=None,
                    fragment=None,
                )
            ).rstrip("/")
    if request_url is None:
        return None
    return str(
        request_url.copy_with(
            username=None,
            password=None,
            path="/",
            query=None,
            fragment=None,
        )
    ).rstrip("/")


def _extract_output(tokens: Sequence[str]) -> OutputMode:
    """Best-effort extraction of serialization mode for parser failures."""
    value = _extract_output_value(tokens)
    if value in {"auto", "text", "json", "jsonl"}:
        return value  # type: ignore[return-value]
    return "auto"


def _extract_output_value(tokens: Sequence[str]) -> str | None:
    """Extract the raw explicit serialization mode when one is present."""
    for index, token in enumerate(tokens):
        if token.startswith(("--output=", "-o=")):
            return token.partition("=")[2]
        if token in {"--output", "-o"} and index + 1 < len(tokens):
            return tokens[index + 1]
    return None


def _is_root_help_invocation(tokens: Sequence[str]) -> bool:
    """Return whether tokens request root help with supported global options."""
    options_with_values = {"--output", "-o", "--server", "--request-timeout"}
    boolean_options = {
        "--machine",
        "--no-machine",
        "--non-interactive",
        "--no-browser",
        "--debug",
        "--traceback",
    }
    value_prefixes = ("--output=", "-o=", "--server=", "--request-timeout=")
    help_requested = False
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in {"--help", "-h"}:
            help_requested = True
            index += 1
            continue
        if token in options_with_values:
            if index + 1 >= len(tokens):
                return False
            index += 2
            continue
        if token in boolean_options or token.startswith(value_prefixes):
            index += 1
            continue
        return False
    return help_requested


def _guess_command(tokens: Sequence[str]) -> str:
    """Best-effort dotted command name for failures before command parsing."""
    names: list[str] = []
    skip_value = False
    options_with_values = {
        "--output",
        "-o",
        "--server",
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
    candidate = tuple(names)
    return ".".join(names) if candidate and get_spec(candidate) is not None else "cli"
