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
"""Authentication command orchestration over the existing SDK flows."""

import getpass
from collections.abc import Callable
from typing import TextIO, cast

from kitaru.analytics.source import AnalyticsSource
from kitaru.api_models.v1.auth import DeviceAuthorizationResponse
from kitaru.api_models.v1.info import AuthScheme
from kitaru.cli import local_runtime
from kitaru.cli.config import validate_server_url
from kitaru.cli.output import CLIError, CommandResult, write_interaction
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.config import set_server_url
from kitaru.client.control_plane import ControlPlaneDeviceAuthorization
from kitaru.client.control_plane_auth import control_plane_login
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken
from kitaru.client.device_auth import device_login
from kitaru.client.exceptions import NotFoundError


async def login(
    *,
    server: str | None,
    local: bool,
    port: int | None = None,
    username: str | None,
    password_stdin: bool,
    api_key_stdin: bool,
    credential_store: CredentialStore,
    timeout: float,
    non_interactive: bool,
    no_browser: bool,
    stdin: TextIO,
    upgrade: bool = False,
    refresh: bool = False,
    password_prompt: Callable[[str], str] = getpass.getpass,
    package_version: str = "",
) -> CommandResult:
    """Authenticate with one server and store its credential when required.

    Args:
        server: Full managed or self-hosted server URL.
        local: Provision and target the local server.
        port: Host port for the local server.
        upgrade: Replace an existing local server with the requested image.
        refresh: Skip stored control plane credentials and force a new device
            login flow.
        username: Local account name for password authentication.
        password_stdin: Read a local password from standard input.
        api_key_stdin: Read an API key from standard input.
        credential_store: Existing secret credential store.
        timeout: Request timeout in seconds.
        non_interactive: Whether prompts and browser actions are forbidden.
        no_browser: Whether browser launch is forbidden.
        stdin: Stream used for secret input.
        password_prompt: Hidden password prompt implementation.
        package_version: Installed Kitaru version used for the local image.

    Returns:
        Secret-safe login receipt.
    """
    if local and server:
        raise CLIError("invalid_arguments", "Pass either SERVER or --local, not both.")
    if not local and not server:
        raise CLIError(
            "invalid_arguments",
            "Login requires a SERVER argument or --local.",
        )
    if upgrade and not local:
        raise CLIError("invalid_arguments", "--upgrade requires --local.")
    if port is not None and not local:
        raise CLIError("invalid_arguments", "--port requires --local.")
    if password_stdin and api_key_stdin:
        raise CLIError(
            "invalid_arguments",
            "--password-stdin and --api-key-stdin cannot be combined.",
        )
    if local:
        _reject_auth_inputs(
            username=username,
            password_stdin=password_stdin,
            api_key_stdin=api_key_stdin,
            scheme=AuthScheme.NONE,
        )
        item, warnings = await local_runtime.start_local_runtime(
            package_version=package_version,
            upgrade=upgrade,
            timeout=timeout,
            port=port,
            progress=None if non_interactive else write_interaction,
        )
        server_url = cast(str, item["server_url"])
        client = KitaruAPIClient(
            base_url=server_url,
            timeout=timeout,
            analytics_source=AnalyticsSource.CLI,
        )
        try:
            info = await client.info.get()
            if info.auth_scheme is not AuthScheme.NONE:
                raise CLIError(
                    "conflict",
                    "The CLI-owned local deployment unexpectedly requires "
                    "authentication.",
                    hint="Run `kitaru local logs` to inspect its configuration.",
                )
            set_server_url(server_url)
        except OSError as error:
            raise CLIError(
                "invalid_configuration",
                f"The local server started but could not be selected: {error}",
            ) from error
        finally:
            await client.close()
        item["auth_scheme"] = info.auth_scheme.value
        item["server_version"] = info.version
        if not no_browser and not non_interactive:
            opened = await local_runtime.open_local_dashboard(server_url)
            if not opened:
                warnings.append(
                    f"The dashboard could not be opened. Visit {server_url}."
                )
        return CommandResult(
            item=item,
            warnings=warnings,
            links={"dashboard": server_url},
            next_actions=["Run `kitaru status` to inspect the local server."],
        )
    server_url = validate_server_url(str(server))
    client = KitaruAPIClient(
        base_url=server_url, timeout=timeout, analytics_source=AnalyticsSource.CLI
    )
    credential_stored = False
    credential_kind = "none"
    try:
        try:
            info = await client.info.get()
        except NotFoundError as error:
            raise CLIError(
                "invalid_configuration",
                f"Kitaru is not available at {server_url}. "
                "Check the URL or deployment.",
                details={
                    "status_code": error.status_code,
                    "server_url": server_url,
                },
            ) from error
        if info.auth_scheme is AuthScheme.NONE:
            _reject_auth_inputs(
                username=username,
                password_stdin=password_stdin,
                api_key_stdin=api_key_stdin,
                scheme=info.auth_scheme,
            )
            authentication = "not_required"
        elif info.auth_scheme is AuthScheme.LOCAL:
            authentication, credential_kind = await _login_local(
                client=client,
                server_url=server_url,
                store=credential_store,
                username=username,
                password_stdin=password_stdin,
                api_key_stdin=api_key_stdin,
                non_interactive=non_interactive,
                no_browser=no_browser,
                stdin=stdin,
                password_prompt=password_prompt,
            )
            credential_stored = True
        else:
            _reject_control_plane_password(username, password_stdin)
            api_key = _read_secret(stdin, "API key") if api_key_stdin else None
            if api_key is None and non_interactive:
                raise CLIError(
                    "interaction_required",
                    "Control-plane device login requires interaction.",
                    hint="Use --api-key-stdin or run interactively.",
                )
            _, credential_kind = await control_plane_login(
                client,
                server_url,
                credential_store,
                api_key=api_key,
                open_browser=not no_browser and not non_interactive,
                prompt=_show_device_prompt,
                refresh=refresh,
            )
            authentication = "authenticated"
            credential_stored = True
        set_server_url(server_url)
    except OSError as error:
        raise CLIError(
            "invalid_configuration",
            "Authentication succeeded but local connection state could not be "
            f"stored: {error}",
        ) from error
    finally:
        await client.close()

    return CommandResult(
        item={
            "server_url": server_url,
            "auth_scheme": info.auth_scheme.value,
            "authentication": authentication,
            "credential_kind": credential_kind,
            "credential_stored": credential_stored,
        }
    )


async def logout(
    *,
    server_url: str | None,
    all_servers: bool,
    credential_store: CredentialStore,
    delete_volumes: bool = False,
    allow_orphan_cleanup: bool = False,
) -> CommandResult:
    """Remove local credentials without changing server state.

    Args:
        server_url: Resolved server whose credential should be removed.
        all_servers: Whether to clear the complete credential store.
        delete_volumes: Whether to delete CLI-owned local deployment data.
        allow_orphan_cleanup: Whether a targetless volume deletion may remove
            labeled resources when ownership state is missing.
        credential_store: Existing secret credential store.

    Returns:
        Logout receipt.
    """
    if all_servers and server_url is not None:
        raise CLIError("invalid_arguments", "SERVER and --all cannot be combined.")
    if all_servers and delete_volumes:
        raise CLIError("invalid_arguments", "--all and --volumes cannot be combined.")
    try:
        if all_servers:
            count = len(credential_store.list())
            credential_store.clear_all()
            return CommandResult(item={"credentials_removed": count, "scope": "all"})
        if server_url is None:
            raise CLIError(
                "invalid_configuration", "No server was resolved for logout."
            )
        orphan_cleanup = (
            allow_orphan_cleanup
            and delete_volumes
            and not local_runtime.has_local_runtime_state()
        )
        if local_runtime.is_local_runtime_url(server_url) or orphan_cleanup:
            item = await local_runtime.stop_local_runtime(delete_volumes=delete_volumes)
            credential_store.clear(server_url)
            set_server_url(None)
            warnings = (
                ["The local Kitaru database and its stored data were deleted."]
                if delete_volumes
                else []
            )
            return CommandResult(item=item, warnings=warnings)
        elif delete_volumes:
            raise CLIError(
                "invalid_arguments",
                "--volumes is only valid for the CLI-owned local deployment.",
            )
        existed = credential_store.get(server_url) is not None
        credential_store.clear(server_url)
    except OSError as error:
        raise CLIError(
            "invalid_configuration", f"Could not update the credential store: {error}"
        ) from error
    return CommandResult(
        item={
            "server_url": server_url,
            "credential_removed": existed,
        }
    )


async def _login_local(
    *,
    client: KitaruAPIClient,
    server_url: str,
    store: CredentialStore,
    username: str | None,
    password_stdin: bool,
    api_key_stdin: bool,
    non_interactive: bool,
    no_browser: bool,
    stdin: TextIO,
    password_prompt: Callable[[str], str],
) -> tuple[str, str]:
    """Run one of the local-auth credential flows."""
    if api_key_stdin:
        if username:
            raise CLIError(
                "invalid_arguments", "--username cannot be used with --api-key-stdin."
            )
        api_key = _read_secret(stdin, "API key")
        await client.auth.exchange_api_key(api_key)
        store.set_api_key(server_url, api_key)
        return "authenticated", "api_key"
    if password_stdin and not username:
        raise CLIError("invalid_arguments", "--password-stdin requires --username.")
    if username:
        if password_stdin:
            password = _read_secret(stdin, "password")
        elif non_interactive:
            raise CLIError(
                "interaction_required",
                "Password login requires a hidden prompt or --password-stdin.",
            )
        else:
            password = password_prompt("Password: ")
            if not password:
                raise CLIError("invalid_arguments", "Password cannot be empty.")
        response = await client.auth.login(username, password)
        store.set_token(server_url, ApiToken.from_response(response))
        return "authenticated", "password"
    if password_stdin:
        raise CLIError("invalid_arguments", "--password-stdin requires --username.")
    if non_interactive:
        raise CLIError(
            "interaction_required",
            "Local device login requires interaction.",
            hint="Use --username with --password-stdin or --api-key-stdin.",
        )
    await device_login(
        client,
        server_url,
        store,
        open_browser=not no_browser,
        prompt=_show_device_prompt,
    )
    return "authenticated", "device"


def _read_secret(stream: TextIO, label: str) -> str:
    """Read exactly one non-empty secret line from standard input."""
    value = stream.readline().rstrip("\r\n")
    if not value:
        raise CLIError("invalid_arguments", f"{label} from stdin cannot be empty.")
    return value


def _show_device_prompt(
    authorization: DeviceAuthorizationResponse | ControlPlaneDeviceAuthorization,
) -> None:
    """Show a device verification instruction without exposing its grant code."""
    if isinstance(authorization, ControlPlaneDeviceAuthorization):
        uri = authorization.verification_uri_complete or authorization.verification_uri
    else:
        uri = authorization.verification_uri_complete
    message = f"Open {uri} and confirm code {authorization.user_code}."
    write_interaction(message)


def _reject_auth_inputs(
    *,
    username: str | None,
    password_stdin: bool,
    api_key_stdin: bool,
    scheme: AuthScheme,
) -> None:
    """Reject credentials supplied to a server that does not use them."""
    if username or password_stdin or api_key_stdin:
        raise CLIError(
            "invalid_arguments",
            f"Server authentication scheme {scheme.value!r} does not accept "
            "these credentials.",
        )


def _reject_control_plane_password(username: str | None, password_stdin: bool) -> None:
    """Reject local password options for control-plane authentication."""
    if username or password_stdin:
        raise CLIError(
            "invalid_arguments",
            "Control-plane login does not accept --username or --password-stdin.",
        )
