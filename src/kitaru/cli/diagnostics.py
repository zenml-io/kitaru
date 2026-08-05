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
"""Connection status, system information, and independent health checks."""

import asyncio
import importlib.metadata
import importlib.util
import json
import os
import platform
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

import httpx
from packaging.version import InvalidVersion, Version
from pydantic import ValidationError

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.cli.config import (
    ConfigStore,
    ResolvedCredential,
    ResolvedTarget,
    build_api_client,
    resolve_credential,
    resolve_target,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.client.config import (
    DIRECTORY_MODE,
    FILE_MODE,
)
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ServerCredentials
from kitaru.client.exceptions import APIError


def package_version() -> str:
    """Return the installed Kitaru distribution version.

    Returns:
        Installed version or ``unknown`` in an unpackaged source tree.
    """
    try:
        return importlib.metadata.version("kitaru")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


async def status(
    *,
    target: ResolvedTarget,
    credential_store: CredentialStore,
    timeout: float,
) -> CommandResult:
    """Build a quick operational view of the selected server.

    Args:
        target: Resolved server and provenance.
        credential_store: Secret store used for authentication.
        timeout: Request timeout in seconds.

    Returns:
        Secret-safe server status.
    """
    credential = resolve_credential(target.server_url, credential_store)
    client = build_api_client(target.server_url, credential, credential_store, timeout)
    warnings: list[str] = []
    try:
        info = await client.info.get()
        live_workers: int | None = None
        authentication = (
            "not_required" if info.auth_scheme is AuthScheme.NONE else "missing"
        )
        if info.auth_scheme is AuthScheme.NONE or credential.source != "none":
            try:
                live_workers = 0
                async for worker in client.workers.iter():
                    if worker.live:
                        live_workers += 1
                authentication = (
                    "not_required"
                    if info.auth_scheme is AuthScheme.NONE
                    else "authenticated"
                )
            except APIError as error:
                if error.status_code not in {401, 403}:
                    raise
                authentication = "rejected"
                warnings.append("The selected credential was rejected by the server.")
        elif info.auth_scheme is not AuthScheme.NONE:
            warnings.append(
                "The server requires authentication but no credential is available."
            )
        compatibility = _compatibility(info.version)
        compatibility_warning = _compatibility_warning(compatibility)
        if compatibility_warning is not None:
            warnings.append(compatibility_warning)
        item = {
            "server_url": target.server_url,
            "server_source": target.source,
            "credential_status": _credential_summary(credential),
            "authentication": authentication,
            "server": info.model_dump(mode="json"),
            "dashboard_url": info.dashboard_url,
            "compatibility": compatibility,
            "live_worker_count": live_workers,
        }
        return CommandResult(item=item, warnings=warnings)
    finally:
        await client.close()


async def info(
    *,
    target: ResolvedTarget,
    credential_store: CredentialStore,
    timeout: float,
) -> CommandResult:
    """Report local runtime details and resolved server information.

    Args:
        target: Resolved server and provenance.
        credential_store: Secret store used to construct the client.
        timeout: Request timeout in seconds.

    Returns:
        Local and remote information.
    """
    credential = resolve_credential(target.server_url, credential_store)
    client = build_api_client(target.server_url, credential, credential_store, timeout)
    try:
        server = await client.info.get()
    finally:
        await client.close()
    client_version = package_version()
    compatibility = _compatibility(server.version, client_version=client_version)
    compatibility_warning = _compatibility_warning(compatibility)
    return CommandResult(
        item={
            "kitaru_version": client_version,
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "executable": sys.executable,
            "server_url": target.server_url,
            "server_source": target.source,
            "server": server.model_dump(mode="json"),
            "compatibility": compatibility,
        },
        warnings=[compatibility_warning] if compatibility_warning is not None else [],
    )


async def doctor(
    *,
    config_store: ConfigStore,
    credential_store: CredentialStore,
    explicit_server: str | None,
    timeout: float,
) -> CommandResult:
    """Run every local and remote diagnostic in a fixed logical order.

    Args:
        config_store: Non-secret CLI configuration store.
        credential_store: Secret credential store.
        explicit_server: Explicit server override.
        timeout: Request timeout in seconds.

    Returns:
        Ordered diagnostic checks and an aggregate exit code.
    """
    checks: list[dict[str, Any]] = []
    failure_categories: set[str] = set()

    try:
        config_store.load()
        _check_mode(
            config_store.path,
            require_private_parent=config_store.manages_parent_directory,
        )
        checks.append(_check("config", "pass", True, str(config_store.path)))
    except (CLIError, OSError) as error:
        failure_categories.add("configuration")
        checks.append(_check("config", "fail", True, str(error)))

    credentials_valid = True
    try:
        _validate_credentials_file(credential_store.path)
        checks.append(_check("credentials", "pass", True, str(credential_store.path)))
    except OSError as error:
        credentials_valid = False
        failure_categories.add("configuration")
        checks.append(_check("credentials", "fail", True, str(error)))
    except (ValueError, ValidationError):
        credentials_valid = False
        failure_categories.add("configuration")
        checks.append(
            _check(
                "credentials",
                "fail",
                True,
                f"Credential document at {credential_store.path} is invalid.",
            )
        )

    target: ResolvedTarget | None = None
    try:
        target = resolve_target(explicit_server=explicit_server)
        checks.append(
            _check(
                "server_resolution",
                "pass",
                True,
                f"{target.server_url} ({target.source})",
            )
        )
    except CLIError as error:
        failure_categories.add("configuration")
        checks.append(_check("server_resolution", "fail", True, error.message))

    server_info: ServerInfoResponse | None = None
    credential: ResolvedCredential | None = None
    if target is None:
        for name in ("liveness", "readiness", "server_info"):
            checks.append(_check(name, "skip", True, "No server resolved."))
        checks.append(_check("compatibility", "skip", False, "No server resolved."))
        checks.append(_check("authentication", "skip", True, "No server resolved."))
    else:
        health_checks = (("liveness", "/health/live"), ("readiness", "/health"))
        async with httpx.AsyncClient(timeout=timeout) as health_client:
            health_results = await asyncio.gather(
                *(
                    _probe(health_client, target.server_url, path)
                    for _, path in health_checks
                ),
                return_exceptions=True,
            )
        for (name, path), result in zip(health_checks, health_results, strict=True):
            try:
                if isinstance(result, BaseException):
                    raise result
                status_code = result
                if status_code >= 400:
                    raise httpx.HTTPStatusError(
                        f"HTTP {status_code}",
                        request=httpx.Request("GET", target.server_url + path),
                        response=httpx.Response(status_code),
                    )
                checks.append(_check(name, "pass", True, f"HTTP {status_code}"))
            except (httpx.HTTPError, TimeoutError) as error:
                failure_categories.add("server")
                checks.append(_check(name, "fail", True, str(error)))

        credential = (
            resolve_credential(target.server_url, credential_store)
            if credentials_valid
            else ResolvedCredential("none")
        )
        client = build_api_client(
            target.server_url, credential, credential_store, timeout
        )
        try:
            try:
                server_info = await client.info.get()
                checks.append(
                    _check(
                        "server_info",
                        "pass",
                        True,
                        f"Kitaru {server_info.version}; "
                        f"auth={server_info.auth_scheme.value}",
                    )
                )
            except (APIError, httpx.HTTPError, TimeoutError) as error:
                failure_categories.add("server")
                checks.append(_check("server_info", "fail", True, str(error)))

            if server_info is None:
                checks.append(
                    _check("compatibility", "skip", False, "Server info unavailable.")
                )
            else:
                compatibility = _compatibility(server_info.version)
                compatibility_warning = _compatibility_warning(compatibility)
                checks.append(
                    _check(
                        "compatibility",
                        "warn" if compatibility_warning is not None else "pass",
                        False,
                        compatibility_warning
                        or "Client and server major versions match.",
                    )
                )

            if server_info is None:
                checks.append(
                    _check("authentication", "skip", True, "Server info unavailable.")
                )
            elif server_info.auth_scheme is AuthScheme.NONE:
                checks.append(
                    _check("authentication", "pass", True, "Not required by server.")
                )
            elif credential.source == "none":
                failure_categories.add("authentication")
                checks.append(
                    _check("authentication", "fail", True, "No credential available.")
                )
            else:
                try:
                    await client.workers.list()
                    checks.append(
                        _check(
                            "authentication",
                            "pass",
                            True,
                            f"Credential source: {credential.source}",
                        )
                    )
                except APIError as error:
                    if error.status_code in {401, 403}:
                        failure_categories.add("authentication")
                    else:
                        failure_categories.add("server")
                    checks.append(_check("authentication", "fail", True, str(error)))
                except (httpx.HTTPError, TimeoutError) as error:
                    failure_categories.add("server")
                    checks.append(_check("authentication", "fail", True, str(error)))
        finally:
            await client.close()

    worker_extra = importlib.util.find_spec("pydantic_settings") is not None
    checks.append(
        _check(
            "worker_extra",
            "pass" if worker_extra else "warn",
            False,
            "available"
            if worker_extra
            else "Install kitaru[cli,worker] to run workers.",
        )
    )
    uv_path = shutil.which("uv")
    checks.append(
        _check(
            "uv",
            "pass" if uv_path else "warn",
            False,
            uv_path or "uv is not installed.",
        )
    )

    if "server" in failure_categories:
        exit_code = 6
    elif "authentication" in failure_categories:
        exit_code = 3
    elif "configuration" in failure_categories:
        exit_code = 2
    else:
        exit_code = 0
    return CommandResult(
        item={
            "healthy": exit_code == 0,
            "checks": checks,
        },
        exit_code=exit_code,
    )


def _credential_summary(credential: ResolvedCredential) -> dict[str, Any]:
    """Describe a resolved credential without serializing its secret values."""
    summary: dict[str, Any] = {
        "source": credential.source,
        "kind": "none",
        "token_state": "absent",
        "renewable": False,
    }
    if credential.source == "environment":
        summary.update(kind="api_key", renewable=True)
        return summary
    stored = credential.stored
    if stored is None:
        return summary
    if stored.api_key is not None:
        kind = "api_key"
    elif stored.device_id is not None:
        kind = "device"
    elif stored.api_token is not None:
        kind = "token"
    else:
        kind = "none"
    token_state = "absent"
    if stored.api_token is not None:
        token_state = "expired" if stored.api_token.expired else "valid"
    summary.update(
        kind=kind,
        token_state=token_state,
        renewable=stored.can_refresh,
        api_type=stored.type.value,
    )
    return summary


def _compatibility_warning(compatibility: dict[str, str | bool]) -> str | None:
    """Return a non-blocking warning for uncertain or incompatible versions."""
    status = compatibility["status"]
    if status == "major_version_mismatch":
        return "The client and server major versions differ; commands are not blocked."
    if status == "unknown":
        return (
            "Client/server compatibility could not be determined; commands are not "
            "blocked."
        )
    return None


def _compatibility(
    server_version: str, *, client_version: str | None = None
) -> dict[str, str | bool]:
    """Compare installed and server major versions conservatively."""
    if client_version is None:
        client_version = package_version()
    try:
        compatible = Version(client_version).major == Version(server_version).major
        status = "compatible" if compatible else "major_version_mismatch"
    except InvalidVersion:
        compatible = False
        status = "unknown"
    return {
        "compatible": compatible,
        "status": status,
        "client_version": client_version,
        "server_version": server_version,
    }


async def _probe(client: httpx.AsyncClient, server_url: str, path: str) -> int:
    """Probe one health endpoint without constructing an SDK resource."""
    response = await client.get(server_url + path)
    return response.status_code


def _check(name: str, status: str, required: bool, detail: str) -> dict[str, Any]:
    """Build one fixed-shape diagnostic result."""
    return {"name": name, "status": status, "required": required, "detail": detail}


def _check_mode(path: Path, *, require_private_parent: bool) -> None:
    """Reject config files or managed directories accessible by other users."""
    _validate_private_path_mode(path, require_private_parent=require_private_parent)


def _validate_credentials_file(path: Path) -> None:
    """Validate credential readability, structure, and POSIX file mode."""
    if not path.exists():
        return
    _validate_private_path_mode(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Credential file must contain a JSON object.")
    for entry in payload.values():
        ServerCredentials.model_validate(entry)


def _validate_private_path_mode(
    path: Path, *, require_private_parent: bool = True
) -> None:
    """Validate owner-only modes for a config file and optionally its directory."""
    if os.name != "posix":
        return
    if require_private_parent and path.parent.exists():
        directory_mode = stat.S_IMODE(path.parent.stat().st_mode)
        if directory_mode != DIRECTORY_MODE:
            raise OSError(
                f"{path.parent} has mode {oct(directory_mode)}; "
                f"expected {oct(DIRECTORY_MODE)}"
            )
    if path.exists():
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode != FILE_MODE:
            raise OSError(f"{path} has mode {oct(mode)}; expected {oct(FILE_MODE)}")
