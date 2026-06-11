"""Sandbox command execution helpers."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict

from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruUsageError,
)

DEFAULT_SANDBOX_COMMAND_MAX_CHARS = 1_048_576
CLEANUP_DESTROY = "destroy"
CLEANUP_CLOSE = "close"
SandboxCleanupPolicy = Literal["destroy", "close"]


class SandboxCommandResult(BaseModel):
    """Public result for one command executed through the active sandbox."""

    command: str | list[str]
    cwd: str | None
    stdout: str
    stderr: str
    exit_code: int
    stdout_truncated: bool
    stderr_truncated: bool
    stack_id: str
    stack_name: str
    sandbox_id: str | None
    sandbox_name: str | None
    session_id: str | None
    cleanup: SandboxCleanupPolicy
    cleanup_succeeded: bool
    cleanup_error: str | None

    model_config = ConfigDict(extra="forbid")


def run_sandbox_command(
    command: str | Sequence[str],
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    max_chars: int = DEFAULT_SANDBOX_COMMAND_MAX_CHARS,
    cleanup: SandboxCleanupPolicy = "destroy",
    client_factory: Callable[[], Any],
) -> SandboxCommandResult:
    """Execute one command through the active stack's sandbox component."""
    normalized_command = _normalize_command(command)
    normalized_cwd = _normalize_cwd(cwd)
    normalized_env = _normalize_env(env)
    normalized_max_chars = _normalize_max_chars(max_chars)
    normalized_cleanup = _normalize_cleanup(cleanup)

    active_stack, active_stack_model = _resolve_active_stack(client_factory)
    sandbox_name, sandbox = _resolve_single_active_sandbox(active_stack)
    _ensure_sandbox_runtime_api_available(active_stack, sandbox=sandbox)

    stack_id = _required_string_attribute(active_stack_model, "id", "active stack ID")
    stack_name = _required_string_attribute(
        active_stack_model,
        "name",
        "active stack name",
    )
    sandbox_id = _optional_string_attribute(sandbox, "id")
    sandbox_name = sandbox_name or _optional_string_attribute(sandbox, "name")

    session: Any | None = None
    try:
        session = sandbox.create_session(settings=None)
        _ensure_session_command_api_available(session)
        process = session.exec(
            normalized_command,
            cwd=normalized_cwd,
            env=normalized_env,
        )
        _ensure_process_collect_api_available(process)
        output = process.collect(max_chars=normalized_max_chars)
    except KitaruFeatureNotAvailableError:
        _cleanup_after_failed_command(session, normalized_cleanup, env=normalized_env)
        raise
    except Exception as exc:
        _cleanup_after_failed_command(session, normalized_cleanup, env=normalized_env)
        error_text = _redact_env_values(str(exc), normalized_env)
        raise KitaruBackendError(
            "Sandbox command execution failed on active stack "
            f"'{stack_name}': {error_text}"
        ) from None

    session_id = _optional_string_attribute(
        session, "id"
    ) or _optional_string_attribute(session, "session_id")
    cleanup_succeeded, cleanup_error = _cleanup_after_success(
        session,
        normalized_cleanup,
        env=normalized_env,
    )
    if not cleanup_succeeded and cleanup_error is None:
        cleanup_error = "Sandbox cleanup did not complete."

    return SandboxCommandResult(
        command=normalized_command,
        cwd=normalized_cwd,
        stdout=_required_output_string(output, "stdout"),
        stderr=_required_output_string(output, "stderr"),
        exit_code=_required_output_int(output, "exit_code"),
        stdout_truncated=_required_output_bool(output, "stdout_truncated"),
        stderr_truncated=_required_output_bool(output, "stderr_truncated"),
        stack_id=stack_id,
        stack_name=stack_name,
        sandbox_id=sandbox_id,
        sandbox_name=sandbox_name,
        session_id=session_id,
        cleanup=normalized_cleanup,
        cleanup_succeeded=cleanup_succeeded,
        cleanup_error=cleanup_error,
    )


def _normalize_command(command: str | Sequence[str]) -> str | list[str]:
    if isinstance(command, str):
        if not command.strip():
            raise KitaruUsageError("Sandbox command must be a non-empty string.")
        return command

    if not isinstance(command, Sequence):
        raise KitaruUsageError(
            "Sandbox command must be a string or a non-empty sequence of strings."
        )

    normalized = list(command)
    if not normalized:
        raise KitaruUsageError("Sandbox command list cannot be empty.")

    for item in normalized:
        if not isinstance(item, str) or not item:
            raise KitaruUsageError(
                "Sandbox command list items must be non-empty strings."
            )

    return normalized


def _normalize_cwd(cwd: str | None) -> str | None:
    if cwd is None:
        return None
    if not isinstance(cwd, str):
        raise KitaruUsageError("Sandbox command cwd must be a string or None.")
    return cwd


def _normalize_env(env: Mapping[str, str] | None) -> dict[str, str] | None:
    if env is None:
        return None
    if not isinstance(env, Mapping):
        raise KitaruUsageError("Sandbox command env must be a mapping of strings.")

    normalized: dict[str, str] = {}
    for key, value in env.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise KitaruUsageError(
                "Sandbox command env keys and values must all be strings."
            )
        normalized[key] = value
    return normalized


def _normalize_max_chars(max_chars: int) -> int:
    if isinstance(max_chars, bool) or not isinstance(max_chars, int):
        raise KitaruUsageError("Sandbox command max_chars must be an integer.")
    if max_chars < 0:
        raise KitaruUsageError("Sandbox command max_chars must be >= 0.")
    return max_chars


def _normalize_cleanup(cleanup: str) -> SandboxCleanupPolicy:
    if cleanup not in {CLEANUP_DESTROY, CLEANUP_CLOSE}:
        raise KitaruUsageError("Sandbox cleanup must be either 'destroy' or 'close'.")
    return cast(SandboxCleanupPolicy, cleanup)


def _resolve_active_stack(
    client_factory: Callable[[], Any],
) -> tuple[Any, Any]:
    try:
        client = client_factory()
        active_stack = client.active_stack
        active_stack_model = client.active_stack_model
    except Exception as exc:
        raise KitaruBackendError(
            "Unable to resolve the active stack for sandbox command execution."
        ) from exc
    return active_stack, active_stack_model


def _ensure_sandbox_runtime_api_available(
    active_stack: Any,
    *,
    sandbox: Any | None = None,
) -> None:
    if not hasattr(active_stack, "sandboxes"):
        raise _sandbox_feature_not_available_error()

    try:
        base_module = importlib.import_module("zenml.sandboxes.base")
        session_module = importlib.import_module("zenml.sandboxes.session")
        process_module = importlib.import_module("zenml.sandboxes.process")
    except Exception as exc:
        raise _sandbox_feature_not_available_error() from exc

    required_methods = (
        (getattr(base_module, "BaseSandbox", None), "create_session"),
        (getattr(session_module, "SandboxSession", None), "exec"),
        (getattr(process_module, "SandboxProcess", None), "collect"),
    )
    if any(
        not callable(getattr(owner, method, None)) for owner, method in required_methods
    ):
        raise _sandbox_feature_not_available_error()

    if sandbox is not None and not callable(getattr(sandbox, "create_session", None)):
        raise _sandbox_feature_not_available_error()


def _ensure_session_command_api_available(session: Any) -> None:
    if not callable(getattr(session, "exec", None)):
        raise _sandbox_feature_not_available_error()


def _ensure_process_collect_api_available(process: Any) -> None:
    if not callable(getattr(process, "collect", None)):
        raise _sandbox_feature_not_available_error()


def _sandbox_feature_not_available_error() -> KitaruFeatureNotAvailableError:
    return KitaruFeatureNotAvailableError(
        "Kitaru sandbox command execution is not available in this environment. "
        "Kitaru needs a sandbox-enabled ZenML runtime dependency that exposes "
        "session creation, command execution, and output collection APIs. Install "
        "the sandbox-enabled ZenML release/SHA and retry."
    )


def _resolve_single_active_sandbox(active_stack: Any) -> tuple[str | None, Any]:
    if not hasattr(active_stack, "sandboxes"):
        raise _sandbox_feature_not_available_error()

    sandboxes = active_stack.sandboxes
    if not isinstance(sandboxes, Mapping):
        raise _sandbox_feature_not_available_error()

    if not sandboxes:
        raise KitaruStateError(
            "The active stack has no sandbox component. Create or select a stack "
            "with exactly one sandbox before calling run_sandbox_command()."
        )
    if len(sandboxes) > 1:
        names = ", ".join(str(name) for name in sandboxes)
        raise KitaruStateError(
            "The active stack has multiple sandbox components "
            f"({names}). Kitaru cannot choose one implicitly."
        )

    name, sandbox = next(iter(sandboxes.items()))
    return str(name) if isinstance(name, str) else None, sandbox


def _required_string_attribute(obj: Any, attr: str, label: str) -> str:
    value = getattr(obj, attr, None)
    if value is None:
        raise KitaruBackendError(f"Unable to read {label} from the active stack.")
    normalized = str(value)
    if not normalized:
        raise KitaruBackendError(f"Unable to read {label} from the active stack.")
    return normalized


def _optional_string_attribute(obj: Any, attr: str) -> str | None:
    value = getattr(obj, attr, None)
    if value is None:
        return None
    normalized = str(value)
    return normalized or None


def _required_output_string(output: Any, attr: str) -> str:
    value = getattr(output, attr, None)
    if not isinstance(value, str):
        raise KitaruBackendError(f"Sandbox output missing string field '{attr}'.")
    return value


def _required_output_int(output: Any, attr: str) -> int:
    value = getattr(output, attr, None)
    if isinstance(value, bool) or not isinstance(value, int):
        raise KitaruBackendError(f"Sandbox output missing integer field '{attr}'.")
    return value


def _required_output_bool(output: Any, attr: str) -> bool:
    value = getattr(output, attr, None)
    if not isinstance(value, bool):
        raise KitaruBackendError(f"Sandbox output missing boolean field '{attr}'.")
    return value


def _redact_env_values(text: str, env: Mapping[str, str] | None) -> str:
    """Redact static sandbox env values from provider error text."""
    if env is None:
        return text

    redacted = text
    values = list(set(env.values()))
    values.sort(key=lambda value: len(value), reverse=True)
    for value in values:
        if not value:
            continue
        redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def _cleanup_after_failed_command(
    session: Any | None,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None,
) -> None:
    if session is None:
        return

    try:
        if cleanup == CLEANUP_DESTROY:
            _destroy_session_or_close_if_unsupported(session, env=env)
        else:
            session.close()
    except Exception:
        return


def _destroy_session_or_close_if_unsupported(
    session: Any,
    *,
    env: Mapping[str, str] | None,
) -> tuple[bool, str | None]:
    try:
        session.destroy()
    except NotImplementedError as exc:
        close_error: str | None = None
        try:
            session.close()
        except Exception as close_exc:
            close_error = (
                " Best-effort close also failed: "
                f"{_redact_env_values(str(close_exc), env)}"
            )
        return (
            False,
            "Sandbox session destroy is not supported by this provider; "
            "the command result is still available. "
            f"{_redact_env_values(str(exc), env)}{close_error or ''}",
        )
    return True, None


def _cleanup_after_success(
    session: Any,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None,
) -> tuple[bool, str | None]:
    if cleanup == CLEANUP_CLOSE:
        try:
            session.close()
        except Exception as exc:
            error_text = _redact_env_values(str(exc), env)
            raise KitaruBackendError(
                "Sandbox command completed, but closing the sandbox session failed: "
                f"{error_text}"
            ) from None
        return True, None

    try:
        return _destroy_session_or_close_if_unsupported(session, env=env)
    except Exception as exc:
        with suppress(Exception):
            session.close()
        error_text = _redact_env_values(str(exc), env)
        raise KitaruBackendError(
            "Sandbox command completed, but destroying the sandbox session failed: "
            f"{error_text}"
        ) from None

    return True, None


__all__ = [
    "DEFAULT_SANDBOX_COMMAND_MAX_CHARS",
    "SandboxCleanupPolicy",
    "SandboxCommandResult",
    "run_sandbox_command",
]
