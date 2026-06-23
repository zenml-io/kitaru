"""Sandbox command execution helpers."""

from __future__ import annotations

import importlib
import math
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Thread
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
_SANDBOX_COMMAND_TIMEOUT_EXIT_CODE = -1
_SANDBOX_COMMAND_TIMEOUT_ERROR = (
    "Sandbox command timeout_seconds must be a finite number greater than 0."
)
_REDACTION_MARKER: str = "[REDACTED]"
_SECRET_KEY_PATTERN = (
    r"token|secret|password|passwd|pwd|api[_-]?key|apikey|access[_-]?key|"
    r"private[_-]?key|credential|credentials|auth|authorization|bearer"
)
_SECRET_KEY_NAME_PATTERN = rf"[A-Za-z0-9_-]*(?:{_SECRET_KEY_PATTERN})[A-Za-z0-9_-]*"
_AUTHORIZATION_HEADER_RE = re.compile(
    r"(?<![\w-])(?P<key_quote>['\"]?)(?P<key>authorization|auth)"
    r"(?P=key_quote)(?P<sep>\s*:\s*)"
    r"(?P<value_quote>['\"]?)(?P<value>(?:[A-Za-z]+\s+)?[^'\"\s\r\n,;]+)(?P=value_quote)",
    re.IGNORECASE,
)
_SECRET_KEY_PREFIX = (
    rf"(?<![\w-])(?P<key_quote>['\"]?)(?P<key>{_SECRET_KEY_NAME_PATTERN})"
    r"(?P=key_quote)(?P<sep>\s*[:=]\s*)"
)
_SECRET_KEY_QUOTED_VALUE_RE = re.compile(
    _SECRET_KEY_PREFIX + r"(?P<value_quote>['\"])(?P<value>.*?)(?P=value_quote)",
    re.IGNORECASE,
)
_SECRET_KEY_UNQUOTED_VALUE_RE = re.compile(
    _SECRET_KEY_PREFIX + r"(?P<value>[^'\"\s,;]+)",
    re.IGNORECASE,
)
_BEARER_TOKEN_RE = re.compile(r"\bBearer\s+([^\s,;'\"]+)", re.IGNORECASE)


@dataclass(frozen=True)
class _ProcessOutputCollection:
    output: Any = None
    timed_out: bool = False


class SandboxCommandResult(BaseModel):
    """Public result for one command executed through the active sandbox."""

    command: str | list[str]
    cwd: str | None
    stdout: str
    stderr: str
    exit_code: int
    stdout_truncated: bool
    stderr_truncated: bool
    timed_out: bool = False
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
    timeout_seconds: float | None = None,
    cleanup: SandboxCleanupPolicy = "destroy",
    client_factory: Callable[[], Any],
) -> SandboxCommandResult:
    """Execute one command through the active stack's sandbox component."""
    normalized_command = _normalize_command(command)
    normalized_cwd = _normalize_cwd(cwd)
    normalized_env = _normalize_env(env)
    normalized_max_chars = _normalize_max_chars(max_chars)
    normalized_timeout_seconds = normalize_timeout_seconds(
        timeout_seconds,
        error_message=_SANDBOX_COMMAND_TIMEOUT_ERROR,
    )
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
        output_collection = _collect_process_output(
            process,
            max_chars=normalized_max_chars,
            timeout_seconds=normalized_timeout_seconds,
        )
        if output_collection.timed_out:
            assert normalized_timeout_seconds is not None
            cleanup_succeeded, cleanup_error = _cleanup_after_timed_out_command(
                session,
                normalized_cleanup,
                env=normalized_env,
                command=normalized_command,
            )
            return SandboxCommandResult(
                command=normalized_command,
                cwd=normalized_cwd,
                stdout="",
                stderr=(
                    "Sandbox command timed out after "
                    f"{normalized_timeout_seconds:g} seconds."
                ),
                exit_code=_SANDBOX_COMMAND_TIMEOUT_EXIT_CODE,
                stdout_truncated=False,
                stderr_truncated=False,
                timed_out=True,
                stack_id=stack_id,
                stack_name=stack_name,
                sandbox_id=sandbox_id,
                sandbox_name=sandbox_name,
                session_id=_session_id(session),
                cleanup=normalized_cleanup,
                cleanup_succeeded=cleanup_succeeded,
                cleanup_error=cleanup_error,
            )
        output = output_collection.output
    except KitaruFeatureNotAvailableError:
        _cleanup_after_failed_command(
            session,
            normalized_cleanup,
            env=normalized_env,
            command=normalized_command,
        )
        raise
    except Exception as exc:
        cleanup_succeeded, cleanup_error = _cleanup_after_failed_command(
            session,
            normalized_cleanup,
            env=normalized_env,
            command=normalized_command,
        )
        redacted_error = _redact_provider_message(
            exc,
            env=normalized_env,
            command=normalized_command,
        )
        error_message = (
            "Sandbox command execution failed on active stack "
            f"'{stack_name}': {redacted_error}"
        )
        if not cleanup_succeeded:
            error_message = _append_cleanup_warning(
                error_message,
                cleanup_error,
                env=normalized_env,
                command=normalized_command,
            )
        raise KitaruBackendError(error_message) from None

    session_id = _optional_string_attribute(
        session, "id"
    ) or _optional_string_attribute(session, "session_id")
    cleanup_succeeded, cleanup_error = _cleanup_after_success(
        session,
        normalized_cleanup,
        env=normalized_env,
        command=normalized_command,
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
        timed_out=False,
        stack_id=stack_id,
        stack_name=stack_name,
        sandbox_id=sandbox_id,
        sandbox_name=sandbox_name,
        session_id=session_id,
        cleanup=normalized_cleanup,
        cleanup_succeeded=cleanup_succeeded,
        cleanup_error=cleanup_error,
    )


def active_sandbox_cache_identity(
    *,
    client_factory: Callable[[], Any],
) -> dict[str, str | None]:
    """Return cache identity for the active stack's single sandbox component."""
    active_stack, active_stack_model = _resolve_active_stack(client_factory)
    sandbox_name, sandbox = _resolve_single_active_sandbox(active_stack)
    _ensure_sandbox_runtime_api_available(active_stack, sandbox=sandbox)

    return {
        "kind": "active_sandbox",
        "stack_id": _required_string_attribute(
            active_stack_model,
            "id",
            "active stack ID",
        ),
        "stack_name": _required_string_attribute(
            active_stack_model,
            "name",
            "active stack name",
        ),
        "sandbox_id": _optional_string_attribute(sandbox, "id"),
        "sandbox_name": sandbox_name or _optional_string_attribute(sandbox, "name"),
    }


def _normalize_sandbox_command_inputs(
    command: str | Sequence[str],
    *,
    cwd: str | None,
    env: Mapping[str, str] | None,
    max_chars: int,
    cleanup: str,
) -> tuple[
    str | list[str], str | None, dict[str, str] | None, int, SandboxCleanupPolicy
]:
    """Normalize sandbox command inputs without touching the active stack."""
    return (
        _normalize_command(command),
        _normalize_cwd(cwd),
        _normalize_env(env),
        _normalize_max_chars(max_chars),
        _normalize_cleanup(cleanup),
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


def _redact_sensitive_text(
    text: str,
    *,
    env: Mapping[str, str] | None,
) -> tuple[str, bool]:
    """Redact known env values and common secret-like text patterns."""
    redacted = _redact_env_values(text, env=env)
    redacted = _redact_bearer_tokens(redacted)
    redacted = _redact_secret_key_values(redacted)
    return redacted, redacted != text


def _redact_provider_message(
    message: object,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None = None,
) -> str:
    text = str(message)
    if not text and isinstance(message, BaseException):
        text = message.__class__.__name__
    text = _redact_command_text(text, command=command)
    redacted, _ = _redact_sensitive_text(text, env=env)
    return redacted


def _redact_command_text(
    text: str,
    *,
    command: str | Sequence[str] | None,
) -> str:
    if command is None:
        return text

    candidates: list[str]
    if isinstance(command, str):
        candidates = [command, repr(command)]
    else:
        command_items = list(command)
        candidates = [str(command_items), repr(command_items), " ".join(command_items)]
        candidates.extend(
            item for item in command_items if _should_redact_command_item(item)
        )

    redacted = text
    unique_candidates: set[str] = set(candidates)
    sorted_candidates: list[str] = sorted(
        unique_candidates,
        key=lambda candidate: len(candidate),
        reverse=True,
    )
    for candidate in sorted_candidates:
        if candidate:
            redacted = redacted.replace(candidate, _REDACTION_MARKER)
    return redacted


def _should_redact_command_item(item: str) -> bool:
    return len(item) >= 8 or any(char in item for char in " '\"=(){}[]")


def _redact_env_values(text: str, *, env: Mapping[str, str] | None) -> str:
    if not env:
        return text

    redacted = text
    values: list[str] = []
    for value in env.values():
        if value:
            values.append(value)
    values.sort(key=len, reverse=True)

    for value in values:
        redacted = redacted.replace(value, _REDACTION_MARKER)
    return redacted


def _redact_secret_key_values(text: str) -> str:
    def replace_authorization(match: re.Match[str]) -> str:
        key_quote = match.group("key_quote")
        value_quote = match.group("value_quote")
        return (
            f"{key_quote}{match.group('key')}{key_quote}{match.group('sep')}"
            f"{value_quote}{_REDACTION_MARKER}{value_quote}"
        )

    def replace_quoted(match: re.Match[str]) -> str:
        key_quote = match.group("key_quote")
        value_quote = match.group("value_quote")
        return (
            f"{key_quote}{match.group('key')}{key_quote}{match.group('sep')}"
            f"{value_quote}{_REDACTION_MARKER}{value_quote}"
        )

    def replace_unquoted(match: re.Match[str]) -> str:
        key_quote = match.group("key_quote")
        return (
            f"{key_quote}{match.group('key')}{key_quote}{match.group('sep')}"
            f"{_REDACTION_MARKER}"
        )

    redacted = _AUTHORIZATION_HEADER_RE.sub(replace_authorization, text)
    redacted = _SECRET_KEY_QUOTED_VALUE_RE.sub(replace_quoted, redacted)
    return _SECRET_KEY_UNQUOTED_VALUE_RE.sub(replace_unquoted, redacted)


def _redact_bearer_tokens(text: str) -> str:
    return _BEARER_TOKEN_RE.sub(f"Bearer {_REDACTION_MARKER}", text)


def _append_cleanup_warning(
    message: str,
    cleanup_error: str | None,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None,
) -> str:
    warning = (
        "Cleanup warning: sandbox session cleanup after the failed command "
        "did not complete."
    )
    if cleanup_error:
        warning = (
            f"{warning} "
            f"{_redact_provider_message(cleanup_error, env=env, command=command)}"
        )
    return f"{message}. {warning}"


def _normalize_max_chars(max_chars: int) -> int:
    if isinstance(max_chars, bool) or not isinstance(max_chars, int):
        raise KitaruUsageError("Sandbox command max_chars must be an integer.")
    if max_chars < 0:
        raise KitaruUsageError("Sandbox command max_chars must be >= 0.")
    return max_chars


def normalize_timeout_seconds(
    timeout_seconds: float | None,
    *,
    error_message: str,
) -> float | None:
    if timeout_seconds is None:
        return None
    if isinstance(timeout_seconds, bool) or not isinstance(
        timeout_seconds, int | float
    ):
        raise KitaruUsageError(error_message)
    normalized = float(timeout_seconds)
    if not math.isfinite(normalized) or normalized <= 0:
        raise KitaruUsageError(error_message)
    return normalized


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


def _session_id(session: Any | None) -> str | None:
    if session is None:
        return None
    return _optional_string_attribute(session, "id") or _optional_string_attribute(
        session, "session_id"
    )


def _collect_process_output(
    process: Any,
    *,
    max_chars: int,
    timeout_seconds: float | None,
) -> _ProcessOutputCollection:
    if timeout_seconds is None:
        return _ProcessOutputCollection(output=process.collect(max_chars=max_chars))

    result_queue: Queue[tuple[bool, Any]] = Queue(maxsize=1)

    def _collect_in_background() -> None:
        try:
            result_queue.put_nowait((True, process.collect(max_chars=max_chars)))
        except Exception as exc:
            result_queue.put_nowait((False, exc))

    collect_thread = Thread(
        target=_collect_in_background,
        name="kitaru-sandbox-process-collect",
        daemon=True,
    )
    collect_thread.start()

    try:
        succeeded, value = result_queue.get(timeout=timeout_seconds)
    except Empty:
        _kill_timed_out_process(process)
        # The sandbox backend only gives us `collect()` and optional `kill()`.
        # If `kill()` does not make the backend's blocking `collect()` return,
        # Python cannot safely stop that in-flight call. The daemon worker keeps
        # interpreter shutdown unblocked, and the session cleanup below still runs
        # best-effort destroy/close against the provider.
        return _ProcessOutputCollection(timed_out=True)

    if not succeeded:
        raise cast(Exception, value)
    return _ProcessOutputCollection(output=value)


def _kill_timed_out_process(process: Any) -> None:
    kill = getattr(process, "kill", None)
    if not callable(kill):
        return
    with suppress(Exception):
        kill()


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


def _cleanup_after_failed_command(
    session: Any | None,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None,
) -> tuple[bool, str | None]:
    if session is None:
        return True, None

    try:
        return _apply_cleanup_policy(session, cleanup, env=env, command=command)
    except Exception as exc:
        error = _redact_provider_message(exc, env=env, command=command)
        return False, f"Sandbox cleanup after command failure failed: {error}"


def _cleanup_after_timed_out_command(
    session: Any | None,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None,
) -> tuple[bool, str | None]:
    if session is None:
        return True, None

    try:
        return _apply_cleanup_policy(session, cleanup, env=env, command=command)
    except Exception as exc:
        error = _redact_provider_message(exc, env=env, command=command)
        return False, f"Sandbox cleanup after command timeout failed: {error}"


def _apply_cleanup_policy(
    session: Any,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None = None,
    command: str | Sequence[str] | None = None,
) -> tuple[bool, str | None]:
    if cleanup == CLEANUP_DESTROY:
        return _destroy_session_or_close_if_unsupported(
            session,
            env=env,
            command=command,
        )
    session.close()
    return True, None


def _destroy_session_or_close_if_unsupported(
    session: Any,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None,
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
                f"{_redact_provider_message(close_exc, env=env, command=command)}"
            )
        destroy_error = _redact_provider_message(exc, env=env, command=command)
        cleanup_error = (
            "Sandbox session destroy is not supported by this provider; "
            f"best-effort close was attempted. {destroy_error}{close_error or ''}"
        )
        return False, cleanup_error
    return True, None


def _cleanup_after_success(
    session: Any,
    cleanup: SandboxCleanupPolicy,
    *,
    env: Mapping[str, str] | None,
    command: str | Sequence[str] | None,
) -> tuple[bool, str | None]:
    if cleanup == CLEANUP_CLOSE:
        try:
            session.close()
        except Exception as exc:
            error = _redact_provider_message(exc, env=env, command=command)
            raise KitaruBackendError(
                "Sandbox command completed, but closing the sandbox session failed: "
                f"{error}"
            ) from None
        return True, None

    try:
        cleanup_succeeded, cleanup_error = _destroy_session_or_close_if_unsupported(
            session,
            env=env,
            command=command,
        )
    except Exception as exc:
        with suppress(Exception):
            session.close()
        error = _redact_provider_message(exc, env=env, command=command)
        raise KitaruBackendError(
            "Sandbox command completed, but destroying the sandbox session failed: "
            f"{error}"
        ) from None

    if cleanup_error is not None:
        cleanup_error = _redact_provider_message(
            cleanup_error,
            env=env,
            command=command,
        )
    return cleanup_succeeded, cleanup_error


__all__ = [
    "DEFAULT_SANDBOX_COMMAND_MAX_CHARS",
    "SandboxCleanupPolicy",
    "SandboxCommandResult",
    "active_sandbox_cache_identity",
    "run_sandbox_command",
]
