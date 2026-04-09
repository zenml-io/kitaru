"""Kitaru exception hierarchy and shared failure helpers."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Final


class FailureOrigin(StrEnum):
    """High-level origin categories for execution failures."""

    USER_CODE = "user_code"
    RUNTIME = "runtime"
    BACKEND = "backend"
    DIVERGENCE = "divergence"
    UNKNOWN = "unknown"


class KitaruError(Exception):
    """Base class for all Kitaru-specific exceptions."""


class KitaruUsageError(KitaruError, ValueError):
    """Raised when API inputs are invalid."""


class KitaruContextError(KitaruError, RuntimeError):
    """Raised when APIs are called outside their valid runtime context."""


class KitaruStateError(KitaruError, RuntimeError):
    """Raised when execution state does not allow the requested operation."""


class KitaruRuntimeError(KitaruError, RuntimeError):
    """Raised for runtime/serialization/materialization failures."""


class KitaruExecutionError(KitaruRuntimeError):
    """Raised when a flow execution finishes unsuccessfully."""

    exec_id: str | None
    status: str | None
    failure_origin: FailureOrigin | None

    def __init__(
        self,
        message: str,
        *,
        exec_id: str | None = None,
        status: str | None = None,
        failure_origin: FailureOrigin | None = None,
    ) -> None:
        super().__init__(message)
        self.exec_id = exec_id
        self.status = status
        self.failure_origin = failure_origin


class KitaruUserCodeError(KitaruExecutionError):
    """Raised when user checkpoint/flow code fails."""


class KitaruBackendError(KitaruRuntimeError):
    """Raised when Kitaru cannot communicate with the backend."""


class KitaruLogRetrievalError(KitaruBackendError):
    """Raised when runtime logs cannot be retrieved from the backend."""


class KitaruDivergenceError(KitaruExecutionError):
    """Raised when replay divergence is detected by the backend."""


class KitaruWaitValidationError(KitaruUsageError):
    """Raised when wait-resume input fails schema validation."""


class KitaruFeatureNotAvailableError(KitaruError, NotImplementedError):
    """Raised when a documented API is intentionally not implemented yet."""


_DIVERGENCE_HINTS: Final[tuple[str, ...]] = (
    "diverg",
    "durable call sequence",
    "replay mismatch",
)
_RUNTIME_HINTS: Final[tuple[str, ...]] = (
    "serialize",
    "serialization",
    "deserialize",
    "materializ",
    "hydrate",
    "reconstruct",
    "materializer",
)
_BACKEND_HINTS: Final[tuple[str, ...]] = (
    "backend communication",
    "connection refused",
    "transport error",
    "failed to fetch",
    "api unavailable",
)


def traceback_last_line(traceback: str | None) -> str | None:
    """Return the last non-empty traceback line when available."""
    if not traceback:
        return None

    for line in reversed(traceback.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def traceback_exception_type(traceback: str | None) -> str | None:
    """Extract the exception type name from traceback tail text."""
    tail = traceback_last_line(traceback)
    if tail is None:
        return None

    match = re.match(r"^([A-Za-z_][A-Za-z0-9_\.]*)\s*:", tail)
    if match:
        return match.group(1)
    return None


def classify_failure_origin(
    *,
    status_reason: str | None,
    traceback: str | None,
    default: FailureOrigin = FailureOrigin.UNKNOWN,
) -> FailureOrigin:
    """Classify failure origin conservatively from available failure text."""
    chunks = [status_reason or "", traceback or ""]
    failure_text = "\n".join(chunks).lower()

    if any(hint in failure_text for hint in _DIVERGENCE_HINTS):
        return FailureOrigin.DIVERGENCE
    if any(hint in failure_text for hint in _RUNTIME_HINTS):
        return FailureOrigin.RUNTIME
    if any(hint in failure_text for hint in _BACKEND_HINTS):
        return FailureOrigin.BACKEND
    return default


def execution_error_from_failure(
    message: str,
    *,
    exec_id: str,
    status: str,
    origin: FailureOrigin,
) -> KitaruExecutionError:
    """Construct a typed execution error from classified failure origin."""
    error_type: type[KitaruExecutionError]
    if origin == FailureOrigin.DIVERGENCE:
        error_type = KitaruDivergenceError
    elif origin == FailureOrigin.USER_CODE:
        error_type = KitaruUserCodeError
    else:
        error_type = KitaruExecutionError

    return error_type(
        message,
        exec_id=exec_id,
        status=status,
        failure_origin=origin,
    )


def build_recovery_command(exec_id: str, *, status: str) -> str | None:
    """Return the CLI recovery command appropriate for a given run status.

    Args:
        exec_id: Execution identifier.
        status: Raw run status value (e.g. ``"failed"``).

    Returns:
        Copy-pasteable CLI command string, or ``None`` when no recovery
        action is available for the status.
    """
    if status == "failed":
        return f"kitaru executions retry {exec_id}"
    return None


def format_recovery_hint(exec_id: str, *, status: str) -> str | None:
    """Format a user-facing recovery hint for a non-successful execution.

    Args:
        exec_id: Execution identifier.
        status: Raw run status value.

    Returns:
        Multi-line hint string, or ``None`` when no recovery action
        is available.
    """
    command = build_recovery_command(exec_id, status=status)
    if command is None:
        return None
    return f"To retry this failed execution, run:\n\n  {command}"


__all__ = [
    "FailureOrigin",
    "KitaruBackendError",
    "KitaruContextError",
    "KitaruDivergenceError",
    "KitaruError",
    "KitaruExecutionError",
    "KitaruFeatureNotAvailableError",
    "KitaruLogRetrievalError",
    "KitaruRuntimeError",
    "KitaruStateError",
    "KitaruUsageError",
    "KitaruUserCodeError",
    "KitaruWaitValidationError",
    "build_recovery_command",
    "classify_failure_origin",
    "execution_error_from_failure",
    "format_recovery_hint",
    "traceback_exception_type",
    "traceback_last_line",
]
