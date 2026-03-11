"""Wait primitive for durable suspension.

``kitaru.wait()`` suspends a running flow until external input arrives.
The execution remains in ``waiting`` status and can be resumed later
via the client API or CLI.

Wait is valid only directly inside a flow, not inside a checkpoint.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, cast

from kitaru.errors import (
    KitaruContextError,
    KitaruFeatureNotAvailableError,
)
from kitaru.runtime import (
    _is_inside_checkpoint,
    _is_inside_flow,
    _sandbox_after_wait,
    _sandbox_before_wait,
)

_WAIT_OUTSIDE_FLOW_ERROR = "wait() can only run inside a @flow."
_WAIT_INSIDE_CHECKPOINT_ERROR = (
    "wait() cannot be called inside a @checkpoint. "
    "Call wait() in the flow body instead."
)
_DEFAULT_WAIT_TIMEOUT_SECONDS = 600


def _resolve_zenml_wait() -> Callable[..., tuple[Any, Any]]:
    """Load the upstream wait primitive from the installed ZenML build."""
    try:
        from zenml import wait as zenml_wait

        return cast(Callable[..., tuple[Any, Any]], zenml_wait)
    except ImportError:
        pass

    try:
        dynamic_utils = importlib.import_module(
            "zenml.execution.pipeline.dynamic.utils"
        )
    except ImportError as exc:
        raise KitaruFeatureNotAvailableError(
            "kitaru.wait() requires a ZenML build that includes wait support."
        ) from exc

    dynamic_wait = getattr(dynamic_utils, "wait", None)
    if callable(dynamic_wait):
        return cast(Callable[..., tuple[Any, Any]], dynamic_wait)

    raise KitaruFeatureNotAvailableError(
        "kitaru.wait() requires a ZenML build that includes wait support."
    )


def wait(
    *,
    schema: Any = bool,
    name: str | None = None,
    question: str | None = None,
    timeout: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    """Suspend the current flow until external input arrives.

    Args:
        schema: Expected type of the input value. Defaults to bool.
        name: Display name for this wait point.
        question: Human-readable prompt describing what input is needed.
        timeout: Resource-retention timeout in seconds (not expiration).
        metadata: Additional metadata to attach to the wait record.

    Returns:
        The validated input value once the execution is resumed.
    """
    if not _is_inside_flow():
        raise KitaruContextError(_WAIT_OUTSIDE_FLOW_ERROR)

    if _is_inside_checkpoint():
        raise KitaruContextError(_WAIT_INSIDE_CHECKPOINT_ERROR)

    resolved_timeout = _DEFAULT_WAIT_TIMEOUT_SECONDS if timeout is None else timeout
    zenml_wait = _resolve_zenml_wait()
    _sandbox_before_wait()
    try:
        resolved_value, _ = zenml_wait(
            schema=schema,
            question=question,
            timeout=resolved_timeout,
            metadata=metadata,
            key_prefix=name,
        )
    except Exception:
        _sandbox_after_wait()
        raise
    _sandbox_after_wait()
    return resolved_value
