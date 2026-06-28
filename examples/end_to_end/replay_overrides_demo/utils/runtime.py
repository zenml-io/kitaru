"""Small runtime helpers for the replay overrides demo."""

from __future__ import annotations

import logging
import warnings
from typing import Any

from kitaru.errors import KitaruExecutionError


def quiet_runtime_logs() -> None:
    """Hide Kitaru/ZenML/HTTP progress noise while examples run."""
    for name in ("", "zenml", "kitaru", "httpx", "httpcore", "openai"):
        logging.getLogger(name).setLevel(logging.WARNING)
    warnings.filterwarnings(
        "ignore",
        message=r"In v2\.0, 'openai:' will resolve to the OpenAI Responses API.*",
        category=DeprecationWarning,
    )


def wait_for_execution(handle: Any) -> str:
    """Block until a flow or replay handle finishes."""
    try:
        handle.wait()
    except KitaruExecutionError as exc:
        exec_id = getattr(handle, "exec_id", "<unknown>")
        status = exc.status.value if exc.status is not None else "failure"
        raise RuntimeError(
            f"Execution {exec_id} finished with {status}."
        ) from exc
    return str(handle.exec_id)
