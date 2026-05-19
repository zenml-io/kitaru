from __future__ import annotations

from typing import Any

import kitaru
from kitaru.errors import KitaruUsageError

from ._constants import ADAPTER_ID, ADAPTER_METADATA_KEY

_SOURCE_METADATA_KEY = 'source'
_SOURCE_TOOL_BODY = 'tool_body'
_ZENML_WORKER_THREAD_ERROR = 'must be called from the pipeline thread'


def wait_for_input(
    *,
    question: str | None = None,
    schema: Any = None,
    name: str | None = None,
    timeout: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    """Pause the running agent turn until the human supplies input.

    This is a thin metadata wrapper around ``kitaru.wait()``. Call it from
    flow scope, or from adapter paths that deliberately avoid a tool
    checkpoint. If an ordinary sync tool body needs to wait for a human,
    prefer ``@hitl_tool`` or pass both
    ``tool_checkpoint_config_by_name={"tool_name": False}`` and
    ``allow_sync_tool_body_waits=True`` to ``KitaruAgent``.
    """
    combined_metadata: dict[str, Any] = {
        **(metadata or {}),
        ADAPTER_METADATA_KEY: ADAPTER_ID,
        _SOURCE_METADATA_KEY: _SOURCE_TOOL_BODY,
    }

    try:
        return kitaru.wait(
            schema=schema,
            name=name,
            question=question,
            timeout=timeout,
            metadata=combined_metadata,
        )
    except RuntimeError as error:
        if _ZENML_WORKER_THREAD_ERROR not in str(error):
            raise
        raise KitaruUsageError(
            "`kp.wait_for_input(...)` was called from a sync Pydantic AI tool "
            "body that ran on a worker thread, but Kitaru waits must be "
            "created on the workflow thread. For an ordinary tool body, keep "
            "the agent in default granular mode, opt that tool out of the "
            "synthetic tool checkpoint with "
            "`tool_checkpoint_config_by_name={\"tool_name\": False}`, and "
            "pass `allow_sync_tool_body_waits=True` to `KitaruAgent`. The "
            "opt-out keeps the wait out of a checkpoint; the explicit flag "
            "asks Pydantic AI to keep supported sync tools on the workflow "
            "thread while the agent run is active. For a pure human-input "
            "tool, prefer `@hitl_tool(...)`. Or move the wait to explicit "
            "`@kitaru.flow` code before or after the agent turn."
        ) from error
