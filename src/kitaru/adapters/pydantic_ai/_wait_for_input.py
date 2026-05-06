from __future__ import annotations

from typing import Any

import kitaru

from ._constants import ADAPTER_ID, ADAPTER_METADATA_KEY

_SOURCE_METADATA_KEY = 'source'
_SOURCE_TOOL_BODY = 'tool_body'


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
    checkpoint. If a tool body needs to wait for a human, prefer
    ``@hitl_tool`` or opt that tool out of granular checkpointing with
    ``tool_checkpoint_config_by_name={"tool_name": False}``.
    """
    combined_metadata: dict[str, Any] = {
        **(metadata or {}),
        ADAPTER_METADATA_KEY: ADAPTER_ID,
        _SOURCE_METADATA_KEY: _SOURCE_TOOL_BODY,
    }

    return kitaru.wait(
        schema=schema,
        name=name,
        question=question,
        timeout=timeout,
        metadata=combined_metadata,
    )
