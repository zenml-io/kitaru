from __future__ import annotations

from typing import Any

import kitaru

from ._toolset import _ADAPTER_ID, _ADAPTER_METADATA_KEY

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
    """Pause the running agent turn until the human supplies input."""
    combined_metadata: dict[str, Any] = {
        _ADAPTER_METADATA_KEY: _ADAPTER_ID,
        _SOURCE_METADATA_KEY: _SOURCE_TOOL_BODY,
    }
    if metadata:
        combined_metadata.update(metadata)

    return kitaru.wait(
        schema=schema,
        name=name,
        question=question,
        timeout=timeout,
        metadata=combined_metadata,
    )
