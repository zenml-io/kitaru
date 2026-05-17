"""Tiny adapter-local accessors for Kitaru runtime scope state."""

try:
    from kitaru.runtime import (
        _get_current_checkpoint as get_current_checkpoint,
    )
    from kitaru.runtime import (
        _get_current_checkpoint_id as get_current_checkpoint_id,
    )
    from kitaru.runtime import (
        _get_current_checkpoint_name as get_current_checkpoint_name,
    )
    from kitaru.runtime import (
        _get_current_execution_id as get_current_execution_id,
    )
    from kitaru.runtime import (
        _is_inside_checkpoint as is_inside_checkpoint,
    )
    from kitaru.runtime import (
        _is_inside_flow as is_inside_flow,
    )
except ImportError as error:  # pragma: no cover - unsupported old Kitaru only
    raise ImportError(
        "Unsupported Kitaru version for `kitaru.adapters.openai_agents`: "
        "install a Kitaru release that exposes the runtime helpers "
        "(`_get_current_checkpoint`, `_get_current_checkpoint_id`, "
        "`_get_current_checkpoint_name`, `_get_current_execution_id`, "
        "`_is_inside_checkpoint`, "
        "`_is_inside_flow`)."
    ) from error

__all__ = [
    "get_current_checkpoint",
    "get_current_checkpoint_id",
    "get_current_checkpoint_name",
    "get_current_execution_id",
    "is_inside_checkpoint",
    "is_inside_flow",
]
