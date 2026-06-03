"""Tiny adapter-local accessors for Kitaru runtime scope state."""

try:
    from kitaru.runtime import _is_inside_checkpoint as is_inside_checkpoint
    from kitaru.runtime import _is_inside_flow as is_inside_flow
except ImportError as error:  # pragma: no cover - unsupported old Kitaru only
    raise ImportError(
        "Unsupported Kitaru version for `kitaru.adapters.gemini`: install a "
        "Kitaru release that exposes `_is_inside_checkpoint` and "
        "`_is_inside_flow`."
    ) from error

__all__ = ["is_inside_checkpoint", "is_inside_flow"]
