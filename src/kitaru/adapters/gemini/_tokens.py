"""Provider token normalization helpers for the Gemini adapter."""

from collections.abc import Sequence

CALL_ID_TYPE_FRAGMENTS = ("function_call", "tool_call")


def normalize_token(value: str | None) -> str | None:
    """Normalize provider event/type tokens for comparisons."""
    if value is None:
        return None
    normalized = "_".join(value.strip().lower().replace("-", "_").split())
    return normalized or None


def normalized_token_contains_any(
    value: str | None,
    fragments: Sequence[str],
) -> bool:
    """Return whether a normalized provider token contains any fragment.

    Google SDK shapes are not consistent about separators: one backend can emit
    ``function_call`` while another exposes a class-like ``FunctionCallStep``.
    Compare both the normalized token and a separator-free variant so one helper
    owns that tolerance.
    """
    normalized = normalize_token(value)
    if normalized is None:
        return False
    compact = normalized.replace("_", "")
    return any(
        fragment in normalized or fragment.replace("_", "") in compact
        for fragment in fragments
    )
