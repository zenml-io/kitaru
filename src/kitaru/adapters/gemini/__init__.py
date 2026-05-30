"""Gemini Interactions adapter for Kitaru durable execution."""

from kitaru.errors import KitaruFeatureNotAvailableError

try:
    import google.genai  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name not in {"google", "google.genai"}:
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.gemini requires optional dependency `google-genai`. "
        "Install with `uv sync --extra gemini` or "
        "`pip install 'kitaru[gemini]'`."
    ) from exc

from ._agent import KitaruGeminiInteractionsRunner
from ._events import (
    GeminiAdapterEvent,
    GeminiInteractionEventError,
    GeminiInteractionRunEvent,
)
from ._policy import GeminiInteractionCapturePolicy
from ._types import (
    GeminiInteractionRequest,
    GeminiInteractionResult,
    GeminiInteractionStepSummary,
)
from ._utils import (
    CheckpointConfig,
    CheckpointRuntime,
    validate_checkpoint_strategy,
)

__all__ = [
    "CheckpointConfig",
    "CheckpointRuntime",
    "GeminiAdapterEvent",
    "GeminiInteractionCapturePolicy",
    "GeminiInteractionEventError",
    "GeminiInteractionRequest",
    "GeminiInteractionResult",
    "GeminiInteractionRunEvent",
    "GeminiInteractionStepSummary",
    "KitaruGeminiInteractionsRunner",
    "validate_checkpoint_strategy",
]
