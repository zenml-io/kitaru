"""Gemini Interactions adapter for Kitaru durable execution."""

import importlib
from typing import Any

from kitaru.errors import KitaruFeatureNotAvailableError

from ._constants import INTERACTIONS_CONTRACT_ERROR_MESSAGE

try:
    import google.genai as _google_genai
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard only
    if exc.name not in {"google", "google.genai"}:
        raise
    raise KitaruFeatureNotAvailableError(
        "kitaru.adapters.gemini requires optional dependency `google-genai`. "
        "Install with `uv sync --extra gemini` or "
        "`pip install 'kitaru[gemini]'`."
    ) from exc


def _annotations_contain(value: Any, field: str) -> bool:
    annotations = getattr(value, "__annotations__", None)
    return isinstance(annotations, dict) and field in annotations


def _validate_interactions_preview_contract() -> None:
    if not hasattr(_google_genai, "Client"):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)
    try:
        interaction_types = importlib.import_module("google.genai._interactions.types")
    except ModuleNotFoundError as exc:
        if exc.name not in {
            "google.genai._interactions",
            "google.genai._interactions.types",
        }:
            raise
        raise KitaruFeatureNotAvailableError(
            INTERACTIONS_CONTRACT_ERROR_MESSAGE
        ) from exc
    if not (
        _annotations_contain(
            getattr(interaction_types, "FunctionCallContent", None), "id"
        )
        and _annotations_contain(
            getattr(interaction_types, "FunctionResultContent", None), "call_id"
        )
    ):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)


_validate_interactions_preview_contract()

from ._agent import KitaruGeminiInteractionsRunner  # noqa: E402
from ._events import (  # noqa: E402
    GeminiAdapterEvent,
    GeminiInteractionEventError,
    GeminiInteractionRunEvent,
)
from ._policy import GeminiInteractionCapturePolicy  # noqa: E402
from ._types import (  # noqa: E402
    GeminiInteractionRequest,
    GeminiInteractionResult,
    GeminiInteractionStepSummary,
)
from ._utils import (  # noqa: E402
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
