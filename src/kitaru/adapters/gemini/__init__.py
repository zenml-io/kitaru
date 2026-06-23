"""Gemini Interactions adapter for Kitaru durable execution."""

import importlib
from typing import Any

from kitaru.errors import KitaruFeatureNotAvailableError

from ._constants import INTERACTIONS_CONTRACT_ERROR_MESSAGE

_INTERACTION_TYPE_MODULE_PATHS = (
    "google.genai._interactions.types",
    "google.genai._gaos.types.interactions.step",
)

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


def _annotations_contain_all(value: Any, fields: set[str]) -> bool:
    annotations = getattr(value, "__annotations__", None)
    return isinstance(annotations, dict) and fields.issubset(annotations)


def _import_interaction_types() -> Any:
    """Import the SDK module that exposes Interactions step model classes."""
    last_error: ModuleNotFoundError | None = None
    for module_path in _INTERACTION_TYPE_MODULE_PATHS:
        try:
            return importlib.import_module(module_path)
        except ModuleNotFoundError as exc:
            if exc.name is not None and (
                module_path == exc.name or module_path.startswith(f"{exc.name}.")
            ):
                last_error = exc
                continue
            raise
    raise KitaruFeatureNotAvailableError(
        INTERACTIONS_CONTRACT_ERROR_MESSAGE
    ) from last_error


def _validate_interactions_preview_contract() -> None:
    if not hasattr(_google_genai, "Client"):
        raise KitaruFeatureNotAvailableError(INTERACTIONS_CONTRACT_ERROR_MESSAGE)
    interaction_types = _import_interaction_types()
    if not (
        _annotations_contain_all(
            getattr(interaction_types, "FunctionCallStep", None),
            {"arguments", "id", "name", "type"},
        )
        and _annotations_contain_all(
            getattr(interaction_types, "FunctionResultStep", None),
            {"call_id", "name", "result", "type"},
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
from ._sandbox_functions import (  # noqa: E402
    GeminiSandboxFunctionExecution,
    GeminiSandboxFunctionSpec,
    execute_gemini_sandbox_function_call,
)
from ._streaming import (  # noqa: E402
    GEMINI_STREAM_COMPLETED,
    GEMINI_STREAM_EVENT,
    GEMINI_STREAM_EVENT_KINDS,
    GEMINI_STREAM_FAILED,
    GEMINI_STREAM_STARTED,
    GEMINI_STREAM_TERMINAL_EVENT_KINDS,
)
from ._types import (  # noqa: E402
    GeminiInteractionFunctionCall,
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
    "GEMINI_STREAM_COMPLETED",
    "GEMINI_STREAM_EVENT",
    "GEMINI_STREAM_EVENT_KINDS",
    "GEMINI_STREAM_FAILED",
    "GEMINI_STREAM_STARTED",
    "GEMINI_STREAM_TERMINAL_EVENT_KINDS",
    "CheckpointConfig",
    "CheckpointRuntime",
    "GeminiAdapterEvent",
    "GeminiInteractionCapturePolicy",
    "GeminiInteractionEventError",
    "GeminiInteractionFunctionCall",
    "GeminiInteractionRequest",
    "GeminiInteractionResult",
    "GeminiInteractionRunEvent",
    "GeminiInteractionStepSummary",
    "GeminiSandboxFunctionExecution",
    "GeminiSandboxFunctionSpec",
    "KitaruGeminiInteractionsRunner",
    "execute_gemini_sandbox_function_call",
    "validate_checkpoint_strategy",
]
