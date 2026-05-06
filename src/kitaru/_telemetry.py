"""Shared privacy-safe telemetry helpers for Kitaru internals."""

import logging

from kitaru.config import (
    classify_stack_deployment_type,
    classify_stack_model_deployment_type,
)

logger = logging.getLogger(__name__)


def _deployment_metadata_from_type(deployment_type: str) -> dict[str, str]:
    """Build the shared privacy-safe deployment metadata payload."""
    return {
        "kitaru_deployment_type": deployment_type,
        "deployment_type_source": "kitaru_stack_inference",
    }


def _unknown_deployment_metadata() -> dict[str, str]:
    """Build the privacy-safe fallback deployment metadata payload."""
    return {
        "kitaru_deployment_type": "unknown",
        "deployment_type_source": "kitaru_stack_inference_failed",
    }


def deployment_metadata_for_stack(stack_name_or_id: str | None) -> dict[str, str]:
    """Return privacy-safe deployment metadata for a stack selector.

    This path may resolve the stack through the backend. Prefer
    ``deployment_metadata_for_stack_model`` when a hydrated stack model is
    already available.
    """
    try:
        deployment_type = classify_stack_deployment_type(stack_name_or_id)
    except Exception:
        logger.debug(
            "Failed to classify stack deployment type for analytics.",
            exc_info=True,
        )
        return _unknown_deployment_metadata()

    return _deployment_metadata_from_type(deployment_type)


def deployment_metadata_for_stack_model(stack_model: object) -> dict[str, str]:
    """Return privacy-safe deployment metadata from an already-resolved stack.

    This avoids a second backend lookup for code paths, such as deployment
    invoke, that already loaded the deployment's hydrated stack model for
    validation.
    """
    try:
        deployment_type = classify_stack_model_deployment_type(stack_model)
    except Exception:
        logger.debug(
            "Failed to classify resolved stack model for analytics.",
            exc_info=True,
        )
        return _unknown_deployment_metadata()

    return _deployment_metadata_from_type(deployment_type)
