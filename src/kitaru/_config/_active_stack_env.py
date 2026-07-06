"""Helpers for ZenML active-stack environment override warnings."""

from __future__ import annotations

import os
from collections.abc import Mapping
from uuid import UUID

from zenml.constants import ENV_ZENML_ACTIVE_STACK_ID

from kitaru._config._active_context import ActiveConfigSelectionProvenance


def _active_stack_env_value(
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Return the non-empty ZenML active-stack environment override."""
    env = os.environ if environ is None else environ
    stack_id = env.get(ENV_ZENML_ACTIVE_STACK_ID, "")
    return stack_id if stack_id.strip() else None


def _active_stack_env_recovery_action(update_target: str) -> str:
    """Return shared recovery action text for stale active-stack env vars."""
    return (
        f"Unset it, update it to {update_target}, or remove it from .envrc "
        "and reload the shell"
    )


def _stack_ids_match(selected_stack_id: str, env_stack_id: str) -> bool:
    """Return whether saved and env stack IDs identify the same stack."""
    normalized_selected_stack_id = selected_stack_id.strip()
    normalized_env_stack_id = env_stack_id.strip()

    try:
        return UUID(normalized_selected_stack_id) == UUID(normalized_env_stack_id)
    except ValueError:
        return normalized_env_stack_id == normalized_selected_stack_id


def active_stack_env_override_warning(
    *,
    selected_stack_name: str,
    selected_stack_id: str,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str] | None:
    """Warn when ZenML's active-stack env var overrides a saved stack change."""
    env_stack_id = _active_stack_env_value(environ)
    if env_stack_id is None:
        return None

    normalized_selected_stack_id = selected_stack_id.strip()
    if _stack_ids_match(normalized_selected_stack_id, env_stack_id):
        return None

    recovery = _active_stack_env_recovery_action("the selected stack ID")
    return (
        f"{ENV_ZENML_ACTIVE_STACK_ID} is set and will override the saved "
        "active stack in this shell.",
        (
            f"Kitaru saved stack '{selected_stack_name}' "
            f"({normalized_selected_stack_id}), but ZenML reads "
            f"{ENV_ZENML_ACTIVE_STACK_ID}='{env_stack_id}' before saved "
            f"config. {recovery}."
        ),
    )


def active_stack_env_resolution_warning(
    provenance: ActiveConfigSelectionProvenance | None,
) -> str | None:
    """Return recovery guidance after env-selected active-stack resolution fails."""
    if provenance is None:
        return None
    if provenance.resource != "active_stack":
        return None
    if provenance.effective_source != "environment":
        return None
    if provenance.effective_source_detail != ENV_ZENML_ACTIVE_STACK_ID:
        return None

    stack_id = provenance.effective_id
    if not stack_id:
        return None

    recovery = _active_stack_env_recovery_action("an existing stack ID")
    return (
        f"{ENV_ZENML_ACTIVE_STACK_ID} is set to '{stack_id}', and ZenML "
        "checks that environment variable before saved stack config. Kitaru "
        f"could not resolve that stack. {recovery} before retrying."
    )
