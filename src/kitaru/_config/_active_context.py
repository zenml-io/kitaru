"""Active stack/project provenance helpers.

These helpers intentionally read the raw config files before constructing a
ZenML ``Client``. The client may sanitize stale active stack/project IDs while
it starts up, so diagnostics and safety checks need one "before" snapshot.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

import yaml
from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.constants import (
    CONFIG_FILE_NAME,
    ENV_ZENML_ACTIVE_PROJECT_ID,
    ENV_ZENML_ACTIVE_STACK_ID,
)
from zenml.utils import yaml_utils

from kitaru._config._env import KITARU_STACK_ENV
from kitaru._env import KITARU_PROJECT_ENV, KITARU_REPOSITORY_DIRECTORY_NAME

logger = logging.getLogger(__name__)

ProvenanceResource = Literal["active_stack", "active_project"]
ProvenanceSource = Literal[
    "environment",
    "repo-local config",
    "global config",
    "unset",
    "unknown",
]


@dataclass(frozen=True)
class ActiveConfigSelectionProvenance:
    """Raw and resolved provenance for an active stack/project selection."""

    resource: ProvenanceResource
    effective_source: ProvenanceSource
    effective_source_detail: str | None
    effective_id: str | None
    resolved_id: str | None = None
    resolved_name: str | None = None
    environment_variable: str | None = None
    environment_id: str | None = None
    repository_root: str | None = None
    repository_config_path: str | None = None
    repository_id: str | None = None
    global_config_path: str | None = None
    global_id: str | None = None
    notes: list[str] = field(default_factory=list)


def read_raw_yaml_mapping(path: Path) -> tuple[dict[str, Any], str | None]:
    """Read a YAML mapping for diagnostics without raising user-facing errors."""
    if not path.exists():
        return {}, None

    try:
        raw = yaml_utils.read_yaml(str(path))
    except (OSError, yaml.YAMLError) as exc:
        logger.debug("Could not read config file %s", path, exc_info=True)
        return {}, f"Could not read config file {path} ({type(exc).__name__}): {exc}"

    if raw is None:
        return {}, None
    if not isinstance(raw, dict):
        return {}, f"Config file exists but is not a mapping: {path}"
    return raw, None


def stringify_config_id(value: Any) -> str | None:
    """Return a non-empty string for an active config ID without validating it."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def find_repository_root_for_diagnostics() -> tuple[Path | None, str | None]:
    """Find the active repository root without constructing a ZenML client."""
    try:
        return Client.find_repository(enable_warnings=False), None
    except OSError as exc:
        return None, f"Could not inspect repository root: {type(exc).__name__}: {exc}"


def repo_local_config_path(repository_root: Path | str) -> Path:
    """Return the `.kitaru/config.yaml` path for a given repository root."""
    return Path(repository_root) / KITARU_REPOSITORY_DIRECTORY_NAME / CONFIG_FILE_NAME


def _build_selection_provenance(
    *,
    resource: ProvenanceResource,
    environment_variable: str,
    environment_id: str | None,
    repository_root: Path | None,
    repository_config_path: Path | None,
    repository_id: str | None,
    global_config_path: Path,
    global_id: str | None,
    notes: list[str],
) -> ActiveConfigSelectionProvenance:
    """Apply ZenML's active context precedence to raw diagnostic candidates."""
    effective_source: ProvenanceSource
    effective_source_detail: str | None
    effective_id: str | None

    if environment_id is not None:
        effective_source = "environment"
        effective_source_detail = environment_variable
        effective_id = environment_id
    elif repository_id is not None:
        effective_source = "repo-local config"
        effective_source_detail = (
            str(repository_config_path) if repository_config_path else None
        )
        effective_id = repository_id
    elif global_id is not None:
        effective_source = "global config"
        effective_source_detail = str(global_config_path)
        effective_id = global_id
    else:
        effective_source = "unset"
        effective_source_detail = None
        effective_id = None

    return ActiveConfigSelectionProvenance(
        resource=resource,
        effective_source=effective_source,
        effective_source_detail=effective_source_detail,
        effective_id=effective_id,
        environment_variable=environment_variable,
        environment_id=environment_id,
        repository_root=str(repository_root) if repository_root else None,
        repository_config_path=(
            str(repository_config_path) if repository_config_path else None
        ),
        repository_id=repository_id,
        global_config_path=str(global_config_path),
        global_id=global_id,
        notes=list(notes),
    )


def collect_active_context_provenance(
    gc: GlobalConfiguration,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[ActiveConfigSelectionProvenance, ActiveConfigSelectionProvenance]:
    """Collect raw active stack/project IDs before Client can sanitize them."""
    env = os.environ if environ is None else environ
    notes: list[str] = []

    repository_root, repo_note = find_repository_root_for_diagnostics()
    if repo_note:
        notes.append(repo_note)

    repository_config_path: Path | None = None
    if repository_root is not None:
        repository_config_path = repo_local_config_path(repository_root)

    global_config_path = Path(gc.config_directory) / CONFIG_FILE_NAME
    repo_config: dict[str, Any] = {}
    global_config: dict[str, Any] = {}

    if repository_config_path is not None:
        repo_config, repo_yaml_note = read_raw_yaml_mapping(repository_config_path)
        if repo_yaml_note:
            notes.append(repo_yaml_note)

    global_config, global_yaml_note = read_raw_yaml_mapping(global_config_path)
    if global_yaml_note:
        notes.append(global_yaml_note)

    stack_notes = list(notes)
    if env.get(KITARU_STACK_ENV):
        stack_notes.append(
            f"{KITARU_STACK_ENV} is an execution default and does not set "
            "ZenML's active stack."
        )

    kitaru_project_id = stringify_config_id(env.get(KITARU_PROJECT_ENV))
    zenml_project_id = stringify_config_id(env.get(ENV_ZENML_ACTIVE_PROJECT_ID))
    project_environment_id = kitaru_project_id or zenml_project_id
    project_environment_variable = ENV_ZENML_ACTIVE_PROJECT_ID
    project_notes = list(notes)
    if kitaru_project_id is not None:
        project_environment_variable = (
            f"{KITARU_PROJECT_ENV} -> {ENV_ZENML_ACTIVE_PROJECT_ID}"
        )
        if zenml_project_id is not None and zenml_project_id != kitaru_project_id:
            project_notes.append(
                f"Both {KITARU_PROJECT_ENV} and {ENV_ZENML_ACTIVE_PROJECT_ID} "
                f"are set with different values; {KITARU_PROJECT_ENV} takes "
                "precedence via Kitaru's init-hook translation."
            )

    active_stack_provenance = _build_selection_provenance(
        resource="active_stack",
        environment_variable=ENV_ZENML_ACTIVE_STACK_ID,
        environment_id=stringify_config_id(env.get(ENV_ZENML_ACTIVE_STACK_ID)),
        repository_root=repository_root,
        repository_config_path=repository_config_path,
        repository_id=stringify_config_id(repo_config.get("active_stack_id")),
        global_config_path=global_config_path,
        global_id=stringify_config_id(global_config.get("active_stack_id")),
        notes=stack_notes,
    )
    active_project_provenance = _build_selection_provenance(
        resource="active_project",
        environment_variable=project_environment_variable,
        environment_id=project_environment_id,
        repository_root=repository_root,
        repository_config_path=repository_config_path,
        repository_id=stringify_config_id(repo_config.get("active_project_id")),
        global_config_path=global_config_path,
        global_id=stringify_config_id(global_config.get("active_project_id")),
        notes=project_notes,
    )
    return active_stack_provenance, active_project_provenance


def with_resolved_selection(
    provenance: ActiveConfigSelectionProvenance | None,
    *,
    resolved_id: str | None,
    resolved_name: str | None,
) -> ActiveConfigSelectionProvenance | None:
    """Attach resolved Client details to a previously captured provenance row."""
    if provenance is None:
        return None
    return replace(
        provenance,
        resolved_id=resolved_id,
        resolved_name=resolved_name,
    )


def selection_resolved_via_fallback(
    provenance: ActiveConfigSelectionProvenance | None,
) -> bool:
    """Return whether a saved config ID resolved to a different resource.

    Environment values are explicit overrides, and Kitaru/ZenML may accept a
    human-readable name there before resolving it to a UUID. Treating that
    name→UUID resolution as a saved-context fallback creates false alarms such
    as ``KITARU_PROJECT=production`` resolving to the production project's ID.
    The fallback warning is only for repo-local/global config values that ZenML
    silently sanitized while loading.
    """
    if provenance is None:
        return False
    if provenance.effective_source not in {"repo-local config", "global config"}:
        return False
    if provenance.effective_id is None or provenance.resolved_id is None:
        return False
    return provenance.effective_id != provenance.resolved_id


def active_context_fallback_warning(
    *,
    active_stack: ActiveConfigSelectionProvenance | None,
    active_project: ActiveConfigSelectionProvenance | None,
) -> str | None:
    """Render a clear warning for active stack/project fallback diagnostics."""
    lines: list[str] = []
    for label, selection in (
        ("active stack", active_stack),
        ("active project", active_project),
    ):
        if not selection_resolved_via_fallback(selection) or selection is None:
            continue
        source = selection.effective_source
        if selection.effective_source_detail:
            source = f"{source} ({selection.effective_source_detail})"
        resolved = selection.resolved_id or "unknown"
        if selection.resolved_name:
            resolved = f"{selection.resolved_name} ({resolved})"
        lines.append(
            f"Configured {label} from {source} points to ID "
            f"{selection.effective_id!r}, but Kitaru loaded {resolved}."
        )

    if not lines:
        return None

    return "\n".join(
        [
            "Kitaru detected that the saved active context changed while loading.",
            *lines,
            (
                "This usually means the saved stack or project no longer exists, "
                "or your current credentials cannot access it."
            ),
            (
                "Choose the intended stack/project explicitly before running "
                "workflows to avoid accidentally using a fallback context."
            ),
        ]
    )
