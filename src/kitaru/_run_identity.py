"""Best-effort identity extraction for ZenML run-like objects."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RunProjectIdentity:
    """Project identity exposed by a ZenML run, when available."""

    project_id: str | None = None
    project_name: str | None = None

    @property
    def name_or_id(self) -> str | None:
        """Return the project route value Kitaru should prefer."""
        return self.project_name or self.project_id


def _non_empty_string(value: Any) -> str | None:
    """Return a stripped string when the value has visible content."""
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _read_project_id(
    run: Any,
    *,
    logger: logging.Logger | None = None,
) -> str | None:
    """Read project ID from direct run fields or the run body."""
    for label, getter in (
        ("project_id", lambda: getattr(run, "project_id", None)),
        ("body.project_id", lambda: getattr(run.get_body(), "project_id", None)),
    ):
        try:
            project_id = _non_empty_string(getter())
        except Exception:
            if logger is not None:
                logger.debug(
                    "Failed to read project ID from run %s via %s.",
                    getattr(run, "id", "<unknown>"),
                    label,
                    exc_info=True,
                )
            continue
        if project_id is not None:
            return project_id
    return None


def _object_dict_value(obj: Any, key: str) -> Any | None:
    """Read a stored object attribute without invoking descriptors/properties."""
    try:
        values = vars(obj)
    except TypeError:
        return None
    return values.get(key)


def _project_name_from_project(value: Any) -> str | None:
    """Read a project object's name once the object is already in hand."""
    if value is None:
        return None
    try:
        return _non_empty_string(getattr(value, "name", None))
    except Exception:
        return None


def _project_name_from_non_fetching_fields(run: Any) -> str | None:
    """Read project name from fields that should already be present in memory."""
    stored_project = _object_dict_value(run, "project")
    project_name = _project_name_from_project(stored_project)
    if project_name is not None:
        return project_name

    resources = getattr(run, "resources", None)
    project_name = _project_name_from_project(getattr(resources, "project", None))
    if project_name is not None:
        return project_name

    try:
        body = run.get_body()
    except Exception:
        body = None
    project_name = _project_name_from_project(getattr(body, "project", None))
    if project_name is not None:
        return project_name

    return _non_empty_string(getattr(body, "project_name", None))


def _read_project_name(
    run: Any,
    *,
    allow_lazy_project_lookup: bool,
    logger: logging.Logger | None = None,
) -> str | None:
    """Read project name without lazy lookup unless explicitly allowed."""
    project_name = _project_name_from_non_fetching_fields(run)
    if project_name is not None or not allow_lazy_project_lookup:
        return project_name

    try:
        return _project_name_from_project(getattr(run, "project", None))
    except Exception:
        if logger is not None:
            logger.debug(
                "Failed to lazily read project name from run %s.",
                getattr(run, "id", "<unknown>"),
                exc_info=True,
            )
        return None


def extract_run_project_identity(
    run: Any,
    *,
    logger: logging.Logger | None = None,
    allow_lazy_project_lookup: bool = False,
) -> RunProjectIdentity:
    """Extract project identity from a ZenML run-like object.

    The default path is safe for bulk mapping: it reads direct IDs, body IDs, and
    already-loaded project objects only. Set ``allow_lazy_project_lookup=True``
    for one-off paths, such as execution URL logging, where resolving a lazy
    ``run.project`` property is acceptable.
    """
    return RunProjectIdentity(
        project_id=_read_project_id(run, logger=logger),
        project_name=_read_project_name(
            run,
            logger=logger,
            allow_lazy_project_lookup=allow_lazy_project_lookup,
        ),
    )
