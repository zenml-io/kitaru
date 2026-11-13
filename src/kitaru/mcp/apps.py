#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""MCP Apps views served by the Kitaru MCP server."""

import base64
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from typing import Any

from mcp.server.apps import Apps, Visibility

FAILURE_MATRIX_URI = "ui://kitaru/failure-matrix.html"
SESSION_VIEWER_URI = "ui://kitaru/session-viewer.html"


def _get_logo_data_uri() -> str:
    logo = (files("kitaru.mcp") / "ui" / "kitaru-logo.svg").read_bytes()
    return f"data:image/svg+xml;base64,{base64.b64encode(logo).decode()}"


@cache
def load_failure_matrix_html() -> str:
    """Read the failure matrix view with the Kitaru wordmark inlined."""
    return (
        (files("kitaru.mcp") / "ui" / "failure_matrix.html")
        .read_text(encoding="utf-8")
        .replace("{{KITARU_LOGO}}", _get_logo_data_uri())
    )


@cache
def load_session_viewer_html() -> str:
    """Read the session viewer loader page with the viewer runtime inlined."""
    ui = files("kitaru.mcp") / "ui"
    runtime = (ui / "kitaru-viewer.js").read_text(encoding="utf-8")
    return (
        (ui / "session_viewer.html")
        .read_text(encoding="utf-8")
        .replace("{{KITARU_LOGO}}", _get_logo_data_uri())
        .replace("/*{{KITARU_VIEWER_RUNTIME}}*/", runtime)
    )


@dataclass(frozen=True, slots=True)
class _View:
    load: Callable[[], str]
    name: str
    title: str
    description: str


# Every view inlines its scripts and styles, which the default MCP Apps content
# security policy allows, so none declares external domains. The session viewer
# mounts custom viewers it reads through a tool call, so they run under the
# same policy: inline code only, no network.
_VIEWS = {
    FAILURE_MATRIX_URI: _View(
        load_failure_matrix_html,
        "failure_matrix_view",
        "Transition failure matrix",
        "Heatmap of where sessions first go wrong.",
    ),
    SESSION_VIEWER_URI: _View(
        load_session_viewer_html,
        "session_viewer_view",
        "Session viewer",
        "Shows one session in a built-in or custom trace viewer and saves the "
        "reviewer's notes and answers to Kitaru.",
    ),
}


def ui_meta(
    resource_uri: str, visibility: Sequence[Visibility] | None = None
) -> dict[str, Any]:
    """Build the `_meta` that links a tool to a view registered on this server.

    Raises:
        ValueError: No view is registered under `resource_uri`.
    """
    if resource_uri not in _VIEWS:
        raise ValueError(f"No MCP Apps view is registered at {resource_uri!r}")
    ui: dict[str, Any] = {"resourceUri": resource_uri}
    if visibility is not None:
        ui["visibility"] = list(visibility)
    return {"ui": ui}


def is_model_visible(meta: Mapping[str, Any] | None) -> bool:
    """Return whether hosts show a tool with this `_meta` to the model.

    A tool with no `ui.visibility` is visible to both the model and views.
    """
    visibility = ((meta or {}).get("ui") or {}).get("visibility")
    return visibility is None or "model" in visibility


def build_apps_extension() -> Apps:
    """Build the MCP Apps extension carrying every Kitaru view resource."""
    apps = Apps()
    for uri, view in _VIEWS.items():
        apps.add_html_resource(
            uri,
            view.load(),
            name=view.name,
            title=view.title,
            description=view.description,
        )
    return apps
