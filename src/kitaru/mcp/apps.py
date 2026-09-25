#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""MCP Apps views served by the Kitaru MCP server."""

import base64
from collections.abc import Sequence
from functools import cache
from importlib.resources import files
from typing import Any

from mcp.server.apps import Apps, Visibility

FAILURE_MATRIX_URI = "ui://kitaru/failure-matrix.html"


@cache
def load_failure_matrix_html() -> str:
    """Read the failure matrix view with the Kitaru wordmark inlined."""
    ui = files("kitaru.mcp") / "ui"
    logo = base64.b64encode((ui / "kitaru-logo.svg").read_bytes()).decode()
    return (
        (ui / "failure_matrix.html")
        .read_text(encoding="utf-8")
        .replace("{{KITARU_LOGO}}", f"data:image/svg+xml;base64,{logo}")
    )


def ui_meta(
    resource_uri: str, visibility: Sequence[Visibility] | None = None
) -> dict[str, Any]:
    """Build the `_meta` that links a tool to a view registered on this server.

    Raises:
        ValueError: No view is registered under `resource_uri`.
    """
    if resource_uri != FAILURE_MATRIX_URI:
        raise ValueError(f"No MCP Apps view is registered at {resource_uri!r}")
    ui: dict[str, Any] = {"resourceUri": resource_uri}
    if visibility is not None:
        ui["visibility"] = list(visibility)
    return {"ui": ui}


def build_apps_extension() -> Apps:
    """Build the MCP Apps extension carrying every Kitaru view resource."""
    apps = Apps()
    # The view inlines its script, styles, and logo, which the default MCP Apps
    # content security policy allows, so it declares no external domains.
    apps.add_html_resource(
        FAILURE_MATRIX_URI,
        load_failure_matrix_html(),
        name="failure_matrix_view",
        title="Transition failure matrix",
        description="Heatmap of where sessions first go wrong.",
    )
    return apps
