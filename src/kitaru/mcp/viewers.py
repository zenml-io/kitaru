#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Trace viewer pages: the built-in viewers and the user's viewers folder."""

import re
from importlib.resources import files
from pathlib import Path

from kitaru.client.config import get_config_directory
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.models.viewer import VIEWER_NAME_PATTERN
from kitaru.mcp.settings import MCPSettings

BUILTIN_VIEWERS = ("default",)
VIEWER_SUFFIX = ".html"
# A viewer is one self-contained page; a bound keeps one stray file from being
# sent through the host to the view in full.
MAX_VIEWER_BYTES = 4 * 1024 * 1024
_NAME = re.compile(VIEWER_NAME_PATTERN)


def get_viewers_directory(settings: MCPSettings) -> Path:
    """Return the folder custom viewers are read from."""
    if settings.viewers_dir is not None:
        return settings.viewers_dir.expanduser()
    return get_config_directory() / "viewers"


def list_viewers(settings: MCPSettings) -> list[str]:
    """List the built-in viewers, then the custom viewers in name order."""
    directory = get_viewers_directory(settings)
    custom: list[str] = []
    if directory.is_dir():
        custom = sorted(
            path.stem
            for path in directory.glob(f"*{VIEWER_SUFFIX}")
            if path.is_file()
            and _NAME.match(path.stem)
            and path.stem not in BUILTIN_VIEWERS
        )
    return [*BUILTIN_VIEWERS, *custom]


def ensure_viewer_exists(settings: MCPSettings, name: str) -> None:
    """Fail with the available viewers when `name` does not name one.

    Raises:
        MCPToolError: The name is invalid or no such viewer exists.
    """
    if name not in BUILTIN_VIEWERS:
        _get_custom_viewer_path(settings, name)


def read_viewer_source(settings: MCPSettings, name: str) -> str:
    """Read one viewer's HTML from disk, fresh on every call.

    Reading on every call means an edited viewer shows on its next open, with
    no server restart and no host-side cache in the way.

    Raises:
        MCPToolError: The name is invalid, no such viewer exists, or the file
            is too large or unreadable.
    """
    if name in BUILTIN_VIEWERS:
        return (
            files("kitaru.mcp") / "ui" / "viewers" / f"{name}{VIEWER_SUFFIX}"
        ).read_text(encoding="utf-8")
    path = _get_custom_viewer_path(settings, name)
    try:
        size = path.stat().st_size
        if size > MAX_VIEWER_BYTES:
            raise MCPToolError(
                "invalid_arguments",
                f"Viewer {name!r} is {size} bytes; the limit is {MAX_VIEWER_BYTES}.",
            )
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise MCPToolError(
            "invalid_configuration", f"Viewer {name!r} could not be read: {error}"
        ) from error


def _get_custom_viewer_path(settings: MCPSettings, name: str) -> Path:
    # The name pattern admits no path separators or dots, so the file cannot
    # sit outside the viewers folder.
    if not _NAME.match(name):
        raise MCPToolError("invalid_arguments", f"{name!r} is not a viewer name.")
    directory = get_viewers_directory(settings)
    path = directory / f"{name}{VIEWER_SUFFIX}"
    if not path.is_file():
        raise MCPToolError(
            "not_found",
            f"No viewer named {name!r}.",
            details={
                "viewers_directory": str(directory),
                "available": list_viewers(settings),
            },
            recovery=f"Save the viewer as {name}{VIEWER_SUFFIX} in the viewers "
            "folder, or open one of the available viewers.",
        )
    return path
