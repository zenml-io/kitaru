"""Repository discovery shared by cleanup and registration."""

from __future__ import annotations

import logging
from pathlib import Path

from zenml.client import Client

from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME

logger = logging.getLogger(__name__)


def find_repository_root() -> Path | None:
    """Resolve the Kitaru repository root without requiring a healthy store."""
    try:
        root = Client.find_repository()
        if root is not None:
            return Path(root).resolve()
    except Exception:
        logger.debug(
            "Client.find_repository() failed; trying the .kitaru marker.",
            exc_info=True,
        )

    cwd = Path.cwd().resolve()
    for parent in (cwd, *cwd.parents):
        if (parent / KITARU_REPOSITORY_DIRECTORY_NAME).is_dir():
            return parent
    return None
