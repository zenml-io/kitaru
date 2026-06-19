"""Pure helpers for selecting chatbot history artifacts.

The Gradio UI imports these helpers, but they deliberately know nothing about
Gradio or the Kitaru client. Tests can pass small fake artifact objects with a
``name`` attribute and a ``load()`` method.
"""

import logging
from collections.abc import Iterable
from typing import Any, Protocol

HISTORY_ARTIFACT_NAME = "history"
_ALLOWED_ROLES = {"assistant", "user"}
Message = dict[str, str]
_LOGGER = logging.getLogger(__name__)


class HistoryArtifact(Protocol):
    """Small contract needed to inspect and load an artifact candidate."""

    name: str

    def load(self) -> Any:
        """Return the stored artifact value."""
        ...


def normalize_history(raw: Any) -> list[Message] | None:
    """Normalize a loaded history value, or return ``None`` if it is malformed.

    A candidate history artifact is malformed if any item lacks readable
    ``role`` and ``content`` values. Valid values are coerced to strings to
    preserve the original UI behavior.
    """
    if raw is None:
        return []

    try:
        iterator = iter(raw)
    except Exception:
        return None

    normalized: list[Message] = []
    while True:
        try:
            item = next(iterator)
        except StopIteration:
            break
        except Exception:
            return None

        message = _normalize_message(item)
        if message is None:
            return None
        normalized.append(message)
    return normalized


def load_longest_usable_history(artifacts: Iterable[HistoryArtifact]) -> list[Message]:
    """Load history artifacts and return the longest usable transcript.

    Unloadable artifacts and malformed loaded values are ignored. When two
    usable histories have the same length, the first encountered candidate wins.
    ``ArtifactRef`` does not expose reliable freshness metadata, so equal-length
    candidates are treated as ties rather than "newer" or "older" versions.
    """
    best: list[Message] = []
    for artifact in artifacts:
        if artifact.name != HISTORY_ARTIFACT_NAME:
            continue
        try:
            raw = artifact.load()
        except Exception:
            _LOGGER.debug("Skipping unloadable history artifact.", exc_info=True)
            continue
        history = normalize_history(raw)
        if history is None:
            _LOGGER.debug("Skipping malformed history artifact.")
            continue
        if len(history) > len(best):
            best = history
    return best


def _normalize_message(item: Any) -> Message | None:
    role = _read_message_value(item, "role")
    content = _read_message_value(item, "content")
    if role is None or content is None or role not in _ALLOWED_ROLES:
        return None
    return {"role": role, "content": content}


def _read_message_value(item: Any, field: str) -> str | None:
    if isinstance(item, dict):
        if field not in item:
            return None
        value = item[field]
    else:
        try:
            value = getattr(item, field)
        except Exception:
            return None

    if value is None:
        return None
    try:
        return str(value)
    except Exception:
        return None
