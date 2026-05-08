"""Best-effort Claude transcript file helpers."""

import re
from pathlib import Path
from typing import Any

_NON_ASCII_PATTERN = re.compile(r"[^A-Za-z0-9]")


def resolve_claude_transcript_path(session_id: str, *, cwd: str | Path) -> str:
    """Return Claude's expected local JSONL transcript path for a session."""
    if not session_id.strip():
        raise ValueError("session_id must be non-empty.")
    resolved_cwd = str(Path(cwd).expanduser().resolve())
    try:
        resolved_cwd.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError(
            "Claude transcript path resolution only supports ASCII cwd values."
        ) from exc
    encoded_cwd = _NON_ASCII_PATTERN.sub("-", resolved_cwd)
    return str(
        Path.home() / ".claude" / "projects" / encoded_cwd / f"{session_id}.jsonl"
    )


def load_transcript_payload(path: str) -> tuple[dict[str, Any] | None, list[str]]:
    """Load a transcript JSONL file if it exists.

    Missing or unreadable transcript files are warnings, not hard failures,
    because SDK transcript flushing and path layout are outside Kitaru's control.
    """
    transcript_path = Path(path)
    if not transcript_path.exists():
        return None, [f"Claude transcript file was not found at {path}."]
    try:
        content = transcript_path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, [f"Claude transcript file could not be read: {exc}."]
    return {"path": path, "format": "jsonl", "content": content}, []
