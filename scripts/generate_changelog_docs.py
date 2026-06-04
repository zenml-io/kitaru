"""Generate the docs changelog page from the repo-root CHANGELOG.md.

Reads CHANGELOG.md, prepends MDX frontmatter, and writes the result to
docs/content/docs/changelog.mdx.  The generated file is gitignored — it
only exists after running this script (or `just generate-docs`).

Usage:
    uv run python scripts/generate_changelog_docs.py
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CHANGELOG_SRC = REPO_ROOT / "CHANGELOG.md"
OUTPUT_FILE = REPO_ROOT / "docs" / "content" / "docs" / "changelog.mdx"

FRONTMATTER = """\
---
title: Changelog
description: Release history for Kitaru
---
"""

# Characters that MDX treats as JSX syntax
_MDX_BRACE = re.compile(r"([{}])")
_MDX_ANGLE = re.compile(r"<(?!/)")  # bare < not already part of a closing tag


def _escape_mdx_line(line: str) -> str:
    """Escape MDX-significant characters in a single line.

    Skips lines inside fenced code blocks (handled by the caller).
    """
    line = _MDX_BRACE.sub(r"\\\1", line)
    line = _MDX_ANGLE.sub("&lt;", line)
    return line


def transform_changelog(source: str) -> str:
    """Convert raw CHANGELOG.md content into MDX page body."""
    lines = source.splitlines()

    # Drop the top-level "# Changelog" heading — FumaDocs renders the
    # frontmatter title as the page heading, so keeping it would duplicate.
    body_lines: list[str] = []
    skipped_title = False
    for line in lines:
        if not skipped_title:
            stripped = line.strip()
            if stripped == "" or stripped.startswith("<!--"):
                body_lines.append(line)
                continue
            if stripped.startswith("# ") and "changelog" in stripped.lower():
                skipped_title = True
                continue
        body_lines.append(line)

    # Escape MDX-significant characters, but not inside fenced code blocks
    result: list[str] = []
    in_code_block = False
    for line in body_lines:
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            result.append(line)
            continue
        if in_code_block:
            result.append(line)
        else:
            result.append(_escape_mdx_line(line))

    return "\n".join(result)


def main() -> int:
    """Generate the changelog docs page."""
    if not CHANGELOG_SRC.exists():
        print(f"ERROR: {CHANGELOG_SRC} not found")
        return 1

    source = CHANGELOG_SRC.read_text()
    body = transform_changelog(source)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(FRONTMATTER + "\n" + body.strip() + "\n")

    rel = OUTPUT_FILE.relative_to(REPO_ROOT)
    print(f"Generated {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
