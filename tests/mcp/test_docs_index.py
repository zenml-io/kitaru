# Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Source and package contracts for the offline MCP documentation index."""

import json
from importlib.resources import files
from pathlib import Path

from scripts import build_mcp_docs_index


def test_packaged_index_matches_sources() -> None:
    """The committed package artifact reflects the current TOC and pages."""
    packaged = files("kitaru.mcp.data").joinpath("docs_index.json").read_bytes()
    assert packaged == build_mcp_docs_index.build_index()


def test_index_covers_toc_with_published_urls() -> None:
    """Every TOC page has an absolute published URL and searchable excerpt."""
    index = json.loads(build_mcp_docs_index.build_index())
    pages = build_mcp_docs_index._get_pages()
    assert len(pages) >= 50
    assert {entry[1] for entry in pages} == {
        entry["source"] for entry in index["entries"]
    }
    assert len(index["source_revision"]) == 64
    assert all(
        entry["title"]
        and entry["heading"]
        and entry["excerpt"]
        and entry["url"].startswith("https://docs.zenml.io/kitaru")
        and len(entry["excerpt"]) <= 900
        for entry in index["entries"]
    )
    assert any(
        "on_miss" in entry["excerpt"] and entry["source"] == "guides/tool-policies.md"
        for entry in index["entries"]
    )


def test_source_change_invalidates_index(monkeypatch) -> None:
    """A docs edit changes index provenance until regeneration."""
    original = build_mcp_docs_index.build_index()
    original_read_bytes = Path.read_bytes

    def changed_read_bytes(path: Path) -> bytes:
        content = original_read_bytes(path)
        return content + b"\n" if path == build_mcp_docs_index.TOC else content

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    assert build_mcp_docs_index.build_index() != original
