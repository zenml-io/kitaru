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
    assert all(
        "<!--" not in entry["excerpt"] and "TODO(v2-launch)" not in entry["excerpt"]
        for entry in index["entries"]
    )
    assert all("x26;" not in entry["excerpt"] for entry in index["entries"])
    assert any("Agents & Sessions" in entry["excerpt"] for entry in index["entries"])
    for identifier in ("KITARU_SERVER_LOG_LEVEL", "exact_set"):
        assert any(identifier in entry["excerpt"] for entry in index["entries"])


def test_source_change_invalidates_index(monkeypatch) -> None:
    """A docs edit changes index provenance until regeneration."""
    original = build_mcp_docs_index.build_index()
    original_read_bytes = Path.read_bytes

    def changed_read_bytes(path: Path) -> bytes:
        content = original_read_bytes(path)
        return content + b"\n" if path == build_mcp_docs_index.TOC else content

    monkeypatch.setattr(Path, "read_bytes", changed_read_bytes)
    assert build_mcp_docs_index.build_index() != original


def test_fenced_code_comments_do_not_split_sections() -> None:
    """Shell comments inside fenced examples remain in their parent section."""
    for fence in ("```", "~~~"):
        markdown = (
            "# Page\n"
            "## Setup\n"
            f"{fence}bash\n"
            "# install the agent\n"
            "kitaru agent register\n"
            f"{fence}\n"
            "## Next step\n"
            "Continue here.\n"
        )
        sections = build_mcp_docs_index._get_sections(markdown)
        assert [heading for heading, _ in sections] == ["Page", "Setup", "Next step"]
        assert "# install the agent" in sections[1][1]


def test_clean_markdown_preserves_literal_angle_brackets() -> None:
    """Compatibility ranges and command placeholders survive HTML cleanup."""
    markdown = (
        "Node `>=22.22.0 <23 || >=26 <27`; "
        "run `kitaru login <server-url>` then `kitaru job watch <job-id>`. "
        '<a href="/docs">Read <strong>more</strong></a>.'
    )
    assert build_mcp_docs_index._clean_markdown(markdown) == (
        "Node >=22.22.0 <23 || >=26 <27 ; "
        "run kitaru login <server-url> then kitaru job watch <job-id> . "
        "Read more ."
    )


def test_multiline_html_comment_does_not_enter_sections() -> None:
    """Hidden maintainer notes do not become headings or excerpts."""
    markdown = (
        "# Page\n"
        "## Visible\n"
        "Published guidance.\n"
        "<!-- TODO(v2-launch): internal note\n"
        "## Hidden heading\n"
        "Do not show this. -->\n"
        "## Next\n"
        "More published guidance.\n"
    )
    sections = build_mcp_docs_index._get_sections(markdown)
    assert [heading for heading, _ in sections] == ["Page", "Visible", "Next"]
    assert "TODO" not in sections[1][1]
    assert "Do not show this" not in sections[1][1]


def test_long_excerpt_keeps_identifiers_together() -> None:
    """A long paragraph splits before configuration terms, not inside them."""
    body = "word " * 178 + "KITARU_SERVER_LOG_LEVEL exact_set"
    excerpts = build_mcp_docs_index._get_excerpts(body)
    assert len(excerpts) == 2
    assert all(0 < len(excerpt) <= 900 for excerpt in excerpts)
    assert "KITARU_SERVER_LOG_LEVEL exact_set" in excerpts[1]


def test_clean_markdown_decodes_html_entities() -> None:
    """GitBook card labels keep their visible ampersands."""
    assert build_mcp_docs_index._clean_markdown("Agents &#x26; Sessions") == (
        "Agents & Sessions"
    )
