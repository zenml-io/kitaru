# Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Build the offline MCP search index from the hand-written GitBook pages."""

import argparse
import hashlib
import json
import re
from html import unescape
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
BOOK = ROOT / "docs/book"
TOC = BOOK / "toc.md"
URL_MAP = ROOT / "scripts/mcp_docs_urls.json"
OUTPUT = ROOT / "src/kitaru/mcp/data/docs_index.json"
TOC_LINK = re.compile(r"^\s*- \[([^]]+)\]\(([^)]+\.md)\)$", re.MULTILINE)
HEADING = re.compile(r"^(#{1,4})\s+(.+)$", re.MULTILINE)
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
FENCE_CLOSE = re.compile(r"^ {0,3}(`{3,}|~{3,})[ \t]*$")
HTML_TAG = re.compile(
    r"</?(?:a|figcaption|figure|img|strong|table|tbody|td|th|thead|tr)"
    r"(?:\s[^>]*|/?)>",
    re.IGNORECASE,
)


def _get_pages() -> list[tuple[str, str, Path, str]]:
    """Read ordered TOC entries and their verified published destinations."""
    links = TOC_LINK.findall(TOC.read_text())
    urls: dict[str, str] = json.loads(URL_MAP.read_text())
    paths = [path for _, path in links]
    if len(paths) != len(set(paths)) or set(urls) != set(paths):
        raise ValueError("Published URL map must match unique TOC pages exactly")
    pages = []
    for title, source in links:
        path = BOOK / source
        if not path.is_file():
            raise ValueError(f"TOC page is missing: {source}")
        url = urls[source]
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "docs.zenml.io"
            or not (parsed.path == "/kitaru" or parsed.path.startswith("/kitaru/"))
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"Invalid published Kitaru URL for {source}: {url}")
        pages.append((title, source, path, url))
    return pages


def _get_revision(pages: list[tuple[str, str, Path, str]]) -> str:
    """Hash every input that changes the generated index."""
    digest = hashlib.sha256()
    for path in [TOC, URL_MAP, *(page[2] for page in pages)]:
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _clean_markdown(value: str) -> str:
    """Remove GitBook and Markdown presentation syntax from a short excerpt."""
    value = unescape(value)
    value = re.sub(r"\{%[^%]*%\}", " ", value)
    value = HTML_TAG.sub(" ", value)
    value = re.sub(r"!\[[^]]*\]\([^)]+\)", " ", value)
    value = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", value)
    value = re.sub(r"(?<!\|)\|(?!\|)", " ", value)
    value = re.sub(r"[`*#]", " ", value)
    return " ".join(value.split())


def _get_sections(markdown: str) -> list[tuple[str, str]]:
    """Split a page into sections headed by H1-H4."""
    markdown = re.sub(r"\A---\n.*?\n---\n", "", markdown, flags=re.DOTALL)
    markdown = re.sub(r"<!--.*?-->", " ", markdown, flags=re.DOTALL)
    headings: list[tuple[int, int, str]] = []
    fence = ""
    offset = 0
    for line in markdown.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        marker = FENCE.match(content)
        if fence:
            closing = FENCE_CLOSE.match(content)
            if (
                closing
                and closing.group(1)[0] == fence[0]
                and len(closing.group(1)) >= len(fence)
            ):
                fence = ""
        elif marker:
            fence = marker.group(1)
        else:
            heading = HEADING.match(content)
            if heading:
                headings.append((offset, offset + heading.end(), heading.group(2)))
        offset += len(line)
    sections = []
    for position, (_, body_start, title) in enumerate(headings):
        end = len(markdown)
        if position + 1 < len(headings):
            end = headings[position + 1][0]
        sections.append((_clean_markdown(title), markdown[body_start:end]))
    return sections


def _get_excerpts(body: str) -> list[str]:
    """Keep bounded, readable section excerpts, including useful code examples."""
    paragraphs = [_clean_markdown(part) for part in re.split(r"\n\s*\n", body)]
    paragraphs = [part for part in paragraphs if part]
    excerpts: list[str] = []
    current = ""
    for paragraph in paragraphs:
        while len(paragraph) > 900:
            if current:
                excerpts.append(current)
                current = ""
            split_at = paragraph.rfind(" ", 0, 901)
            if split_at < 1:
                split_at = 900
            excerpts.append(paragraph[:split_at].rstrip())
            paragraph = paragraph[split_at:].lstrip()
        if current and len(current) + len(paragraph) + 1 > 900:
            excerpts.append(current)
            current = ""
        current = f"{current} {paragraph}".strip()
    if current:
        excerpts.append(current)
    return excerpts


def build_index() -> bytes:
    """Generate the deterministic packaged index as UTF-8 JSON."""
    pages = _get_pages()
    entries = []
    for title, source, path, url in pages:
        for heading, body in _get_sections(path.read_text()):
            for excerpt in _get_excerpts(body):
                entries.append(
                    {
                        "title": title,
                        "heading": heading,
                        "excerpt": excerpt,
                        "url": url,
                        "source": source,
                    }
                )
    if not entries:
        raise ValueError("Documentation index would be empty")
    document = {"source_revision": _get_revision(pages), "entries": entries}
    return (
        json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()


def main() -> None:
    """Write the index or check that the committed artifact is current."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="fail if the index is stale"
    )
    arguments = parser.parse_args()
    content = build_index()
    if arguments.check:
        if not OUTPUT.is_file() or OUTPUT.read_bytes() != content:
            parser.error("MCP docs index is stale; run scripts/build_mcp_docs_index.py")
    else:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_bytes(content)


if __name__ == "__main__":
    main()
