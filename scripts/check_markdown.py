"""Check deterministic Markdown rules (codes KMD001 and up) in tracked files.

Prose is never hard-wrapped. GitBook pages under docs/book carry a description,
appear in toc.md, and link other pages without anchors, because Git Sync
rewrites cross-file anchors into broken URLs.
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

from markdown_it import MarkdownIt

if __package__:
    from scripts.build_mcp_docs_index import TOC_LINK
    from scripts.check_python_standards import Violation
else:
    from build_mcp_docs_index import TOC_LINK
    from check_python_standards import Violation

BOOK_PATH = Path("docs/book")
EXCLUDED_NAMES = {"CHANGELOG.md"}
UNPUBLISHED_BOOK_PAGES = {BOOK_PATH / "AGENTS.md", BOOK_PATH / "toc.md"}
MESSAGES = {
    "KMD001": "Join with the next line; `just fix` joins wrapped prose.",
    "KMD002": "Link the page without an anchor; GitBook breaks cross-file anchors.",
    "KMD003": "Add a description to the page's front matter.",
    "KMD004": "Register the page in docs/book/toc.md.",
}

PARSER = MarkdownIt("commonmark").enable("table")
# GitBook block tags such as {% hint %} are not Markdown, so they would otherwise
# merge with the paragraph under them.
GITBOOK_TAG = re.compile(r"^\s*\{%.*%\}\s*$")
HARD_BREAK = ("  ", "\\", "<br>")
BLOCKQUOTE_PREFIX = re.compile(r"^\s*(>\s*)*")
CROSS_FILE_ANCHOR = re.compile(r"[^:#]+\.md#")


def _get_front_matter_end(lines: list[str]) -> int:
    """Return the index of the first line after YAML front matter."""
    if not lines or lines[0].strip() != "---":
        return 0
    closing = next(
        (index for index, line in enumerate(lines[1:], 1) if line.strip() == "---"),
        None,
    )
    return 0 if closing is None else closing + 1


def _get_inline_blocks(text: str) -> list[tuple[int, list[str]]]:
    """Return each paragraph or heading as its first line index and source lines."""
    lines = text.split("\n")
    end = _get_front_matter_end(lines)
    markdown = "\n".join(
        "" if index < end or GITBOOK_TAG.match(line) else line
        for index, line in enumerate(lines)
    )
    return [
        (token.map[0], token.content.split("\n"))
        for token in PARSER.parse(markdown)
        if token.type == "inline" and token.map
    ]


def check_wrapping(text: str, path: Path) -> list[Violation]:
    """Find prose lines that a soft line break continues onto the next line."""
    return [
        Violation(path.as_posix(), start + offset + 1, "KMD001", MESSAGES["KMD001"])
        for start, block in _get_inline_blocks(text)
        for offset, line in enumerate(block[:-1])
        if not line.endswith(HARD_BREAK)
    ]


def join_wrapped_lines(text: str, path: Path) -> str:
    """Join every hard-wrapped prose line with the line after it."""
    while findings := check_wrapping(text, path):
        lines = text.split("\n")
        # Join from the bottom so the line numbers still to process stay valid.
        for finding in reversed(findings):
            following = BLOCKQUOTE_PREFIX.sub("", lines[finding.line])
            lines[finding.line - 1] = f"{lines[finding.line - 1].rstrip()} {following}"
            del lines[finding.line]
        text = "\n".join(lines)
    return text


def check_book_page(text: str, path: Path) -> list[Violation]:
    """Find anchored cross-file links and a missing front matter description."""
    findings = [
        Violation(path.as_posix(), start + offset + 1, "KMD002", MESSAGES["KMD002"])
        for start, block in _get_inline_blocks(text)
        for offset, line in enumerate(block)
        if any(
            token.type == "link_open"
            and CROSS_FILE_ANCHOR.match(str(token.attrs.get("href", "")))
            for token in PARSER.parseInline(line)[0].children or []
        )
    ]
    lines = text.split("\n")
    front_matter = lines[: _get_front_matter_end(lines)]
    if not any(re.match(r"description:\s*\S", line) for line in front_matter):
        findings.append(Violation(path.as_posix(), 1, "KMD003", MESSAGES["KMD003"]))
    return findings


def check_toc(toc_text: str, pages: list[Path]) -> list[Violation]:
    """Find book pages the table of contents does not register."""
    registered = {BOOK_PATH / target for _, target in TOC_LINK.findall(toc_text)}
    return [
        Violation(page.as_posix(), 1, "KMD004", MESSAGES["KMD004"])
        for page in pages
        if page not in registered
    ]


def main(argv: list[str] | None = None) -> int:
    """Check tracked Markdown files under the selected root and print diagnostics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument(
        "--fix", action="store_true", help="Join hard-wrapped prose in place first."
    )
    args = parser.parse_args(argv)
    listed = subprocess.run(
        ["git", "ls-files", "*.md"],
        cwd=args.root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    paths = [Path(name) for name in listed if Path(name).name not in EXCLUDED_NAMES]
    pages = [
        path
        for path in paths
        if path.is_relative_to(BOOK_PATH) and path not in UNPUBLISHED_BOOK_PAGES
    ]
    if not pages:
        print(f"{BOOK_PATH}: checker failed: no book pages found", file=sys.stderr)
        return 2
    findings: list[Violation] = []
    for path in paths:
        text = (args.root / path).read_text(encoding="utf-8")
        if args.fix and (fixed := join_wrapped_lines(text, path)) != text:
            (args.root / path).write_text(fixed, encoding="utf-8")
            text = fixed
        findings.extend(check_wrapping(text, path))
        if path in pages:
            findings.extend(check_book_page(text, path))
    toc = (args.root / BOOK_PATH / "toc.md").read_text(encoding="utf-8")
    findings.extend(check_toc(toc, pages))
    for finding in sorted(findings):
        print(f"{finding.path}:{finding.line}: {finding.code} {finding.message}")
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
