"""Exercise Markdown diagnostics on small in-memory documents."""

import subprocess
from pathlib import Path

import pytest
from scripts.check_markdown import (
    BOOK_PATH,
    check_book_page,
    check_toc,
    check_wrapping,
    join_wrapped_lines,
    main,
)

PAGE = BOOK_PATH / "guides/page.md"


@pytest.mark.parametrize(
    ("text", "lines"),
    [
        ("A sentence that\ncontinues here.\n", [1]),
        ("- A list item that\n  continues here.\n", [1]),
        ("1. A step that\n   continues here.\n", [1]),
        ("Three\nwrapped\nlines.\n", [1, 2]),
        ("> A quote that\n> continues here.\n", [1]),
        ("{% hint %}\nA hint that\ncontinues.\n{% endhint %}\n", [2]),
    ],
)
def test_wrapped_prose_is_reported(text: str, lines: list[int]) -> None:
    """Report every prose line that a soft line break continues."""
    findings = check_wrapping(text, PAGE)
    assert [finding.line for finding in findings] == lines
    assert {finding.code for finding in findings} == {"KMD001"}


@pytest.mark.parametrize(
    "text",
    [
        "One paragraph.\n\nAnother paragraph.\n",
        "Intro:\n- item\n- item\n",
        "# Heading\nBody.\n",
        "| a | b |\n| - | - |\n| c | d |\n",
        "````\n```\ncode that\nwraps\n```\n````\n",
        "---\ndescription: x\ntitle: y\n---\n",
        "Line with hard break  \nnext line\n",
        "{% hint %}\nBody.\n{% endhint %}\n",
        "<details>\n<summary>x</summary>\n</details>\n",
        "    indented code\n    more code\n",
    ],
)
def test_markdown_structure_is_allowed(text: str) -> None:
    """Allow line breaks that Markdown structure needs."""
    assert check_wrapping(text, PAGE) == []


def test_fix_joins_wrapped_prose() -> None:
    """Join wrapped lines without touching Markdown structure."""
    text = "# Title\nThree\nwrapped\nlines.\n- item that\n  wraps\n\n> a\n> b\n"
    assert join_wrapped_lines(text, PAGE) == (
        "# Title\nThree wrapped lines.\n- item that wraps\n\n> a b\n"
    )


def test_book_page_rules() -> None:
    """Report anchored cross-file links and a missing description."""
    text = "---\nicon: x\n---\n\nSee [a](other.md#section) and `[b](c.md#d)`.\n"
    assert [(f.code, f.line) for f in check_book_page(text, PAGE)] == [
        ("KMD002", 5),
        ("KMD003", 1),
    ]
    clean = (
        "---\ndescription: Page.\n---\n\nSee [a](other.md), [b](#local), "
        "and [c](https://example.com/x.md#y).\n"
    )
    assert check_book_page(clean, PAGE) == []


def test_toc_registration() -> None:
    """Report book pages the table of contents does not list."""
    toc = "- [Page](guides/page.md)\n"
    orphan = BOOK_PATH / "guides/orphan.md"
    assert [f.path for f in check_toc(toc, [PAGE, orphan])] == [orphan.as_posix()]


def test_cli_checks_tracked_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fail on tracked violations, then fix them in place."""
    book = tmp_path / BOOK_PATH
    (book / "guides").mkdir(parents=True)
    (book / "toc.md").write_text("- [Page](guides/page.md)\n", encoding="utf-8")
    page = tmp_path / PAGE
    page.write_text(
        "---\ndescription: Page.\n---\n\nWrapped\nline.\n", encoding="utf-8"
    )
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    assert main(["--root", str(tmp_path)]) == 1
    assert f"{PAGE.as_posix()}:5: KMD001" in capsys.readouterr().out
    assert main(["--root", str(tmp_path), "--fix"]) == 0
    assert page.read_text(encoding="utf-8").endswith("\nWrapped line.\n")
