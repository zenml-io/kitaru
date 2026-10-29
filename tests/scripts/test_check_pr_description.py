"""Exercise the pull request title and body conventions."""

import pytest
from scripts.check_pr_description import check_pr

BODY = "## What changed?\n\nThings.\n\n## Reviewer Notes\n\nWalkthrough.\n"


@pytest.mark.parametrize(
    "title",
    [
        "feat: Add tags",
        "fix(replay): Keep checkpoints",
        "Docs: Explain replay",
        "refactor!: Drop v1 routes",
        "[Codex] Add tags",
    ],
)
def test_title_prefixes_are_rejected(title: str) -> None:
    """Reject Conventional Commits and agent prefixes in titles."""
    assert [problem.split(":")[0] for problem in check_pr(title, BODY)] == ["Title"]


@pytest.mark.parametrize(
    "title",
    [
        "Add tags to sessions",
        "Upgrade `certifi` to clear an advisory",
        "MCP: show failure matrix",
        "Fix the fixture cleanup",
    ],
)
def test_plain_titles_pass(title: str) -> None:
    """Accept human-readable titles, including ones that start with a noun."""
    assert check_pr(title, BODY) == []


@pytest.mark.parametrize(
    "body",
    [
        "",
        "## Summary\n\nReviewer Notes are below.\n",
        "#### Reviewer Notes\n\nText.\n",
        "## Reviewer Notes\n\n<!-- Write a walkthrough. -->\n\n## Reproduction\n",
        "## Reviewer Notes\n\n## Local checks run\n\nTests.\n",
    ],
)
def test_body_needs_reviewer_notes_heading(body: str) -> None:
    """Require Reviewer Notes as an H2 or H3 heading."""
    assert [problem.split(":")[0] for problem in check_pr("Add tags", body)] == ["Body"]


@pytest.mark.parametrize(
    "body",
    [
        "## Why\n\n### Reviewer Notes\n\nText.\n",
        "## Reviewer Notes\n\nStory.\n\n### Reproduction\n\nSteps.\n",
        "## Reviewer Notes\n\n### Reproduction\n\nSteps.\n",
    ],
)
def test_filled_reviewer_notes_pass(body: str) -> None:
    """Accept Reviewer Notes with text in the section or its subsections."""
    assert check_pr("Add tags", body) == []
