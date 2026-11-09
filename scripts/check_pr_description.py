"""Check a pull request title and body against the repository's PR conventions.

Reads the title and body from the PR_TITLE and PR_BODY environment variables.
"""

import os
import re
import sys

TITLE_PREFIX = re.compile(
    r"^\s*(\[Codex\]\s|"
    r"(build|chore|ci|docs?|feat|fix|perf|refactor|revert|style|test)(\([^)]*\))?!?:\s)",
    re.IGNORECASE,
)
HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)


def _get_reviewer_notes(body: str) -> str | None:
    """Return the text under a Reviewer Notes H2 or H3, or None without one."""
    text = HTML_COMMENT.sub("", body)
    headings = list(HEADING.finditer(text))
    for index, heading in enumerate(headings):
        level = len(heading.group(1))
        if level in {2, 3} and heading.group(2).lower() == "reviewer notes":
            end = next(
                (
                    later.start()
                    for later in headings[index + 1 :]
                    if len(later.group(1)) <= level
                ),
                len(text),
            )
            return text[heading.end() : end]
    return None


def check_pr(title: str, body: str) -> list[str]:
    """Return a remediation message for each convention the PR breaks."""
    problems = []
    if TITLE_PREFIX.match(title):
        problems.append(
            "Title: drop the Conventional Commits or [Codex] prefix and describe "
            "the change in plain words."
        )
    notes = _get_reviewer_notes(body)
    if notes is None or not notes.strip():
        problems.append(
            "Body: fill in a '## Reviewer Notes' section that walks the reviewer "
            "through the change, with a concrete reproduction."
        )
    return problems


def main() -> int:
    """Print each broken convention and fail when there is any."""
    problems = check_pr(os.environ.get("PR_TITLE", ""), os.environ.get("PR_BODY", ""))
    for problem in problems:
        print(problem, file=sys.stderr)
    return int(bool(problems))


if __name__ == "__main__":
    raise SystemExit(main())
