#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Build prefilled GitHub issue links for Kitaru feedback."""

import platform
from collections.abc import Sequence
from importlib.metadata import version
from typing import NamedTuple
from urllib.parse import quote

from kitaru.redaction import redact

ISSUE_URL = "https://github.com/zenml-io/kitaru/issues/new"
# GitHub rejects issue links much longer than about 8 KB, so stay below that.
MAX_URL_LENGTH = 7500
# Titles are never cut, so a cap keeps them from using up the link budget.
MAX_TITLE_LENGTH = 120
TRUNCATION_MARKER = "\n\n[cut to fit the issue link]"

Section = tuple[str, str]


class IssueDraft(NamedTuple):
    """A prefilled issue link, the body it carries, and whether text was cut."""

    url: str
    body: str
    truncated: bool


def build_issue_draft(
    title: str, sections: Sequence[tuple[str, str | None]], environment: str
) -> IssueDraft:
    """Build a prefilled issue link that fits GitHub's URL limit.

    Recognizable credentials are masked before encoding, because the masking
    patterns no longer match once text is percent-encoded.

    Args:
        title: Issue title.
        sections: Heading and text pairs; empty texts are left out.
        environment: Version details, always kept whole as the last section.

    Returns:
        The issue link, its body, and whether any section text was cut.
    """
    title = redact(title)
    kept = [(heading, redact(text.strip())) for heading, text in sections if text]
    kept = [(heading, text) for heading, text in kept if text]
    tail = ("Environment", redact(environment))
    # Encoded length adds up per character, so rendering the headings with empty
    # texts measures everything except the texts themselves.
    empty = [(heading, "") for heading, _ in kept]
    fixed_length = len(_build_url(title, _render_body([*empty, tail])))
    trimmed = _trim_sections(kept, MAX_URL_LENGTH - fixed_length)
    body = _render_body([*trimmed, tail])
    return IssueDraft(_build_url(title, body), body, trimmed != kept)


def draft_feedback(
    source: str,
    title: str,
    problem: str,
    *,
    expected: str | None = None,
    proposed_fix: str | None = None,
    context: str = "",
) -> IssueDraft:
    """Draft a feedback issue with the standard sections and version details.

    Args:
        source: Where the report comes from, such as `CLI` or `MCP`.
        title: Issue title, prefixed with the source.
        problem: What went wrong.
        expected: What the reporter expected instead.
        proposed_fix: The change that would fix it.
        context: Extra environment details appended to the version line.

    Returns:
        The issue link, its body, and whether any section text was cut.
    """
    environment = (
        f"Kitaru {version('kitaru')}, {source}, "
        f"Python {platform.python_version()} on {platform.system()}"
    )
    return build_issue_draft(
        f"{source} feedback: {title.strip()}",
        [
            ("What happened", problem),
            ("What I expected", expected),
            ("Proposed fix", proposed_fix),
        ],
        f"{environment}, {context}" if context else environment,
    )


def _encode(text: str) -> str:
    # Encode "_" too: CLI and MCP output redact everything again, and a masked
    # "KITKEY_***" followed by percent-encoded text has no space or comma to
    # end the match, so that pass would cut off the rest of the link.
    return quote(text, safe="").replace("_", "%5F")


def _get_encoded_length(text: str) -> int:
    return len(_encode(text))


def _trim_sections(sections: list[Section], budget: int) -> list[Section]:
    # Short sections stay whole; the space they leave is split evenly between
    # the longer ones, so one huge stack trace cannot crowd out the fix.
    lengths = [_get_encoded_length(text) for _, text in sections]
    allowed = [0] * len(sections)
    remaining = budget
    by_length = sorted(range(len(sections)), key=lambda index: lengths[index])
    for position, index in enumerate(by_length):
        allowed[index] = min(lengths[index], remaining // (len(sections) - position))
        remaining -= allowed[index]
    return [
        (heading, _cut_text(text, allowed[index]))
        for index, (heading, text) in enumerate(sections)
    ]


def _cut_text(text: str, limit: int) -> str:
    if _get_encoded_length(text) <= limit:
        return text
    limit -= _get_encoded_length(TRUNCATION_MARKER)
    used = 0
    for position, char in enumerate(text):
        used += _get_encoded_length(char)
        if used > limit:
            return text[:position].rstrip() + TRUNCATION_MARKER
    return text


def _render_body(sections: list[Section]) -> str:
    return "\n\n".join(f"### {heading}\n\n{text}" for heading, text in sections)


def _build_url(title: str, body: str) -> str:
    return f"{ISSUE_URL}?title={_encode(title)}&body={_encode(body)}"
