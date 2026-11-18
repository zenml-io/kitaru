#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Prefilled GitHub issue links for Kitaru feedback."""

from urllib.parse import parse_qs, urlsplit

from kitaru.feedback import ISSUE_URL, MAX_URL_LENGTH, build_issue_draft


def _query(url: str) -> dict[str, str]:
    parts = urlsplit(url)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == ISSUE_URL
    return {key: values[0] for key, values in parse_qs(parts.query).items()}


def test_short_report_round_trips_through_the_issue_link() -> None:
    draft = build_issue_draft(
        "session_read drops tool errors",
        [
            ("What happened", "kitaru_session_read with full=true hid a tool error."),
            ("What I expected", "   "),
            ("Proposed fix", "Keep error nodes when full is set."),
        ],
        "Kitaru 1.0.0",
    )

    assert draft.truncated is False
    query = _query(draft.url)
    assert query["title"] == "session_read drops tool errors"
    assert query["body"] == draft.body
    assert draft.body == (
        "### What happened\n\nkitaru_session_read with full=true hid a tool error."
        "\n\n### Proposed fix\n\nKeep error nodes when full is set."
        "\n\n### Environment\n\nKitaru 1.0.0"
    )


def test_credentials_are_masked_before_encoding() -> None:
    draft = build_issue_draft(
        "Leaked KITKEY_abc123secret",
        [("What happened", "Failed with Authorization: Bearer abc123secret.")],
        "Kitaru 1.0.0",
    )

    assert "abc123secret" not in draft.url
    query = _query(draft.url)
    assert query["title"] == "Leaked KITKEY_***"
    assert "Bearer ***" in query["body"]


def test_long_report_cuts_the_longest_section_and_keeps_the_rest() -> None:
    fix = "Keep error nodes when full is set."
    draft = build_issue_draft(
        "Very long report",
        [("What happened", "é" * 6000), ("Proposed fix", fix)],
        "Kitaru 1.0.0",
    )

    assert draft.truncated is True
    assert len(draft.url) <= MAX_URL_LENGTH
    assert _query(draft.url)["body"] == draft.body
    assert "[cut to fit the issue link]\n\n### Proposed fix" in draft.body
    assert draft.body.endswith(f"{fix}\n\n### Environment\n\nKitaru 1.0.0")
