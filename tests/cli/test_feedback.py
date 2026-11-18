#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""`kitaru feedback` and the hint that points crashes at it."""

import json
from urllib.parse import parse_qs, urlsplit

import pytest

from kitaru.cli import app as app_module
from kitaru.cli import diagnostics
from kitaru.cli.output import CLIError
from kitaru.client.exceptions import APIError


def test_feedback_prints_a_prefilled_issue_link(capsys) -> None:
    exit_code = app_module.main(
        [
            "feedback",
            "--output",
            "json",
            "--title",
            "session read hides errors",
            "--problem",
            "kitaru session read dropped an error node for KITKEY_abc123.",
            "--proposed-fix",
            "Keep error nodes in full mode.",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["warnings"] == [
        "Nothing was sent. Open the issue link to review and submit it."
    ]
    query = parse_qs(urlsplit(payload["links"]["issue"]).query)
    assert query["title"] == ["CLI feedback: session read hides errors"]
    # Output redaction runs over the link too and must leave it whole.
    assert query["body"] == [payload["item"]["body"]]
    assert "for KITKEY_***\n" in payload["item"]["body"]
    assert (
        "### Proposed fix\n\nKeep error nodes in full mode." in payload["item"]["body"]
    )
    assert ", CLI, Python " in payload["item"]["body"]
    assert payload["item"]["truncated"] is False


def test_feedback_rejects_an_empty_title(capsys) -> None:
    exit_code = app_module.main(
        ["feedback", "--output", "json", "--title", " ", "--problem", "It broke."]
    )

    assert exit_code == 2
    assert json.loads(capsys.readouterr().err)["error"]["kind"] == "invalid_arguments"


@pytest.mark.parametrize(
    ("exception", "hinted"),
    [
        (RuntimeError("boom"), True),
        (APIError(500, "Internal Server Error"), True),
        (APIError(503, "Service Unavailable"), False),
        (APIError(404, "Session not found"), False),
        (CLIError("invalid_arguments", "Bad flag."), False),
        (CLIError("internal_error", "Broken.", hint="Run it again."), False),
    ],
)
def test_only_likely_bugs_suggest_kitaru_feedback(
    monkeypatch, capsys, exception: Exception, hinted: bool
) -> None:
    def fail_version() -> str:
        raise exception

    monkeypatch.setattr(diagnostics, "package_version", fail_version)
    assert app_module.main(["version", "--output", "json"]) != 0

    hint = json.loads(capsys.readouterr().err)["error"].get("hint", "")
    assert ("kitaru feedback" in hint) is hinted
