#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""The session viewer page's runtime, driven through a fake MCP Apps host."""

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from kitaru.mcp.apps import load_session_viewer_html

_DRIVER = Path(__file__).with_name("viewer_page_driver.mjs")


@pytest.fixture(scope="module")
def page(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Every scenario's result, from one run of the page in Node."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("needs Node.js")
    html = load_session_viewer_html()
    runtime = tmp_path_factory.mktemp("viewer") / "runtime.js"
    runtime.write_text(html.split("<script>", 1)[1].rsplit("</script>", 1)[0])
    finished = subprocess.run(
        [node, str(_DRIVER), str(runtime)],
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    return json.loads(finished.stdout)


def test_the_page_keeps_each_session_in_the_workspace_it_was_opened_in(
    page: dict[str, Any],
) -> None:
    calls = page

    shown = [(c["operation"], c["session"], c["host"]) for c in calls["hosted"]]
    first, next_ = {"workspace": "research"}, {"workspace": "second"}
    assert shown == [
        ("view", "first", first),
        ("viewer_source", None, first),
        ("nodes", "first", first),
        # The next input arrived, but the page still shows the first session.
        ("note", "first", first),
        ("view", "next", next_),
        ("note", "next", next_),
    ]
    assert all(call["host"] == {} for call in calls["local"])


def test_edits_in_progress_never_land_on_the_next_session(page: dict[str, Any]) -> None:
    shown = page["sessionChange"]

    assert shown == {
        "current": "next",
        # The first session's save reached Kitaru, but the next session's
        # notes never show it.
        "appended": [],
        # The note box opened on the first session closes with it.
        "composerWasStillOpen": False,
        "noteTargets": ["first"],
    }


def test_edits_survive_the_same_session_rendering_again(page: dict[str, Any]) -> None:
    shown = page

    assert shown["sameSessionRerender"] == {
        "current": "first",
        "appended": ["Pending save for first"],
        "composerStillOpen": True,
        "noteTargets": ["first"],
    }
    assert shown["investigationPositions"] == {
        "position": 1,
        "keptOnSamePosition": True,
        "closedOnNextPosition": True,
    }


def test_the_first_session_never_touches_the_next_sessions_edits(
    page: dict[str, Any],
) -> None:
    shown = page

    assert shown["earlierSaveFinishesLater"] == {
        "nextBoxStillOpen": True,
        "nextNotes": [],
        "noteTargets": ["first"],
    }
    assert shown["noteButtonAcrossSessions"] == {
        "offered": True,
        "removedWithFirst": True,
    }


def test_a_saved_verdict_shows_after_the_same_session_renders_again(
    page: dict[str, Any],
) -> None:
    shown = page["verdictDuringRerender"]

    assert shown == {"shownVerdict": "problematic"}


def test_a_draft_answer_stays_with_its_investigation(page: dict[str, Any]) -> None:
    shown = page["answerDraftAcrossInvestigations"]

    assert shown == {
        "keptOnRerender": "Draft for the first review",
        "shownInSecond": "",
        "keptOnReturn": "Draft for the first review",
    }
