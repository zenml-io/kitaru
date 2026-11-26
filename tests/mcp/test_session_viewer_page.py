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


def _drive_page(tmp_path: Path) -> dict[str, list[dict[str, Any]]]:
    html = load_session_viewer_html()
    runtime = html.split("<script>", 1)[1].rsplit("</script>", 1)[0]
    (tmp_path / "runtime.js").write_text(runtime)
    node = shutil.which("node")
    assert node is not None
    finished = subprocess.run(
        [node, str(_DRIVER), str(tmp_path / "runtime.js")],
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    return json.loads(finished.stdout)


@pytest.mark.skipif(shutil.which("node") is None, reason="needs Node.js")
def test_the_page_keeps_each_session_in_the_workspace_it_was_opened_in(
    tmp_path: Path,
) -> None:
    calls = _drive_page(tmp_path)

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


@pytest.mark.skipif(shutil.which("node") is None, reason="needs Node.js")
def test_edits_in_progress_never_land_on_the_next_session(tmp_path: Path) -> None:
    shown = _drive_page(tmp_path)["sessionChange"]

    assert shown == {
        "current": "next",
        # The first session's save reached Kitaru, but the next session's
        # notes never show it.
        "appended": [],
        # The note box opened on the first session closes with it.
        "composerWasStillOpen": False,
        "noteTargets": ["first"],
    }


@pytest.mark.skipif(shutil.which("node") is None, reason="needs Node.js")
def test_edits_survive_the_same_session_rendering_again(tmp_path: Path) -> None:
    shown = _drive_page(tmp_path)

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


@pytest.mark.skipif(shutil.which("node") is None, reason="needs Node.js")
def test_the_first_session_never_touches_the_next_sessions_edits(
    tmp_path: Path,
) -> None:
    shown = _drive_page(tmp_path)

    assert shown["earlierSaveFinishesLater"] == {
        "nextBoxStillOpen": True,
        "nextNotes": [],
        "noteTargets": ["first"],
    }
    assert shown["noteButtonAcrossSessions"] == {
        "offered": True,
        "removedWithFirst": True,
    }


@pytest.mark.skipif(shutil.which("node") is None, reason="needs Node.js")
def test_a_saved_verdict_shows_after_the_same_session_renders_again(
    tmp_path: Path,
) -> None:
    shown = _drive_page(tmp_path)["verdictDuringRerender"]

    assert shown == {"shownVerdict": "problematic"}
