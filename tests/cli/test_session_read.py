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
"""`kitaru session read` CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context

NOW = datetime(2026, 10, 31, tzinfo=UTC)


def _session(session_id: uuid.UUID, reply: str) -> SessionWithNodesResponse:
    detail = SessionDetailResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        created=NOW,
        updated=NOW,
        number=1,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        name="Returns ticket",
        metadata={},
        llm_call_count=0,
        tool_call_count=1,
        inputs="Refund order 48213",
        outputs=reply,
    )
    node = SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=session_id,
        external_id="lookup",
        links=[],
        node_type=NodeType.TOOL_CALL,
        name="lookup_order",
        status=NodeStatus.COMPLETED,
        inputs={"order_id": "48213"},
        outputs={"found": True},
        attributes=None,
        metadata={},
    )
    return SessionWithNodesResponse(session=detail, nodes=[node])


@pytest.fixture
def sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[uuid.UUID, SessionWithNodesResponse]:
    stored = {
        session_id: _session(session_id, reply)
        for session_id, reply in (
            (uuid.uuid4(), "Refunded."),
            (uuid.uuid4(), "x" * 400),
        )
    }

    async def get_with_nodes(session_id: uuid.UUID) -> SessionWithNodesResponse:
        return stored[session_id]

    client = SimpleNamespace(sessions=SimpleNamespace(get_with_nodes=get_with_nodes))

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)
    return stored


def test_session_read_prints_transcripts_as_text(
    sessions: dict[uuid.UUID, SessionWithNodesResponse],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Text output is the transcript itself, with a hint only when text was cut."""
    first, second = sessions

    assert app_module.main(["session", "read", str(first), "--output", "text"]) == 0
    text = capsys.readouterr().out
    assert text.startswith(f"session {first} · Returns ticket · completed · recorded\n")
    assert (
        '[0] tool_call lookup_order · completed\n    args: {"order_id":"48213"}' in text
    )
    assert "Next:" not in text

    assert app_module.main(["session", "read", str(second), "--output", "text"]) == 0
    assert "final output: " + "x" * 300 + "… [+100 chars]" in capsys.readouterr().out


def test_session_read_json_lists_one_item_per_session_in_order(
    sessions: dict[uuid.UUID, SessionWithNodesResponse],
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """JSON output carries full node IDs and reads IDs from a sessions file."""
    first, second = sessions
    sessions_file = tmp_path / "sessions.txt"
    sessions_file.write_text(f"{second}\n", encoding="utf-8")

    assert (
        app_module.main(
            ["session", "read", str(first), "--sessions-file", str(sessions_file)]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "session.read"
    assert [item["session_id"] for item in payload["items"]] == [
        str(first),
        str(second),
    ]
    assert payload["items"][0]["steps"][0]["node_id"] == str(
        sessions[first].nodes[0].id
    )
    assert payload["items"][1]["truncated"] is True
    assert payload["next_actions"] == [
        "kitaru session read SESSION --node INDEX --full shows one node without cuts."
    ]


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--node", "0"], "Showing one node needs exactly one session."),
        (["--max-chars", "0"], "--max-chars must be at least 1."),
    ],
)
def test_session_read_rejects_invalid_arguments(
    sessions: dict[uuid.UUID, SessionWithNodesResponse],
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
    message: str,
) -> None:
    """Zooming needs one session, and the field limit must be positive."""
    first, second = sessions

    assert app_module.main(["session", "read", str(first), str(second), *extra]) == 2
    assert json.loads(capsys.readouterr().err)["error"]["message"] == message


def test_session_read_reports_a_node_outside_the_session(
    sessions: dict[uuid.UUID, SessionWithNodesResponse],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A node position past the end is an argument error, not a crash."""
    first, _ = sessions

    assert app_module.main(["session", "read", str(first), "--node", "5"]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["message"] == "Node 5 is outside this session's 1 nodes."
