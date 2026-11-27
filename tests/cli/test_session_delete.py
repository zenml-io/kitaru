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
"""CLI tests for `kitaru session delete`."""

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.cli.output_rendering import render_text

from kitaru.cli import app as app_module
from kitaru.cli import invocation_context
from kitaru.cli.output import CommandResult
from kitaru.client.exceptions import APIError


class StubDeleteSessions:
    """Session resource fake supporting iter and delete with configurable errors."""

    def __init__(
        self,
        *,
        iter_ids: list[uuid.UUID] | None = None,
        errors_by_id: dict[uuid.UUID, Exception] | None = None,
    ) -> None:
        self.iter_ids = iter_ids or []
        self.errors_by_id = errors_by_id or {}
        self.iter_calls: list[Any] = []
        self.deleted: list[uuid.UUID] = []

    async def iter(self, params: Any):
        self.iter_calls.append(params)
        for session_id in self.iter_ids:
            yield SimpleNamespace(id=session_id)

    async def delete(self, session_id: uuid.UUID) -> None:
        error = self.errors_by_id.get(session_id)
        if error is not None:
            raise error
        self.deleted.append(session_id)


def _patch_sessions(
    monkeypatch: pytest.MonkeyPatch, resource: StubDeleteSessions
) -> None:
    @asynccontextmanager
    async def open_client():
        yield SimpleNamespace(sessions=resource)

    monkeypatch.setattr(invocation_context, "open_asset_client", open_client)


def test_session_delete_requires_force_before_network_access(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Deleting sessions without --force fails locally without opening a client."""

    def fail_open():
        raise AssertionError("network client should not open without --force")

    monkeypatch.setattr(invocation_context, "open_asset_client", fail_open)

    assert app_module.main(["session", "delete", str(uuid.uuid4())]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "--force" in error["message"]


@pytest.mark.parametrize(
    ("args", "fragment"),
    [
        (["--force"], "Select sessions using"),
        (
            [
                str(uuid.uuid4()),
                "--filter",
                '{"field":"status","op":"eq","value":"completed"}',
                "--force",
            ],
            "Select sessions using",
        ),
        (["--filter", "{bad json", "--force"], "--filter is not valid JSON:"),
    ],
)
def test_session_delete_rejects_invalid_selection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    fragment: str,
) -> None:
    """Missing, overlapping, or malformed selectors fail cleanly before deleting."""
    resource = StubDeleteSessions()
    _patch_sessions(monkeypatch, resource)

    assert app_module.main(["session", "delete", *args]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert fragment in error["message"]
    assert resource.deleted == []


def test_session_delete_single_multiple_file_and_filter(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Sessions can be deleted by positional IDs, --sessions-file, or --filter."""
    first, second, third = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    resource = StubDeleteSessions()
    _patch_sessions(monkeypatch, resource)

    assert app_module.main(["session", "delete", str(first), "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "session.delete"
    assert payload["item"] == {"deleted_ids": [str(first)], "deleted_count": 1}
    assert resource.deleted == [first]

    ids_file = tmp_path / "sessions.txt"
    ids_file.write_text(f"{second}\n\n{third}\n", encoding="utf-8")
    assert (
        app_module.main(
            ["session", "delete", "--sessions-file", str(ids_file), "--force"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"] == {
        "deleted_ids": [str(second), str(third)],
        "deleted_count": 2,
    }
    assert resource.deleted == [first, second, third]

    import_id = uuid.uuid4()
    filtered_ids = [uuid.uuid4(), uuid.uuid4()]
    filtered_resource = StubDeleteSessions(iter_ids=filtered_ids)
    _patch_sessions(monkeypatch, filtered_resource)
    filter_expr = json.dumps(
        {"field": "import_id", "op": "eq", "value": str(import_id)}
    )
    assert (
        app_module.main(["session", "delete", "--filter", filter_expr, "--force"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"] == {
        "deleted_ids": [str(sid) for sid in filtered_ids],
        "deleted_count": 2,
    }
    assert filtered_resource.deleted == filtered_ids
    assert json.loads(
        filtered_resource.iter_calls[0].model_dump(mode="json")["filter"]
    ) == json.loads(filter_expr)


def test_session_delete_empty_filter_match_fails_cleanly(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A --filter matching zero sessions returns exit 2 instead of succeeding."""
    resource = StubDeleteSessions(iter_ids=[])
    _patch_sessions(monkeypatch, resource)

    assert (
        app_module.main(
            [
                "session",
                "delete",
                "--filter",
                '{"field":"status","op":"eq","value":"failed"}',
                "--force",
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "No sessions matched" in error["message"]


def test_session_delete_single_in_use_conflict_names_referencing_resource(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A 409 SessionInUse surfaces as exit 5 conflict with the server's detail."""
    session_id = uuid.uuid4()
    detail = (
        f"Session {session_id} is referenced by a cohort version and cannot be deleted"
    )
    resource = StubDeleteSessions(errors_by_id={session_id: APIError(409, detail)})
    _patch_sessions(monkeypatch, resource)

    assert app_module.main(["session", "delete", str(session_id), "--force"]) == 5
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "conflict"
    assert "referenced by a cohort version" in error["message"]


def test_session_delete_reports_partial_failure_and_all_conflict_batches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Batch deletes report deleted_ids alongside blocked sessions on 409."""
    ok_id, blocked_id, ok_id_2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    conflict = APIError(
        409,
        f"Session {blocked_id} is referenced by an investigation and cannot be deleted",
    )
    resource = StubDeleteSessions(errors_by_id={blocked_id: conflict})
    _patch_sessions(monkeypatch, resource)

    assert (
        app_module.main(
            [
                "session",
                "delete",
                str(ok_id),
                str(blocked_id),
                str(ok_id_2),
                "--force",
            ]
        )
        == 8
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "partial_failure"
    assert error["details"] == {
        "deleted_ids": [str(ok_id), str(ok_id_2)],
        "deleted_count": 2,
        "failed_count": 1,
        "failures": [
            {
                "id": str(blocked_id),
                "status_code": 409,
                "detail": conflict.detail,
            }
        ],
    }

    blocked_a, blocked_b = uuid.uuid4(), uuid.uuid4()
    all_conflict = StubDeleteSessions(
        errors_by_id={
            blocked_a: APIError(
                409,
                f"Session {blocked_a} is referenced by a replay "
                "(as baseline session) and cannot be deleted",
            ),
            blocked_b: APIError(
                409,
                f"Session {blocked_b} is referenced by a cohort version "
                "and cannot be deleted",
            ),
        }
    )
    _patch_sessions(monkeypatch, all_conflict)
    assert (
        app_module.main(
            ["session", "delete", str(blocked_a), str(blocked_b), "--force"]
        )
        == 5
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "conflict"
    assert error["details"]["deleted_count"] == 0
    assert error["details"]["failed_count"] == 2


def test_session_delete_renders_human_summary() -> None:
    """Human detail view displays deleted count and IDs."""
    session_id = str(uuid.uuid4())
    stdout, _ = render_text(
        "session.delete",
        CommandResult(item={"deleted_ids": [session_id], "deleted_count": 1}),
        rich=True,
    )
    assert "Deleted" in stdout
    assert session_id in stdout
