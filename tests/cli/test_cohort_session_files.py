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
"""Cohort session-file options and bounded retry-hint CLI tests."""

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from tests.cli.test_cohorts import StubCohortClient, StubModel

from kitaru.cli import app as app_module
from kitaru.cli import cohorts, invocation_context
from kitaru.cli.output import CLIError


@pytest.fixture
def argv_client(
    monkeypatch: pytest.MonkeyPatch,
) -> StubCohortClient:
    """Route public CLI invocations through one recording client."""
    client = StubCohortClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)
    return client


async def test_cohort_create_retry_hint_uses_sessions_file_when_provided(
    tmp_path: Path,
) -> None:
    """Retry a failed --sessions-file initial version with --add-sessions-file."""
    client = StubCohortClient()
    client.version_error = RuntimeError("version rejected")
    session_file = tmp_path / "sessions.txt"
    session_file.write_text(
        "\n".join(str(session.id) for session in client.selected_sessions) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(CLIError) as error:
        await cohorts.create_cohort(
            client,
            "nightly",
            agent="assistant",
            description=None,
            metadata=None,
            sessions_file=session_file,
            display_version="v1",
        )

    assert error.value.kind == "partial_failure"
    assert error.value.hint == (
        "The cohort exists. Retry with `kitaru cohort version create "
        f"{client.cohort.id} --add-sessions-file {session_file} "
        "--display-version v1`."
    )


async def test_cohort_create_retry_hint_bounds_large_selections() -> None:
    """Large failed initial versions direct callers to --add-sessions-file."""
    client = StubCohortClient()
    client.selected_sessions = [StubModel(uuid.uuid4()) for _ in range(12)]
    client.version_error = RuntimeError("version rejected")

    with pytest.raises(CLIError) as error:
        await cohorts.create_cohort(
            client,
            "nightly",
            agent="assistant",
            description=None,
            metadata=None,
            tag="baseline",
        )

    assert error.value.kind == "partial_failure"
    assert error.value.hint is not None
    assert "--add-sessions-file" in error.value.hint
    assert "details.selected_session_ids" in error.value.hint
    assert "--add-session " not in error.value.hint


async def test_version_create_reads_add_and_remove_session_files(
    tmp_path: Path,
) -> None:
    """File-based add/remove UUIDs combine with inline flags in order."""
    client = StubCohortClient()
    inline_add = uuid.uuid4()
    file_add_1 = uuid.uuid4()
    file_add_2 = uuid.uuid4()
    file_remove = uuid.uuid4()
    add_file = tmp_path / "add.txt"
    add_file.write_text(f"\n{file_add_1}\n  {file_add_2}  \n\n", encoding="utf-8")
    remove_file = tmp_path / "remove.txt"
    remove_file.write_text(f"{file_remove}\n", encoding="utf-8")

    result = await cohorts.create_cohort_version(
        client,
        "regression",
        add_session_ids=[inline_add],
        add_sessions_file=add_file,
        remove_session_ids=None,
        remove_sessions_file=remove_file,
        display_version="from-files",
    )

    assert result.warnings == []
    _, request = client.created_versions[-1]
    assert request.add_session_ids == [inline_add, file_add_1, file_add_2]
    assert request.remove_session_ids == [file_remove]


def test_public_cohort_version_create_accepts_session_files(
    argv_client: StubCohortClient,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Public CLI forwards --add-sessions-file and --remove-sessions-file."""
    add_id = uuid.uuid4()
    remove_id = uuid.uuid4()
    add_file = tmp_path / "add.txt"
    add_file.write_text(f"{add_id}\n", encoding="utf-8")
    remove_file = tmp_path / "remove.txt"
    remove_file.write_text(f"{remove_id}\n", encoding="utf-8")

    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "create",
                "regression",
                "--add-sessions-file",
                str(add_file),
                "--remove-sessions-file",
                str(remove_file),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "cohort.version.create"
    _, request = argv_client.created_versions[-1]
    assert request.add_session_ids == [add_id]
    assert request.remove_session_ids == [remove_id]


def test_public_cohort_version_create_rejects_bad_session_files(
    argv_client: StubCohortClient,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """Invalid UUID lines and empty files fail cleanly before any API call."""
    bad_file = tmp_path / "bad.txt"
    bad_file.write_text("not-a-uuid\n", encoding="utf-8")
    empty_file = tmp_path / "empty.txt"
    empty_file.write_text("   \n\n", encoding="utf-8")

    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "create",
                "regression",
                "--add-sessions-file",
                str(bad_file),
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "--add-sessions-file line 1 must contain an exact UUID." in error["message"]

    assert (
        app_module.main(
            [
                "cohort",
                "version",
                "create",
                "regression",
                "--remove-sessions-file",
                str(empty_file),
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert (
        "--remove-sessions-file must contain at least one session UUID."
        in (error["message"])
    )
    assert argv_client.cohort_lookups == 0
    assert argv_client.created_versions == []
