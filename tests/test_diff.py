"""Tests for execution diff utilities."""

from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from kitaru._client._models import CheckpointCall, Execution, ExecutionStatus
from kitaru._ui_urls import UiUrlContext
from kitaru.diff import diff, serialize_execution_diff

_diff_module = import_module("kitaru.diff")


def _pro_server_info() -> SimpleNamespace:
    return SimpleNamespace(
        deployment_type="cloud",
        pro_dashboard_url="https://staging.cloud.zenml.io",
        pro_workspace_name="kitaru-dev",
        metadata={"workspace_name": "kitaru-dev"},
    )


def _checkpoint(
    *,
    call_id: str,
    name: str,
    original_call_id: str | None = None,
) -> CheckpointCall:
    started = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    ended = datetime(2026, 3, 9, 10, 1, tzinfo=UTC)
    return CheckpointCall(
        call_id=call_id,
        name=name,
        status=ExecutionStatus.COMPLETED,
        started_at=started,
        ended_at=ended,
        metadata={},
        original_call_id=original_call_id,
        parent_call_ids=[],
        failure=None,
        attempts=[],
        artifacts=[],
        checkpoint_type="tool_call",
    )


def _execution(
    exec_id: str,
    *,
    original_exec_id: str | None = None,
    checkpoints: list[CheckpointCall],
    project_id: str | None = "project-123",
    project_name: str | None = "default",
    metadata: dict[str, object] | None = None,
) -> Execution:
    return Execution(
        exec_id=exec_id,
        flow_id="flow-1",
        flow_name="support_copilot_flow",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        stack_name=None,
        metadata=metadata or {},
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=original_exec_id,
        checkpoints=checkpoints,
        artifacts=[],
        _client=MagicMock(),
        project_id=project_id,
        project_name=project_name,
    )


def test_diff_aligns_checkpoints_by_original_call_id() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[
            _checkpoint(call_id="cp-1", name="lookup_policy_tool"),
            _checkpoint(call_id="cp-2", name="decide"),
        ],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-3",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            ),
            _checkpoint(
                call_id="cp-4",
                name="decide",
                original_call_id="cp-2",
            ),
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = []

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original", "kr-replay")

    assert result.original_exec_id == "kr-original"
    assert len(result.compared) == 1
    replay_id, checkpoint_diffs = result.compared[0]
    assert replay_id == "kr-replay"
    assert len(checkpoint_diffs) == 2
    assert all(item.status_match for item in checkpoint_diffs)


def test_diff_cohort_returns_one_row_per_original() -> None:
    original_a = _execution(
        "kr-original-a",
        checkpoints=[_checkpoint(call_id="cp-a", name="lookup_policy_tool")],
    )
    replay_a = _execution(
        "kr-replay-a",
        original_exec_id="kr-original-a",
        checkpoints=[
            _checkpoint(
                call_id="cp-a2",
                name="lookup_policy_tool",
                original_call_id="cp-a",
            )
        ],
    )
    original_b = _execution(
        "kr-original-b",
        checkpoints=[_checkpoint(call_id="cp-b", name="lookup_policy_tool")],
    )
    replay_b = _execution(
        "kr-replay-b",
        original_exec_id="kr-original-b",
        checkpoints=[
            _checkpoint(
                call_id="cp-b2",
                name="lookup_policy_tool",
                original_call_id="cp-b",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [
        original_a,
        replay_a,
        original_b,
        replay_b,
    ]
    fake_client.executions.list.side_effect = [
        [replay_a],
        [replay_b],
    ]

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        from kitaru.diff import diff_cohort

        matrix = diff_cohort(["kr-original-a", "kr-original-b"])

    assert len(matrix.rows) == 2
    assert matrix.rows[0].original_exec_id == "kr-original-a"
    assert matrix.rows[1].original_exec_id == "kr-original-b"
    assert matrix.rows[0].compared[0][0] == "kr-replay-a"
    assert matrix.rows[1].compared[0][0] == "kr-replay-b"


def test_build_compare_url_for_executions_supports_three_way_compare() -> None:
    from kitaru.diff import build_compare_url_for_executions

    url = build_compare_url_for_executions(
        server_url="https://demo.kitaru.zenml.io",
        flow_id="flow-1",
        exec_ids=[
            "91f4a9d3-2ebb-4607-9b3d-3d1258d47a4d",
            "4427d903-79b2-48e1-8fa2-a0f499809abf",
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        ],
    )

    assert url == (
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions="
        "91f4a9d3-2ebb-4607-9b3d-3d1258d47a4d,"
        "4427d903-79b2-48e1-8fa2-a0f499809abf,"
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    )


def test_build_compare_url_for_executions_supports_pro_route() -> None:
    from kitaru.diff import build_compare_url_for_executions

    url = build_compare_url_for_executions(
        server_url=None,
        flow_id="flow-1",
        exec_ids=["kr-original", "kr-replay-a", "kr-replay-b"],
        flow_version="7",
        project_name_or_id="default",
        ui_context=UiUrlContext(
            base_url="https://staging.cloud.zenml.io",
            route_kind="pro",
            source="server_info",
            workspace="kitaru-dev",
        ),
    )

    assert url == (
        "https://staging.cloud.zenml.io/kitaru-workspaces/kitaru-dev"
        "/projects/default/flows/flow-1/v/7/compare"
        "?executions=kr-original,kr-replay-a,kr-replay-b"
    )


def test_diff_with_two_explicit_replays_returns_one_three_way_url() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay_a = _execution(
        "kr-replay-a",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )
    replay_b = _execution(
        "kr-replay-b",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-3",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay_a, replay_b]
    fake_client._client.return_value.zen_store.url = "https://demo.kitaru.zenml.io"

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original", "kr-replay-a", "kr-replay-b")

    assert result.urls == [
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay-a,kr-replay-b"
    ]


def test_build_compare_url_matches_ui_route() -> None:
    from kitaru.diff import build_compare_url

    url = build_compare_url(
        server_url="https://demo.kitaru.zenml.io",
        flow_id="1f7a49ed-0c8d-47c9-afeb-bb161bb535fa",
        original_exec_id="91f4a9d3-2ebb-4607-9b3d-3d1258d47a4d",
        replay_exec_id="4427d903-79b2-48e1-8fa2-a0f499809abf",
    )

    assert url == (
        "https://demo.kitaru.zenml.io/flows/1f7a49ed-0c8d-47c9-afeb-bb161bb535fa"
        "/v/local/compare?executions="
        "91f4a9d3-2ebb-4607-9b3d-3d1258d47a4d,4427d903-79b2-48e1-8fa2-a0f499809abf"
    )


def test_diff_auto_discovers_replays_and_returns_one_multi_exec_url() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay_a = _execution(
        "kr-replay-a",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )
    replay_b = _execution(
        "kr-replay-b",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-3",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay_a, replay_b]
    fake_client.executions.list.return_value = [replay_a, replay_b]
    fake_client._client.return_value.zen_store.url = "https://demo.kitaru.zenml.io"

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    assert len(result.compared) == 2
    assert result.warnings == []
    assert result.urls == [
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay-a,kr-replay-b"
    ]


def test_build_compare_urls_returns_one_url_per_replay() -> None:
    from kitaru.diff import build_compare_urls

    urls = build_compare_urls(
        server_url="https://demo.kitaru.zenml.io",
        flow_id="flow-1",
        original_exec_id="kr-original",
        replay_exec_ids=["kr-replay-a", "kr-replay-b"],
    )

    assert urls == [
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay-a",
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay-b",
    ]


def test_diff_sets_compare_urls_when_server_and_flow_are_available() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = []
    fake_client._client.return_value.zen_store.url = "https://demo.kitaru.zenml.io"

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original", "kr-replay")

    assert result.urls == [
        "https://demo.kitaru.zenml.io/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay"
    ]


def test_diff_uses_pro_compare_url_when_server_metadata_is_pro() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = []

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._ui_urls._server_info_from_client",
            return_value=_pro_server_info(),
        ),
    ):
        result = diff("kr-original", "kr-replay")

    assert result.urls == [
        "https://staging.cloud.zenml.io/kitaru-workspaces/kitaru-dev"
        "/projects/default/flows/flow-1/v/local/compare"
        "?executions=kr-original,kr-replay"
    ]


def test_diff_omits_pro_compare_url_when_project_identity_is_missing() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
        project_id=None,
        project_name=None,
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = []

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._ui_urls._server_info_from_client",
            return_value=_pro_server_info(),
        ),
    ):
        result = diff("kr-original", "kr-replay")

    assert result.compared[0][0] == "kr-replay"
    assert result.urls == []


def test_diff_omits_compare_url_when_cloud_url_metadata_is_incomplete() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = []
    fake_client._client.return_value.zen_store.url = (
        "https://67e44b28-zenml.staging.cloudinfra.zenml.io"
    )

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch(
            "kitaru._ui_urls._server_info_from_client",
            return_value=SimpleNamespace(
                deployment_type="cloud",
                pro_dashboard_url="https://staging.cloud.zenml.io",
                pro_workspace_name=None,
                pro_workspace_id=None,
                metadata={},
            ),
        ),
    ):
        result = diff("kr-original", "kr-replay")

    assert result.compared[0][0] == "kr-replay"
    assert result.urls == []


def test_diff_discovers_replay_beyond_first_200_candidates() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )
    candidates = [
        _execution(
            f"kr-candidate-{index}",
            original_exec_id="kr-other",
            checkpoints=[],
        )
        for index in range(200)
    ] + [replay]

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client.executions.list.return_value = candidates

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions.list.assert_called_once_with(
        flow="support_copilot_flow",
        limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
    )
    assert [replay_id for replay_id, _ in result.compared] == ["kr-replay"]
    assert result.warnings == []


def test_diff_auto_discovery_does_not_warn_below_scan_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    candidates = [
        _execution("kr-candidate-1", original_exec_id="kr-other", checkpoints=[]),
        _execution("kr-candidate-2", original_exec_id="kr-other", checkpoints=[]),
    ]

    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client.executions.list.return_value = candidates

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions.list.assert_called_once_with(
        flow="support_copilot_flow",
        limit=3,
    )
    assert result.compared == []
    assert result.warnings == []


def test_diff_warns_when_auto_discovery_hits_scan_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    candidates = [
        _execution(
            f"kr-candidate-{index}",
            original_exec_id="kr-other",
            checkpoints=[],
        )
        for index in range(3)
    ]

    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client.executions.list.return_value = candidates

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions.list.assert_called_once_with(
        flow="support_copilot_flow",
        limit=3,
    )
    assert result.compared == []
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert "auto-discovery" in warning
    assert "stopped after scanning 3 executions" in warning
    assert "support_copilot_flow" in warning
    assert "execution IDs explicitly" in warning
    assert serialize_execution_diff(result)["warnings"] == result.warnings


def test_diff_does_not_warn_for_explicit_replay_ids_at_candidate_limit() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-2",
                name="lookup_policy_tool",
                original_call_id="cp-1",
            )
        ],
    )

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original", "kr-replay")

    fake_client.executions.list.assert_not_called()
    assert result.warnings == []
    assert serialize_execution_diff(result)["warnings"] == []
