"""Tests for execution diff utilities."""

from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from kitaru._client._models import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
)
from kitaru._llm_usage import (
    LLM_USAGE_METADATA_KEY,
    LLMBillingEffect,
    build_usage_record,
)
from kitaru._ui_urls import UiUrlContext
from kitaru.diff import (
    ExecutionDiff,
    diff,
    serialize_checkpoint_diff,
    serialize_execution_diff,
)
from tests._diff_helpers import checkpoint_diff_from_usage_records

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
    artifact_ids: list[str] | None = None,
    attempts: list[CheckpointAttempt] | None = None,
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
        attempts=attempts or [],
        artifacts=[
            ArtifactRef(
                artifact_id=artifact_id,
                name="output",
                kind="dict",
                save_type="dict",
                producing_call=call_id,
                metadata={},
                _client=MagicMock(),
            )
            for artifact_id in artifact_ids or []
        ],
        checkpoint_type="tool_call",
    )


def _usage_record(
    *,
    record_id: str | None = None,
    input_tokens: int = 10,
    output_tokens: int = 5,
    total_tokens: int = 15,
    actual_cost_usd: float | None = None,
    estimated_cost_usd: float | None = None,
    billing_effect: LLMBillingEffect = "incurred",
) -> dict[str, Any]:
    return build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id=record_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        actual_cost_usd=actual_cost_usd,
        estimated_cost_usd=estimated_cost_usd,
        billing_effect=billing_effect,
    )


@pytest.mark.parametrize(
    ("original_record", "replay_record", "expected_tokens", "expected_cost"),
    [
        (
            _usage_record(actual_cost_usd=0.1),
            _usage_record(
                input_tokens=20,
                output_tokens=8,
                total_tokens=28,
                actual_cost_usd=0.4,
            ),
            {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
            0.3,
        ),
        (
            _usage_record(
                input_tokens=20,
                output_tokens=8,
                total_tokens=28,
                actual_cost_usd=0.4,
            ),
            _usage_record(actual_cost_usd=0.1),
            {"prompt_tokens": -10, "completion_tokens": -3, "total_tokens": -13},
            -0.3,
        ),
        (
            _usage_record(actual_cost_usd=0.1),
            _usage_record(actual_cost_usd=0.1),
            {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            0.0,
        ),
    ],
)
def test_checkpoint_diff_uses_canonical_records_for_signed_usage_deltas(
    original_record: dict[str, Any],
    replay_record: dict[str, Any],
    expected_tokens: dict[str, int],
    expected_cost: float,
) -> None:
    result = checkpoint_diff_from_usage_records(
        original_records=[original_record],
        replay_records=[replay_record],
    )

    assert result.token_delta == expected_tokens
    assert result.cost_delta_usd == expected_cost


@pytest.mark.parametrize(
    ("original_present", "expected_sign"),
    [(True, -1), (False, 1)],
)
def test_checkpoint_diff_counts_usage_on_checkpoint_present_on_only_one_side(
    original_present: bool,
    expected_sign: int,
) -> None:
    record = _usage_record(actual_cost_usd=0.25)
    result = checkpoint_diff_from_usage_records(
        original_records=[record] if original_present else None,
        replay_records=None if original_present else [record],
    )

    assert result.token_delta == {
        "prompt_tokens": expected_sign * 10,
        "completion_tokens": expected_sign * 5,
        "total_tokens": expected_sign * 15,
    }
    assert result.cost_delta_usd == expected_sign * 0.25


def test_checkpoint_diff_distinguishes_no_usage_from_zero_valued_usage() -> None:
    no_usage = checkpoint_diff_from_usage_records()
    zero_usage = checkpoint_diff_from_usage_records(
        replay_records=[
            _usage_record(
                input_tokens=0,
                output_tokens=0,
                total_tokens=0,
                actual_cost_usd=0.0,
            )
        ]
    )

    assert no_usage.token_delta is None
    assert no_usage.cost_delta_usd == 0.0
    assert zero_usage.token_delta == {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }
    assert zero_usage.cost_delta_usd == 0.0


@pytest.mark.parametrize(
    ("record", "expected_cost"),
    [
        (
            _usage_record(actual_cost_usd=0.2, estimated_cost_usd=0.9),
            0.2,
        ),
        (_usage_record(estimated_cost_usd=0.3), 0.3),
        (_usage_record(actual_cost_usd=0.0), 0.0),
        (_usage_record(), None),
        (_usage_record(billing_effect="reused_not_incurred"), 0.0),
        (_usage_record(billing_effect="unknown"), None),
    ],
)
def test_checkpoint_diff_applies_canonical_cost_policy(
    record: dict[str, Any],
    expected_cost: float | None,
) -> None:
    result = checkpoint_diff_from_usage_records(replay_records=[record])

    assert result.cost_delta_usd == expected_cost
    assert result.token_delta == {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
    }


def test_checkpoint_diff_deduplicates_within_attempt_not_across_retries() -> None:
    first = _usage_record(
        record_id="same-call",
        input_tokens=7,
        output_tokens=4,
        total_tokens=11,
        actual_cost_usd=0.1,
    )
    second = _usage_record(
        record_id="same-call",
        input_tokens=8,
        output_tokens=5,
        total_tokens=13,
        actual_cost_usd=0.2,
    )
    attempts = [
        CheckpointAttempt(
            attempt_id="attempt-1",
            status=ExecutionStatus.COMPLETED,
            started_at=None,
            ended_at=None,
            metadata={LLM_USAGE_METADATA_KEY: [first, first]},
            failure=None,
        ),
        CheckpointAttempt(
            attempt_id="attempt-2",
            status=ExecutionStatus.COMPLETED,
            started_at=None,
            ended_at=None,
            metadata={LLM_USAGE_METADATA_KEY: [second]},
            failure=None,
        ),
    ]

    result = _diff_module._compare_checkpoints(
        original_cp=None,
        replay_cp=_checkpoint(
            call_id="cp-replay",
            name="model_call",
            attempts=attempts,
        ),
        client=MagicMock(),
        artifact_hash_cache={},
    )

    assert result.token_delta == {
        "prompt_tokens": 15,
        "completion_tokens": 9,
        "total_tokens": 24,
    }
    assert result.cost_delta_usd == 0.3


def test_diff_serializers_include_cost_numbers_and_null() -> None:
    priced = checkpoint_diff_from_usage_records(
        replay_records=[_usage_record(actual_cost_usd=0.25)]
    )
    unpriced = checkpoint_diff_from_usage_records(replay_records=[_usage_record()])
    execution = ExecutionDiff(
        original_exec_id="kr-original",
        compared=[("kr-replay", [priced, unpriced])],
    )

    serialized_checkpoints = serialize_execution_diff(execution)["compared"][0][
        "checkpoints"
    ]
    assert serialize_checkpoint_diff(priced)["cost_delta_usd"] == 0.25
    assert serialized_checkpoints[0]["cost_delta_usd"] == 0.25
    assert serialized_checkpoints[1]["cost_delta_usd"] is None


def _execution(
    exec_id: str,
    *,
    original_exec_id: str | None = None,
    checkpoints: list[CheckpointCall],
    project_id: str | None = "project-123",
    project_name: str | None = "default",
    metadata: dict[str, object] | None = None,
    flow_id: str = "flow-1",
    flow_name: str | None = "support_copilot_flow",
) -> Execution:
    return Execution(
        exec_id=exec_id,
        flow_id=flow_id,
        flow_name=flow_name,
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
        original_b,
        replay_a,
        replay_b,
    ]
    fake_client.executions._list_replays_for_originals.return_value = (
        [replay_a, replay_b],
        False,
    )

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        from kitaru.diff import diff_cohort

        matrix = diff_cohort(["kr-original-a", "kr-original-b"])

    assert len(matrix.rows) == 2
    assert matrix.rows[0].original_exec_id == "kr-original-a"
    assert matrix.rows[1].original_exec_id == "kr-original-b"
    assert matrix.rows[0].compared[0][0] == "kr-replay-a"
    assert matrix.rows[1].compared[0][0] == "kr-replay-b"
    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=["kr-original-a", "kr-original-b"],
        expected_flow_name="support_copilot_flow",
        limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
    )


def test_diff_uses_loaded_original_id_and_resolves_ui_context_once() -> None:
    original = _execution("kr-canonical", checkpoints=[])
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-canonical",
        checkpoints=[],
    )
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch(
            "kitaru.diff._client_ui_url_context",
            return_value=None,
        ) as ui_context_mock,
    ):
        result = diff("kr-requested-alias", "kr-replay")

    assert result.original_exec_id == "kr-canonical"
    ui_context_mock.assert_called_once_with(fake_client)
    fake_client.executions._list_replays_for_originals.assert_not_called()


def test_diff_cohort_uses_canonical_ids_for_aliases_and_duplicates() -> None:
    from kitaru.diff import diff_cohort

    original_a = _execution("kr-original-a", checkpoints=[])
    original_b = _execution("kr-original-b", checkpoints=[])
    replay_a = _execution(
        "kr-replay-a",
        original_exec_id="kr-original-a",
        checkpoints=[],
    )
    replay_b = _execution(
        "kr-replay-b",
        original_exec_id="kr-original-b",
        checkpoints=[],
    )
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [
        original_a,
        original_b,
        original_a,
        replay_a,
        replay_b,
    ]
    fake_client.executions._list_replays_for_originals.return_value = (
        [replay_a, replay_b],
        False,
    )

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch(
            "kitaru.diff._client_ui_url_context",
            return_value=None,
        ) as ui_context_mock,
    ):
        matrix = diff_cohort(["latest-a", "kr-original-b", "kr-original-a", "latest-a"])

    assert [row.original_exec_id for row in matrix.rows] == [
        "kr-original-a",
        "kr-original-b",
        "kr-original-a",
        "kr-original-a",
    ]
    assert matrix.rows[0] is matrix.rows[2] is matrix.rows[3]
    assert fake_client.executions.get.call_args_list == [
        call("latest-a"),
        call("kr-original-b"),
        call("kr-original-a"),
        call("kr-replay-a"),
        call("kr-replay-b"),
    ]
    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=["kr-original-a", "kr-original-b"],
        expected_flow_name="support_copilot_flow",
        limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
    )
    ui_context_mock.assert_called_once_with(fake_client)


def test_diff_cohort_discovers_once_and_excludes_unrelated_rows() -> None:
    from kitaru.diff import diff_cohort

    original_ids = [f"kr-original-{index}" for index in range(50)]
    originals = [_execution(exec_id, checkpoints=[]) for exec_id in original_ids]
    replays = [
        _execution(
            f"kr-replay-{index}",
            original_exec_id=original_id,
            checkpoints=[],
        )
        for index, original_id in enumerate(original_ids)
    ]
    unrelated = [
        _execution(
            f"kr-unrelated-{index}",
            original_exec_id="outside-cohort",
            checkpoints=[],
        )
        for index in range(1_000)
    ]
    candidates = [*replays, *unrelated]
    returned_rows = 0

    def list_filtered_replays(
        *,
        original_exec_ids: list[str],
        expected_flow_name: str | None,
        limit: int,
    ) -> tuple[list[Execution], bool]:
        nonlocal returned_rows
        requested_original_ids = set(original_exec_ids)
        matching = [
            candidate
            for candidate in candidates
            if candidate.original_exec_id in requested_original_ids
            and candidate.flow_name == expected_flow_name
        ][:limit]
        returned_rows += len(matching)
        return matching, False

    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [*originals, *replays]
    fake_client.executions._list_replays_for_originals.side_effect = (
        list_filtered_replays
    )

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        matrix = diff_cohort(original_ids)

    assert [row.original_exec_id for row in matrix.rows] == original_ids
    assert [row.compared[0][0] for row in matrix.rows] == [
        replay.exec_id for replay in replays
    ]
    assert returned_rows == 50
    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=original_ids,
        expected_flow_name="support_copilot_flow",
        limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
    )


def test_diff_cohort_discovers_once_per_flow_and_preserves_input_order() -> None:
    from kitaru.diff import diff_cohort

    original_ids = ["flow-b-1", "flow-a-1", "flow-b-2"]
    originals = [
        _execution("flow-b-1", checkpoints=[], flow_id="flow-b", flow_name="flow_b"),
        _execution("flow-a-1", checkpoints=[], flow_id="flow-a", flow_name="flow_a"),
        _execution("flow-b-2", checkpoints=[], flow_id="flow-b", flow_name="flow_b"),
    ]
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = originals
    fake_client.executions._list_replays_for_originals.side_effect = [
        ([], False),
        ([], False),
    ]

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        matrix = diff_cohort(original_ids)

    assert [row.original_exec_id for row in matrix.rows] == original_ids
    assert fake_client.executions._list_replays_for_originals.call_args_list == [
        call(
            original_exec_ids=["flow-b-1", "flow-b-2"],
            expected_flow_name="flow_b",
            limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
        ),
        call(
            original_exec_ids=["flow-a-1"],
            expected_flow_name="flow_a",
            limit=_diff_module._AUTO_DISCOVERY_SCAN_LIMIT,
        ),
    ]


def test_diff_cohort_attaches_truncation_warning_to_every_row_in_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.diff import diff_cohort

    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original_ids = ["flow-a-1", "flow-b-1", "flow-a-2"]
    originals = [
        _execution("flow-a-1", checkpoints=[], flow_id="flow-a", flow_name="flow_a"),
        _execution("flow-b-1", checkpoints=[], flow_id="flow-b", flow_name="flow_b"),
        _execution("flow-a-2", checkpoints=[], flow_id="flow-a", flow_name="flow_a"),
    ]
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = originals
    fake_client.executions._list_replays_for_originals.side_effect = [
        ([], True),
        ([], False),
    ]

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        matrix = diff_cohort(original_ids)

    assert len(matrix.rows[0].warnings) == 1
    assert matrix.rows[1].warnings == []
    assert matrix.rows[2].warnings == matrix.rows[0].warnings
    warning = matrix.rows[0].warnings[0]
    assert "flow flow_a" in warning
    assert "Replay discovery" in warning
    assert "scanned 3 executions" in warning
    assert "older executions remain" in warning
    assert "This row may omit older replays" in warning
    assert serialize_execution_diff(matrix.rows[0])["warnings"] == [warning]


def test_diff_cohort_warning_describes_missing_flow_name_plainly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.diff import diff_cohort

    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original = _execution("kr-original", checkpoints=[], flow_name=None)
    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client.executions._list_replays_for_originals.return_value = ([], True)

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        matrix = diff_cohort(["kr-original"])

    warning = matrix.rows[0].warnings[0]
    assert "executions without an available flow name" in warning
    assert "This row may omit older replays" in warning


def test_diff_cohort_reuses_artifact_hashes_across_original_rows() -> None:
    from kitaru.diff import diff_cohort

    originals = [
        _execution(
            f"kr-original-{suffix}",
            checkpoints=[
                _checkpoint(
                    call_id=f"cp-original-{suffix}",
                    name="checkpoint",
                    artifact_ids=["artifact-shared"],
                )
            ],
        )
        for suffix in ("a", "b")
    ]
    replays = [
        _execution(
            f"kr-replay-{suffix}",
            original_exec_id=f"kr-original-{suffix}",
            checkpoints=[
                _checkpoint(
                    call_id=f"cp-replay-{suffix}",
                    name="checkpoint",
                    original_call_id=f"cp-original-{suffix}",
                    artifact_ids=["artifact-shared"],
                )
            ],
        )
        for suffix in ("a", "b")
    ]
    shared_artifact = SimpleNamespace(load=MagicMock(return_value={"value": "shared"}))
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [*originals, *replays]
    fake_client.executions._list_replays_for_originals.return_value = (
        replays,
        False,
    )
    fake_client._get_artifact_version.return_value = shared_artifact

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        matrix = diff_cohort(["kr-original-a", "kr-original-b"])

    fake_client._get_artifact_version.assert_called_once_with(
        "artifact-shared", hydrate=True
    )
    shared_artifact.load.assert_called_once_with()
    assert [row.compared[0][1][0].artifact_hashes["output"] for row in matrix.rows] == [
        (matrix.rows[0].compared[0][1][0].artifact_hashes["output"])
    ] * 2


def test_diff_caches_artifact_hashes_for_one_request() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-original",
                name="checkpoint",
                artifact_ids=["artifact-original"],
            )
        ],
    )
    replays = [
        _execution(
            replay_id,
            original_exec_id="kr-original",
            checkpoints=[
                _checkpoint(
                    call_id=f"cp-{replay_id}",
                    name="checkpoint",
                    original_call_id="cp-original",
                    artifact_ids=["artifact-replay"],
                )
            ],
        )
        for replay_id in ("kr-replay-a", "kr-replay-b")
    ]
    loaded_artifacts = {
        "artifact-original": SimpleNamespace(
            load=MagicMock(return_value={"value": "original"})
        ),
        "artifact-replay": SimpleNamespace(
            load=MagicMock(return_value={"value": "replay"})
        ),
    }
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, *replays]
    fake_client._get_artifact_version.side_effect = lambda artifact_id, *, hydrate: (
        loaded_artifacts[artifact_id]
    )

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        result = diff("kr-original", "kr-replay-a", "kr-replay-b")

    assert fake_client._get_artifact_version.call_count == 2
    assert loaded_artifacts["artifact-original"].load.call_count == 1
    assert loaded_artifacts["artifact-replay"].load.call_count == 1
    first_hashes = result.compared[0][1][0].artifact_hashes["output"]
    second_hashes = result.compared[1][1][0].artifact_hashes["output"]
    assert first_hashes == second_hashes
    assert all(value is not None for value in first_hashes)


def test_diff_retries_artifact_hash_after_transient_failure() -> None:
    original = _execution(
        "kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-original",
                name="checkpoint",
                artifact_ids=["shared-artifact"],
            )
        ],
    )
    replay = _execution(
        "kr-replay",
        original_exec_id="kr-original",
        checkpoints=[
            _checkpoint(
                call_id="cp-replay",
                name="checkpoint",
                original_call_id="cp-original",
                artifact_ids=["shared-artifact"],
            )
        ],
    )
    loaded_artifact = SimpleNamespace(
        load=MagicMock(return_value={"value": "available"})
    )
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, replay]
    fake_client._get_artifact_version.side_effect = [
        RuntimeError("transient"),
        loaded_artifact,
    ]

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        result = diff("kr-original", "kr-replay")

    assert fake_client._get_artifact_version.call_args_list == [
        call("shared-artifact", hydrate=True),
        call("shared-artifact", hydrate=True),
    ]
    loaded_artifact.load.assert_called_once_with()
    original_hash, replay_hash = result.compared[0][1][0].artifact_hashes["output"]
    assert original_hash is None
    assert replay_hash is not None


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
    fake_client.executions._list_replays_for_originals.return_value = (
        [replay_a, replay_b],
        False,
    )
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


def test_diff_auto_discovery_uses_bounded_flow_scan() -> None:
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
    fake_client.executions._list_replays_for_originals.return_value = (
        [replay],
        False,
    )

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=["kr-original"],
        expected_flow_name="support_copilot_flow",
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
    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client.executions._list_replays_for_originals.return_value = ([], False)

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=["kr-original"],
        expected_flow_name="support_copilot_flow",
        limit=3,
    )
    assert result.compared == []
    assert result.warnings == []


def test_diff_auto_discovery_returns_exact_scan_limit_without_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original = _execution("kr-original", checkpoints=[])
    replays = [
        _execution(
            f"kr-replay-{index}",
            original_exec_id="kr-original",
            checkpoints=[],
        )
        for index in range(3)
    ]
    fake_client = MagicMock()
    fake_client.executions.get.side_effect = [original, *replays]
    fake_client.executions._list_replays_for_originals.return_value = (
        replays,
        False,
    )

    with (
        patch("kitaru.diff.KitaruClient", return_value=fake_client),
        patch("kitaru.diff._client_ui_url_context", return_value=None),
    ):
        result = diff("kr-original")

    assert [replay_exec_id for replay_exec_id, _ in result.compared] == [
        replay.exec_id for replay in replays
    ]
    assert result.warnings == []


def test_diff_warns_when_auto_discovery_hits_scan_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_diff_module, "_AUTO_DISCOVERY_SCAN_LIMIT", 3)
    original = _execution(
        "kr-original",
        checkpoints=[_checkpoint(call_id="cp-1", name="lookup_policy_tool")],
    )
    fake_client = MagicMock()
    fake_client.executions.get.return_value = original
    fake_client.executions._list_replays_for_originals.return_value = ([], True)

    with patch("kitaru.diff.KitaruClient", return_value=fake_client):
        result = diff("kr-original")

    fake_client.executions._list_replays_for_originals.assert_called_once_with(
        original_exec_ids=["kr-original"],
        expected_flow_name="support_copilot_flow",
        limit=3,
    )
    assert result.compared == []
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert "Replay discovery" in warning
    assert "scanned 3 executions" in warning
    assert "older executions remain" in warning
    assert "This row may omit older replays" in warning
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

    fake_client.executions._list_replays_for_originals.assert_not_called()
    assert result.warnings == []
    assert serialize_execution_diff(result)["warnings"] == []
