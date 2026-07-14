"""Small builders for checkpoint usage diff tests."""

from collections.abc import Mapping, Sequence
from typing import Any
from unittest.mock import MagicMock

from kitaru._client._models import CheckpointCall, ExecutionStatus
from kitaru._llm_usage import usage_records_metadata
from kitaru.diff import CheckpointDiff, _compare_checkpoints


def checkpoint_diff_from_usage_records(
    *,
    original_records: Sequence[Mapping[str, Any]] | None = (),
    replay_records: Sequence[Mapping[str, Any]] | None = (),
) -> CheckpointDiff:
    """Calculate one checkpoint diff; ``None`` means the checkpoint is absent."""

    def build_checkpoint(
        call_id: str,
        records: Sequence[Mapping[str, Any]],
        *,
        original_call_id: str | None,
    ) -> CheckpointCall:
        return CheckpointCall(
            call_id=call_id,
            name="model_call",
            status=ExecutionStatus.COMPLETED,
            started_at=None,
            ended_at=None,
            metadata=usage_records_metadata(records),
            original_call_id=original_call_id,
            parent_call_ids=[],
            failure=None,
            attempts=[],
            artifacts=[],
            checkpoint_type="llm_call",
        )

    original = (
        build_checkpoint("cp-original", original_records, original_call_id=None)
        if original_records is not None
        else None
    )
    replay = (
        build_checkpoint(
            "cp-replay",
            replay_records,
            original_call_id="cp-original" if original is not None else None,
        )
        if replay_records is not None
        else None
    )
    return _compare_checkpoints(
        original_cp=original,
        replay_cp=replay,
        client=MagicMock(),
    )
