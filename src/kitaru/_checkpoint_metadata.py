"""Shared checkpoint metadata contract helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

KITARU_METADATA_NAMESPACE = "kitaru"
CHECKPOINT_ORIGIN_KEY = "kitaru_checkpoint_origin"
ADAPTER_KEY = "kitaru_adapter"
ADAPTER_CHECKPOINT_KIND_KEY = "kitaru_adapter_checkpoint_kind"
REPLAY_INPUT_SLOTS_KEY = "kitaru_replay_input_slots"
REPLAY_OUTPUT_SLOTS_KEY = "kitaru_replay_output_slots"

CheckpointOrigin = Literal["user", "adapter"]
CHECKPOINT_ORIGIN_USER: CheckpointOrigin = "user"
CHECKPOINT_ORIGIN_ADAPTER: CheckpointOrigin = "adapter"


def adapter_checkpoint_metadata(
    *,
    adapter: str,
    kind: str,
    input_slots: Sequence[str] = (),
    output_slots: Sequence[str] = (),
) -> dict[str, Any]:
    """Return metadata recorded on adapter-generated checkpoints."""
    return {
        CHECKPOINT_ORIGIN_KEY: CHECKPOINT_ORIGIN_ADAPTER,
        ADAPTER_KEY: adapter,
        ADAPTER_CHECKPOINT_KIND_KEY: kind,
        REPLAY_INPUT_SLOTS_KEY: list(input_slots),
        REPLAY_OUTPUT_SLOTS_KEY: list(output_slots),
    }


def checkpoint_metadata_from_step(step: Any) -> dict[str, Any]:
    """Return checkpoint metadata persisted on a ZenML step run."""
    metadata: dict[str, Any] = {}
    run_metadata = getattr(step, "run_metadata", None)
    if isinstance(run_metadata, Mapping):
        metadata.update(dict(run_metadata))

    response_metadata = getattr(step, "metadata", None)
    nested_run_metadata = getattr(response_metadata, "run_metadata", None)
    if isinstance(nested_run_metadata, Mapping):
        metadata.update(dict(nested_run_metadata))

    config = getattr(step, "config", None) or getattr(response_metadata, "config", None)
    extra = getattr(config, "extra", None)
    if isinstance(extra, Mapping):
        metadata.update(dict(extra))
    return metadata


def checkpoint_metadata_value(metadata: Mapping[str, Any], key: str) -> Any:
    """Return a checkpoint metadata value from direct or Kitaru-nested metadata."""
    if key in metadata:
        return metadata[key]
    nested = metadata.get(KITARU_METADATA_NAMESPACE)
    if isinstance(nested, Mapping):
        return nested.get(key)
    return None


def checkpoint_metadata_str(metadata: Mapping[str, Any], key: str) -> str | None:
    """Return a string checkpoint metadata value if present."""
    value = checkpoint_metadata_value(metadata, key)
    return value if isinstance(value, str) and value else None


def checkpoint_metadata_slots(metadata: Mapping[str, Any], key: str) -> list[str]:
    """Return replay slot names from checkpoint metadata."""
    value = checkpoint_metadata_value(metadata, key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [slot for slot in value if isinstance(slot, str) and slot]
