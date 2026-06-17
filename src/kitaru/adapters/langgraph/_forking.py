"""Private helpers for LangGraph native fork selection."""

from collections.abc import Iterable, Mapping
from typing import Any

from kitaru.errors import KitaruUsageError

from ._types import LangGraphCheckpointSelector


def config_checkpoint_id(config: Any) -> str | None:
    """Return a normalized LangGraph checkpoint id from a config mapping."""
    return _non_empty_string(_configurable_value(config, "checkpoint_id"))


def config_checkpoint_ns(config: Any) -> str | None:
    """Return a normalized LangGraph checkpoint namespace from a config mapping."""
    return _non_empty_string(_configurable_value(config, "checkpoint_ns"))


def snapshot_checkpoint_id(snapshot: Any) -> str | None:
    """Return the checkpoint id from a LangGraph state snapshot."""
    return config_checkpoint_id(getattr(snapshot, "config", None))


def snapshot_checkpoint_ns(snapshot: Any) -> str | None:
    """Return the checkpoint namespace from a LangGraph state snapshot."""
    return config_checkpoint_ns(getattr(snapshot, "config", None))


def snapshot_next_nodes(snapshot: Any) -> tuple[str, ...]:
    """Return the next-node tuple from a LangGraph state snapshot."""
    return tuple(str(node) for node in (getattr(snapshot, "next", ()) or ()))


def select_history_snapshot(
    history: Iterable[Any],
    selector: LangGraphCheckpointSelector,
) -> tuple[Any, int]:
    """Select one snapshot from LangGraph history using the public selector."""
    snapshots = list(history)
    matches = [snapshot for snapshot in snapshots if _matches(snapshot, selector)]
    if not matches:
        raise KitaruUsageError(_no_match_message(snapshots, selector))
    if selector.match_index >= len(matches):
        raise KitaruUsageError(
            "LangGraph checkpoint selector matched "
            f"{len(matches)} snapshot(s), but match_index={selector.match_index} "
            "was requested. "
            f"Seen snapshots: {_history_summary(snapshots)}"
        )
    return matches[selector.match_index], len(matches)


def _matches(snapshot: Any, selector: LangGraphCheckpointSelector) -> bool:
    if (
        selector.checkpoint_id is not None
        and snapshot_checkpoint_id(snapshot) != selector.checkpoint_id
    ):
        return False
    if (
        selector.checkpoint_ns is not None
        and snapshot_checkpoint_ns(snapshot) != selector.checkpoint_ns
    ):
        return False
    return not (
        selector.next_nodes is not None
        and snapshot_next_nodes(snapshot) != tuple(selector.next_nodes)
    )


def _configurable_value(config: Any, key: str) -> Any:
    if not isinstance(config, Mapping):
        return None
    configurable = config.get("configurable", {})
    if not isinstance(configurable, Mapping):
        return None
    return configurable.get(key)


def _non_empty_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _no_match_message(
    snapshots: list[Any], selector: LangGraphCheckpointSelector
) -> str:
    return (
        "No LangGraph checkpoint history snapshot matched "
        f"checkpoint_id={selector.checkpoint_id!r}, "
        f"checkpoint_ns={selector.checkpoint_ns!r}, "
        f"next_nodes={selector.next_nodes!r}. "
        f"Seen snapshots: {_history_summary(snapshots)}"
    )


def _history_summary(snapshots: list[Any]) -> list[dict[str, object]]:
    return [
        {
            "checkpoint_id": snapshot_checkpoint_id(snapshot),
            "checkpoint_ns": snapshot_checkpoint_ns(snapshot),
            "next": list(snapshot_next_nodes(snapshot)),
        }
        for snapshot in snapshots
    ]
