"""Tests for shared memory transport helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kitaru._interface_memory import (
    compact_memory_payload,
    compaction_log_memory_payload,
    delete_memory_payload,
    get_memory_payload,
    history_memory_payload,
    list_memory_payload,
    normalize_memory_compaction_source_mode,
    normalize_memory_keep,
    normalize_memory_key,
    normalize_memory_prefix,
    normalize_memory_scope,
    normalize_memory_scope_type,
    normalize_memory_version,
    purge_memory_payload,
    purge_scope_memory_payload,
    reindex_memory_payload,
    set_memory_payload,
)
from kitaru.client import KitaruClient
from kitaru.memory import (
    CompactionRecord,
    CompactResult,
    MemoryEntry,
    MemoryReindexIssue,
    MemoryReindexResult,
    PurgeResult,
)


def _sample_memory_entry(
    *,
    key: str = "prefs",
    scope: str = "repo_scope",
    scope_type: str = "namespace",
    version: int = 2,
    is_deleted: bool = False,
    execution_id: str | None = None,
    flow_id: str | None = None,
    flow_name: str | None = None,
) -> MemoryEntry:
    return MemoryEntry(
        key=key,
        value_type="dict",
        version=version,
        scope=scope,
        scope_type=scope_type,
        created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        is_deleted=is_deleted,
        artifact_id=str(uuid4()),
        execution_id=execution_id,
        flow_id=flow_id,
        flow_name=flow_name,
    )


def _client_with_mocks() -> tuple[Any, Any, Any]:
    memories = SimpleNamespace(
        get=MagicMock(),
        list=MagicMock(),
        history=MagicMock(),
        set=MagicMock(),
        delete=MagicMock(),
        purge=MagicMock(),
        purge_scope=MagicMock(),
        compact=MagicMock(),
        compaction_log=MagicMock(),
        reindex=MagicMock(),
    )
    artifacts = SimpleNamespace(get=MagicMock())
    client = cast(Any, SimpleNamespace(memories=memories, artifacts=artifacts))
    return client, memories, artifacts


@pytest.mark.parametrize(
    ("func", "value", "match"),
    [
        (normalize_memory_scope, "bad:scope", "Memory scope"),
        (normalize_memory_key, "bad:key", "Memory key"),
        (normalize_memory_prefix, "bad:prefix", "Memory prefix"),
        (normalize_memory_scope_type, "bogus", "Memory scope_type"),
        (normalize_memory_version, 0, "Memory version"),
    ],
)
def test_normalizers_raise_value_error_for_invalid_transport_input(
    func: Any,
    value: Any,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        func(value)


def test_get_memory_payload_merges_entry_and_serialized_value() -> None:
    client, memories, artifacts = _client_with_mocks()
    entry = _sample_memory_entry(
        scope="exec-123",
        scope_type="execution",
        version=3,
        execution_id=None,
        flow_id="flow-456",
        flow_name="repo_memory_demo",
    )
    memories.get.return_value = entry
    artifacts.get.return_value = SimpleNamespace(
        load=MagicMock(return_value={"theme": "dark"})
    )

    payload = get_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        scope="exec-123",
        version=3,
    )

    assert payload == {
        "key": "prefs",
        "value_type": "dict",
        "version": 3,
        "scope": "exec-123",
        "scope_type": "execution",
        "created_at": "2026-04-01T12:00:00+00:00",
        "is_deleted": False,
        "artifact_id": entry.artifact_id,
        "execution_id": None,
        "flow_id": "flow-456",
        "flow_name": "repo_memory_demo",
        "value": {"theme": "dark"},
        "value_format": "json",
    }
    memories.get.assert_called_once_with("prefs", scope="exec-123", version=3)
    artifacts.get.assert_called_once_with(entry.artifact_id)


def test_get_memory_payload_returns_none_when_key_missing() -> None:
    client, memories, artifacts = _client_with_mocks()
    memories.get.return_value = None

    payload = get_memory_payload(cast(KitaruClient, client), key="prefs", scope="repo")

    assert payload is None
    artifacts.get.assert_not_called()


def test_list_and_history_memory_payloads_serialize_entries() -> None:
    client, memories, _artifacts = _client_with_mocks()
    entries = [
        _sample_memory_entry(key="repo_alpha", scope="repo_scope", version=2),
        _sample_memory_entry(key="repo_beta", scope="repo_scope", version=1),
    ]
    history = [
        _sample_memory_entry(
            key="prefs", scope="repo_scope", version=2, is_deleted=True
        ),
        _sample_memory_entry(key="prefs", scope="repo_scope", version=1),
    ]
    memories.list.return_value = entries
    memories.history.return_value = history

    list_payload = list_memory_payload(
        cast(KitaruClient, client),
        scope="repo_scope",
        prefix="repo_",
    )
    history_payload = history_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        scope="repo_scope",
    )

    assert [entry["key"] for entry in list_payload] == ["repo_alpha", "repo_beta"]
    assert [entry["version"] for entry in history_payload] == [2, 1]
    assert [entry["is_deleted"] for entry in history_payload] == [True, False]
    memories.list.assert_called_once_with(scope="repo_scope", prefix="repo_")
    memories.history.assert_called_once_with("prefs", scope="repo_scope")


def test_set_and_delete_memory_payloads_delegate_to_client_namespace() -> None:
    client, memories, _artifacts = _client_with_mocks()
    created = _sample_memory_entry(scope="repo_scope", scope_type="flow", version=4)
    tombstone = _sample_memory_entry(
        scope="repo_scope",
        version=5,
        is_deleted=True,
    )
    memories.set.return_value = created
    memories.delete.return_value = tombstone

    set_payload = set_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        value={"theme": "dark"},
        scope="repo_scope",
        scope_type="flow",
    )
    delete_payload = delete_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        scope="repo_scope",
    )

    assert set_payload["version"] == 4
    assert set_payload["scope_type"] == "flow"
    assert delete_payload is not None
    assert delete_payload["is_deleted"] is True
    memories.set.assert_called_once_with(
        "prefs",
        {"theme": "dark"},
        scope="repo_scope",
        scope_type="flow",
    )
    memories.delete.assert_called_once_with("prefs", scope="repo_scope")


def test_reindex_memory_payload_delegates_and_serializes_result() -> None:
    client, memories, _artifacts = _client_with_mocks()
    memories.reindex.return_value = MemoryReindexResult(
        dry_run=False,
        versions_scanned=4,
        execution_scope_versions_scanned=2,
        already_indexed=1,
        versions_needing_updates=3,
        versions_updated=0,
        scope_type_tags_identified=3,
        flow_tags_identified=2,
        scope_type_tags_added=0,
        flow_tags_added=0,
        issues_count=1,
        issue_samples=[
            MemoryReindexIssue(
                artifact_id="artifact-1",
                artifact_name="kitaru_mem:exec-123:scratch",
                scope="exec-123",
                key="scratch",
                reason="execution scope 'exec-123': lookup failed",
            )
        ],
    )

    payload = reindex_memory_payload(cast(KitaruClient, client), apply=True)

    assert payload["dry_run"] is False
    assert payload["versions_scanned"] == 4
    assert payload["flow_tags_identified"] == 2
    assert payload["issues_count"] == 1
    assert payload["issue_samples"][0]["scope"] == "exec-123"
    memories.reindex.assert_called_once_with(apply=True)


def test_delete_memory_payload_returns_none_when_key_missing() -> None:
    client, memories, _artifacts = _client_with_mocks()
    memories.delete.return_value = None

    assert (
        delete_memory_payload(cast(KitaruClient, client), key="prefs", scope="repo")
        is None
    )


# ---------------------------------------------------------------------------
# normalize_memory_keep
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keep", [0, 1, 5, 100])
def test_normalize_memory_keep_accepts_valid_values(keep: int) -> None:
    assert normalize_memory_keep(keep) == keep


def test_normalize_memory_keep_accepts_none() -> None:
    assert normalize_memory_keep(None) is None


@pytest.mark.parametrize("keep", [-1, -100])
def test_normalize_memory_keep_rejects_negative(keep: int) -> None:
    with pytest.raises(ValueError, match=r"non-negative"):
        normalize_memory_keep(keep)


# ---------------------------------------------------------------------------
# normalize_memory_compaction_source_mode
# ---------------------------------------------------------------------------


def test_normalize_memory_compaction_source_mode_defaults_to_current() -> None:
    assert normalize_memory_compaction_source_mode(None) == "current"


@pytest.mark.parametrize("source_mode", ["current", "history"])
def test_normalize_memory_compaction_source_mode_accepts_known_values(
    source_mode: str,
) -> None:
    assert normalize_memory_compaction_source_mode(source_mode) == source_mode


def test_normalize_memory_compaction_source_mode_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="source_mode"):
        normalize_memory_compaction_source_mode("future")


# ---------------------------------------------------------------------------
# Purge payloads
# ---------------------------------------------------------------------------


def test_purge_memory_payload_delegates_and_serializes() -> None:
    client, memories, _artifacts = _client_with_mocks()
    memories.purge.return_value = PurgeResult(
        versions_deleted=3, keys_affected=1, scope="repo"
    )

    result = purge_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        scope="repo",
        keep=2,
    )

    assert result["versions_deleted"] == 3
    assert result["keys_affected"] == 1
    assert result["scope"] == "repo"
    memories.purge.assert_called_once_with("prefs", scope="repo", keep=2)


def test_purge_scope_memory_payload_delegates() -> None:
    client, memories, _artifacts = _client_with_mocks()
    memories.purge_scope.return_value = PurgeResult(
        versions_deleted=5, keys_affected=2, scope="repo"
    )

    result = purge_scope_memory_payload(
        cast(KitaruClient, client),
        scope="repo",
        keep=1,
        include_deleted=True,
    )

    assert result["versions_deleted"] == 5
    memories.purge_scope.assert_called_once_with(
        scope="repo", keep=1, include_deleted=True
    )


# ---------------------------------------------------------------------------
# Compact payload
# ---------------------------------------------------------------------------


def test_compact_memory_payload_delegates_with_source_mode() -> None:
    client, memories, _artifacts = _client_with_mocks()
    entry = _sample_memory_entry(key="prefs", scope="repo", version=4)
    record = CompactionRecord(
        operation="compact",
        scope="repo",
        timestamp=datetime(2026, 4, 1, tzinfo=UTC),
        source_keys=["prefs"],
        source_versions=[3],
        target_key="prefs",
        target_version=4,
        instruction=None,
        model="gpt-test",
        source_mode="current",
        keys_affected=0,
        versions_deleted=0,
        keep=None,
    )
    memories.compact.return_value = CompactResult(
        entry=entry,
        sources_read=1,
        scope="repo",
        compaction_record=record,
    )

    result = compact_memory_payload(
        cast(KitaruClient, client),
        scope="repo",
        key="prefs",
        source_mode="current",
    )

    assert result["entry"]["key"] == "prefs"
    assert result["compaction_record"]["source_mode"] == "current"
    memories.compact.assert_called_once_with(
        scope="repo",
        key="prefs",
        keys=None,
        source_mode="current",
        target_key=None,
        instruction=None,
        model=None,
        max_tokens=None,
    )


# ---------------------------------------------------------------------------
# Compaction log payload
# ---------------------------------------------------------------------------


def test_compaction_log_memory_payload_delegates() -> None:
    client, memories, _artifacts = _client_with_mocks()
    record = CompactionRecord(
        operation="purge",
        scope="repo",
        timestamp=datetime(2026, 4, 1, tzinfo=UTC),
        source_keys=["k1"],
        source_versions=[1, 2],
        target_key=None,
        target_version=None,
        instruction=None,
        model=None,
        keys_affected=1,
        versions_deleted=2,
        keep=1,
    )
    memories.compaction_log.return_value = [record]

    result = compaction_log_memory_payload(
        cast(KitaruClient, client),
        scope="repo",
    )

    assert len(result) == 1
    assert result[0]["operation"] == "purge"
    assert result[0]["versions_deleted"] == 2
    assert result[0]["source_mode"] is None
    memories.compaction_log.assert_called_once_with(scope="repo")
