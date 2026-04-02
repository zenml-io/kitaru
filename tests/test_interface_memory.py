"""Tests for shared memory transport helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kitaru._interface_memory import (
    delete_memory_payload,
    get_memory_payload,
    history_memory_payload,
    list_memory_payload,
    normalize_memory_key,
    normalize_memory_prefix,
    normalize_memory_scope,
    normalize_memory_scope_type,
    normalize_memory_version,
    set_memory_payload,
)
from kitaru.client import KitaruClient
from kitaru.memory import MemoryEntry


def _sample_memory_entry(
    *,
    key: str = "prefs",
    scope: str = "repo_scope",
    scope_type: str = "namespace",
    version: int = 2,
    is_deleted: bool = False,
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
        execution_id=None,
    )


def _client_with_mocks() -> tuple[Any, Any, Any]:
    memories = SimpleNamespace(
        get=MagicMock(),
        list=MagicMock(),
        history=MagicMock(),
        set=MagicMock(),
        delete=MagicMock(),
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
    entry = _sample_memory_entry(scope="repo_scope", version=3)
    memories.get.return_value = entry
    artifacts.get.return_value = SimpleNamespace(
        load=MagicMock(return_value={"theme": "dark"})
    )

    payload = get_memory_payload(
        cast(KitaruClient, client),
        key="prefs",
        scope="repo_scope",
        version=3,
    )

    assert payload == {
        "key": "prefs",
        "value_type": "dict",
        "version": 3,
        "scope": "repo_scope",
        "scope_type": "namespace",
        "created_at": "2026-04-01T12:00:00+00:00",
        "is_deleted": False,
        "artifact_id": entry.artifact_id,
        "execution_id": None,
        "value": {"theme": "dark"},
        "value_format": "json",
    }
    memories.get.assert_called_once_with("prefs", scope="repo_scope", version=3)
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


def test_delete_memory_payload_returns_none_when_key_missing() -> None:
    client, memories, _artifacts = _client_with_mocks()
    memories.delete.return_value = None

    assert (
        delete_memory_payload(cast(KitaruClient, client), key="prefs", scope="repo")
        is None
    )
