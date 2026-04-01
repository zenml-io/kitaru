"""Tests for `kitaru.memory` configurable-scope memory behavior."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactType

from kitaru import memory
from kitaru.errors import KitaruContextError, KitaruStateError, KitaruUsageError
from kitaru.memory import (
    MemoryEntry,
    _delete_impl,
    _get_impl,
    _history_impl,
    _list_impl,
    _MemoryScope,
    _set_impl,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope


def _page(
    *items: SimpleNamespace,
    index: int = 1,
    total_pages: int = 1,
) -> SimpleNamespace:
    """Create a lightweight list_artifact_versions page."""
    return SimpleNamespace(items=[*items], index=index, total_pages=total_pages)


def _memory_artifact(
    *,
    scope: str,
    key: str,
    version: int,
    value: Any,
    deleted: bool = False,
    created_at: datetime | None = None,
    scope_type: str = "flow",
    value_type_import_path: str = "builtins.dict",
    execution_id: UUID | None = None,
) -> SimpleNamespace:
    """Build a lightweight artifact-version-like object for memory tests."""
    timestamp = created_at or datetime(2026, 4, 1, tzinfo=UTC)
    artifact_name = f"kitaru_mem:{scope}:{key}"
    return SimpleNamespace(
        id=uuid4(),
        artifact=SimpleNamespace(name=artifact_name),
        name=artifact_name,
        version=str(version),
        created=timestamp,
        run_metadata={
            "kitaru_memory_scope_type": scope_type,
            "kitaru_memory_deleted": deleted,
        },
        data_type=SimpleNamespace(import_path=value_type_import_path),
        producer_pipeline_run_id=execution_id,
        load=MagicMock(return_value=value),
    )


def _flow_memory_scope(name: str = "demo_flow") -> _MemoryScope:
    """Return a flow-scoped memory scope for impl tests."""
    return _MemoryScope(scope=name, scope_type="flow")


def _memory_entry(
    *,
    key: str = "prefs",
    value_type: str = "dict",
    version: int = 1,
    scope: str = "demo_flow",
    scope_type: str = "flow",
    created_at: datetime | None = None,
    is_deleted: bool = False,
    artifact_id: str | None = None,
    execution_id: str | None = None,
) -> MemoryEntry:
    """Build a ``MemoryEntry`` with sensible defaults for tests."""
    return MemoryEntry(
        key=key,
        value_type=value_type,
        version=version,
        scope=scope,
        scope_type=scope_type,
        created_at=created_at or datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        is_deleted=is_deleted,
        artifact_id=artifact_id or str(uuid4()),
        execution_id=execution_id,
    )


@pytest.mark.parametrize(
    "call",
    [
        lambda: memory.set("prefs", {"theme": "dark"}),
        lambda: memory.get("prefs"),
        lambda: memory.list(),
        lambda: memory.history("prefs"),
        lambda: memory.delete("prefs"),
    ],
)
def test_memory_apis_require_configured_scope_outside_flow(
    call: Callable[[], object],
) -> None:
    with pytest.raises(
        KitaruStateError,
        match=r"requires an explicit scope.*memory\.configure\(scope=\.\.\.\)",
    ):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: memory.set("prefs", {"theme": "dark"}),
        lambda: memory.get("prefs"),
        lambda: memory.list(),
        lambda: memory.history("prefs"),
        lambda: memory.delete("prefs"),
    ],
)
def test_memory_apis_reject_checkpoint_context(call: Callable[[], object]) -> None:
    with (
        _flow_scope(name="demo_flow"),
        _checkpoint_scope(name="demo_checkpoint", checkpoint_type=None),
        pytest.raises(KitaruContextError, match=r"@checkpoint"),
    ):
        call()


def test_memory_configure_outside_flow_sets_process_default() -> None:
    memory.configure(scope="repo_seed")

    assert memory._RUNTIME_MEMORY_SCOPE_DEFAULT is not None
    assert memory._RUNTIME_MEMORY_SCOPE_DEFAULT.scope == "repo_seed"
    assert memory._RUNTIME_MEMORY_SCOPE_DEFAULT.scope_type == "namespace"


def test_memory_configure_rejects_checkpoint_context() -> None:
    with (
        _flow_scope(name="demo_flow"),
        _checkpoint_scope(name="demo_checkpoint", checkpoint_type=None),
        pytest.raises(KitaruContextError, match=r"@checkpoint"),
    ):
        memory.configure(scope="repo_seed")


@pytest.mark.parametrize("bad_key", ["", " ", "bad:key", "bad key"])
def test_memory_set_rejects_invalid_keys_before_dispatch(bad_key: str) -> None:
    with (
        _flow_scope(name="demo_flow"),
        patch("kitaru.memory._memory_set_step") as memory_set_step,
        pytest.raises(KitaruUsageError, match="Memory key"),
    ):
        memory.set(bad_key, {"theme": "dark"})

    memory_set_step.assert_not_called()


def test_memory_rejects_invalid_flow_name_as_scope() -> None:
    with (
        _flow_scope(name="bad:scope"),
        pytest.raises(KitaruUsageError, match="Memory scope"),
    ):
        memory.list()


def test_memory_configure_rejects_invalid_scope_before_dispatch() -> None:
    with (
        _flow_scope(name="demo_flow"),
        patch("kitaru.memory._memory_list_step") as memory_list_step,
        pytest.raises(KitaruUsageError, match="Memory scope"),
    ):
        memory.configure(scope="bad:scope")
        memory.list()

    memory_list_step.assert_not_called()


def test_memory_configure_requires_scope_or_scope_type() -> None:
    with pytest.raises(
        KitaruUsageError,
        match=r"requires `scope=` or `scope_type=`",
    ):
        memory.configure()


def test_memory_configure_namespace_scope_type_requires_explicit_scope() -> None:
    with pytest.raises(KitaruUsageError, match=r"requires an explicit `scope=`"):
        memory.configure(scope_type="namespace")


def test_memory_configure_rejects_invalid_scope_type() -> None:
    with pytest.raises(KitaruUsageError, match="Memory scope_type"):
        memory.configure(scope="repo_seed", scope_type="bogus")  # type: ignore[arg-type]


@pytest.mark.parametrize("scope_type", ["flow", "execution"])
def test_memory_configure_cannot_infer_flowish_scope_outside_flow(
    scope_type: str,
) -> None:
    with pytest.raises(KitaruContextError, match=r"inside a @flow"):
        memory.configure(scope_type=scope_type)  # type: ignore[arg-type]


def test_memory_set_dispatches_to_synthetic_step() -> None:
    payload = {"language": "en", "theme": "dark"}

    with (
        _flow_scope(name="research_agent"),
        patch("kitaru.memory._memory_set_step") as memory_set_step,
    ):
        result = memory.set("user_preferences", payload)

    assert result is None
    memory_set_step.assert_called_once_with(
        "research_agent",
        "flow",
        "user_preferences",
        payload,
    )


def test_memory_set_outside_flow_dispatches_to_direct_impl() -> None:
    payload = {"language": "en", "theme": "dark"}
    memory.configure(scope="repo_seed")

    with (
        patch("kitaru.memory._set_impl") as set_impl,
        patch("kitaru.memory._memory_set_step") as memory_set_step,
    ):
        result = memory.set("user_preferences", payload)

    assert result is None
    set_impl.assert_called_once_with(
        _MemoryScope(scope="repo_seed", scope_type="namespace"),
        "user_preferences",
        payload,
    )
    memory_set_step.assert_not_called()


def test_memory_get_dispatches_to_synthetic_step() -> None:
    with (
        _flow_scope(name="demo_flow"),
        patch(
            "kitaru.memory._memory_get_step",
            return_value={"theme": "dark"},
        ) as memory_get_step,
    ):
        result = memory.get("prefs", version=2)

    assert result == {"theme": "dark"}
    memory_get_step.assert_called_once_with("demo_flow", "flow", "prefs", 2)


def test_memory_get_outside_flow_dispatches_to_direct_impl() -> None:
    memory.configure(scope="repo_seed")

    with (
        patch(
            "kitaru.memory._get_impl",
            return_value={"theme": "dark"},
        ) as get_impl,
        patch("kitaru.memory._memory_get_step") as memory_get_step,
    ):
        result = memory.get("prefs", version=2)

    assert result == {"theme": "dark"}
    get_impl.assert_called_once_with(
        _MemoryScope(scope="repo_seed", scope_type="namespace"),
        "prefs",
        2,
    )
    memory_get_step.assert_not_called()


def test_memory_list_dispatches_to_synthetic_step() -> None:
    fake_entries = [_memory_entry(version=2)]

    with (
        _flow_scope(name="demo_flow"),
        patch(
            "kitaru.memory._memory_list_step",
            return_value=fake_entries,
        ) as memory_list_step,
    ):
        result = memory.list()

    assert result == fake_entries
    memory_list_step.assert_called_once_with("demo_flow", "flow")


def test_memory_list_outside_flow_dispatches_to_direct_impl() -> None:
    fake_entries = [_memory_entry(scope="repo_seed", scope_type="namespace")]
    memory.configure(scope="repo_seed")

    with (
        patch(
            "kitaru.memory._list_impl",
            return_value=fake_entries,
        ) as list_impl,
        patch("kitaru.memory._memory_list_step") as memory_list_step,
    ):
        result = memory.list()

    assert result == fake_entries
    list_impl.assert_called_once_with(
        _MemoryScope(scope="repo_seed", scope_type="namespace")
    )
    memory_list_step.assert_not_called()


def test_memory_history_dispatches_to_synthetic_step() -> None:
    fake_entries = [_memory_entry(version=3, is_deleted=True)]

    with (
        _flow_scope(name="demo_flow"),
        patch(
            "kitaru.memory._memory_history_step",
            return_value=fake_entries,
        ) as memory_history_step,
    ):
        result = memory.history("prefs")

    assert result == fake_entries
    memory_history_step.assert_called_once_with("demo_flow", "flow", "prefs")


def test_memory_history_outside_flow_dispatches_to_direct_impl() -> None:
    fake_entries = [_memory_entry(scope="repo_seed", scope_type="namespace")]
    memory.configure(scope="repo_seed")

    with (
        patch(
            "kitaru.memory._history_impl",
            return_value=fake_entries,
        ) as history_impl,
        patch("kitaru.memory._memory_history_step") as memory_history_step,
    ):
        result = memory.history("prefs")

    assert result == fake_entries
    history_impl.assert_called_once_with(
        _MemoryScope(scope="repo_seed", scope_type="namespace"),
        "prefs",
    )
    memory_history_step.assert_not_called()


def test_memory_delete_dispatches_to_synthetic_step() -> None:
    fake_entry = _memory_entry(version=3, is_deleted=True)

    with (
        _flow_scope(name="demo_flow"),
        patch(
            "kitaru.memory._memory_delete_step",
            return_value=fake_entry,
        ) as memory_delete_step,
    ):
        result = memory.delete("prefs")

    assert result == fake_entry
    memory_delete_step.assert_called_once_with("demo_flow", "flow", "prefs")


def test_memory_delete_outside_flow_dispatches_to_direct_impl() -> None:
    fake_entry = _memory_entry(
        scope="repo_seed",
        scope_type="namespace",
        version=2,
        is_deleted=True,
    )
    memory.configure(scope="repo_seed")

    with (
        patch(
            "kitaru.memory._delete_impl",
            return_value=fake_entry,
        ) as delete_impl,
        patch("kitaru.memory._memory_delete_step") as memory_delete_step,
    ):
        result = memory.delete("prefs")

    assert result == fake_entry
    delete_impl.assert_called_once_with(
        _MemoryScope(scope="repo_seed", scope_type="namespace"),
        "prefs",
    )
    memory_delete_step.assert_not_called()


def test_memory_configure_sets_namespace_scope_for_subsequent_calls() -> None:
    fake_entries = [_memory_entry(scope="my_repo", scope_type="namespace")]

    with (
        _flow_scope(name="demo_flow"),
        patch("kitaru.memory._memory_set_step") as memory_set_step,
        patch(
            "kitaru.memory._memory_get_step",
            return_value={"theme": "dark"},
        ) as memory_get_step,
        patch(
            "kitaru.memory._memory_list_step",
            return_value=fake_entries,
        ) as memory_list_step,
        patch(
            "kitaru.memory._memory_history_step",
            return_value=fake_entries,
        ) as memory_history_step,
        patch(
            "kitaru.memory._memory_delete_step",
            return_value=fake_entries[0],
        ) as memory_delete_step,
    ):
        memory.configure(scope="my_repo")
        memory.set("prefs", {"theme": "dark"})
        assert memory.get("prefs") == {"theme": "dark"}
        assert memory.list() == fake_entries
        assert memory.history("prefs") == fake_entries
        assert memory.delete("prefs") == fake_entries[0]

    memory_set_step.assert_called_once_with(
        "my_repo",
        "namespace",
        "prefs",
        {"theme": "dark"},
    )
    memory_get_step.assert_called_once_with("my_repo", "namespace", "prefs", None)
    memory_list_step.assert_called_once_with("my_repo", "namespace")
    memory_history_step.assert_called_once_with("my_repo", "namespace", "prefs")
    memory_delete_step.assert_called_once_with("my_repo", "namespace", "prefs")


def test_memory_configure_scope_type_flow_uses_current_flow_name() -> None:
    fake_entries = [_memory_entry(scope="demo_flow", scope_type="flow")]

    with (
        _flow_scope(name="demo_flow"),
        patch(
            "kitaru.memory._memory_list_step",
            return_value=fake_entries,
        ) as memory_list_step,
    ):
        memory.configure(scope_type="flow")
        result = memory.list()

    assert result == fake_entries
    memory_list_step.assert_called_once_with("demo_flow", "flow")


def test_memory_configure_scope_type_execution_uses_execution_id() -> None:
    fake_entries = [_memory_entry(scope="exec-123", scope_type="execution")]

    with (
        _flow_scope(name="demo_flow", execution_id="exec-123"),
        patch(
            "kitaru.memory._memory_list_step",
            return_value=fake_entries,
        ) as memory_list_step,
    ):
        memory.configure(scope_type="execution")
        result = memory.list()

    assert result == fake_entries
    memory_list_step.assert_called_once_with("exec-123", "execution")


def test_memory_configure_execution_scope_requires_execution_id() -> None:
    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(KitaruStateError, match="active execution ID"),
    ):
        memory.configure(scope_type="execution")


def test_memory_configure_outside_flow_seeds_later_flow_session() -> None:
    fake_entries = [_memory_entry(scope="repo_seed", scope_type="namespace")]
    memory.configure(scope="repo_seed")

    with (
        _flow_scope(name="demo_flow"),
        memory._memory_scope_session(),
        patch(
            "kitaru.memory._memory_list_step",
            return_value=fake_entries,
        ) as memory_list_step,
    ):
        result = memory.list()

    assert result == fake_entries
    memory_list_step.assert_called_once_with("repo_seed", "namespace")


def test_memory_configure_inside_flow_overrides_process_default_without_mutation() -> (
    None
):
    memory.configure(scope="repo_seed")

    with (
        _flow_scope(name="first_flow"),
        memory._memory_scope_session(),
        patch("kitaru.memory._memory_list_step", return_value=[]) as memory_list_step,
    ):
        memory.configure(scope="repo_override")
        memory.list()

    with (
        _flow_scope(name="second_flow"),
        memory._memory_scope_session(),
        patch("kitaru.memory._memory_list_step", return_value=[]) as memory_list_step_2,
    ):
        memory.list()

    memory_list_step.assert_called_once_with("repo_override", "namespace")
    memory_list_step_2.assert_called_once_with("repo_seed", "namespace")


def test_memory_outside_flow_public_roundtrip_uses_detached_artifacts(
    primed_zenml: None,
) -> None:
    del primed_zenml

    memory.configure(scope="repo_seed")

    memory.set("prefs", {"theme": "dark"})
    assert memory.get("prefs") == {"theme": "dark"}

    listed = memory.list()
    assert len(listed) == 1
    assert listed[0].key == "prefs"
    assert listed[0].scope == "repo_seed"
    assert listed[0].scope_type == "namespace"
    assert listed[0].version == 1
    assert listed[0].execution_id is None
    assert listed[0].is_deleted is False

    initial_history = memory.history("prefs")
    assert [entry.version for entry in initial_history] == [1]
    assert [entry.is_deleted for entry in initial_history] == [False]
    assert all(entry.execution_id is None for entry in initial_history)

    deleted = memory.delete("prefs")
    assert deleted is not None
    assert deleted.scope == "repo_seed"
    assert deleted.scope_type == "namespace"
    assert deleted.version == 2
    assert deleted.is_deleted is True
    assert deleted.execution_id is None

    assert memory.get("prefs") is None
    assert memory.list() == []

    final_history = memory.history("prefs")
    assert [entry.version for entry in final_history] == [2, 1]
    assert [entry.is_deleted for entry in final_history] == [True, False]
    assert all(entry.execution_id is None for entry in final_history)


def test_set_impl_persists_expected_artifact_contract() -> None:
    payload = {"language": "en", "theme": "dark"}

    with patch("kitaru.memory.save_artifact") as save_artifact_mock:
        _set_impl(_flow_memory_scope("research_agent"), "user_preferences", payload)

    save_artifact_mock.assert_called_once_with(
        data=payload,
        name="kitaru_mem:research_agent:user_preferences",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:research_agent",
            "kitaru:memory:key:user_preferences",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "flow",
            "kitaru_memory_deleted": False,
        },
    )


def test_get_impl_returns_none_when_key_does_not_exist() -> None:
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()

    with patch("kitaru.memory.Client", return_value=client_mock):
        assert _get_impl(_flow_memory_scope(), "prefs") is None


def test_get_impl_returns_latest_value() -> None:
    latest = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value={"theme": "dark"},
        created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(latest)

    with patch("kitaru.memory.Client", return_value=client_mock):
        result = _get_impl(_flow_memory_scope(), "prefs")

    assert result == {"theme": "dark"}
    latest.load.assert_called_once_with()
    call_kwargs = client_mock.list_artifact_versions.call_args.kwargs
    assert call_kwargs["artifact"] == "kitaru_mem:demo_flow:prefs"
    assert call_kwargs["sort_by"] == "desc:version_number"
    assert call_kwargs["hydrate"] is True
    assert call_kwargs["size"] == 1


def test_get_impl_returns_requested_version() -> None:
    historical = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=1,
        value={"theme": "light"},
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(historical)

    with patch("kitaru.memory.Client", return_value=client_mock):
        result = _get_impl(_flow_memory_scope(), "prefs", version=1)

    assert result == {"theme": "light"}
    call_kwargs = client_mock.list_artifact_versions.call_args.kwargs
    assert call_kwargs["artifact"] == "kitaru_mem:demo_flow:prefs"
    assert call_kwargs["version"] == 1


def test_get_impl_returns_none_when_latest_version_is_tombstone() -> None:
    tombstone = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=3,
        value=None,
        deleted=True,
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(tombstone)

    with patch("kitaru.memory.Client", return_value=client_mock):
        result = _get_impl(_flow_memory_scope(), "prefs")

    assert result is None
    tombstone.load.assert_not_called()


def test_get_impl_returns_none_for_tombstone_history_version() -> None:
    tombstone = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value=None,
        deleted=True,
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(tombstone)

    with patch("kitaru.memory.Client", return_value=client_mock):
        result = _get_impl(_flow_memory_scope(), "prefs", version=2)

    assert result is None
    tombstone.load.assert_not_called()


def test_list_impl_dedupes_versions_and_excludes_deleted_latest_keys() -> None:
    base_time = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    prefs_v1 = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=1,
        value={"theme": "light"},
        created_at=base_time,
    )
    prefs_v2 = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value={"theme": "dark"},
        created_at=base_time + timedelta(minutes=1),
    )
    notes_v2 = _memory_artifact(
        scope="demo_flow",
        key="notes",
        version=2,
        value="keep this",
        value_type_import_path="builtins.str",
        created_at=base_time + timedelta(minutes=2),
    )
    notes_v3_deleted = _memory_artifact(
        scope="demo_flow",
        key="notes",
        version=3,
        value=None,
        deleted=True,
        value_type_import_path="builtins.str",
        created_at=base_time + timedelta(minutes=3),
    )
    alpha_v1 = _memory_artifact(
        scope="demo_flow",
        key="alpha",
        version=1,
        value=[1, 2, 3],
        value_type_import_path="builtins.list",
        created_at=base_time + timedelta(minutes=4),
        execution_id=uuid4(),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(
        prefs_v1,
        notes_v3_deleted,
        alpha_v1,
        prefs_v2,
        notes_v2,
    )

    with patch("kitaru.memory.Client", return_value=client_mock):
        entries = _list_impl(_flow_memory_scope())

    assert [entry.key for entry in entries] == ["alpha", "prefs"]
    assert all(isinstance(entry, MemoryEntry) for entry in entries)
    assert entries[0].value_type == "list"
    assert entries[1].version == 2
    assert entries[1].is_deleted is False


def test_history_impl_returns_all_versions_newest_first_across_pages() -> None:
    base_time = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    version_1 = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=1,
        value={"theme": "light"},
        created_at=base_time,
    )
    version_2 = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value={"theme": "dark"},
        created_at=base_time + timedelta(minutes=1),
    )
    tombstone = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=3,
        value=None,
        deleted=True,
        created_at=base_time + timedelta(minutes=2),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.side_effect = [
        _page(tombstone, version_2, index=1, total_pages=2),
        _page(version_1, index=2, total_pages=2),
    ]

    with patch("kitaru.memory.Client", return_value=client_mock):
        entries = _history_impl(_flow_memory_scope(), "prefs")

    assert [entry.version for entry in entries] == [3, 2, 1]
    assert [entry.is_deleted for entry in entries] == [True, False, False]
    assert client_mock.list_artifact_versions.call_count == 2


def test_delete_impl_returns_none_when_key_does_not_exist() -> None:
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch("kitaru.memory.save_artifact") as save_artifact_mock,
    ):
        result = _delete_impl(_flow_memory_scope(), "prefs")

    assert result is None
    save_artifact_mock.assert_not_called()


def test_delete_impl_writes_tombstone_and_returns_entry() -> None:
    existing = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=1,
        value={"theme": "dark"},
    )
    tombstone = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value=None,
        deleted=True,
        execution_id=uuid4(),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.side_effect = [
        _page(existing),
        _page(tombstone),
    ]

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch("kitaru.memory.save_artifact") as save_artifact_mock,
    ):
        result = _delete_impl(_flow_memory_scope(), "prefs")

    save_artifact_mock.assert_called_once_with(
        data=None,
        name="kitaru_mem:demo_flow:prefs",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:demo_flow",
            "kitaru:memory:key:prefs",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "flow",
            "kitaru_memory_deleted": True,
        },
    )
    assert isinstance(result, MemoryEntry)
    assert result is not None
    assert result.version == 2
    assert result.is_deleted is True
    assert result.execution_id == str(tombstone.producer_pipeline_run_id)
    # Two list calls: existence check + tombstone re-fetch (both size=1).
    assert client_mock.list_artifact_versions.call_count == 2
    for call in client_mock.list_artifact_versions.call_args_list:
        assert call.kwargs["size"] == 1


def test_delete_impl_returns_existing_tombstone_when_key_already_deleted() -> None:
    tombstone = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value=None,
        deleted=True,
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(tombstone)

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch("kitaru.memory.save_artifact") as save_artifact_mock,
    ):
        result = _delete_impl(_flow_memory_scope(), "prefs")

    assert isinstance(result, MemoryEntry)
    assert result is not None
    assert result.version == 2
    assert result.is_deleted is True
    save_artifact_mock.assert_not_called()
