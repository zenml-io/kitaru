"""Tests for `kitaru.memory` configurable-scope memory behavior."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactType

from kitaru import memory
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.memory import (
    _COMPACTION_LOG_PREFIX,
    MemoryEntry,
    MemoryReindexResult,
    PurgeResult,
    _compact_impl,
    _compaction_log_impl,
    _delete_impl,
    _get_entry_impl,
    _get_impl,
    _history_impl,
    _list_impl,
    _MemoryScope,
    _purge_impl,
    _purge_scope_impl,
    _reindex_impl,
    _set_entry_impl,
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
    flow_id: str | None = None,
    flow_name: str | None = None,
    tags: list[object] | None = None,
) -> SimpleNamespace:
    """Build a lightweight artifact-version-like object for memory tests."""
    timestamp = created_at or datetime(2026, 4, 1, tzinfo=UTC)
    artifact_name = f"kitaru_mem:{scope_type}:{scope}:{key}"
    run_metadata = {
        "kitaru_memory_scope_type": scope_type,
        "kitaru_memory_deleted": deleted,
    }
    if flow_id is not None:
        run_metadata["kitaru_memory_flow_id"] = flow_id
    if flow_name is not None:
        run_metadata["kitaru_memory_flow_name"] = flow_name

    return SimpleNamespace(
        id=uuid4(),
        artifact=SimpleNamespace(name=artifact_name),
        name=artifact_name,
        version=str(version),
        created=timestamp,
        tags=list(tags) if tags is not None else [],
        run_metadata=run_metadata,
        data_type=SimpleNamespace(import_path=value_type_import_path),
        producer_pipeline_run_id=execution_id,
        load=MagicMock(return_value=value),
    )


def _flow_memory_scope(name: str = "demo_flow") -> _MemoryScope:
    """Return a flow-scoped memory scope for impl tests."""
    return _MemoryScope(scope=name, scope_type="flow")


def _created_artifact_response(artifact_id: UUID | None = None) -> SimpleNamespace:
    """Build the lightweight response returned by ``save_artifact``."""
    return SimpleNamespace(id=artifact_id or uuid4())


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
    flow_id: str | None = None,
    flow_name: str | None = None,
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
        flow_id=flow_id,
        flow_name=flow_name,
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
    created = _memory_artifact(
        scope="research_agent",
        key="user_preferences",
        version=1,
        value=payload,
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ) as save_artifact_mock,
    ):
        _set_impl(_flow_memory_scope("research_agent"), "user_preferences", payload)

    save_artifact_mock.assert_called_once_with(
        data=payload,
        name="kitaru_mem:flow:research_agent:user_preferences",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:research_agent",
            "kitaru:memory:key:user_preferences",
            "kitaru:memory:scope_type:flow",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "flow",
            "kitaru_memory_deleted": False,
        },
    )
    client_mock.get_artifact_version.assert_called_once_with(
        name_id_or_prefix=str(created.id),
        hydrate=True,
    )


def test_set_entry_impl_returns_created_memory_entry() -> None:
    payload = {"language": "en", "theme": "dark"}
    created = _memory_artifact(
        scope="research_agent",
        key="user_preferences",
        version=2,
        value=payload,
        execution_id=uuid4(),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ),
    ):
        entry = _set_entry_impl(
            _flow_memory_scope("research_agent"),
            "user_preferences",
            payload,
        )

    assert entry.key == "user_preferences"
    assert entry.scope == "research_agent"
    assert entry.version == 2
    assert entry.execution_id == str(created.producer_pipeline_run_id)
    assert entry.flow_id is None
    assert entry.flow_name is None


def test_set_entry_impl_execution_scope_indexes_detached_flow_context() -> None:
    payload = {"draft": True}
    flow_id = uuid4()
    created = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=1,
        value=payload,
        scope_type="execution",
        flow_id=str(flow_id),
        flow_name="__kitaru_pipeline_source_repo_memory_demo",
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created
    client_mock.get_pipeline_run.return_value = SimpleNamespace(
        pipeline=SimpleNamespace(
            id=flow_id,
            name="__kitaru_pipeline_source_repo_memory_demo",
        )
    )

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ) as save_artifact_mock,
    ):
        entry = _set_entry_impl(
            _MemoryScope(scope="exec-123", scope_type="execution"),
            "scratch",
            payload,
        )

    save_artifact_mock.assert_called_once_with(
        data=payload,
        name="kitaru_mem:execution:exec-123:scratch",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-123",
            "kitaru:memory:key:scratch",
            "kitaru:memory:scope_type:execution",
            f"kitaru:memory:flow_id:{flow_id}",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "execution",
            "kitaru_memory_deleted": False,
            "kitaru_memory_flow_id": str(flow_id),
            "kitaru_memory_flow_name": "repo_memory_demo",
        },
    )
    client_mock.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix="exec-123",
        allow_name_prefix_match=False,
        hydrate=True,
        project=None,
    )
    assert entry.flow_id == str(flow_id)
    assert entry.flow_name == "repo_memory_demo"
    assert entry.execution_id is None


def test_execution_scope_write_stays_non_breaking_when_flow_lookup_fails() -> None:
    payload = {"draft": True}
    created = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=1,
        value=payload,
        scope_type="execution",
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created
    client_mock.get_pipeline_run.side_effect = RuntimeError("run lookup failed")

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ) as save_artifact_mock,
        patch("kitaru.memory.logger.warning") as warning_mock,
    ):
        entry = _set_entry_impl(
            _MemoryScope(scope="exec-123", scope_type="execution"),
            "scratch",
            payload,
        )

    save_artifact_mock.assert_called_once_with(
        data=payload,
        name="kitaru_mem:execution:exec-123:scratch",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-123",
            "kitaru:memory:key:scratch",
            "kitaru:memory:scope_type:execution",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "execution",
            "kitaru_memory_deleted": False,
        },
    )
    warning_mock.assert_called_once()
    assert entry.flow_id is None
    assert entry.flow_name is None


def test_execution_scope_prefers_active_step_context_for_current_execution() -> None:
    payload = {"draft": True}
    flow_id = uuid4()
    created = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=1,
        value=payload,
        scope_type="execution",
        flow_id=str(flow_id),
        flow_name="__kitaru_pipeline_source_demo_flow",
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created
    step_context = SimpleNamespace(
        pipeline_run=SimpleNamespace(
            pipeline=SimpleNamespace(
                id=flow_id,
                name="__kitaru_pipeline_source_demo_flow",
            )
        )
    )

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ) as save_artifact_mock,
        patch("kitaru.memory._get_current_execution_id", return_value="exec-123"),
        patch("kitaru.memory.StepContext.get", return_value=step_context),
    ):
        entry = _set_entry_impl(
            _MemoryScope(scope="exec-123", scope_type="execution"),
            "scratch",
            payload,
        )

    client_mock.get_pipeline_run.assert_not_called()
    save_artifact_mock.assert_called_once_with(
        data=payload,
        name="kitaru_mem:execution:exec-123:scratch",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-123",
            "kitaru:memory:key:scratch",
            "kitaru:memory:scope_type:execution",
            f"kitaru:memory:flow_id:{flow_id}",
        ],
        user_metadata={
            "kitaru_memory_scope_type": "execution",
            "kitaru_memory_deleted": False,
            "kitaru_memory_flow_id": str(flow_id),
            "kitaru_memory_flow_name": "demo_flow",
        },
    )
    assert entry.flow_id == str(flow_id)
    assert entry.flow_name == "demo_flow"


def test_reindex_impl_dry_run_identifies_missing_tags_without_mutating() -> None:
    flow_artifact = _memory_artifact(
        scope="repo_memory_demo",
        key="flow_notes",
        version=3,
        value={"summary": "hello"},
        scope_type="flow",
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:repo_memory_demo",
            "kitaru:memory:key:flow_notes",
        ],
    )
    execution_artifact = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=2,
        value={"draft": True},
        scope_type="execution",
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-123",
            "kitaru:memory:key:scratch",
        ],
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(
        flow_artifact,
        execution_artifact,
    )
    client_mock.get_pipeline_run.return_value = SimpleNamespace(
        pipeline=SimpleNamespace(
            id="flow-456",
            name="__kitaru_pipeline_source_repo_memory_demo",
        )
    )

    result = _reindex_impl(client_factory=lambda: client_mock)

    assert result == MemoryReindexResult(
        dry_run=True,
        versions_scanned=2,
        execution_scope_versions_scanned=1,
        already_indexed=0,
        versions_needing_updates=2,
        versions_updated=0,
        scope_type_tags_identified=2,
        flow_tags_identified=1,
        scope_type_tags_added=0,
        flow_tags_added=0,
        issues_count=0,
        issue_samples=[],
    )
    client_mock.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix="exec-123",
        allow_name_prefix_match=False,
        hydrate=True,
        project=None,
    )
    client_mock.update_artifact_version.assert_not_called()


def test_reindex_impl_apply_updates_missing_tags_and_prefers_producer_run() -> None:
    producer_run_id = uuid4()
    missing_tags_artifact = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=2,
        value={"draft": True},
        scope_type="execution",
        execution_id=producer_run_id,
        tags=[
            SimpleNamespace(name="kitaru:memory"),
            SimpleNamespace(name="kitaru:memory:scope:exec-123"),
            SimpleNamespace(name="kitaru:memory:key:scratch"),
        ],
    )
    already_indexed_artifact = _memory_artifact(
        scope="exec-999",
        key="notes",
        version=1,
        value={"done": True},
        scope_type="execution",
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-999",
            "kitaru:memory:key:notes",
            "kitaru:memory:scope_type:execution",
            "kitaru:memory:flow_id:flow-existing",
        ],
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(
        missing_tags_artifact,
        already_indexed_artifact,
    )
    client_mock.get_pipeline_run.return_value = SimpleNamespace(
        pipeline=SimpleNamespace(
            id="flow-456",
            name="__kitaru_pipeline_source_repo_memory_demo",
        )
    )

    result = _reindex_impl(
        dry_run=False,
        client_factory=lambda: client_mock,
        project="project-override",
    )

    assert result == MemoryReindexResult(
        dry_run=False,
        versions_scanned=2,
        execution_scope_versions_scanned=2,
        already_indexed=1,
        versions_needing_updates=1,
        versions_updated=1,
        scope_type_tags_identified=1,
        flow_tags_identified=1,
        scope_type_tags_added=1,
        flow_tags_added=1,
        issues_count=0,
        issue_samples=[],
    )
    client_mock.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix=str(producer_run_id),
        allow_name_prefix_match=False,
        hydrate=True,
        project="project-override",
    )
    client_mock.update_artifact_version.assert_called_once_with(
        name_id_or_prefix=str(missing_tags_artifact.id),
        add_tags=[
            "kitaru:memory:scope_type:execution",
            "kitaru:memory:flow_id:flow-456",
        ],
        project="project-override",
    )


def test_reindex_impl_records_issue_but_still_updates_scope_type_tag() -> None:
    execution_artifact = _memory_artifact(
        scope="exec-123",
        key="scratch",
        version=2,
        value={"draft": True},
        scope_type="execution",
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:exec-123",
            "kitaru:memory:key:scratch",
        ],
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(execution_artifact)
    client_mock.get_pipeline_run.side_effect = RuntimeError("run lookup failed")

    result = _reindex_impl(
        dry_run=False,
        client_factory=lambda: client_mock,
    )

    assert result.dry_run is False
    assert result.versions_scanned == 1
    assert result.execution_scope_versions_scanned == 1
    assert result.already_indexed == 0
    assert result.versions_needing_updates == 1
    assert result.versions_updated == 1
    assert result.scope_type_tags_identified == 1
    assert result.flow_tags_identified == 0
    assert result.scope_type_tags_added == 1
    assert result.flow_tags_added == 0
    assert result.issues_count == 1
    assert len(result.issue_samples) == 1
    assert "execution scope 'exec-123'" in result.issue_samples[0].reason
    client_mock.update_artifact_version.assert_called_once_with(
        name_id_or_prefix=str(execution_artifact.id),
        add_tags=["kitaru:memory:scope_type:execution"],
    )


def test_set_entry_impl_temporarily_switches_project_for_write() -> None:
    payload = {"language": "en"}
    created = _memory_artifact(
        scope="research_agent",
        key="user_preferences",
        version=1,
        value=payload,
    )
    default_project_id = uuid4()
    client_mock = MagicMock()
    client_mock.active_project = SimpleNamespace(
        id=default_project_id,
        name="default",
    )
    client_mock.list_artifact_versions.return_value = _page()
    client_mock.get_artifact_version.return_value = created

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(created.id),
        ),
    ):
        _set_entry_impl(
            _flow_memory_scope("research_agent"),
            "user_preferences",
            payload,
            project="project-override",
        )

    assert client_mock.set_active_project.call_args_list == [
        call("project-override"),
        call(str(default_project_id)),
    ]


def test_set_entry_impl_rejects_scope_type_mismatch_with_existing_history() -> None:
    existing = _memory_artifact(
        scope="shared_scope",
        key="prefs",
        version=1,
        value={"theme": "dark"},
        scope_type="flow",
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(existing)

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch("kitaru.memory.save_artifact") as save_artifact_mock,
        pytest.raises(KitaruUsageError, match="scope_type mismatch"),
    ):
        _set_entry_impl(
            _MemoryScope(scope="shared_scope", scope_type="namespace"),
            "prefs",
            {"theme": "light"},
        )

    save_artifact_mock.assert_not_called()


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
    assert call_kwargs["artifact"] == "kitaru_mem:flow:demo_flow:prefs"
    assert call_kwargs["sort_by"] == "desc:version_number"
    assert call_kwargs["hydrate"] is True
    assert call_kwargs["size"] == 1


def test_get_entry_impl_returns_latest_memory_entry() -> None:
    latest = _memory_artifact(
        scope="demo_flow",
        key="prefs",
        version=2,
        value={"theme": "dark"},
        created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        execution_id=uuid4(),
    )
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(latest)

    with patch("kitaru.memory.Client", return_value=client_mock):
        entry = _get_entry_impl(_flow_memory_scope(), "prefs")

    assert entry is not None
    assert entry.key == "prefs"
    assert entry.version == 2
    assert entry.execution_id == str(latest.producer_pipeline_run_id)


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
    assert call_kwargs["artifact"] == "kitaru_mem:flow:demo_flow:prefs"
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


def test_list_impl_filters_by_prefix_after_deduping() -> None:
    client_mock = MagicMock()
    client_mock.list_artifact_versions.return_value = _page(
        _memory_artifact(scope="demo_flow", key="repo_alpha", version=2, value=1),
        _memory_artifact(scope="demo_flow", key="repo_beta", version=1, value=2),
        _memory_artifact(scope="demo_flow", key="notes", version=3, value="x"),
    )

    with patch("kitaru.memory.Client", return_value=client_mock):
        entries = _list_impl(_flow_memory_scope(), prefix="repo_")

    assert [entry.key for entry in entries] == ["repo_alpha", "repo_beta"]


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
    client_mock.list_artifact_versions.return_value = _page(existing)
    client_mock.get_artifact_version.return_value = tombstone

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(tombstone.id),
        ) as save_artifact_mock,
    ):
        result = _delete_impl(_flow_memory_scope(), "prefs")

    save_artifact_mock.assert_called_once_with(
        data=None,
        name="kitaru_mem:flow:demo_flow:prefs",
        artifact_type=ArtifactType.DATA,
        tags=[
            "kitaru:memory",
            "kitaru:memory:scope:demo_flow",
            "kitaru:memory:key:prefs",
            "kitaru:memory:scope_type:flow",
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
    client_mock.get_artifact_version.assert_called_once_with(
        name_id_or_prefix=str(tombstone.id),
        hydrate=True,
    )
    assert client_mock.list_artifact_versions.call_count == 1


def test_delete_impl_temporarily_switches_project_for_tombstone_write() -> None:
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
    )
    default_project_id = uuid4()
    client_mock = MagicMock()
    client_mock.active_project = SimpleNamespace(
        id=default_project_id,
        name="default",
    )
    client_mock.list_artifact_versions.return_value = _page(existing)
    client_mock.get_artifact_version.return_value = tombstone

    with (
        patch("kitaru.memory.Client", return_value=client_mock),
        patch(
            "kitaru.memory.save_artifact",
            return_value=_created_artifact_response(tombstone.id),
        ),
    ):
        _delete_impl(
            _flow_memory_scope(),
            "prefs",
            project="project-override",
        )

    assert client_mock.set_active_project.call_args_list == [
        call("project-override"),
        call(str(default_project_id)),
    ]


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


# ---------------------------------------------------------------------------
# Compaction implementation
# ---------------------------------------------------------------------------


class TestCompactImpl:
    def test_single_key_compact_defaults_to_current_value(self) -> None:
        latest = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key="prefs",
            version=3,
            value="latest summary candidate",
        )
        new_entry = _memory_entry(
            key="prefs", scope="s", scope_type="namespace", version=4
        )
        client_mock = MagicMock()

        def client_factory() -> MagicMock:
            return client_mock

        with (
            patch(
                "kitaru.memory._fetch_memory_artifact",
                return_value=latest,
            ) as fetch_artifact,
            patch(
                "kitaru.memory._paginate_artifact_versions",
                side_effect=AssertionError("history path should not run"),
            ),
            patch(
                "kitaru.memory._set_entry_impl",
                return_value=new_entry,
            ) as set_entry_impl,
            patch("kitaru.memory._write_compaction_record") as write_record,
            patch(
                "kitaru.llm.resolve_model_selection",
                return_value=SimpleNamespace(resolved_model="resolved-model"),
            ),
            patch(
                "kitaru.llm._normalize_messages",
                return_value=[{"role": "user", "content": "prompt"}],
            ) as normalize_messages,
            patch(
                "kitaru.llm._dispatch_provider_call",
                return_value=SimpleNamespace(response_text="Compacted latest value"),
            ),
        ):
            result = _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                key="prefs",
                client_factory=client_factory,
            )

        assert result.sources_read == 1
        assert result.entry == new_entry
        assert result.compaction_record.source_mode == "current"
        assert result.compaction_record.source_versions == [3]
        fetch_artifact.assert_called_once()
        normalize_messages.assert_called_once()
        prompt = normalize_messages.call_args.args[0]
        assert "latest summary candidate" in prompt
        assert "version 3" in prompt
        set_entry_impl.assert_called_once_with(
            _MemoryScope(scope="s", scope_type="namespace"),
            "prefs",
            "Compacted latest value",
            client_factory=client_factory,
            project=None,
        )
        record = write_record.call_args.args[1]
        assert record.source_mode == "current"
        assert record.target_key == "prefs"

    def test_single_key_history_mode_reads_all_non_deleted_versions(self) -> None:
        newest = _memory_artifact(scope="s", scope_type="namespace", key="prefs", version=3, value="latest")
        middle = _memory_artifact(scope="s", scope_type="namespace", key="prefs", version=2, value="middle")
        deleted = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key="prefs",
            version=1,
            value=None,
            deleted=True,
        )
        new_entry = _memory_entry(
            key="prefs", scope="s", scope_type="namespace", version=4
        )
        client_mock = MagicMock()

        with (
            patch(
                "kitaru.memory._fetch_memory_artifact",
                side_effect=AssertionError("current-value path should not run"),
            ),
            patch(
                "kitaru.memory._paginate_artifact_versions",
                return_value=[newest, middle, deleted],
            ),
            patch(
                "kitaru.memory._set_entry_impl",
                return_value=new_entry,
            ),
            patch("kitaru.memory._write_compaction_record") as write_record,
            patch(
                "kitaru.llm.resolve_model_selection",
                return_value=SimpleNamespace(resolved_model="resolved-model"),
            ),
            patch(
                "kitaru.llm._normalize_messages",
                return_value=[{"role": "user", "content": "prompt"}],
            ) as normalize_messages,
            patch(
                "kitaru.llm._dispatch_provider_call",
                return_value=SimpleNamespace(response_text="History summary"),
            ),
        ):
            result = _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                key="prefs",
                source_mode="history",
                client_factory=lambda: client_mock,
            )

        assert result.sources_read == 2
        assert result.compaction_record.source_mode == "history"
        assert result.compaction_record.source_versions == [3, 2]
        prompt = normalize_messages.call_args.args[0]
        assert "latest" in prompt
        assert "middle" in prompt
        assert "version 1" not in prompt
        record = write_record.call_args.args[1]
        assert record.source_mode == "history"

    def test_multi_key_compact_keeps_current_value_behavior(self) -> None:
        runner = _memory_artifact(scope="s", scope_type="namespace", key="runner", version=2, value="just test")
        python = _memory_artifact(scope="s", scope_type="namespace", key="python", version=5, value="uv run")
        tombstone = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key="obsolete",
            version=1,
            value=None,
            deleted=True,
        )
        new_entry = _memory_entry(
            key="summary",
            scope="s",
            scope_type="namespace",
            version=6,
        )
        fetch_side_effect = [runner, python, tombstone]

        with (
            patch(
                "kitaru.memory._fetch_memory_artifact",
                side_effect=fetch_side_effect,
            ) as fetch_artifact,
            patch(
                "kitaru.memory._paginate_artifact_versions",
                side_effect=AssertionError("history path should not run"),
            ),
            patch(
                "kitaru.memory._set_entry_impl",
                return_value=new_entry,
            ),
            patch("kitaru.memory._write_compaction_record") as write_record,
            patch(
                "kitaru.llm.resolve_model_selection",
                return_value=SimpleNamespace(resolved_model="resolved-model"),
            ),
            patch(
                "kitaru.llm._normalize_messages",
                return_value=[{"role": "user", "content": "prompt"}],
            ) as normalize_messages,
            patch(
                "kitaru.llm._dispatch_provider_call",
                return_value=SimpleNamespace(response_text="Merged summary"),
            ),
        ):
            result = _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                keys=["runner", "python", "obsolete"],
                target_key="summary",
                client_factory=lambda: MagicMock(),
            )

        assert result.sources_read == 2
        assert fetch_artifact.call_count == 3
        assert result.compaction_record.source_mode == "current"
        prompt = normalize_messages.call_args.args[0]
        assert "just test" in prompt
        assert "uv run" in prompt
        assert "obsolete" not in prompt
        record = write_record.call_args.args[1]
        assert record.source_mode == "current"
        assert record.target_key == "summary"

    def test_compact_rejects_history_mode_for_multi_key(self) -> None:
        with pytest.raises(KitaruUsageError, match="single-key mode"):
            _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                keys=["runner"],
                target_key="summary",
                source_mode="history",
                client_factory=lambda: MagicMock(),
            )

    def test_single_key_current_mode_rejects_missing_key(self) -> None:
        with (
            patch("kitaru.memory._fetch_memory_artifact", return_value=None),
            pytest.raises(KitaruUsageError, match="found no current value"),
        ):
            _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                key="prefs",
                client_factory=lambda: MagicMock(),
            )

    def test_single_key_current_mode_rejects_tombstoned_key(self) -> None:
        tombstone = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key="prefs",
            version=4,
            value=None,
            deleted=True,
        )
        with (
            patch("kitaru.memory._fetch_memory_artifact", return_value=tombstone),
            pytest.raises(KitaruUsageError, match="current value is deleted"),
        ):
            _compact_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                key="prefs",
                client_factory=lambda: MagicMock(),
            )


# ---------------------------------------------------------------------------
# Reserved _compaction/ prefix
# ---------------------------------------------------------------------------


def test_compaction_prefix_rejected_in_normal_set() -> None:
    with pytest.raises(
        KitaruUsageError,
        match=r"reserved for compaction audit logs",
    ):
        memory.configure(scope="test", scope_type="namespace")
        memory.set(f"{_COMPACTION_LOG_PREFIX}test", "value")


def test_validate_identifier_rejects_compaction_prefix() -> None:
    from kitaru.memory import _validate_memory_identifier

    with pytest.raises(KitaruUsageError, match=r"reserved"):
        _validate_memory_identifier(f"{_COMPACTION_LOG_PREFIX}scope", kind="key")


def test_validate_identifier_allows_compaction_prefix_when_flag_set() -> None:
    from kitaru.memory import _validate_memory_identifier

    result = _validate_memory_identifier(
        f"{_COMPACTION_LOG_PREFIX}scope",
        kind="key",
        _allow_compaction_prefix=True,
    )
    assert result == f"{_COMPACTION_LOG_PREFIX}scope"


# ---------------------------------------------------------------------------
# Purge implementation
# ---------------------------------------------------------------------------


class TestPurgeImpl:
    def test_purge_deletes_old_versions_keeping_newest(self) -> None:
        artifacts = [
            _memory_artifact(scope="s", scope_type="namespace", key="k", version=i, value=f"v{i}")
            for i in range(1, 6)
        ]
        client_mock = MagicMock()
        client_mock.list_artifact_versions.side_effect = [
            _page(*artifacts),
            _page(*artifacts[:3]),
        ]

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch(
                "kitaru.memory.save_artifact", return_value=_created_artifact_response()
            ),
        ):
            client_mock.get_artifact_version.return_value = _memory_artifact(
                scope="s",
                scope_type="namespace",
                key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
                version=1,
                value={},
            )
            result = _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=2,
            )

        assert isinstance(result, PurgeResult)
        assert result.versions_deleted == 3
        assert result.keys_affected == 1
        assert result.scope == "s"
        assert result.scope_type == "namespace"
        client_mock.delete_artifact_version.assert_not_called()
        assert client_mock.zen_store.delete_artifact_version.call_args_list == [
            call(artifacts[2].id),
            call(artifacts[1].id),
            call(artifacts[0].id),
        ]
        assert client_mock.list_artifact_versions.call_count == 2
        assert (
            client_mock.list_artifact_versions.call_args_list[0].kwargs["artifact"]
            == "kitaru_mem:namespace:s:k"
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[0].kwargs["hydrate"]
            is True
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["artifact"]
            == "kitaru_mem:namespace:s:k"
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["only_unused"]
            is True
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["hydrate"]
            is False
        )

    def test_purge_with_keep_none_deletes_all(self) -> None:
        artifacts = [
            _memory_artifact(scope="s", scope_type="namespace", key="k", version=i, value=f"v{i}")
            for i in range(1, 4)
        ]
        client_mock = MagicMock()
        client_mock.list_artifact_versions.side_effect = [
            _page(*artifacts),
            _page(*artifacts),
        ]

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch(
                "kitaru.memory.save_artifact", return_value=_created_artifact_response()
            ),
        ):
            client_mock.get_artifact_version.return_value = _memory_artifact(
                scope="s",
                scope_type="namespace",
                key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
                version=1,
                value={},
            )
            result = _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=None,
            )

        assert result.versions_deleted == 3
        client_mock.delete_artifact_version.assert_not_called()
        assert client_mock.zen_store.delete_artifact_version.call_args_list == [
            call(artifacts[2].id),
            call(artifacts[1].id),
            call(artifacts[0].id),
        ]

    def test_purge_no_versions_returns_zero(self) -> None:
        client_mock = MagicMock()
        client_mock.list_artifact_versions.return_value = _page()

        with patch("kitaru.memory.Client", return_value=client_mock):
            result = _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=2,
            )

        assert result.versions_deleted == 0
        assert result.keys_affected == 0
        client_mock.delete_artifact_version.assert_not_called()
        client_mock.zen_store.delete_artifact_version.assert_not_called()
        assert client_mock.list_artifact_versions.call_count == 1

    def test_purge_aborts_before_delete_when_preflight_blocks_versions(self) -> None:
        artifacts = [
            _memory_artifact(scope="s", scope_type="namespace", key="k", version=i, value=f"v{i}")
            for i in range(1, 4)
        ]
        client_mock = MagicMock()
        client_mock.list_artifact_versions.side_effect = [
            _page(*artifacts),
            _page(artifacts[0]),
        ]

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch("kitaru.memory.save_artifact") as save_artifact_mock,
            pytest.raises(KitaruBackendError, match="not unused"),
        ):
            _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=1,
            )

        client_mock.zen_store.delete_artifact_version.assert_not_called()
        client_mock.delete_artifact_version.assert_not_called()
        save_artifact_mock.assert_not_called()

    def test_purge_passes_project_to_preflight_queries(self) -> None:
        artifacts = [
            _memory_artifact(scope="s", scope_type="namespace", key="k", version=i, value=f"v{i}")
            for i in range(1, 3)
        ]
        client_mock = MagicMock()
        client_mock.list_artifact_versions.side_effect = [
            _page(*artifacts),
            _page(artifacts[0]),
        ]

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch(
                "kitaru.memory.save_artifact", return_value=_created_artifact_response()
            ),
        ):
            client_mock.get_artifact_version.return_value = _memory_artifact(
                scope="s",
                scope_type="namespace",
                key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
                version=1,
                value={},
            )
            _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=1,
                project="project-override",
            )

        assert (
            client_mock.list_artifact_versions.call_args_list[0].kwargs["project"]
            == "project-override"
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["project"]
            == "project-override"
        )

    def test_purge_rejects_negative_keep(self) -> None:
        with pytest.raises(KitaruUsageError, match=r"keep.*>= 0"):
            _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=-1,
            )

    def test_purge_writes_record_with_null_source_mode(self) -> None:
        artifacts = [
            _memory_artifact(scope="s", scope_type="namespace", key="k", version=i, value=f"v{i}")
            for i in range(1, 3)
        ]

        with (
            patch(
                "kitaru.memory._paginate_artifact_versions",
                return_value=artifacts,
            ),
            patch(
                "kitaru.memory._delete_preflighted_memory_versions",
                return_value=1,
            ),
            patch("kitaru.memory._write_compaction_record") as write_record,
        ):
            _purge_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                "k",
                keep=1,
                client_factory=lambda: MagicMock(),
            )

        record = write_record.call_args.args[1]
        assert record.operation == "purge"
        assert record.source_mode is None


class TestPurgeScopeImpl:
    def test_purge_scope_deletes_across_keys(self) -> None:
        a1 = _memory_artifact(scope="s", scope_type="namespace", key="k1", version=1, value="v1")
        a2 = _memory_artifact(scope="s", scope_type="namespace", key="k1", version=2, value="v2")
        a3 = _memory_artifact(scope="s", scope_type="namespace", key="k2", version=1, value="v3")

        client_mock = MagicMock()
        client_mock.list_artifact_versions.side_effect = [
            _page(a1, a2, a3),
            _page(a1),
        ]

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch(
                "kitaru.memory.save_artifact", return_value=_created_artifact_response()
            ),
        ):
            client_mock.get_artifact_version.return_value = _memory_artifact(
                scope="s",
                scope_type="namespace",
                key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
                version=1,
                value={},
            )
            result = _purge_scope_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                keep=1,
            )

        assert result.versions_deleted == 1
        assert result.keys_affected == 1
        client_mock.delete_artifact_version.assert_not_called()
        assert client_mock.zen_store.delete_artifact_version.call_args_list == [
            call(a1.id)
        ]
        assert client_mock.list_artifact_versions.call_count == 2
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["artifact"]
            == "kitaru_mem:namespace:s:k1"
        )
        assert (
            client_mock.list_artifact_versions.call_args_list[1].kwargs["only_unused"]
            is True
        )

    def test_purge_scope_with_include_deleted(self) -> None:
        active = _memory_artifact(scope="s", scope_type="namespace", key="k1", version=1, value="v1")
        tombstone = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key="k2",
            version=1,
            value=None,
            deleted=True,
        )

        client_mock = MagicMock()

        def _list_side_effect(*_args: Any, **kwargs: Any) -> SimpleNamespace:
            if kwargs.get("only_unused"):
                if kwargs["artifact"] == "kitaru_mem:namespace:s:k1":
                    return _page(active)
                if kwargs["artifact"] == "kitaru_mem:namespace:s:k2":
                    return _page(tombstone)
                raise AssertionError(f"Unexpected artifact query: {kwargs!r}")
            return _page(active, tombstone)

        client_mock.list_artifact_versions.side_effect = _list_side_effect

        with (
            patch("kitaru.memory.Client", return_value=client_mock),
            patch(
                "kitaru.memory.save_artifact", return_value=_created_artifact_response()
            ),
        ):
            client_mock.get_artifact_version.return_value = _memory_artifact(
                scope="s",
                scope_type="namespace",
                key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
                version=1,
                value={},
            )
            result = _purge_scope_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
                keep=0,
                include_deleted=True,
            )

        assert result.versions_deleted == 2
        assert result.keys_affected == 2
        client_mock.delete_artifact_version.assert_not_called()
        zen_delete = client_mock.zen_store.delete_artifact_version
        deleted_ids = {call_args.args[0] for call_args in zen_delete.call_args_list}
        assert deleted_ids == {active.id, tombstone.id}


# ---------------------------------------------------------------------------
# Compaction log
# ---------------------------------------------------------------------------


class TestCompactionLog:
    def test_compaction_log_returns_records(self) -> None:
        record_data = {
            "operation": "purge",
            "scope": "s",
            "scope_type": "namespace",
            "timestamp": datetime(2026, 4, 1, tzinfo=UTC).isoformat(),
            "source_keys": ["k1"],
            "source_versions": [1, 2],
            "target_key": None,
            "target_version": None,
            "instruction": None,
            "model": None,
            "keys_affected": 1,
            "versions_deleted": 2,
            "keep": 1,
        }
        artifact = _memory_artifact(
            scope="s",
            scope_type="namespace",
            key=f"{_COMPACTION_LOG_PREFIX}namespace/s",
            version=1,
            value=record_data,
        )
        client_mock = MagicMock()
        client_mock.list_artifact_versions.return_value = _page(artifact)

        with patch("kitaru.memory.Client", return_value=client_mock):
            records = _compaction_log_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
            )

        assert len(records) == 1
        assert records[0].operation == "purge"
        assert records[0].versions_deleted == 2
        assert records[0].scope_type == "namespace"
        assert records[0].source_mode is None

    def test_compaction_log_empty_scope(self) -> None:
        client_mock = MagicMock()
        client_mock.list_artifact_versions.return_value = _page()

        with patch("kitaru.memory.Client", return_value=client_mock):
            records = _compaction_log_impl(
                _MemoryScope(scope="s", scope_type="namespace"),
            )

        assert records == []
