"""Tests for Project-backed Agent metadata and reconciliation."""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, Mock, patch

import pytest

from kitaru._config._agents import (
    _agent_info_from_project_model,
    _AgentExecutable,
    _AgentMetadata,
    _AgentMetadataEnvelope,
    _AgentVersionManifest,
    _parse_agent_metadata,
    _reconcile_active_project_agent_metadata,
    _reconcile_project_agent_metadata,
    create_agent,
    current_agent,
    delete_agent,
    get_agent,
    list_agents,
    use_agent,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruMetadataConflictError,
    KitaruMetadataReconciliationError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.inspection import serialize_agent, serialize_agent_version


def _manifest(
    version_id: str,
    *,
    fingerprint: str | None = None,
    registered_at: str = "2026-07-17T08:30:00Z",
) -> _AgentVersionManifest:
    return _AgentVersionManifest(
        schema_version=1,
        agent_version_id=version_id,
        pipeline_id=version_id,
        pipeline_name=f"support-agent--{version_id}",
        fingerprint=fingerprint or f"sha256:{version_id}",
        git_sha="7f192aa456789",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash=f"sha256:config-{version_id}",
        worldview_hash=f"sha256:worldview-{version_id}",
        entrypoint="evals.register:kagent",
        registered_at=registered_at,
        source="registration",
    )


def _envelope(
    *,
    project_id: str = "project-id",
    agent_name: str = "support-agent",
    versions: dict[str, _AgentVersionManifest] | None = None,
    aliases: dict[str, str] | None = None,
    order: list[str] | None = None,
    default_version_id: str | None = None,
    **extra: Any,
) -> _AgentMetadataEnvelope:
    return _AgentMetadataEnvelope(
        schema_version=1,
        agent=_AgentMetadata(
            agent_id=project_id,
            name=agent_name,
            default_agent_version_id=default_version_id,
            default_executable=(
                _AgentExecutable(
                    kind="entrypoint",
                    entrypoint="evals.register:kagent",
                    repo_root_marker=".kitaru",
                )
                if default_version_id is not None
                else None
            ),
        ),
        agent_version_order=order,
        agent_version_aliases=aliases or {},
        agent_versions=versions or {},
        **extra,
    )


def _project_model(
    *,
    project_id: str = "project-id",
    project_name: str = "support-agent",
    metadata: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=project_id,
        name=project_name,
        display_name="Project display name",
        description="Support automation",
        project_metadata=deepcopy(metadata or {}),
    )


def _stored_metadata(
    envelope: _AgentMetadataEnvelope,
    *,
    foreign: dict[str, Any] | None = None,
    kitaru_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    namespace = envelope.model_dump(mode="json")
    namespace.update(deepcopy(kitaru_extra or {}))
    return {
        **deepcopy(foreign or {}),
        "kitaru": namespace,
    }


class _StatefulProjectClient:
    """Stateful whole-dictionary Project metadata replacement fake."""

    def __init__(self, metadata: dict[str, Any] | None = None) -> None:
        self.metadata: dict[str, Any] = deepcopy(metadata or {})
        self.get_calls = 0
        self.update_calls: list[dict[str, Any]] = []
        self.discard_updates = False
        self.drop_first_verification = False
        self._dropped = False
        self.zen_store = Mock()

    @property
    def active_project(self) -> SimpleNamespace:
        return _project_model(metadata=self.metadata)

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> SimpleNamespace:
        assert selector == "project-id"
        assert allow_name_prefix_match is False
        assert hydrate is True
        self.get_calls += 1
        if (
            self.drop_first_verification
            and len(self.update_calls) == 1
            and not self._dropped
        ):
            self._dropped = True
            self.metadata = {"other_product": {"revision": 2}}
        return _project_model(metadata=self.metadata)

    def update_project(
        self,
        selector: str,
        *,
        project_metadata: dict[str, Any],
    ) -> SimpleNamespace:
        assert selector == "project-id"
        payload = deepcopy(project_metadata)
        self.update_calls.append(payload)
        if not self.discard_updates:
            self.metadata = payload
        return _project_model(metadata=self.metadata)


class _RecoverableAgentProjectClient(_StatefulProjectClient):
    """Fake with an inactive request-matching Project from a failed first call."""

    def __init__(self, *, description: str = "Support automation") -> None:
        super().__init__()
        self.description = description
        self.activated = False
        self.get_selectors: list[str] = []

    def _agent_project(self) -> SimpleNamespace:
        return SimpleNamespace(
            id="project-id",
            name="support-agent",
            display_name="Support Agent",
            description=self.description,
            project_metadata=deepcopy(self.metadata),
        )

    @property
    def active_project(self) -> SimpleNamespace:
        if self.activated:
            return self._agent_project()
        return SimpleNamespace(
            id="production-id",
            name="production",
            display_name=None,
            description=None,
            project_metadata={},
        )

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> SimpleNamespace:
        assert allow_name_prefix_match is False
        assert hydrate is True
        assert selector in {"support-agent", "project-id"}
        self.get_selectors.append(selector)
        return self._agent_project()


def _replace_with(desired: _AgentMetadataEnvelope):
    def mutation(
        _current: _AgentMetadataEnvelope | None,
    ) -> _AgentMetadataEnvelope:
        return desired.model_copy(deep=True)

    return mutation


def test_missing_namespace_is_an_uninitialized_agent() -> None:
    project = _project_model(metadata={"other_product": {"preserve_me": True}})

    assert (
        _agent_info_from_project_model(project, active_project_id="project-id") is None
    )


def test_agent_metadata_name_parses_and_serializes_exactly() -> None:
    envelope = _envelope(agent_name="logical-agent")

    serialized = envelope.model_dump(mode="json")
    parsed = _parse_agent_metadata(
        "project-id",
        {"kitaru": serialized},
    )

    assert serialized["agent"] == {
        "agent_id": "project-id",
        "name": "logical-agent",
        "default_agent_version_id": None,
        "default_executable": None,
    }
    assert parsed == envelope
    assert parsed is not None
    assert parsed.agent.name == "logical-agent"


def test_local_project_projection_uses_logical_agent_display_identity() -> None:
    agent = _agent_info_from_project_model(
        SimpleNamespace(
            id="project-id",
            name="default",
            display_name="Default Project",
            description=None,
            project_metadata=_stored_metadata(_envelope(agent_name="registered-agent")),
        ),
        active_project_id="project-id",
    )

    assert agent is not None
    assert agent.agent_id == "project-id"
    assert agent.name == "registered-agent"
    assert agent.display_name == "registered-agent"


def test_valid_metadata_builds_project_backed_agent_projection() -> None:
    older = _manifest("pipeline-old", registered_at="2026-07-16T10:00:00Z")
    newer = _manifest("pipeline-new", registered_at="2026-07-17T10:00:00Z")
    metadata = _stored_metadata(
        _envelope(
            versions={"pipeline-new": newer, "pipeline-old": older},
            aliases={"v2.3": "pipeline-new", "stable": "pipeline-new"},
            default_version_id="pipeline-new",
        ),
        foreign={"other_product": {"preserve_me": True}},
        kitaru_extra={"experiments": {"opaque": {"keep": True}}},
    )

    agent = _agent_info_from_project_model(
        _project_model(metadata=metadata),
        active_project_id="project-id",
    )

    assert agent is not None
    assert agent.agent_id == "project-id"
    assert agent.name == "support-agent"
    assert agent.display_name == "Project display name"
    assert agent.description == "Support automation"
    assert agent.is_active is True
    assert agent.version_count == 2
    assert [version.agent_version_id for version in agent.agent_versions] == [
        "pipeline-old",
        "pipeline-new",
    ]
    assert agent.agent_versions[1].aliases == ["stable", "v2.3"]
    assert not hasattr(agent, "experiments")
    assert not hasattr(agent, "other_product")


def test_explicit_order_precedes_timestamp_fallback() -> None:
    first = _manifest("pipeline-first", registered_at="2026-07-18T10:00:00Z")
    second = _manifest("pipeline-second", registered_at="2026-07-16T10:00:00Z")
    third = _manifest("pipeline-third", registered_at="2026-07-17T10:00:00Z")
    metadata = _stored_metadata(
        _envelope(
            versions={
                "pipeline-first": first,
                "pipeline-second": second,
                "pipeline-third": third,
            },
            order=["pipeline-first"],
        )
    )

    agent = _agent_info_from_project_model(
        _project_model(metadata=metadata),
        active_project_id=None,
    )

    assert agent is not None
    assert [version.agent_version_id for version in agent.agent_versions] == [
        "pipeline-first",
        "pipeline-second",
        "pipeline-third",
    ]


@pytest.mark.parametrize(
    "namespace",
    [
        {"schema_version": 2},
        "not-a-mapping",
        {
            "schema_version": 1,
            "agent": {
                "agent_id": "project-id",
            },
            "agent_version_order": None,
            "agent_version_aliases": {},
            "agent_versions": {},
        },
        {
            "schema_version": 1,
            "agent": {
                "agent_id": "different-project",
                "name": "support-agent",
            },
            "agent_version_order": None,
            "agent_version_aliases": {},
            "agent_versions": {},
        },
    ],
)
def test_malformed_or_unsupported_kitaru_metadata_is_rejected(
    namespace: Any,
) -> None:
    with pytest.raises(KitaruStateError):
        _parse_agent_metadata("project-id", {"kitaru": namespace})


@pytest.mark.parametrize(
    ("versions", "aliases", "order"),
    [
        (
            {"wrong-key": _manifest("pipeline-id").model_dump(mode="json")},
            {},
            None,
        ),
        (
            {"pipeline-id": _manifest("pipeline-id").model_dump(mode="json")},
            {"stable": "missing-id"},
            None,
        ),
        (
            {"pipeline-id": _manifest("pipeline-id").model_dump(mode="json")},
            {},
            ["missing-id"],
        ),
    ],
)
def test_invalid_version_keys_aliases_and_order_are_rejected(
    versions: dict[str, Any],
    aliases: dict[str, str],
    order: list[str] | None,
) -> None:
    raw = _envelope().model_dump(mode="json")
    raw["agent_versions"] = versions
    raw["agent_version_aliases"] = aliases
    raw["agent_version_order"] = order

    with pytest.raises(KitaruStateError, match="malformed"):
        _parse_agent_metadata("project-id", {"kitaru": raw})


def test_aliases_that_collide_after_normalization_are_rejected() -> None:
    raw = _envelope(versions={"pipeline-id": _manifest("pipeline-id")}).model_dump(
        mode="json"
    )
    raw["agent_version_aliases"] = {
        "stable": "pipeline-id",
        " stable ": "pipeline-id",
    }

    with pytest.raises(KitaruStateError, match="malformed"):
        _parse_agent_metadata("project-id", {"kitaru": raw})


def test_manifest_requires_pipeline_uuid_as_only_version_id() -> None:
    raw_manifest = _manifest("pipeline-id").model_dump()
    raw_manifest["agent_version_id"] = "second-version-id"

    with pytest.raises(ValueError, match="must be identical"):
        _AgentVersionManifest.model_validate(raw_manifest)


def test_agent_and_version_serialization_is_exact_and_structured() -> None:
    manifest = _manifest("pipeline-id")
    envelope = _envelope(
        versions={"pipeline-id": manifest},
        aliases={"stable": "pipeline-id"},
        order=["pipeline-id"],
        default_version_id="pipeline-id",
    )
    agent = _agent_info_from_project_model(
        _project_model(metadata=_stored_metadata(envelope)),
        active_project_id="project-id",
    )

    assert agent is not None
    assert serialize_agent_version(agent.agent_versions[0]) == {
        "schema_version": 1,
        "agent_version_id": "pipeline-id",
        "pipeline_id": "pipeline-id",
        "pipeline_name": "support-agent--pipeline-id",
        "fingerprint": "sha256:pipeline-id",
        "git_sha": "7f192aa456789",
        "git_dirty": False,
        "working_tree_hash": None,
        "configuration_hash": "sha256:config-pipeline-id",
        "worldview_hash": "sha256:worldview-pipeline-id",
        "entrypoint": "evals.register:kagent",
        "registered_at": "2026-07-17T08:30:00Z",
        "source": "registration",
        "aliases": ["stable"],
    }
    assert serialize_agent(agent) == {
        "agent_id": "project-id",
        "name": "support-agent",
        "display_name": "Project display name",
        "description": "Support automation",
        "is_active": True,
        "default_agent_version_id": "pipeline-id",
        "default_executable": {
            "kind": "entrypoint",
            "entrypoint": "evals.register:kagent",
            "repo_root_marker": ".kitaru",
        },
        "agent_version_aliases": {"stable": "pipeline-id"},
        "agent_versions": [serialize_agent_version(agent.agent_versions[0])],
        "version_count": 1,
    }


def test_reconciliation_preserves_foreign_metadata_and_opaque_kitaru_keys() -> None:
    desired = _envelope()
    client = _StatefulProjectClient(
        {
            "other_product": {"preserve_me": True},
            "kitaru": {"future_stage": {"opaque": [1, 2, 3]}},
        }
    )

    result = _reconcile_project_agent_metadata(
        "project-id",
        _replace_with(desired),
        lambda actual: actual.agent.agent_id == "project-id",
        client_factory=lambda: client,
    )

    assert result.agent == desired.agent
    assert result.agent_versions == desired.agent_versions
    assert len(client.update_calls) == 1
    assert client.metadata["other_product"] == {"preserve_me": True}
    assert client.metadata["kitaru"]["future_stage"] == {"opaque": [1, 2, 3]}


def test_identical_reconciliation_skips_update_but_rereads_and_verifies() -> None:
    desired = _envelope()
    client = _StatefulProjectClient(_stored_metadata(desired))

    result = _reconcile_project_agent_metadata(
        "project-id",
        _replace_with(desired),
        lambda actual: actual == desired,
        client_factory=lambda: client,
    )

    assert result == desired
    assert client.update_calls == []
    assert client.get_calls == 2


def test_immutable_agent_name_conflict_makes_zero_update_calls() -> None:
    existing = _envelope(agent_name="alpha")
    changed = _envelope(agent_name="beta")
    client = _StatefulProjectClient(_stored_metadata(existing))

    with pytest.raises(KitaruMetadataConflictError, match="immutable Agent name"):
        _reconcile_project_agent_metadata(
            "project-id",
            _replace_with(changed),
            lambda actual: actual == changed,
            client_factory=lambda: client,
        )

    assert client.update_calls == []


def test_immutable_manifest_conflict_makes_zero_update_calls() -> None:
    existing_manifest = _manifest("pipeline-id")
    existing = _envelope(versions={"pipeline-id": existing_manifest})
    changed_manifest = _manifest("pipeline-id", fingerprint="sha256:different")
    changed = _envelope(versions={"pipeline-id": changed_manifest})
    client = _StatefulProjectClient(_stored_metadata(existing))

    with pytest.raises(KitaruMetadataConflictError, match="immutable"):
        _reconcile_project_agent_metadata(
            "project-id",
            _replace_with(changed),
            lambda _actual: True,
            client_factory=lambda: client,
        )

    assert client.update_calls == []


def test_alias_conflict_makes_zero_update_calls() -> None:
    first = _manifest("pipeline-first")
    second = _manifest("pipeline-second")
    existing = _envelope(
        versions={"pipeline-first": first, "pipeline-second": second},
        aliases={"stable": "pipeline-first"},
    )
    changed = _envelope(
        versions={"pipeline-first": first, "pipeline-second": second},
        aliases={"stable": "pipeline-second"},
    )
    client = _StatefulProjectClient(_stored_metadata(existing))

    with pytest.raises(KitaruMetadataConflictError, match="alias"):
        _reconcile_project_agent_metadata(
            "project-id",
            _replace_with(changed),
            lambda _actual: True,
            client_factory=lambda: client,
        )

    assert client.update_calls == []


def test_failed_verification_retries_from_fresh_foreign_state() -> None:
    desired = _envelope()
    client = _StatefulProjectClient({"other_product": {"revision": 1}})
    client.drop_first_verification = True

    result = _reconcile_project_agent_metadata(
        "project-id",
        _replace_with(desired),
        lambda actual: actual == desired,
        client_factory=lambda: client,
    )

    assert result == desired
    assert len(client.update_calls) == 2
    assert client.update_calls[0]["other_product"] == {"revision": 1}
    assert client.update_calls[1]["other_product"] == {"revision": 2}
    assert client.metadata["other_product"] == {"revision": 2}


def test_reconciliation_exhaustion_is_bounded_and_does_not_leak_values() -> None:
    secret_value = "do-not-include-this-value"
    client = _StatefulProjectClient({"foreign": {"secret": secret_value}})
    client.discard_updates = True

    with pytest.raises(
        KitaruMetadataReconciliationError,
        match="after 3 attempts",
    ) as exc_info:
        _reconcile_project_agent_metadata(
            "project-id",
            _replace_with(_envelope()),
            lambda _actual: True,
            client_factory=lambda: client,
        )

    assert len(client.update_calls) == 3
    assert secret_value not in str(exc_info.value)


def test_malformed_existing_metadata_fails_before_update() -> None:
    client = _StatefulProjectClient(
        {"kitaru": {"schema_version": 999, "sensitive": "do-not-overwrite"}}
    )

    with pytest.raises(KitaruStateError, match="unsupported schema"):
        _reconcile_project_agent_metadata(
            "project-id",
            _replace_with(_envelope()),
            lambda _actual: True,
            client_factory=lambda: client,
        )

    assert client.update_calls == []


def test_public_agent_reads_skip_uninitialized_projects() -> None:
    initialized = _project_model(metadata=_stored_metadata(_envelope()))
    uninitialized = SimpleNamespace(
        id="other-project",
        name="other",
        display_name=None,
        description=None,
        project_metadata={},
    )
    client = Mock()
    client.active_project = initialized
    client.list_projects.return_value = SimpleNamespace(
        items=[initialized, uninitialized],
        total_pages=1,
        max_size=20,
    )
    client.get_project.return_value = initialized

    assert current_agent(client_factory=lambda: client).agent_id == "project-id"
    assert [agent.agent_id for agent in list_agents(client_factory=lambda: client)] == [
        "project-id"
    ]
    assert (
        get_agent("project-id", client_factory=lambda: client).name == "support-agent"
    )


def test_get_agent_resolves_uuid_directly_without_scanning_projects() -> None:
    project_id = "12345678-1234-5678-1234-567812345678"
    initialized = _project_model(
        project_id=project_id,
        project_name="default",
        metadata=_stored_metadata(_envelope(project_id=project_id)),
    )
    client = Mock()
    client.active_project = SimpleNamespace(id="other-active-id")
    client.get_project.return_value = initialized

    agent = get_agent(project_id, client_factory=lambda: client)

    assert agent.agent_id == project_id
    client.list_projects.assert_not_called()


def test_get_agent_resolves_local_default_project_by_logical_name() -> None:
    initialized = _project_model(
        project_name="default",
        metadata=_stored_metadata(_envelope(agent_name="support-agent")),
    )
    client = Mock()
    client.active_project = initialized
    client.get_project.side_effect = RuntimeError("Project name does not match")
    client.list_projects.return_value = SimpleNamespace(
        items=[initialized], total_pages=1, max_size=20
    )

    agent = get_agent("support-agent", client_factory=lambda: client)

    assert agent.agent_id == "project-id"
    assert agent.name == "support-agent"


def test_get_agent_rejects_duplicate_logical_names_across_pages() -> None:
    first = _project_model(
        project_id="first-id",
        project_name="default",
        metadata=_stored_metadata(
            _envelope(project_id="first-id", agent_name="support-agent")
        ),
    )
    second = _project_model(
        project_id="second-id",
        project_name="other-project",
        metadata=_stored_metadata(
            _envelope(project_id="second-id", agent_name="support-agent")
        ),
    )
    client = Mock()
    client.get_project.side_effect = RuntimeError("Project name does not match")
    client.list_projects.side_effect = [
        SimpleNamespace(items=[first], total_pages=2, max_size=1),
        SimpleNamespace(items=[second], total_pages=2, max_size=1),
    ]

    with pytest.raises(KitaruStateError, match="Multiple initialized"):
        get_agent("support-agent", client_factory=lambda: client)


def test_list_agents_applies_pagination_after_filtering_initialized_agents() -> None:
    first_agent = _project_model(
        project_id="first-agent-id",
        metadata=_stored_metadata(_envelope(project_id="first-agent-id")),
    )
    second_agent = _project_model(
        project_id="second-agent-id",
        metadata=_stored_metadata(_envelope(project_id="second-agent-id")),
    )
    uninitialized = SimpleNamespace(
        id="plain-project-id",
        name="plain-project",
        display_name=None,
        description=None,
        project_metadata={},
    )
    pages = [
        SimpleNamespace(items=[uninitialized], total_pages=4, max_size=1),
        SimpleNamespace(items=[first_agent], total_pages=4, max_size=1),
        SimpleNamespace(items=[uninitialized], total_pages=4, max_size=1),
        SimpleNamespace(items=[second_agent], total_pages=4, max_size=1),
    ]
    client = Mock()
    client.active_project = SimpleNamespace(id="active-project-id")
    client.list_projects.side_effect = pages

    agents = list_agents(page=2, size=1, client_factory=lambda: client)

    assert [agent.agent_id for agent in agents] == ["second-agent-id"]
    assert len(client.list_projects.call_args_list) == 4
    assert client.list_projects.call_args_list[0].kwargs == {"hydrate": True}
    assert [call.kwargs for call in client.list_projects.call_args_list[1:]] == [
        {"page": 2, "size": 1, "hydrate": True},
        {"page": 3, "size": 1, "hydrate": True},
        {"page": 4, "size": 1, "hydrate": True},
    ]


@pytest.mark.parametrize(
    ("page", "size"),
    [(0, 1), (-1, 1), (1, 0), (1, -1)],
)
def test_list_agents_rejects_non_positive_pagination(
    page: int,
    size: int,
) -> None:
    client_factory = Mock()

    with pytest.raises(KitaruUsageError, match="positive integer"):
        list_agents(page=page, size=size, client_factory=client_factory)

    client_factory.assert_not_called()


def test_current_agent_rejects_an_uninitialized_active_project() -> None:
    client = Mock()
    client.active_project = _project_model(metadata={})

    with pytest.raises(KitaruStateError, match="Register the Agent first"):
        current_agent(client_factory=lambda: client)


def test_create_agent_uses_guarded_project_backend_then_initializes_metadata() -> None:
    client = _StatefulProjectClient()
    project = SimpleNamespace(
        id="project-id",
        name="support-agent",
        display_name="Support Agent",
        description="Support automation",
        is_active=True,
    )
    project_result = SimpleNamespace(
        project=project,
        previous_active_project="production",
        activated=False,
    )

    def activate_after_initialization(*_args: Any, **_kwargs: Any) -> Any:
        assert client.metadata["kitaru"]["agent"]["agent_id"] == "project-id"
        return project

    with (
        patch(
            "kitaru._config._agents.project_ops.create_project",
            return_value=project_result,
        ) as create_project,
        patch(
            "kitaru._config._agents.project_ops.use_project",
            side_effect=activate_after_initialization,
        ) as use_project,
    ):
        result = create_agent(
            "support-agent",
            description="Support automation",
            display_name="Support Agent",
            client_factory=lambda: client,
        )

    create_project.assert_called_once_with(
        "support-agent",
        description="Support automation",
        display_name="Support Agent",
        activate=False,
        client_factory=ANY,
    )
    use_project.assert_called_once_with("project-id", client_factory=ANY)
    assert result.agent.agent_id == "project-id"
    assert result.previous_active_agent == "production"
    assert result.activated is True
    metadata: Any = client.metadata
    assert metadata["kitaru"]["agent"] == {
        "agent_id": "project-id",
        "name": "support-agent",
        "default_agent_version_id": None,
        "default_executable": None,
    }


def test_create_agent_recovers_matching_uninitialized_project_then_activates() -> None:
    client = _RecoverableAgentProjectClient()

    def activate_after_initialization(*_args: Any, **_kwargs: Any) -> Any:
        assert client.metadata["kitaru"]["agent"]["agent_id"] == "project-id"
        client.activated = True
        return client._agent_project()

    with (
        patch(
            "kitaru._config._agents.project_ops.create_project",
            side_effect=KitaruBackendError("Project already exists"),
        ) as create_project,
        patch(
            "kitaru._config._agents.project_ops.use_project",
            side_effect=activate_after_initialization,
        ) as use_project,
    ):
        result = create_agent(
            "support-agent",
            description="Support automation",
            display_name="Support Agent",
            client_factory=lambda: client,
        )

    create_project.assert_called_once_with(
        "support-agent",
        description="Support automation",
        display_name="Support Agent",
        activate=False,
        client_factory=ANY,
    )
    use_project.assert_called_once_with("project-id", client_factory=ANY)
    assert client.get_selectors[0] == "support-agent"
    assert result.agent.agent_id == "project-id"
    assert result.agent.is_active is True
    assert result.previous_active_agent == "production"
    assert result.activated is True
    assert len(client.update_calls) == 1


def test_create_agent_retry_recovers_after_activation_failure() -> None:
    client = _RecoverableAgentProjectClient()
    project = SimpleNamespace(
        id="project-id",
        name="support-agent",
        display_name="Support Agent",
        description="Support automation",
        is_active=False,
    )
    create_results = [
        SimpleNamespace(
            project=project,
            previous_active_project="production",
            activated=False,
        ),
        KitaruBackendError("Project already exists"),
    ]
    activation_attempts = 0

    def activate(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal activation_attempts
        activation_attempts += 1
        if activation_attempts == 1:
            raise KitaruBackendError("temporary activation failure")
        client.activated = True
        return client._agent_project()

    with (
        patch(
            "kitaru._config._agents.project_ops.create_project",
            side_effect=create_results,
        ),
        patch(
            "kitaru._config._agents.project_ops.use_project",
            side_effect=activate,
        ),
    ):
        with pytest.raises(KitaruBackendError, match="activation failure"):
            create_agent(
                "support-agent",
                description="Support automation",
                display_name="Support Agent",
                client_factory=lambda: client,
            )

        result = create_agent(
            "support-agent",
            description="Support automation",
            display_name="Support Agent",
            client_factory=lambda: client,
        )

    assert result.agent.agent_id == "project-id"
    assert result.agent.is_active is True
    assert activation_attempts == 2
    assert len(client.update_calls) == 1


def test_create_agent_does_not_recover_request_mismatched_project() -> None:
    client = _RecoverableAgentProjectClient(description="Owned by another caller")

    with (
        patch(
            "kitaru._config._agents.project_ops.create_project",
            side_effect=KitaruBackendError("Project already exists"),
        ),
        patch("kitaru._config._agents.project_ops.use_project") as use_project,
        pytest.raises(KitaruBackendError, match="already exists"),
    ):
        create_agent(
            "support-agent",
            description="Support automation",
            display_name="Support Agent",
            client_factory=lambda: client,
        )

    assert client.update_calls == []
    use_project.assert_not_called()


def test_agent_use_and_delete_delegate_to_guarded_project_backend() -> None:
    client = _StatefulProjectClient(_stored_metadata(_envelope()))
    active_project = SimpleNamespace(
        id="project-id",
        name="support-agent",
        display_name="Support Agent",
        description="Support automation",
        is_active=True,
    )

    with (
        patch(
            "kitaru._config._agents.project_ops.use_project",
            return_value=active_project,
        ) as use_project,
        patch(
            "kitaru._config._agents.project_ops.delete_project",
            return_value=SimpleNamespace(),
        ) as delete_project,
    ):
        activated = use_agent("project-id", client_factory=lambda: client)
        deleted = delete_agent("project-id", client_factory=lambda: client)

    assert activated.is_active is True
    assert deleted.deleted_agent.agent_id == "project-id"
    use_project.assert_called_once()
    delete_project.assert_called_once()


def test_use_agent_activates_validated_id_when_name_is_reused() -> None:
    original = _project_model(
        project_id="original-id",
        project_name="default",
        metadata=_stored_metadata(_envelope(project_id="original-id")),
    )
    client = Mock()
    client.active_project = SimpleNamespace(id="other-active-id")
    client.get_project.side_effect = lambda selector, **_kwargs: original
    client.list_projects.return_value = SimpleNamespace(
        items=[original], total_pages=1, max_size=20
    )
    replacement_activated = False

    def use_project(selector: str, **_kwargs: Any) -> Any:
        nonlocal replacement_activated
        if selector == "support-agent":
            replacement_activated = True
            return SimpleNamespace(id="replacement-id")
        assert selector == "original-id"
        return SimpleNamespace(id="original-id")

    with patch(
        "kitaru._config._agents.project_ops.use_project",
        side_effect=use_project,
    ) as use_project_mock:
        activated = use_agent("support-agent", client_factory=lambda: client)

    assert activated.agent_id == "original-id"
    assert replacement_activated is False
    use_project_mock.assert_called_once_with("original-id", client_factory=ANY)


def test_delete_agent_deletes_validated_id_when_name_is_reused() -> None:
    original = _project_model(
        project_id="original-id",
        project_name="default",
        metadata=_stored_metadata(_envelope(project_id="original-id")),
    )
    client = Mock()
    client.active_project = SimpleNamespace(id="other-active-id")
    client.get_project.return_value = original
    client.list_projects.return_value = SimpleNamespace(
        items=[original], total_pages=1, max_size=20
    )
    replacement_deleted = False

    def delete_project(selector: str, **_kwargs: Any) -> Any:
        nonlocal replacement_deleted
        if selector == "support-agent":
            replacement_deleted = True
        assert selector == "original-id"
        return SimpleNamespace()

    with patch(
        "kitaru._config._agents.project_ops.delete_project",
        side_effect=delete_project,
    ) as delete_project_mock:
        deleted = delete_agent("support-agent", client_factory=lambda: client)

    assert deleted.deleted_agent.agent_id == "original-id"
    assert replacement_deleted is False
    delete_project_mock.assert_called_once_with("original-id", client_factory=ANY)


def test_active_local_default_initialization_bypasses_lifecycle_guard() -> None:
    client = _StatefulProjectClient()

    result = _reconcile_active_project_agent_metadata(
        _replace_with(_envelope()),
        lambda actual: actual.agent.agent_id == "project-id",
        client_factory=lambda: client,
    )

    assert result.agent.agent_id == "project-id"
    assert len(client.update_calls) == 1
    client.zen_store.get_store_info.assert_not_called()
