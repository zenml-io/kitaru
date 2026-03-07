"""Tests for Kitaru configuration helpers."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.config.global_config import GlobalConfiguration
from zenml.utils import yaml_utils

from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    KITARU_CACHE_ENV,
    KITARU_IMAGE_ENV,
    KITARU_LOG_STORE_BACKEND_ENV,
    KITARU_LOG_STORE_ENDPOINT_ENV,
    KITARU_PROJECT_ENV,
    KITARU_RETRIES_ENV,
    KITARU_SERVER_URL_ENV,
    KITARU_STACK_ENV,
    FrozenExecutionSpec,
    ImageSettings,
    KitaruConfig,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
    build_frozen_execution_spec,
    configure,
    current_stack,
    image_settings_to_docker_settings,
    list_model_aliases,
    list_stacks,
    persist_frozen_execution_spec,
    register_model_alias,
    reset_global_log_store,
    resolve_connection_config,
    resolve_execution_config,
    resolve_log_store,
    resolve_model_selection,
    set_global_log_store,
    use_stack,
)


class _FakeStackPage:
    """Simple iterable page used to test stack pagination behavior."""

    def __init__(
        self,
        *,
        items: list[SimpleNamespace],
        total_pages: int,
        max_size: int,
    ) -> None:
        self.items = items
        self.total_pages = total_pages
        self.max_size = max_size

    def __iter__(self) -> Iterator[SimpleNamespace]:
        return iter(self.items)


def _kitaru_config_path() -> Path:
    """Return the path used for persisted Kitaru global config in tests."""
    return Path(GlobalConfiguration().config_directory) / "kitaru.yaml"


def test_log_store_defaults_to_artifact_store() -> None:
    """Runtime logs should resolve to artifact-store by default."""
    snapshot = resolve_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.api_key is None
    assert snapshot.source == "default"


def test_set_log_store_persists_global_override() -> None:
    """Setting a backend should persist and become the resolved global default."""
    snapshot = set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key="{{ DATADOG_KEY }}",
    )

    assert snapshot.backend == "datadog"
    assert snapshot.endpoint == "https://logs.datadoghq.com"
    assert snapshot.api_key == "{{ DATADOG_KEY }}"
    assert snapshot.source == "global user config"

    persisted = yaml_utils.read_yaml(str(_kitaru_config_path()))
    assert persisted["log_store"]["backend"] == "datadog"
    assert persisted["log_store"]["endpoint"] == "https://logs.datadoghq.com"


def test_set_log_store_preserves_model_registry() -> None:
    """Log-store updates should not clobber persisted model aliases."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    aliases = list_model_aliases()
    assert len(aliases) == 1
    assert aliases[0].alias == "fast"
    assert aliases[0].model == "openai/gpt-4o-mini"
    assert aliases[0].secret == "openai-creds"


def test_register_model_alias_preserves_log_store_settings() -> None:
    """Model alias writes should preserve existing log-store overrides."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    alias = register_model_alias("fast", model="openai/gpt-4o-mini")

    assert alias.alias == "fast"
    assert alias.is_default is True

    snapshot = resolve_log_store()
    assert snapshot.backend == "datadog"
    assert snapshot.endpoint == "https://logs.datadoghq.com"


def test_environment_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment variables should override persisted global config."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
        api_key="{{ DATADOG_KEY }}",
    )
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "honeycomb")
    monkeypatch.setenv(KITARU_LOG_STORE_ENDPOINT_ENV, "https://api.honeycomb.io")

    snapshot = resolve_log_store()

    assert snapshot.backend == "honeycomb"
    assert snapshot.endpoint == "https://api.honeycomb.io"
    assert snapshot.source == "environment"


def test_environment_can_force_artifact_store_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment should be able to override persisted state back to default."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "artifact-store")

    snapshot = resolve_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.api_key is None
    assert snapshot.source == "environment"


def test_environment_artifact_store_rejects_extra_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """artifact-store env override should not accept endpoint/api-key values."""
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "artifact-store")
    monkeypatch.setenv(KITARU_LOG_STORE_ENDPOINT_ENV, "https://should-not-be-used")

    with pytest.raises(ValueError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
        resolve_log_store()


def test_reset_clears_persisted_log_store_override() -> None:
    """Reset should remove the persisted override and restore defaults."""
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    snapshot = reset_global_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.endpoint is None
    assert snapshot.source == "default"


def test_reset_log_store_preserves_model_registry() -> None:
    """Resetting log-store config should keep model aliases intact."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    reset_global_log_store()

    aliases = list_model_aliases()
    assert len(aliases) == 1
    assert aliases[0].alias == "fast"


def test_partial_env_override_raises_helpful_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend-only environment override should fail with clear guidance."""
    monkeypatch.setenv(KITARU_LOG_STORE_BACKEND_ENV, "datadog")

    with pytest.raises(ValueError, match=KITARU_LOG_STORE_ENDPOINT_ENV):
        resolve_log_store()


def test_set_rejects_artifact_store_override() -> None:
    """artifact-store should stay an implicit default, not an override target."""
    with pytest.raises(ValueError, match="already the default"):
        set_global_log_store(
            "artifact-store",
            endpoint="https://unused.example.com",
        )


def test_invalid_persisted_config_raises_error() -> None:
    """Malformed persisted config should raise a clear ValueError."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    with pytest.raises(ValueError, match="global config file is invalid"):
        resolve_log_store()


def test_reset_recovers_from_invalid_persisted_config() -> None:
    """Reset should recover by overwriting malformed persisted config."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    snapshot = reset_global_log_store()

    assert snapshot.backend == "artifact-store"
    assert snapshot.source == "default"


def test_set_overwrites_invalid_persisted_config() -> None:
    """Set should recover by replacing malformed persisted config contents."""
    yaml_utils.write_yaml(str(_kitaru_config_path()), ["invalid"])

    snapshot = set_global_log_store(
        "datadog",
        endpoint="https://logs.datadoghq.com",
    )

    assert snapshot.backend == "datadog"
    assert snapshot.source == "global user config"


def test_register_model_alias_sets_first_alias_as_default() -> None:
    """The first registered alias should become the default model alias."""
    fast = register_model_alias("FAST", model="openai/gpt-4o-mini")
    smart = register_model_alias("smart", model="anthropic/claude-sonnet-4-20250514")

    aliases = list_model_aliases()

    assert fast.alias == "fast"
    assert fast.is_default is True
    assert smart.is_default is False
    assert [entry.alias for entry in aliases] == ["fast", "smart"]


def test_register_model_alias_updates_existing_alias() -> None:
    """Re-registering an alias should update model/secret values."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    updated = register_model_alias(
        "fast",
        model="openai/gpt-4.1-mini",
        secret="openai-prod",
    )

    assert updated.alias == "fast"
    assert updated.model == "openai/gpt-4.1-mini"
    assert updated.secret == "openai-prod"
    assert updated.is_default is True


def test_resolve_model_selection_prefers_aliases_and_defaults() -> None:
    """Model resolution should honor aliases and default fallback behavior."""
    register_model_alias("fast", model="openai/gpt-4o-mini", secret="openai-creds")

    alias_selection = resolve_model_selection("fast")
    concrete_selection = resolve_model_selection("openai/gpt-4.1-mini")
    default_selection = resolve_model_selection(None)

    assert alias_selection.alias == "fast"
    assert alias_selection.resolved_model == "openai/gpt-4o-mini"
    assert alias_selection.secret == "openai-creds"

    assert concrete_selection.alias is None
    assert concrete_selection.resolved_model == "openai/gpt-4.1-mini"
    assert concrete_selection.secret is None

    assert default_selection.alias == "fast"
    assert default_selection.resolved_model == "openai/gpt-4o-mini"


def test_resolve_model_selection_requires_default_or_explicit_model() -> None:
    """`kitaru.llm(model=None)` should fail without a configured default alias."""
    with pytest.raises(ValueError, match="No model alias is configured"):
        resolve_model_selection(None)


def test_current_stack_returns_active_stack_info() -> None:
    """current_stack should expose the currently active stack."""
    active_stack = SimpleNamespace(id="stack-local-id", name="local")
    client_mock = SimpleNamespace(active_stack_model=active_stack)

    with patch("kitaru.config.Client", return_value=client_mock):
        stack = current_stack()

    assert stack.id == "stack-local-id"
    assert stack.name == "local"
    assert stack.is_active is True


def test_list_stacks_marks_active_stack() -> None:
    """list_stacks should flag only the active stack in the returned list."""
    local = SimpleNamespace(id="stack-local-id", name="local")
    prod = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = SimpleNamespace(
        active_stack_model=prod,
        list_stacks=lambda: [local, prod],
    )

    with patch("kitaru.config.Client", return_value=client_mock):
        stacks = list_stacks()

    assert [(stack.name, stack.is_active) for stack in stacks] == [
        ("local", False),
        ("prod", True),
    ]


def test_list_stacks_fetches_all_pages() -> None:
    """list_stacks should collect stacks from all pages exposed by the runtime."""
    local = SimpleNamespace(id="stack-local-id", name="local")
    staging = SimpleNamespace(id="stack-staging-id", name="staging")
    prod = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = Mock()
    client_mock.active_stack_model = prod
    client_mock.list_stacks.side_effect = [
        _FakeStackPage(items=[local], total_pages=2, max_size=1),
        _FakeStackPage(items=[staging, prod], total_pages=2, max_size=1),
    ]

    with patch("kitaru.config.Client", return_value=client_mock):
        stacks = list_stacks()

    assert [stack.name for stack in stacks] == ["local", "staging", "prod"]
    assert [stack.is_active for stack in stacks] == [False, False, True]
    client_mock.list_stacks.assert_has_calls([call(), call(page=2, size=1)])


def test_use_stack_switches_active_stack() -> None:
    """use_stack should delegate activation and return the new active stack."""
    local_stack = SimpleNamespace(id="stack-local-id", name="local")
    prod_stack = SimpleNamespace(id="stack-prod-id", name="prod")
    client_mock = SimpleNamespace(active_stack_model=local_stack)

    def _activate_stack(_: str) -> None:
        client_mock.active_stack_model = prod_stack

    activate_stack = Mock(side_effect=_activate_stack)
    client_mock.activate_stack = activate_stack

    with patch("kitaru.config.Client", return_value=client_mock):
        selected = use_stack("prod")

    activate_stack.assert_called_once_with("prod")
    assert selected.name == "prod"
    assert selected.id == "stack-prod-id"
    assert selected.is_active is True


def test_use_stack_rejects_empty_selector() -> None:
    """use_stack should fail fast on empty stack names/IDs."""
    with pytest.raises(ValueError, match="cannot be empty"):
        use_stack("   ")


def test_configure_sets_runtime_execution_defaults() -> None:
    """configure should update process-local execution defaults."""
    snapshot = configure(
        stack="gpu-prod",
        cache=False,
        retries=2,
        image={
            "base_image": "python:3.12-slim",
            "environment": {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
        },
    )

    assert snapshot.stack == "gpu-prod"
    assert snapshot.cache is False
    assert snapshot.retries == 2
    assert snapshot.image is not None
    assert snapshot.image.base_image == "python:3.12-slim"
    assert snapshot.image.environment == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}


def test_configure_can_clear_runtime_override_fields() -> None:
    """configure should allow clearing previously set runtime overrides."""
    configure(stack="gpu-prod", cache=False, retries=2)

    snapshot = configure(stack=None, cache=None, retries=None, image=None)

    assert snapshot.stack is None
    assert snapshot.cache is None
    assert snapshot.retries is None
    assert snapshot.image is None


def test_resolve_execution_config_applies_phase10_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execution config resolution should follow the Phase 10 precedence chain."""
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        """
[tool.kitaru]
stack = "project-stack"
cache = false
retries = 1

[tool.kitaru.image]
base_image = "python:3.12"

[tool.kitaru.image.environment]
FROM_PROJECT = "1"
SHARED = "project"
""".strip()
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(KITARU_STACK_ENV, "env-stack")
    monkeypatch.setenv(KITARU_CACHE_ENV, "true")
    monkeypatch.setenv(KITARU_RETRIES_ENV, "3")
    monkeypatch.setenv(
        KITARU_IMAGE_ENV,
        (
            '{"base_image": "python:3.13", '
            '"environment": {"FROM_ENV": "1", "SHARED": "env"}}'
        ),
    )
    configure(
        stack="runtime-stack",
        cache=False,
        retries=4,
        image={"environment": {"FROM_RUNTIME": "1", "SHARED": "runtime"}},
    )

    with patch(
        "kitaru.config.current_stack",
        return_value=SimpleNamespace(name="global-stack"),
    ):
        resolved = resolve_execution_config(
            decorator_overrides=KitaruConfig(
                cache=True,
                retries=5,
                image=ImageSettings(
                    environment={"FROM_DECORATOR": "1", "SHARED": "decorator"}
                ),
            ),
            invocation_overrides=KitaruConfig(
                stack="invocation-stack",
                retries=6,
                image=ImageSettings(
                    environment={"FROM_INVOCATION": "1", "SHARED": "invocation"}
                ),
            ),
            start_dir=tmp_path,
        )

    assert resolved.stack == "invocation-stack"
    assert resolved.cache is True
    assert resolved.retries == 6
    assert resolved.image is not None
    assert resolved.image.base_image == "python:3.13"
    assert resolved.image.environment == {
        "FROM_PROJECT": "1",
        "SHARED": "invocation",
        "FROM_ENV": "1",
        "FROM_RUNTIME": "1",
        "FROM_DECORATOR": "1",
        "FROM_INVOCATION": "1",
    }


def test_resolve_execution_config_rejects_invalid_cache_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid cache env values should raise clear parse errors."""
    monkeypatch.setenv(KITARU_CACHE_ENV, "not-a-bool")

    with pytest.raises(ValueError, match=KITARU_CACHE_ENV):
        resolve_execution_config()


def test_resolve_execution_config_supports_string_image_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_IMAGE should accept plain image strings for compatibility."""
    monkeypatch.setenv(KITARU_IMAGE_ENV, "python:3.12-slim")

    with patch(
        "kitaru.config.current_stack",
        return_value=SimpleNamespace(name="global-stack"),
    ):
        resolved = resolve_execution_config()

    assert resolved.image is not None
    assert resolved.image.base_image == "python:3.12-slim"


def test_image_settings_can_be_converted_to_docker_settings() -> None:
    """Resolved image settings should map cleanly to ZenML Docker settings."""
    image_settings = ImageSettings(
        base_image="python:3.12",
        requirements=["httpx"],
        dockerfile="Dockerfile",
        environment={"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
    )

    docker_settings = image_settings_to_docker_settings(image_settings)

    assert isinstance(docker_settings, DockerSettings)
    assert docker_settings is not None
    assert docker_settings.parent_image == "python:3.12"
    assert docker_settings.requirements == ["httpx"]
    assert docker_settings.dockerfile == "Dockerfile"
    assert docker_settings.environment == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}


def test_connection_resolution_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connection config should resolve as explicit > env > global."""
    monkeypatch.setenv(KITARU_SERVER_URL_ENV, "https://env.example.com")
    monkeypatch.setenv(KITARU_PROJECT_ENV, "env-project")

    with patch(
        "kitaru.config._read_global_connection_config",
        return_value=KitaruConfig(
            server_url="https://global.example.com",
            auth_token="global-token",
            project="global-project",
        ),
    ):
        resolved = resolve_connection_config(
            explicit=KitaruConfig(project="explicit-project"),
        )

    assert isinstance(resolved, ResolvedConnectionConfig)
    assert resolved.server_url == "https://env.example.com"
    assert resolved.project == "explicit-project"
    assert resolved.auth_token == "global-token"


def test_build_and_persist_frozen_execution_spec() -> None:
    """Frozen execution specs should be serializable and persisted as metadata."""
    frozen_execution_spec = build_frozen_execution_spec(
        resolved_execution=ResolvedExecutionConfig(
            stack="prod",
            cache=False,
            retries=2,
            image=ImageSettings(
                base_image="python:3.12",
                environment={"OPENAI_API_KEY": "{{ OPENAI_KEY }}"},
            ),
        ),
        flow_defaults=KitaruConfig(cache=False),
        connection=ResolvedConnectionConfig(
            server_url="https://server.example.com",
            project="demo",
        ),
    )

    assert isinstance(frozen_execution_spec, FrozenExecutionSpec)
    assert (
        frozen_execution_spec.resolved_execution.image is not None
        and frozen_execution_spec.resolved_execution.image.environment
        == {"OPENAI_API_KEY": "{{ OPENAI_KEY }}"}
    )

    with patch("kitaru.config.Client") as client_cls:
        persist_frozen_execution_spec(
            run_id="00000000-0000-0000-0000-000000000123",
            frozen_execution_spec=frozen_execution_spec,
        )

    create_metadata = client_cls.return_value.create_run_metadata
    create_metadata.assert_called_once()
    metadata_payload = create_metadata.call_args.kwargs["metadata"]
    assert FROZEN_EXECUTION_SPEC_METADATA_KEY in metadata_payload
    assert (
        metadata_payload[FROZEN_EXECUTION_SPEC_METADATA_KEY]["resolved_execution"][
            "stack"
        ]
        == "prod"
    )
