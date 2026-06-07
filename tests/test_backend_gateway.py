"""Regression tests for the private Kitaru backend gateway."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call, patch

import pytest
from zenml.models import PipelineRunResponse

from kitaru._cli._dependencies import CLIDependencies
from kitaru._client._backend_gateway import KitaruBackendGateway
from kitaru._config._stacks import StackInfo, StackType
from kitaru.client import KitaruClient
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruLogRetrievalError


def test_gateway_uses_injected_client_factory_without_caching() -> None:
    """Every gateway client request should ask the injected factory again."""
    created_clients: list[object] = []

    def _client_factory() -> object:
        client = object()
        created_clients.append(client)
        return client

    gateway = KitaruBackendGateway(project="project-id", client_factory=_client_factory)

    first = gateway.zenml_client()
    second = gateway.zenml_client()

    assert first is created_clients[0]
    assert second is created_clients[1]
    assert first is not second


def test_kitaru_client_client_wrapper_delegates_to_gateway() -> None:
    """The compatibility `_client()` wrapper should use the gateway."""
    backend_client = object()
    with patch(
        "kitaru.client.resolve_connection_config",
        return_value=SimpleNamespace(project="project-id"),
    ):
        client = KitaruClient()

    zenml_client = Mock(return_value=backend_client)
    client._backend = cast(
        KitaruBackendGateway,
        SimpleNamespace(zenml_client=zenml_client),
    )

    assert client._client() is backend_client
    zenml_client.assert_called_once_with()


def test_kitaru_client_preserves_client_patch_point_for_gateway_factory() -> None:
    """Patching `kitaru.client.Client` should still affect backend client creation."""
    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=SimpleNamespace(project="project-id"),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client = KitaruClient()
        assert client._client() is client_cls.return_value

    client_cls.assert_called_once_with()


def test_gateway_rest_store_preserves_caller_specific_error_type() -> None:
    """Logs and events pass different errors through the same REST-store check."""
    gateway = KitaruBackendGateway(
        project="project-id",
        client_factory=lambda: SimpleNamespace(zen_store=object()),
    )
    unavailable_error = KitaruFeatureNotAvailableError("events need REST")

    with pytest.raises(KitaruFeatureNotAvailableError) as exc_info:
        gateway.require_rest_store(unavailable_error)

    assert exc_info.value is unavailable_error


def test_gateway_restart_uses_the_activated_client_active_stack() -> None:
    """Retry/resume should activate and resume through the same locked client."""
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(resume_run=Mock()))
    zenml_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(id="old-stack"),
        active_stack=active_stack,
        activate_stack=Mock(),
    )
    run = SimpleNamespace(
        id="run-1",
        snapshot=SimpleNamespace(stack=SimpleNamespace(id="retry-stack")),
    )
    client_factory = Mock(return_value=zenml_client)
    gateway = KitaruBackendGateway(project="project-id", client_factory=client_factory)

    gateway.restart_run_from_snapshot(
        run=cast(PipelineRunResponse, run),
        operation_name="retry",
    )

    client_factory.assert_called_once_with()
    zenml_client.activate_stack.assert_has_calls(
        [call("retry-stack"), call("old-stack")]
    )
    active_stack.orchestrator.resume_run.assert_called_once_with(
        snapshot=run.snapshot,
        run=run,
        stack=active_stack,
    )


def test_gateway_fetch_log_payload_uses_owned_log_store_helpers() -> None:
    """OTEL log errors should use gateway-held log helper dependencies."""
    store = Mock()
    store.get.side_effect = RuntimeError(
        "NotImplementedError: OTEL log store fetch is not implemented"
    )
    gateway = KitaruBackendGateway(
        project="project-id",
        client_factory=Mock(),
        active_stack_log_store_getter=Mock(
            return_value=SimpleNamespace(endpoint="https://logs.example.com")
        ),
        resolve_log_store_getter=Mock(side_effect=AssertionError("unused")),
    )

    with pytest.raises(KitaruLogRetrievalError) as exc_info:
        gateway.fetch_log_payload(
            path="/runs/run-1/logs",
            source="runner",
            store=store,
        )

    assert "OTEL backend" in str(exc_info.value)
    assert "https://logs.example.com" in str(exc_info.value)
    store.get.assert_called_once_with(
        "/runs/run-1/logs",
        params={"source": "runner"},
    )


def test_gateway_api_key_create_handles_local_activation_internally() -> None:
    """API-key create should not require callers to pass a backend client around."""
    api_key = SimpleNamespace(key="raw-secret")
    zenml_client = SimpleNamespace(
        create_api_key=Mock(return_value=api_key),
        set_api_key=Mock(),
        zen_store=SimpleNamespace(url="https://server.example.com"),
    )
    credentials_store = SimpleNamespace(get_api_key=Mock(return_value=None))
    gateway = KitaruBackendGateway(
        project="project-id",
        client_factory=Mock(return_value=zenml_client),
    )

    with patch(
        "kitaru._client._backend_gateway.get_credentials_store",
        return_value=credentials_store,
    ):
        result = gateway.create_api_key(
            service_account="ci-runner",
            name="default",
            description="Default CI key",
            set_key=True,
        )

    assert result.api_key is api_key
    assert result.local_key_activation is not None
    assert result.local_key_activation.succeeded is True
    zenml_client.create_api_key.assert_called_once_with(
        service_account_name_id_or_prefix="ci-runner",
        name="default",
        description="Default CI key",
        set_key=False,
    )
    zenml_client.set_api_key.assert_called_once_with(key="raw-secret")
    credentials_store.get_api_key.assert_called_once_with(
        server_url="https://server.example.com"
    )


def test_gateway_stack_list_calls_terminal_stack_operation_not_config_wrapper() -> None:
    """Gateway stack methods should call `_config._stacks`, not `config.py` wrappers."""
    entry = SimpleNamespace(
        stack=StackInfo(id="stack-id", name="dev", is_active=True),
        is_managed=True,
    )
    gateway = KitaruBackendGateway(project=None, client_factory=Mock())

    with (
        patch("kitaru.config._list_stack_entries", side_effect=AssertionError),
        patch(
            "kitaru._config._stacks._list_stack_entries",
            return_value=[entry],
        ) as terminal_list,
    ):
        assert gateway.list_stack_entries() == [entry]

    terminal_list.assert_called_once_with()


def test_gateway_patched_stack_create_bridge_keeps_existing_call_shape() -> None:
    """Patched terminal stack operations should still receive the old arguments."""
    result = SimpleNamespace(stack=StackInfo(id="stack-id", name="dev", is_active=True))
    gateway = KitaruBackendGateway(project=None, client_factory=Mock())

    with patch(
        "kitaru._config._stacks._create_stack_operation",
        return_value=result,
    ) as terminal_create:
        assert (
            gateway.create_stack_operation(
                "dev",
                stack_type=StackType.LOCAL,
                activate=True,
                remote_spec=None,
            )
            is result
        )

    terminal_create.assert_called_once_with(
        "dev",
        stack_type=StackType.LOCAL,
        activate=True,
        remote_spec=None,
    )


def test_legacy_cli_stack_patch_point_overrides_gateway_default() -> None:
    """Existing `kitaru.cli.*` stack patches should still beat gateway defaults."""
    patched_entries = [SimpleNamespace(stack="patched-stack", is_managed=False)]
    dependencies = CLIDependencies()

    with (
        patch(
            "kitaru.cli._list_stack_entries",
            return_value=patched_entries,
        ) as patched,
        patch.object(
            CLIDependencies,
            "backend_gateway",
            side_effect=AssertionError("gateway should not be used"),
        ),
    ):
        assert dependencies.list_stack_entries() == patched_entries

    patched.assert_called_once_with()


def test_kitaru_client_does_not_expose_public_stacks_namespace() -> None:
    """The gateway adds internal stack routing without adding `client.stacks`."""
    with patch(
        "kitaru.client.resolve_connection_config",
        return_value=SimpleNamespace(project="project-id"),
    ):
        client = KitaruClient()

    assert not hasattr(client, "stacks")
