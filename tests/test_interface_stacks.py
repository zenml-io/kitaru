"""Tests for stack requests shared by CLI, YAML, and MCP interfaces."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from kitaru._config._stacks import (
    CloudProvider,
    LocalCloudArtifactStoreSpec,
    StackType,
    VertexStackSpec,
)
from kitaru._interface_stacks import (
    CLI_STACK_OPTION_LABELS,
    ManageStackCreateRequest,
    build_stack_create_request,
    execute_stack_create_request,
)


def _build_request(**updates: Any) -> ManageStackCreateRequest:
    inputs: dict[str, Any] = {
        "name": "dev",
        "activate": True,
        "stack_type": "local",
        "artifact_store": None,
        "container_registry": None,
        "cluster": None,
        "region": None,
        "subscription_id": None,
        "resource_group": None,
        "workspace": None,
        "execution_role": None,
        "namespace": None,
        "credentials": None,
        "sandbox": None,
        "verify": True,
        "labels": CLI_STACK_OPTION_LABELS,
    }
    inputs.update(updates)
    return build_stack_create_request(**inputs)


def test_fully_local_request_keeps_cloud_storage_spec_absent() -> None:
    """Omitting artifact storage should retain the existing fully local path."""
    request = _build_request()

    assert request.stack_type == StackType.LOCAL
    assert request.remote_spec is None
    assert request.local_cloud_artifact_store_spec is None
    assert request.sandbox_flavor == "local"


@pytest.mark.parametrize(
    ("artifact_store", "provider"),
    [
        ("s3://bucket/path", CloudProvider.AWS),
        ("gs://bucket/path", CloudProvider.GCP),
        ("az://container/path", CloudProvider.AZURE),
        ("abfs://container/path", CloudProvider.AZURE),
        ("abfss://container/path", CloudProvider.AZURE),
        (
            "abfs://filesystem@account.dfs.core.windows.net/path",
            CloudProvider.AZURE,
        ),
        (
            "abfss://filesystem@account.dfs.core.windows.net/path",
            CloudProvider.AZURE,
        ),
    ],
)
def test_local_cloud_uri_builds_separate_storage_spec(
    artifact_store: str,
    provider: CloudProvider,
) -> None:
    """All supported cloud URI forms should produce the shared hybrid spec."""
    request = _build_request(artifact_store=f"  {artifact_store}  ")

    assert request.remote_spec is None
    spec = request.local_cloud_artifact_store_spec
    assert spec is not None
    assert spec == LocalCloudArtifactStoreSpec(
        artifact_store=artifact_store,
    )
    assert spec.provider == provider
    assert request.sandbox_flavor == "local"


@pytest.mark.parametrize(
    ("field_name", "value", "label"),
    [
        ("credentials", "aws-profile:team", "--credentials"),
        ("region", "eu-west-1", "--region"),
        ("subscription_id", "subscription", "--subscription-id"),
        ("verify", False, "--no-verify"),
    ],
)
def test_local_cloud_fields_require_artifact_store(
    field_name: str,
    value: Any,
    label: str,
) -> None:
    """Credential controls without cloud storage should have a targeted error."""
    with pytest.raises(
        ValueError,
        match=rf"{label}.*require --artifact-store",
    ):
        _build_request(**{field_name: value})


def test_local_cloud_uri_requires_bucket_or_container() -> None:
    """A provider scheme without a resource should fail during request building."""
    with pytest.raises(ValueError, match="with a bucket or container name"):
        _build_request(artifact_store="s3://")


@pytest.mark.parametrize(
    "artifact_store",
    [
        "s3://access-key:secret@bucket/path",
        "s3://bucket:443/path",
        "s3://bucket/path?X-Amz-Signature=secret",
        "gs://bucket/path#private-fragment",
        "az://container/path?sig=secret",
        "az://container@account.blob.core.windows.net/path",
        "abfss://filesystem@untrusted.example.com/path",
        "s3://[malformed-authority/path",
        "https://secret.example.com/path",
    ],
)
def test_local_cloud_uri_rejects_unsafe_values_without_echoing_them(
    artifact_store: str,
) -> None:
    """Rejected URIs should not expose embedded credentials or signed values."""
    with pytest.raises(ValueError) as exc_info:
        _build_request(artifact_store=artifact_store)

    message = str(exc_info.value)
    assert artifact_store not in message
    assert "without query parameters, fragments, embedded credentials, or ports" in (
        message
    )


def test_local_connectorless_storage_accepts_no_verify() -> None:
    """--no-verify without credentials skips reused-connector verification."""
    request = _build_request(
        artifact_store="s3://bucket/path",
        verify=False,
    )

    spec = request.local_cloud_artifact_store_spec
    assert spec is not None
    assert spec.verify is False
    assert spec.credentials is None


@pytest.mark.parametrize(
    ("artifact_store", "updates", "message"),
    [
        (
            "gs://bucket/path",
            {"region": "us-central1"},
            "--region only applies to s3://",
        ),
        (
            "s3://bucket/path",
            {"subscription_id": "subscription"},
            "--subscription-id only applies to Azure",
        ),
    ],
)
def test_local_cloud_rejects_irrelevant_provider_hints(
    artifact_store: str,
    updates: dict[str, str],
    message: str,
) -> None:
    """Provider hints should not be silently ignored for another provider."""
    with pytest.raises(ValueError, match=message):
        _build_request(artifact_store=artifact_store, **updates)


def test_local_cloud_keeps_registry_and_async_fields_disabled() -> None:
    """Cloud storage should not turn a local stack into a remote runner."""
    with pytest.raises(ValueError, match="Remote stack options require"):
        _build_request(
            artifact_store="s3://bucket/path",
            container_registry="registry.example.com/repo",
        )
    with pytest.raises(ValueError, match="--async requires"):
        _build_request(
            artifact_store="s3://bucket/path",
            async_enabled=True,
        )


def test_remote_stack_request_contract_is_unchanged() -> None:
    """Existing remote stack types should still use only RemoteStackSpec."""
    request = _build_request(
        stack_type="vertex",
        artifact_store="gs://bucket/path",
        container_registry="us-docker.pkg.dev/demo/repo",
        region="us-central1",
    )

    assert request.local_cloud_artifact_store_spec is None
    assert request.remote_spec == VertexStackSpec(
        artifact_store="gs://bucket/path",
        container_registry="us-docker.pkg.dev/demo/repo",
        region="us-central1",
    )


def test_execute_stack_create_request_forwards_local_cloud_spec() -> None:
    """All interface surfaces should dispatch the same validated hybrid request."""
    spec = LocalCloudArtifactStoreSpec(
        artifact_store="s3://bucket/path",
    )
    request = ManageStackCreateRequest(
        name="dev",
        activate=False,
        stack_type=StackType.LOCAL,
        local_cloud_artifact_store_spec=spec,
        sandbox_flavor="local",
    )
    expected = SimpleNamespace(stack=SimpleNamespace(name="dev"))
    operation = Mock(return_value=expected)

    result = execute_stack_create_request(
        request,
        create_stack_operation=operation,
    )

    operation.assert_called_once_with(
        "dev",
        stack_type=StackType.LOCAL,
        activate=False,
        remote_spec=None,
        local_cloud_artifact_store_spec=spec,
        sandbox_flavor="local",
    )
    assert result is expected
