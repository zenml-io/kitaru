"""Stack lifecycle and inspection helpers."""

from __future__ import annotations

import difflib
import re
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from zenml.artifact_stores.local_artifact_store import LocalArtifactStoreFlavor
from zenml.client import Client
from zenml.constants import DOCKER_REGISTRY_RESOURCE_TYPE
from zenml.container_registries.azure_container_registry import (
    AzureContainerRegistryFlavor,
)
from zenml.container_registries.gcp_container_registry import (
    GCPContainerRegistryFlavor,
)
from zenml.enums import ContainerRegistryFlavor, StackComponentType
from zenml.exceptions import EntityExistsError
from zenml.integrations.aws import (
    AWS_CONNECTOR_TYPE,
    AWS_CONTAINER_REGISTRY_FLAVOR,
    AWS_RESOURCE_TYPE,
    AWS_SAGEMAKER_ORCHESTRATOR_FLAVOR,
    S3_RESOURCE_TYPE,
)
from zenml.integrations.aws.flavors.aws_container_registry_flavor import (
    AWSContainerRegistryFlavor,
)
from zenml.integrations.aws.flavors.sagemaker_orchestrator_flavor import (
    SagemakerOrchestratorFlavor,
)
from zenml.integrations.azure import (
    AZURE_ARTIFACT_STORE_FLAVOR,
    AZURE_CONNECTOR_TYPE,
    AZURE_RESOURCE_TYPE,
    AZUREML_ORCHESTRATOR_FLAVOR,
    BLOB_RESOURCE_TYPE,
)
from zenml.integrations.azure.flavors.azure_artifact_store_flavor import (
    AzureArtifactStoreFlavor,
)
from zenml.integrations.azure.flavors.azureml_orchestrator_flavor import (
    AzureMLOrchestratorFlavor,
)
from zenml.integrations.gcp import (
    GCP_ARTIFACT_STORE_FLAVOR,
    GCP_CONNECTOR_TYPE,
    GCP_RESOURCE_TYPE,
    GCP_VERTEX_ORCHESTRATOR_FLAVOR,
    GCS_RESOURCE_TYPE,
)
from zenml.integrations.gcp.flavors.gcp_artifact_store_flavor import (
    GCPArtifactStoreFlavor,
)
from zenml.integrations.gcp.flavors.vertex_orchestrator_flavor import (
    VertexOrchestratorFlavor,
)
from zenml.integrations.kubernetes.flavors.kubernetes_orchestrator_flavor import (
    KubernetesOrchestratorFlavor,
)
from zenml.integrations.s3.flavors.s3_artifact_store_flavor import (
    S3ArtifactStoreFlavor,
)
from zenml.models.v2.core.stack import StackRequest
from zenml.models.v2.misc.info_models import ComponentInfo, ServiceConnectorInfo
from zenml.orchestrators.local.local_orchestrator import LocalOrchestratorFlavor
from zenml.stack.utils import validate_stack_component_config

from kitaru._config import _sandbox_stack_components as _sandbox_components
from kitaru._modal_registry import (
    container_registry_host,
    ecr_region_from_registry,
    gcp_location_from_registry,
    infer_modal_registry_provider,
)
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError

_STACK_MANAGED_LABEL_KEY = "kitaru.managed"
_STACK_MANAGED_LABEL_VALUE = "true"
_STACK_REUSED_SERVICE_CONNECTORS_LABEL_KEY = "kitaru.reused_service_connectors"
_STACK_REUSED_SERVICE_CONNECTORS_LABEL_VALUE = "true"
MODAL_ORCHESTRATOR_FLAVOR = "modal"
_MODAL_INSTALL_HINT = (
    "Modal stack support requires the Modal Python dependencies. "
    'Install them with `uv add "kitaru[modal]"` or '
    '`pip install "kitaru[modal]"`, then rerun the Modal stack command.'
)


class StackInfo(BaseModel):
    """Public stack information exposed by Kitaru SDK helpers."""

    id: str
    name: str
    is_active: bool


class StackType(StrEnum):
    """Supported internal stack creation modes."""

    LOCAL = "local"
    KUBERNETES = "kubernetes"
    VERTEX = "vertex"
    SAGEMAKER = "sagemaker"
    AZUREML = "azureml"
    MODAL = "modal"


class CloudProvider(StrEnum):
    """Supported cloud providers for remote stacks."""

    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


class StackComponentTarget(StrEnum):
    """Logical stack component targets used for advanced config overrides."""

    ORCHESTRATOR = "orchestrator"
    ARTIFACT_STORE = "artifact_store"
    CONTAINER_REGISTRY = "container_registry"
    SANDBOX = "sandbox"


class StackComponentConfigOverrides(BaseModel):
    """Per-component config overrides applied during stack creation."""

    orchestrator: dict[str, Any] = Field(default_factory=dict)
    artifact_store: dict[str, Any] = Field(default_factory=dict)
    container_registry: dict[str, Any] = Field(default_factory=dict)
    sandbox: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")

    def is_empty(self) -> bool:
        """Return whether the override payload contains any component entries."""
        return not (
            self.orchestrator
            or self.artifact_store
            or self.container_registry
            or self.sandbox
        )


class KubernetesStackSpec(BaseModel):
    """Internal request model for Kubernetes stack creation."""

    provider: CloudProvider
    artifact_store: str
    container_registry: str
    cluster: str
    region: str
    namespace: str = "default"
    credentials: str | None = None
    verify: bool = True

    model_config = ConfigDict(extra="forbid")


class VertexStackSpec(BaseModel):
    """Request model for Vertex AI stack creation."""

    artifact_store: str
    container_registry: str
    region: str
    credentials: str | None = None
    verify: bool = True

    model_config = ConfigDict(extra="forbid")


class SagemakerStackSpec(BaseModel):
    """Request model for SageMaker stack creation."""

    artifact_store: str
    container_registry: str
    region: str
    execution_role: str
    credentials: str | None = None
    verify: bool = True

    model_config = ConfigDict(extra="forbid")


class AzureMLStackSpec(BaseModel):
    """Request model for future AzureML stack creation."""

    artifact_store: str
    container_registry: str
    subscription_id: str
    resource_group: str
    workspace: str
    region: str | None = None
    credentials: str | None = None
    verify: bool = True

    model_config = ConfigDict(extra="forbid")


class ModalStackSpec(BaseModel):
    """Request model for Modal stack creation."""

    artifact_store: str
    container_registry: str
    region: str | None = None
    subscription_id: str | None = None
    credentials: str | None = None
    verify: bool = True

    model_config = ConfigDict(extra="forbid")


RemoteStackSpec = (
    KubernetesStackSpec
    | VertexStackSpec
    | SagemakerStackSpec
    | AzureMLStackSpec
    | ModalStackSpec
)


@dataclass(frozen=True)
class _ResolvedConnectorSpec:
    """Resolved ZenML connector information for remote stack creation."""

    connector_info: ServiceConnectorInfo
    verify_resource_type: str


@dataclass(frozen=True)
class _ModalConnectorReference:
    """Existing service connector selected for one Modal component."""

    connector_id: UUID
    resource_id: str


@dataclass(frozen=True)
class _ResolvedModalExistingConnectors:
    """Existing Modal storage and registry service connectors."""

    artifact_store: _ModalConnectorReference
    container_registry: _ModalConnectorReference


@dataclass(frozen=True)
class _ModalConnectorLookup:
    """Connector discovery inputs for one Modal component."""

    component_label: str
    connector_type: str
    resource_type: str
    target_resource_id: str
    acceptable_resource_ids: frozenset[str]


@dataclass(frozen=True)
class _ModalConnectorDiscoveryResult:
    """Connector discovery result for one Modal component."""

    lookup: _ModalConnectorLookup
    scoped_matches: tuple[Any, ...]
    unscoped_matches: tuple[Any, ...]


@dataclass(frozen=True)
class _ComponentValidationMetadata:
    """Validation metadata for one Kitaru-managed ZenML component flavor."""

    config_class: type[BaseModel]
    docs_url: str | None


_StackComponentKind = Literal[
    "orchestrator",
    "artifact_store",
    "container_registry",
    "sandbox",
]


@dataclass(frozen=True)
class _StackComponent:
    """Internal reference to a stack-owned stack component."""

    component_id: str
    name: str
    kind: _StackComponentKind


@dataclass(frozen=True)
class _StackListEntry:
    """Internal structured stack list item with managed-state metadata."""

    stack: StackInfo
    is_managed: bool


@dataclass(frozen=True)
class _StackCreateResult:
    """Structured result for stack creation operations."""

    stack: StackInfo
    previous_active_stack: str | None
    components_created: tuple[str, ...]
    stack_type: str = StackType.LOCAL.value
    service_connectors_created: tuple[str, ...] = ()
    resources: dict[str, str] | None = None


@dataclass(frozen=True)
class _StackDeleteResult:
    """Structured result for stack deletion operations."""

    deleted_stack: str
    components_deleted: tuple[str, ...]
    new_active_stack: str | None
    recursive: bool


_STACK_COMPONENT_TARGET_TO_TYPE: dict[StackComponentTarget, StackComponentType] = {
    StackComponentTarget.ORCHESTRATOR: StackComponentType.ORCHESTRATOR,
    StackComponentTarget.ARTIFACT_STORE: StackComponentType.ARTIFACT_STORE,
    StackComponentTarget.CONTAINER_REGISTRY: StackComponentType.CONTAINER_REGISTRY,
    StackComponentTarget.SANDBOX: StackComponentType.SANDBOX,
}
_STACK_COMPONENT_TYPE_TO_TARGET: dict[StackComponentType, StackComponentTarget] = {
    component_type: target
    for target, component_type in _STACK_COMPONENT_TARGET_TO_TYPE.items()
}


def _build_component_validation_registry() -> dict[
    tuple[StackComponentType, str], _ComponentValidationMetadata
]:
    """Build validation metadata for the ZenML flavors Kitaru creates."""
    flavors = (
        LocalOrchestratorFlavor(),
        KubernetesOrchestratorFlavor(),
        VertexOrchestratorFlavor(),
        SagemakerOrchestratorFlavor(),
        AzureMLOrchestratorFlavor(),
        LocalArtifactStoreFlavor(),
        S3ArtifactStoreFlavor(),
        GCPArtifactStoreFlavor(),
        AzureArtifactStoreFlavor(),
        AWSContainerRegistryFlavor(),
        GCPContainerRegistryFlavor(),
        AzureContainerRegistryFlavor(),
    )
    return {
        (flavor.type, flavor.name): _ComponentValidationMetadata(
            config_class=flavor.config_class,
            docs_url=flavor.docs_url,
        )
        for flavor in flavors
    }


_COMPONENT_VALIDATION_METADATA = _build_component_validation_registry()


def _is_missing_modal_dependency(exc: BaseException) -> bool:
    """Return whether an exception represents a missing Modal package import."""
    if not isinstance(exc, ModuleNotFoundError):
        return False
    missing_name = getattr(exc, "name", None)
    return missing_name == "modal" or (
        isinstance(missing_name, str) and missing_name.startswith("modal.")
    )


def _exception_chain_has_missing_modal_dependency(exc: BaseException) -> bool:
    """Return whether any exception in the chain is a missing Modal import."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if _is_missing_modal_dependency(current):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


@lru_cache(maxsize=1)
def _load_modal_component_validation_metadata() -> dict[
    tuple[StackComponentType, str], _ComponentValidationMetadata
]:
    """Load Modal validation metadata only when Modal support is requested."""
    try:
        __import__("modal")
        from zenml.integrations.modal.flavors.modal_orchestrator_flavor import (
            ModalOrchestratorFlavor,
        )
        from zenml.integrations.modal.flavors.modal_sandbox_flavor import (
            ModalSandboxFlavor,
        )
    except ModuleNotFoundError as exc:
        if _is_missing_modal_dependency(exc):
            raise KitaruUsageError(_MODAL_INSTALL_HINT) from exc
        raise

    flavors = (ModalOrchestratorFlavor(), ModalSandboxFlavor())
    return {
        (flavor.type, flavor.name): _ComponentValidationMetadata(
            config_class=flavor.config_class,
            docs_url=flavor.docs_url,
        )
        for flavor in flavors
    }


def _require_modal_stack_support() -> None:
    """Raise a clear Kitaru error when Modal stack dependencies are missing."""
    try:
        __import__("modal")
        _load_modal_component_validation_metadata()
    except ModuleNotFoundError as exc:
        if _is_missing_modal_dependency(exc):
            raise KitaruUsageError(_MODAL_INSTALL_HINT) from exc
        raise
    except KitaruUsageError:
        raise


def _merge_configuration_dicts(
    base: Mapping[str, Any],
    overrides: Mapping[str, Any],
) -> dict[str, Any]:
    """Recursively merge two configuration mappings without mutating inputs."""
    merged = {key: value for key, value in base.items()}
    for key, value in overrides.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_configuration_dicts(existing, value)
        else:
            merged[key] = value
    return merged


def _component_override_values(
    overrides: StackComponentConfigOverrides | None,
    target: StackComponentTarget,
) -> dict[str, Any]:
    """Return override values for one component target."""
    if overrides is None:
        return {}
    return dict(getattr(overrides, target.value))


def _build_component_configuration(
    base: Mapping[str, Any],
    *,
    overrides: StackComponentConfigOverrides | None,
    target: StackComponentTarget,
) -> dict[str, Any]:
    """Merge base component config with user-provided overrides."""
    return _merge_configuration_dicts(
        base, _component_override_values(overrides, target)
    )


def _format_validation_path(path: tuple[Any, ...] | list[Any] | Any) -> str:
    """Render a validation location path for user-facing errors."""
    if isinstance(path, (tuple, list)):
        return ".".join(str(part) for part in path if part not in {"__root__", ""})
    return str(path)


def _format_option_list(option_paths: list[str]) -> str:
    """Render one or more invalid option paths for an error message."""
    return ", ".join(f"`{path}`" for path in option_paths)


def _format_component_validation_help(
    docs_url: str | None,
    *,
    suggestion: str | None = None,
) -> str:
    """Render optional help text for rewritten component-validation errors."""
    parts: list[str] = []
    if suggestion is not None:
        parts.append(f"Did you mean `{suggestion}`?")
    if docs_url:
        parts.append(f"See ZenML docs for supported fields: {docs_url}")
    return " ".join(parts)


def _component_validation_metadata(
    *,
    target: StackComponentTarget,
    flavor: str,
) -> _ComponentValidationMetadata | None:
    """Return validation metadata for one managed component flavor."""
    component_type = _STACK_COMPONENT_TARGET_TO_TYPE[target]
    metadata = _COMPONENT_VALIDATION_METADATA.get((component_type, flavor))
    if metadata is not None:
        return metadata

    if flavor == MODAL_ORCHESTRATOR_FLAVOR:
        modal_metadata = _load_modal_component_validation_metadata().get(
            (component_type, flavor)
        )
        if modal_metadata is not None:
            return modal_metadata

    if target == StackComponentTarget.SANDBOX:
        sandbox_metadata = _sandbox_components.local_sandbox_validation_metadata(
            flavor=flavor,
        )
        if sandbox_metadata is None:
            return None
        if sandbox_metadata.component_type != StackComponentType.SANDBOX:
            return None
        return _ComponentValidationMetadata(
            config_class=sandbox_metadata.config_class,
            docs_url=sandbox_metadata.docs_url,
        )

    return None


def _rewrite_invalid_component_options(
    *,
    target: StackComponentTarget,
    flavor: str,
    invalid_paths: list[str],
    metadata: _ComponentValidationMetadata | None,
) -> str:
    """Build a user-facing error for unknown ZenML component options."""
    suggestion: str | None = None
    if metadata is not None and len(invalid_paths) == 1:
        invalid_leaf = invalid_paths[0].split(".")[-1]
        suggestions = difflib.get_close_matches(
            invalid_leaf,
            list(metadata.config_class.model_fields),
            n=1,
        )
        suggestion = suggestions[0] if suggestions else None

    help_text = _format_component_validation_help(
        metadata.docs_url if metadata is not None else None,
        suggestion=suggestion,
    )
    message = (
        f"Invalid ZenML option for {target.value} ({flavor} flavor): "
        f"{_format_option_list(invalid_paths)}"
    )
    if help_text:
        message = f"{message}. {help_text}"
    return message


def _rewrite_component_validation_error(
    *,
    target: StackComponentTarget,
    flavor: str,
    exc: ValidationError,
    metadata: _ComponentValidationMetadata | None,
) -> str:
    """Rewrite a Pydantic validation error for one component config."""
    errors = exc.errors()
    extra_paths = sorted(
        {
            _format_validation_path(error["loc"])
            for error in errors
            if error.get("type") == "extra_forbidden"
        }
    )
    if extra_paths:
        return _rewrite_invalid_component_options(
            target=target,
            flavor=flavor,
            invalid_paths=extra_paths,
            metadata=metadata,
        )

    first_error = errors[0]
    field_path = _format_validation_path(first_error.get("loc", ()))
    detail = first_error.get("msg", str(exc))
    message = f"Invalid ZenML value for {target.value} ({flavor} flavor)"
    if field_path:
        message += f" at `{field_path}`"
    message += f": {detail}"
    help_text = _format_component_validation_help(
        metadata.docs_url if metadata is not None else None,
    )
    if help_text:
        message = f"{message}. {help_text}"
    return message


def _rewrite_component_value_error(
    *,
    target: StackComponentTarget,
    flavor: str,
    exc: ValueError,
    metadata: _ComponentValidationMetadata | None,
) -> str:
    """Rewrite a generic component-config validation ValueError."""
    message = f"Invalid ZenML configuration for {target.value} ({flavor} flavor): {exc}"
    help_text = _format_component_validation_help(
        metadata.docs_url if metadata is not None else None,
    )
    if help_text:
        message = f"{message}. {help_text}"
    return message


def _prevalidate_component_configuration(
    *,
    target: StackComponentTarget,
    flavor: str,
    configuration: Mapping[str, Any] | None,
) -> None:
    """Validate one component config and rewrite ZenML errors for Kitaru users."""
    config_dict = dict(configuration or {})
    metadata = _component_validation_metadata(target=target, flavor=flavor)
    if metadata is not None:
        extra_keys = sorted(set(config_dict) - set(metadata.config_class.model_fields))
        if extra_keys:
            raise KitaruUsageError(
                _rewrite_invalid_component_options(
                    target=target,
                    flavor=flavor,
                    invalid_paths=extra_keys,
                    metadata=metadata,
                )
            )

    component_type = _STACK_COMPONENT_TARGET_TO_TYPE[target]
    try:
        validate_stack_component_config(
            configuration_dict=config_dict,
            flavor=flavor,
            component_type=component_type,
            validate_custom_flavors=True,
        )
    except ValidationError as exc:
        raise KitaruUsageError(
            _rewrite_component_validation_error(
                target=target,
                flavor=flavor,
                exc=exc,
                metadata=metadata,
            )
        ) from exc
    except ValueError as exc:
        raise KitaruUsageError(
            _rewrite_component_value_error(
                target=target,
                flavor=flavor,
                exc=exc,
                metadata=metadata,
            )
        ) from exc


def _prevalidate_stack_request_components(stack_request: StackRequest) -> None:
    """Prevalidate all inline component configs inside a stack request."""
    if stack_request.components is None:
        return

    for component_type, components in stack_request.components.items():
        target = _STACK_COMPONENT_TYPE_TO_TARGET.get(component_type)
        if target is None:
            continue
        for component in components:
            if not isinstance(component, ComponentInfo):
                continue
            raw_flavor = component.flavor
            if isinstance(raw_flavor, str):
                flavor = raw_flavor
            else:
                flavor = str(getattr(raw_flavor, "value", raw_flavor))
            _prevalidate_component_configuration(
                target=target,
                flavor=flavor,
                configuration=component.configuration,
            )


_StackShowType = Literal[
    "local",
    "kubernetes",
    "vertex",
    "sagemaker",
    "azureml",
    "modal",
    "custom",
]
_StackComponentRole = Literal[
    "runner",
    "storage",
    "image_registry",
    "sandbox",
    "additional_component",
]


@dataclass(frozen=True)
class StackComponentDetails:
    """Translated stack-component metadata for stack inspection surfaces."""

    role: _StackComponentRole
    name: str
    backend: str | None = None
    details: tuple[tuple[str, str], ...] = ()
    purpose: str | None = None


@dataclass(frozen=True)
class StackDetails:
    """Structured stack inspection result for `stack show` style commands."""

    stack: StackInfo
    is_managed: bool
    stack_type: _StackShowType
    components: tuple[StackComponentDetails, ...]


def _infer_gcp_project_id_from_container_registry(container_registry: str) -> str:
    """Infer the GCP project ID from a GAR or GCR container registry URI."""
    normalized_registry = container_registry.strip()
    if not normalized_registry:
        raise KitaruUsageError("Container registry URI cannot be empty.")

    normalized_registry = re.sub(r"^[a-z]+://", "", normalized_registry)
    normalized_registry = normalized_registry.rstrip("/")
    host, _, raw_path = normalized_registry.partition("/")
    path_parts = [part for part in raw_path.split("/") if part]

    gar_hosts = {"docker.pkg.dev"}
    gcr_hosts = {"gcr.io", "us.gcr.io", "eu.gcr.io", "asia.gcr.io"}
    if (
        host in gar_hosts or host.endswith("-docker.pkg.dev") or host in gcr_hosts
    ) and path_parts:
        return path_parts[0]

    raise KitaruUsageError(
        "Cannot infer GCP project ID from container registry URI "
        f"'{container_registry}'. Use an Artifact Registry or GCR URI that "
        "includes the project segment."
    )


def _artifact_store_resource_id(
    artifact_store_uri: str,
    provider: CloudProvider,
) -> str:
    """Return the canonical connector resource ID for an artifact store URI."""
    parsed = urlparse(artifact_store_uri)
    if provider == CloudProvider.AWS and parsed.scheme == "s3" and parsed.netloc:
        return f"s3://{parsed.netloc}"
    if provider == CloudProvider.GCP and parsed.scheme == "gs" and parsed.netloc:
        return f"gs://{parsed.netloc}"
    if (
        provider == CloudProvider.AZURE
        and parsed.scheme in {"az", "abfs", "abfss"}
        and parsed.netloc
    ):
        return f"{parsed.scheme}://{parsed.netloc}"
    raise KitaruUsageError(
        f"Unsupported artifact store URI '{artifact_store_uri}' for provider "
        f"'{provider.value}'."
    )


def _normalize_azure_artifact_store_uri(artifact_store_uri: str) -> str:
    """Normalize Azure artifact store URIs to ZenML's supported schemes."""
    if artifact_store_uri.startswith("abfss://"):
        return "abfs://" + artifact_store_uri.removeprefix("abfss://")
    return artifact_store_uri


def _infer_cloud_provider_from_artifact_store(
    artifact_store_uri: str,
) -> CloudProvider:
    """Infer a cloud provider from an artifact-store URI."""
    parsed = urlparse(artifact_store_uri.strip())
    if parsed.scheme == "s3" and parsed.netloc:
        return CloudProvider.AWS
    if parsed.scheme == "gs" and parsed.netloc:
        return CloudProvider.GCP
    if parsed.scheme in {"az", "abfs", "abfss"} and parsed.netloc:
        return CloudProvider.AZURE
    raise KitaruUsageError(
        f"Cannot infer cloud provider from artifact store URI '{artifact_store_uri}'. "
        "Use an s3://, gs://, az://, abfs://, or abfss:// URI."
    )


def _artifact_store_flavor(provider: CloudProvider) -> str:
    """Return the provider-specific artifact-store component flavor."""
    return {
        CloudProvider.AWS: "s3",
        CloudProvider.GCP: GCP_ARTIFACT_STORE_FLAVOR,
        CloudProvider.AZURE: AZURE_ARTIFACT_STORE_FLAVOR,
    }[provider]


def _container_registry_flavor(provider: CloudProvider) -> str:
    """Return the provider-specific container-registry component flavor."""
    return {
        CloudProvider.AWS: AWS_CONTAINER_REGISTRY_FLAVOR,
        CloudProvider.GCP: ContainerRegistryFlavor.GCP.value,
        CloudProvider.AZURE: ContainerRegistryFlavor.AZURE.value,
    }[provider]


def _container_registry_resource_id(
    container_registry: str,
    provider: CloudProvider,
) -> str:
    """Return the connector resource ID for a container registry URI."""
    try:
        host = container_registry_host(container_registry)
    except ValueError as exc:
        raise KitaruUsageError(str(exc)) from exc

    if provider in {CloudProvider.AWS, CloudProvider.AZURE}:
        return host
    normalized_registry = re.sub(r"^[a-z]+://", "", container_registry.strip())
    return normalized_registry.rstrip("/")


def _normalize_modal_connector_resource_id(resource_id: str | None) -> str | None:
    """Normalize a service connector resource ID for tolerant comparisons."""
    if resource_id is None:
        return None
    normalized = resource_id.strip()
    if not normalized:
        return None
    normalized = re.sub(r"^[a-z]+://", "", normalized)
    return normalized.rstrip("/")


def _modal_artifact_store_resource_id_variants(
    resource_id: str,
    provider: CloudProvider,
) -> frozenset[str]:
    """Return acceptable connector resource IDs for a Modal artifact store."""
    parsed = urlparse(resource_id)
    variants = {resource_id.rstrip("/")}
    if parsed.netloc:
        variants.add(parsed.netloc)
    return frozenset(
        normalized
        for value in variants
        if (normalized := _normalize_modal_connector_resource_id(value)) is not None
    )


def _modal_container_registry_resource_id_variants(
    container_registry: str,
    resource_id: str,
) -> frozenset[str]:
    """Return acceptable connector resource IDs for a Modal registry."""
    variants = {resource_id.rstrip("/")}
    normalized_registry = re.sub(r"^[a-z]+://", "", container_registry.strip())
    normalized_registry = normalized_registry.rstrip("/")
    if normalized_registry:
        variants.add(normalized_registry)
    with suppress(ValueError):
        variants.add(container_registry_host(container_registry))
    return frozenset(
        normalized
        for value in variants
        if (normalized := _normalize_modal_connector_resource_id(value)) is not None
    )


def _modal_connector_type(provider: CloudProvider) -> str:
    """Return the provider-specific service connector type."""
    return {
        CloudProvider.AWS: AWS_CONNECTOR_TYPE,
        CloudProvider.GCP: GCP_CONNECTOR_TYPE,
        CloudProvider.AZURE: AZURE_CONNECTOR_TYPE,
    }[provider]


def _modal_artifact_store_connector_resource_type(provider: CloudProvider) -> str:
    """Return the provider-specific artifact-store connector resource type."""
    return {
        CloudProvider.AWS: S3_RESOURCE_TYPE,
        CloudProvider.GCP: GCS_RESOURCE_TYPE,
        CloudProvider.AZURE: BLOB_RESOURCE_TYPE,
    }[provider]


def _modal_connector_type_matches(connector: Any, connector_type: str) -> bool:
    """Return whether a connector model reports the expected connector type."""
    raw_type = getattr(connector, "type", None)
    if raw_type is None:
        raw_type = getattr(connector, "connector_type", None)
    if raw_type is None:
        return True
    if not isinstance(raw_type, str) and hasattr(raw_type, "connector_type"):
        raw_type = raw_type.connector_type
    return str(raw_type) == connector_type


def _modal_connector_resource_type_matches(connector: Any, resource_type: str) -> bool:
    """Return whether a connector model supports the expected resource type."""
    raw_resource_types = getattr(connector, "resource_types", None)
    if raw_resource_types is None:
        raw_resource_type = getattr(connector, "resource_type", None)
        if raw_resource_type is None:
            return True
        raw_resource_types = [raw_resource_type]
    if isinstance(raw_resource_types, str):
        connector_resource_types = {raw_resource_types}
    else:
        connector_resource_types = {str(item) for item in raw_resource_types}
    return resource_type in connector_resource_types


def _modal_connector_resource_id(connector: Any) -> str | None:
    """Return a connector resource ID normalized for discovery matching."""
    return _normalize_modal_connector_resource_id(
        getattr(connector, "resource_id", None)
    )


def _modal_connector_label(connector: Any) -> str:
    """Render a connector for user-facing discovery errors."""
    name = _normalize_stack_detail_value(getattr(connector, "name", None))
    connector_id = _normalize_stack_detail_value(getattr(connector, "id", None))
    resource_id = _normalize_stack_detail_value(getattr(connector, "resource_id", None))
    label = name or connector_id or "<unnamed connector>"
    if connector_id is not None and connector_id != label:
        label = f"{label} ({connector_id})"
    if resource_id is not None:
        label = f"{label} for {resource_id}"
    return label


def _modal_existing_connector_reference(
    connector: Any,
    resource_id: str,
) -> _ModalConnectorReference:
    """Build a stack-request connector reference from a ZenML connector model."""
    connector_id_raw = getattr(connector, "id", None)
    try:
        connector_id = UUID(str(connector_id_raw))
    except (TypeError, ValueError, AttributeError) as exc:
        raise KitaruBackendError(
            "ZenML returned a matching service connector without a UUID id: "
            f"{_modal_connector_label(connector)}"
        ) from exc
    return _ModalConnectorReference(
        connector_id=connector_id,
        resource_id=resource_id,
    )


def _collect_modal_connector_candidates(
    client: Client,
    lookup: _ModalConnectorLookup,
) -> _ModalConnectorDiscoveryResult:
    """Collect scoped and unscoped Modal connector candidates."""
    scoped_matches: list[Any] = []
    unscoped_matches: list[Any] = []
    page = 1
    page_size = 100
    while True:
        connector_page = client.list_service_connectors(
            connector_type=lookup.connector_type,
            resource_type=lookup.resource_type,
            page=page,
            size=page_size,
            hydrate=True,
        )
        if not isinstance(connector_page, Iterable) or isinstance(
            connector_page,
            (str, bytes),
        ):
            raise KitaruStateError(
                "Unexpected service connector list response from the configured "
                "runtime."
            )
        for connector in connector_page:
            if not _modal_connector_type_matches(connector, lookup.connector_type):
                continue
            if not _modal_connector_resource_type_matches(
                connector, lookup.resource_type
            ):
                continue

            connector_resource_id = _modal_connector_resource_id(connector)
            if connector_resource_id is None:
                unscoped_matches.append(connector)
            elif connector_resource_id in lookup.acceptable_resource_ids:
                scoped_matches.append(connector)

        total_pages_raw = getattr(connector_page, "total_pages", 1)
        try:
            total_pages = int(total_pages_raw)
        except (TypeError, ValueError):
            total_pages = 1
        if page >= total_pages:
            break
        page += 1
    return _ModalConnectorDiscoveryResult(
        lookup=lookup,
        scoped_matches=tuple(scoped_matches),
        unscoped_matches=tuple(unscoped_matches),
    )


def _selected_modal_connector_matches(
    discovery: _ModalConnectorDiscoveryResult,
) -> tuple[Any, ...]:
    """Return the connector candidates used after scoped/unscoped precedence."""
    if discovery.scoped_matches:
        return discovery.scoped_matches
    return discovery.unscoped_matches


def _modal_connector_lookup_message(lookup: _ModalConnectorLookup) -> str:
    """Render what Kitaru looked for during existing connector discovery."""
    return (
        f"{lookup.component_label} connector type '{lookup.connector_type}', "
        f"resource type '{lookup.resource_type}', resource ID "
        f"'{lookup.target_resource_id}'"
    )


def _resolve_modal_existing_connectors(
    spec: ModalStackSpec,
    *,
    provider: CloudProvider,
    client: Client,
) -> _ResolvedModalExistingConnectors | None:
    """Find existing server-side service connectors for a Modal stack."""
    if _modal_cloud_connector_inputs_requested(spec):
        return None

    artifact_store_uri = (
        _normalize_azure_artifact_store_uri(spec.artifact_store)
        if provider == CloudProvider.AZURE
        else spec.artifact_store
    )
    artifact_resource_id = _artifact_store_resource_id(artifact_store_uri, provider)
    registry_resource_id = _container_registry_resource_id(
        spec.container_registry,
        provider,
    )
    connector_type = _modal_connector_type(provider)
    lookups = (
        _ModalConnectorLookup(
            component_label="artifact store",
            connector_type=connector_type,
            resource_type=_modal_artifact_store_connector_resource_type(provider),
            target_resource_id=artifact_resource_id,
            acceptable_resource_ids=_modal_artifact_store_resource_id_variants(
                artifact_resource_id,
                provider,
            ),
        ),
        _ModalConnectorLookup(
            component_label="container registry",
            connector_type=connector_type,
            resource_type=DOCKER_REGISTRY_RESOURCE_TYPE,
            target_resource_id=registry_resource_id,
            acceptable_resource_ids=_modal_container_registry_resource_id_variants(
                spec.container_registry,
                registry_resource_id,
            ),
        ),
    )
    discoveries = tuple(
        _collect_modal_connector_candidates(client, lookup) for lookup in lookups
    )
    selected_matches = tuple(
        _selected_modal_connector_matches(discovery) for discovery in discoveries
    )
    if all(not matches for matches in selected_matches):
        return None

    scoped_ambiguous = [
        discovery for discovery in discoveries if len(discovery.scoped_matches) > 1
    ]
    unscoped_ambiguous = [
        discovery
        for discovery in discoveries
        if not discovery.scoped_matches and len(discovery.unscoped_matches) > 1
    ]
    ambiguous = [*scoped_ambiguous, *unscoped_ambiguous]
    if ambiguous:
        details = "; ".join(
            f"{_modal_connector_lookup_message(discovery.lookup)} matched "
            f"{len(_selected_modal_connector_matches(discovery))} connectors: "
            + ", ".join(
                _modal_connector_label(match)
                for match in _selected_modal_connector_matches(discovery)
            )
            for discovery in ambiguous
        )
        raise KitaruUsageError(
            "Kitaru found ambiguous existing service connectors for this Modal "
            f"stack. {details}. Narrow the connectors to one matching resource "
            "or pass explicit cloud credentials so Kitaru creates a new connector."
        )

    missing = [
        discovery
        for discovery, matches in zip(discoveries, selected_matches, strict=True)
        if len(matches) == 0
    ]
    if missing:
        found = [
            (discovery, matches[0])
            for discovery, matches in zip(discoveries, selected_matches, strict=True)
            if len(matches) == 1
        ]
        found_text = "; ".join(
            f"found {_modal_connector_label(match)} for "
            f"{discovery.lookup.component_label}"
            for discovery, match in found
        )
        missing_text = "; ".join(
            _modal_connector_lookup_message(discovery.lookup) for discovery in missing
        )
        raise KitaruUsageError(
            "Kitaru found only part of the existing service connector pair needed "
            f"for this Modal stack: {found_text}. Missing: {missing_text}. "
            "Create the missing server-side ZenML service connector, remove the "
            "partial match if this should be connectorless, or pass explicit cloud "
            "credentials so Kitaru creates a new connector."
        )

    return _ResolvedModalExistingConnectors(
        artifact_store=_modal_existing_connector_reference(
            selected_matches[0][0],
            artifact_resource_id,
        ),
        container_registry=_modal_existing_connector_reference(
            selected_matches[1][0],
            registry_resource_id,
        ),
    )


def _modal_cloud_connector_inputs_requested(spec: ModalStackSpec) -> bool:
    """Return whether a Modal stack asks Kitaru to create cloud credentials."""
    return bool(
        spec.region is not None
        or spec.subscription_id is not None
        or spec.credentials is not None
    )


def _validate_modal_cloud_connector_inputs(
    *,
    provider: CloudProvider,
    spec: ModalStackSpec,
) -> None:
    """Reject Modal cloud credential inputs that cannot match the stack resources."""
    try:
        registry_provider = infer_modal_registry_provider(spec.container_registry)
    except ValueError as exc:
        raise KitaruUsageError(str(exc)) from exc
    if registry_provider != provider.value:
        raise KitaruUsageError(
            "Modal stacks must use an artifact store and container registry "
            "from the same cloud provider. "
            f"Artifact store '{spec.artifact_store}' is {provider.value}, but "
            f"registry '{spec.container_registry}' is {registry_provider}."
        )

    if not _modal_cloud_connector_inputs_requested(spec):
        if not spec.verify:
            raise KitaruUsageError(
                "`verify=False` only applies when Kitaru is creating a cloud "
                "connector for a Modal stack. Add the needed cloud connector "
                "input, such as `region`, `subscription_id`, or `credentials`, "
                "or remove `verify=False`."
            )
        return

    if provider == CloudProvider.AWS:
        if spec.subscription_id is not None:
            raise KitaruUsageError(
                "AWS-backed Modal stacks do not use `subscription_id`. Use "
                "`region` for the AWS/ECR region, or use an Azure artifact store "
                "and ACR registry for Azure credentials."
            )
        if spec.region is None:
            raise KitaruUsageError(
                "AWS-backed Modal cloud credentials require `region`. This is "
                "the AWS/ECR region for the S3/ECR connector. Modal placement "
                "still uses `--extra orchestrator.region=...`."
            )
        ecr_region = ecr_region_from_registry(spec.container_registry)
        if ecr_region is not None and ecr_region != spec.region:
            raise KitaruUsageError(
                "AWS-backed Modal cloud credentials must use the same region as "
                "the ECR registry host. "
                f"`region` was '{spec.region}', but the registry host uses "
                f"'{ecr_region}'. Modal placement still uses "
                "`--extra orchestrator.region=...`."
            )
        return

    if provider == CloudProvider.GCP:
        if spec.subscription_id is not None:
            raise KitaruUsageError(
                "GCP-backed Modal stacks do not use `subscription_id`. Use a "
                "gs:// artifact store and a GAR/GCR registry; Kitaru infers the "
                "GCP project from the registry URI."
            )
        gcp_location = gcp_location_from_registry(spec.container_registry)
        if (
            spec.region is not None
            and gcp_location is not None
            and spec.region != gcp_location
        ):
            raise KitaruUsageError(
                "GCP-backed Modal cloud credentials must use a `region` that "
                "matches the GAR/GCR registry host when both are provided. "
                f"`region` was '{spec.region}', but the registry host uses "
                f"'{gcp_location}'. Modal placement still uses "
                "`--extra orchestrator.region=...`."
            )
        return

    if provider == CloudProvider.AZURE:
        if spec.subscription_id is None:
            raise KitaruUsageError(
                "Azure-backed Modal cloud credentials require `subscription_id` "
                "so Kitaru can create the Azure service connector for Blob/ADLS "
                "and ACR. Modal placement still uses "
                "`--extra orchestrator.region=...`."
            )
        return


def _resolve_modal_connector_spec(
    spec: ModalStackSpec,
    *,
    provider: CloudProvider,
) -> _ResolvedConnectorSpec | None:
    """Translate optional Modal cloud credential inputs into a connector spec."""
    _validate_modal_cloud_connector_inputs(provider=provider, spec=spec)
    if not _modal_cloud_connector_inputs_requested(spec):
        return None

    if provider == CloudProvider.AWS:
        assert spec.region is not None
        return _resolve_aws_connector_spec(
            region=spec.region,
            credentials=spec.credentials,
        )
    if provider == CloudProvider.GCP:
        return _resolve_gcp_connector_spec(
            container_registry=spec.container_registry,
            credentials=spec.credentials,
        )
    if provider == CloudProvider.AZURE:
        assert spec.subscription_id is not None
        return _resolve_azure_connector_spec(
            subscription_id=spec.subscription_id,
            credentials=spec.credentials,
        )
    raise KitaruUsageError(f"Unsupported cloud provider: {provider}")


def _merge_managed_labels(labels: dict[str, str] | None) -> dict[str, str]:
    """Ensure stack labels always include Kitaru's managed marker."""
    merged_labels = dict(labels or {})
    merged_labels[_STACK_MANAGED_LABEL_KEY] = _STACK_MANAGED_LABEL_VALUE
    return merged_labels


def _resolve_aws_connector_spec(
    *,
    region: str,
    credentials: str | None,
) -> _ResolvedConnectorSpec:
    """Translate Kitaru AWS credentials into ZenML connector info."""
    normalized_credentials = credentials.strip() if credentials else None
    auth_method = "implicit"
    configuration: dict[str, Any] = {"region": region}

    if normalized_credentials:
        method, separator, raw_value = normalized_credentials.partition(":")
        if not separator:
            raise KitaruUsageError(
                "Invalid AWS credentials format. Use one of: "
                "aws-profile:PROFILE, aws-access-keys:KEY:SECRET, "
                "aws-session-token:KEY:SECRET:TOKEN."
            )

        normalized_method = method.strip().lower()
        credential_value = raw_value.strip()
        if normalized_method == "aws-profile":
            if not credential_value:
                raise KitaruUsageError("AWS profile name cannot be empty.")
            configuration["profile_name"] = credential_value
        elif normalized_method in {"aws-access-key", "aws-access-keys"}:
            access_key_id, middle, secret_access_key = credential_value.partition(":")
            if not middle or not access_key_id.strip() or not secret_access_key.strip():
                raise KitaruUsageError(
                    "aws-access-keys credentials must be in the format "
                    "aws-access-keys:ACCESS_KEY_ID:SECRET_ACCESS_KEY."
                )
            auth_method = "secret-key"
            configuration.update(
                {
                    "aws_access_key_id": access_key_id.strip(),
                    "aws_secret_access_key": secret_access_key.strip(),
                }
            )
        elif normalized_method == "aws-session-token":
            access_key_id, first_sep, remainder = credential_value.partition(":")
            secret_access_key, second_sep, session_token = remainder.partition(":")
            if (
                not first_sep
                or not second_sep
                or not access_key_id.strip()
                or not secret_access_key.strip()
                or not session_token.strip()
            ):
                raise KitaruUsageError(
                    "aws-session-token credentials must be in the format "
                    "aws-session-token:ACCESS_KEY_ID:SECRET_ACCESS_KEY:SESSION_TOKEN."
                )
            auth_method = "sts-token"
            configuration.update(
                {
                    "aws_access_key_id": access_key_id.strip(),
                    "aws_secret_access_key": secret_access_key.strip(),
                    "aws_session_token": session_token.strip(),
                }
            )
        else:
            raise KitaruUsageError(
                "Unsupported AWS credentials method. Use one of: "
                "aws-profile, aws-access-keys, aws-session-token."
            )

    return _ResolvedConnectorSpec(
        connector_info=ServiceConnectorInfo(
            type=AWS_CONNECTOR_TYPE,
            auth_method=auth_method,
            configuration=dict(configuration),
        ),
        verify_resource_type=AWS_RESOURCE_TYPE,
    )


def _resolve_gcp_connector_spec(
    *,
    container_registry: str,
    credentials: str | None,
) -> _ResolvedConnectorSpec:
    """Translate Kitaru GCP credentials into ZenML connector info."""
    project_id = _infer_gcp_project_id_from_container_registry(container_registry)
    normalized_credentials = credentials.strip() if credentials else None
    auth_method = "implicit"
    configuration: dict[str, Any] = {"project_id": project_id}

    if normalized_credentials:
        method, separator, raw_value = normalized_credentials.partition(":")
        if not separator:
            raise KitaruUsageError(
                "Invalid GCP credentials format. Use "
                "gcp-service-account:/path/to/key.json."
            )
        normalized_method = method.strip().lower()
        if normalized_method != "gcp-service-account":
            raise KitaruUsageError(
                "Unsupported GCP credentials method. Use: gcp-service-account."
            )

        credential_path_raw = raw_value.strip()
        if not credential_path_raw:
            raise KitaruUsageError("GCP service account file path cannot be empty.")
        credential_path = Path(credential_path_raw).expanduser()
        try:
            service_account_json = credential_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise KitaruUsageError(
                f"Unable to read GCP service account file '{credential_path}': {exc}"
            ) from exc

        auth_method = "service-account"
        configuration.update({"service_account_json": service_account_json})

    return _ResolvedConnectorSpec(
        connector_info=ServiceConnectorInfo(
            type=GCP_CONNECTOR_TYPE,
            auth_method=auth_method,
            configuration=dict(configuration),
        ),
        verify_resource_type=GCP_RESOURCE_TYPE,
    )


def _resolve_azure_connector_spec(
    *,
    subscription_id: str,
    credentials: str | None,
) -> _ResolvedConnectorSpec:
    """Translate Kitaru Azure credentials into ZenML connector info."""
    normalized_credentials = credentials.strip() if credentials else None
    auth_method = "implicit"
    configuration: dict[str, Any] = {"subscription_id": subscription_id}

    if normalized_credentials:
        if normalized_credentials.lower() == "implicit":
            pass
        else:
            method, separator, raw_value = normalized_credentials.partition(":")
            if not separator:
                raise KitaruUsageError(
                    "Invalid Azure credentials format. Use one of: implicit, "
                    "azure-service-principal:TENANT_ID:CLIENT_ID:CLIENT_SECRET, "
                    "azure-access-token:TOKEN."
                )

            normalized_method = method.strip().lower()
            credential_value = raw_value.strip()
            if normalized_method == "azure-service-principal":
                tenant_id, first_sep, remainder = credential_value.partition(":")
                client_id, second_sep, client_secret = remainder.partition(":")
                if (
                    not first_sep
                    or not second_sep
                    or not tenant_id.strip()
                    or not client_id.strip()
                    or not client_secret.strip()
                ):
                    raise KitaruUsageError(
                        "azure-service-principal credentials must be in the format "
                        "azure-service-principal:TENANT_ID:CLIENT_ID:CLIENT_SECRET."
                    )
                auth_method = "service-principal"
                configuration.update(
                    {
                        "tenant_id": tenant_id.strip(),
                        "client_id": client_id.strip(),
                        "client_secret": client_secret.strip(),
                    }
                )
            elif normalized_method == "azure-access-token":
                if not credential_value:
                    raise KitaruUsageError("Azure access token cannot be empty.")
                auth_method = "access-token"
                configuration["token"] = credential_value
            else:
                raise KitaruUsageError(
                    "Unsupported Azure credentials method. Use one of: "
                    "implicit, azure-service-principal, azure-access-token."
                )

    return _ResolvedConnectorSpec(
        connector_info=ServiceConnectorInfo(
            type=AZURE_CONNECTOR_TYPE,
            auth_method=auth_method,
            configuration=dict(configuration),
        ),
        verify_resource_type=AZURE_RESOURCE_TYPE,
    )


def _resolve_kubernetes_connector_spec(
    spec: KubernetesStackSpec,
) -> _ResolvedConnectorSpec:
    """Translate Kitaru's Kubernetes credentials into ZenML connector info."""
    if spec.provider == CloudProvider.AWS:
        return _resolve_aws_connector_spec(
            region=spec.region,
            credentials=spec.credentials,
        )

    if spec.provider == CloudProvider.GCP:
        return _resolve_gcp_connector_spec(
            container_registry=spec.container_registry,
            credentials=spec.credentials,
        )

    raise KitaruUsageError(f"Unsupported cloud provider: {spec.provider}")


def _build_connector_services_list(
    connector_spec: _ResolvedConnectorSpec,
) -> list[UUID | ServiceConnectorInfo]:
    """Build the service_connectors list shared by all remote stack requests."""
    return [
        ServiceConnectorInfo(
            type=connector_spec.connector_info.type,
            auth_method=connector_spec.connector_info.auth_method,
            configuration=dict(connector_spec.connector_info.configuration),
        )
    ]


def _add_remote_sandbox_component_info(
    components: MutableMapping[StackComponentType, list[UUID | ComponentInfo]],
    *,
    sandbox_flavor: str | None,
    component_overrides: StackComponentConfigOverrides | None,
) -> None:
    """Attach a remote sandbox component when the stack request asks for one."""
    sandbox_overrides = _component_override_values(
        component_overrides,
        StackComponentTarget.SANDBOX,
    )
    if sandbox_flavor is None:
        if sandbox_overrides:
            raise KitaruUsageError(
                "`sandbox` component overrides require `sandbox_flavor` because "
                "remote stacks do not create a sandbox unless a sandbox flavor "
                "is selected."
            )
        return

    configuration = _build_component_configuration(
        {},
        overrides=component_overrides,
        target=StackComponentTarget.SANDBOX,
    )
    _sandbox_components.add_remote_sandbox_component_info(
        components,
        sandbox_flavor=sandbox_flavor,
        configuration=configuration,
    )


def _build_kubernetes_stack_request(
    name: str,
    *,
    spec: KubernetesStackSpec,
    connector_spec: _ResolvedConnectorSpec,
    labels: dict[str, str] | None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
) -> StackRequest:
    """Build the one-shot ZenML stack request for a Kubernetes stack."""
    merged_labels = _merge_managed_labels(labels)

    artifact_store_flavor = (
        "s3" if spec.provider == CloudProvider.AWS else GCP_ARTIFACT_STORE_FLAVOR
    )
    container_registry_flavor = (
        AWS_CONTAINER_REGISTRY_FLAVOR
        if spec.provider == CloudProvider.AWS
        else ContainerRegistryFlavor.GCP.value
    )

    stack_request = StackRequest(
        name=name,
        labels=merged_labels,
        components={
            StackComponentType.ORCHESTRATOR: [
                ComponentInfo(
                    flavor="kubernetes",
                    service_connector_index=0,
                    service_connector_resource_id=spec.cluster,
                    configuration=_build_component_configuration(
                        {"kubernetes_namespace": spec.namespace},
                        overrides=component_overrides,
                        target=StackComponentTarget.ORCHESTRATOR,
                    ),
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                ComponentInfo(
                    flavor=artifact_store_flavor,
                    service_connector_index=0,
                    service_connector_resource_id=_artifact_store_resource_id(
                        spec.artifact_store,
                        spec.provider,
                    ),
                    configuration=_build_component_configuration(
                        {"path": spec.artifact_store},
                        overrides=component_overrides,
                        target=StackComponentTarget.ARTIFACT_STORE,
                    ),
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                ComponentInfo(
                    flavor=container_registry_flavor,
                    service_connector_index=0,
                    service_connector_resource_id=_container_registry_resource_id(
                        spec.container_registry,
                        spec.provider,
                    ),
                    configuration=_build_component_configuration(
                        {"uri": spec.container_registry},
                        overrides=component_overrides,
                        target=StackComponentTarget.CONTAINER_REGISTRY,
                    ),
                )
            ],
        },
        service_connectors=_build_connector_services_list(connector_spec),
    )
    assert stack_request.components is not None
    _add_remote_sandbox_component_info(
        stack_request.components,
        sandbox_flavor=sandbox_flavor,
        component_overrides=component_overrides,
    )
    return stack_request


def _build_vertex_stack_request(
    name: str,
    *,
    spec: VertexStackSpec,
    connector_spec: _ResolvedConnectorSpec,
    labels: dict[str, str] | None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
) -> StackRequest:
    """Build the one-shot ZenML stack request for a Vertex AI stack."""
    merged_labels = _merge_managed_labels(labels)

    stack_request = StackRequest(
        name=name,
        labels=merged_labels,
        components={
            StackComponentType.ORCHESTRATOR: [
                ComponentInfo(
                    flavor=GCP_VERTEX_ORCHESTRATOR_FLAVOR,
                    service_connector_index=0,
                    configuration=_build_component_configuration(
                        {"location": spec.region},
                        overrides=component_overrides,
                        target=StackComponentTarget.ORCHESTRATOR,
                    ),
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                ComponentInfo(
                    flavor=GCP_ARTIFACT_STORE_FLAVOR,
                    service_connector_index=0,
                    service_connector_resource_id=_artifact_store_resource_id(
                        spec.artifact_store,
                        CloudProvider.GCP,
                    ),
                    configuration=_build_component_configuration(
                        {"path": spec.artifact_store},
                        overrides=component_overrides,
                        target=StackComponentTarget.ARTIFACT_STORE,
                    ),
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                ComponentInfo(
                    flavor=ContainerRegistryFlavor.GCP.value,
                    service_connector_index=0,
                    service_connector_resource_id=_container_registry_resource_id(
                        spec.container_registry,
                        CloudProvider.GCP,
                    ),
                    configuration=_build_component_configuration(
                        {"uri": spec.container_registry},
                        overrides=component_overrides,
                        target=StackComponentTarget.CONTAINER_REGISTRY,
                    ),
                )
            ],
        },
        service_connectors=_build_connector_services_list(connector_spec),
    )
    assert stack_request.components is not None
    _add_remote_sandbox_component_info(
        stack_request.components,
        sandbox_flavor=sandbox_flavor,
        component_overrides=component_overrides,
    )
    return stack_request


def _build_sagemaker_stack_request(
    name: str,
    *,
    spec: SagemakerStackSpec,
    connector_spec: _ResolvedConnectorSpec,
    labels: dict[str, str] | None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
) -> StackRequest:
    """Build the one-shot ZenML stack request for a SageMaker stack."""
    merged_labels = _merge_managed_labels(labels)

    stack_request = StackRequest(
        name=name,
        labels=merged_labels,
        components={
            StackComponentType.ORCHESTRATOR: [
                ComponentInfo(
                    flavor=AWS_SAGEMAKER_ORCHESTRATOR_FLAVOR,
                    service_connector_index=0,
                    configuration=_build_component_configuration(
                        {"execution_role": spec.execution_role},
                        overrides=component_overrides,
                        target=StackComponentTarget.ORCHESTRATOR,
                    ),
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                ComponentInfo(
                    flavor="s3",
                    service_connector_index=0,
                    service_connector_resource_id=_artifact_store_resource_id(
                        spec.artifact_store,
                        CloudProvider.AWS,
                    ),
                    configuration=_build_component_configuration(
                        {"path": spec.artifact_store},
                        overrides=component_overrides,
                        target=StackComponentTarget.ARTIFACT_STORE,
                    ),
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                ComponentInfo(
                    flavor=AWS_CONTAINER_REGISTRY_FLAVOR,
                    service_connector_index=0,
                    service_connector_resource_id=_container_registry_resource_id(
                        spec.container_registry,
                        CloudProvider.AWS,
                    ),
                    configuration=_build_component_configuration(
                        {"uri": spec.container_registry},
                        overrides=component_overrides,
                        target=StackComponentTarget.CONTAINER_REGISTRY,
                    ),
                )
            ],
        },
        service_connectors=_build_connector_services_list(connector_spec),
    )
    assert stack_request.components is not None
    _add_remote_sandbox_component_info(
        stack_request.components,
        sandbox_flavor=sandbox_flavor,
        component_overrides=component_overrides,
    )
    return stack_request


def _build_azureml_stack_request(
    name: str,
    *,
    spec: AzureMLStackSpec,
    connector_spec: _ResolvedConnectorSpec,
    labels: dict[str, str] | None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
) -> StackRequest:
    """Build the one-shot ZenML stack request for an AzureML stack."""
    merged_labels = _merge_managed_labels(labels)
    artifact_store_uri = _normalize_azure_artifact_store_uri(spec.artifact_store)

    orchestrator_configuration: dict[str, str] = {
        "subscription_id": spec.subscription_id,
        "resource_group": spec.resource_group,
        "workspace": spec.workspace,
    }
    if spec.region is not None:
        orchestrator_configuration["location"] = spec.region

    stack_request = StackRequest(
        name=name,
        labels=merged_labels,
        components={
            StackComponentType.ORCHESTRATOR: [
                ComponentInfo(
                    flavor=AZUREML_ORCHESTRATOR_FLAVOR,
                    service_connector_index=0,
                    configuration=_build_component_configuration(
                        orchestrator_configuration,
                        overrides=component_overrides,
                        target=StackComponentTarget.ORCHESTRATOR,
                    ),
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                ComponentInfo(
                    flavor=AZURE_ARTIFACT_STORE_FLAVOR,
                    service_connector_index=0,
                    service_connector_resource_id=_artifact_store_resource_id(
                        artifact_store_uri,
                        CloudProvider.AZURE,
                    ),
                    configuration=_build_component_configuration(
                        {"path": artifact_store_uri},
                        overrides=component_overrides,
                        target=StackComponentTarget.ARTIFACT_STORE,
                    ),
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                ComponentInfo(
                    flavor=ContainerRegistryFlavor.AZURE.value,
                    service_connector_index=0,
                    service_connector_resource_id=_container_registry_resource_id(
                        spec.container_registry,
                        CloudProvider.AZURE,
                    ),
                    configuration=_build_component_configuration(
                        {"uri": spec.container_registry},
                        overrides=component_overrides,
                        target=StackComponentTarget.CONTAINER_REGISTRY,
                    ),
                )
            ],
        },
        service_connectors=_build_connector_services_list(connector_spec),
    )
    assert stack_request.components is not None
    _add_remote_sandbox_component_info(
        stack_request.components,
        sandbox_flavor=sandbox_flavor,
        component_overrides=component_overrides,
    )
    return stack_request


def _modal_container_registry_configuration(
    container_registry: str,
    provider: CloudProvider,
) -> dict[str, str]:
    """Return provider-specific container-registry configuration for Modal."""
    if provider != CloudProvider.AWS:
        return {"uri": container_registry}

    normalized_registry = re.sub(r"^[a-z]+://", "", container_registry.strip())
    normalized_registry = normalized_registry.rstrip("/")
    uri, _, default_repository = normalized_registry.partition("/")
    configuration = {"uri": uri}
    if default_repository:
        configuration["default_repository"] = default_repository
    return configuration


def _build_modal_stack_request(
    name: str,
    *,
    spec: ModalStackSpec,
    connector_spec: _ResolvedConnectorSpec | None = None,
    existing_connectors: _ResolvedModalExistingConnectors | None = None,
    labels: dict[str, str] | None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
) -> StackRequest:
    """Build the one-shot ZenML stack request for a Modal stack."""
    merged_labels = _merge_managed_labels(labels)
    if existing_connectors is not None:
        merged_labels[_STACK_REUSED_SERVICE_CONNECTORS_LABEL_KEY] = (
            _STACK_REUSED_SERVICE_CONNECTORS_LABEL_VALUE
        )
    provider = _infer_cloud_provider_from_artifact_store(spec.artifact_store)
    artifact_store_uri = (
        _normalize_azure_artifact_store_uri(spec.artifact_store)
        if provider == CloudProvider.AZURE
        else spec.artifact_store
    )
    if connector_spec is not None and existing_connectors is not None:
        raise KitaruUsageError(
            "Modal stack creation cannot both create a new cloud connector and "
            "reuse existing service connectors."
        )

    artifact_store_connector_kwargs: dict[str, Any] = {}
    container_registry_connector_kwargs: dict[str, Any] = {}
    service_connectors: list[UUID | ServiceConnectorInfo] = []
    if connector_spec is not None:
        connector_index = 0
        service_connectors = _build_connector_services_list(connector_spec)
        artifact_store_connector_kwargs = {
            "service_connector_index": connector_index,
            "service_connector_resource_id": _artifact_store_resource_id(
                artifact_store_uri,
                provider,
            ),
        }
        container_registry_connector_kwargs = {
            "service_connector_index": connector_index,
            "service_connector_resource_id": _container_registry_resource_id(
                spec.container_registry,
                provider,
            ),
        }
    elif existing_connectors is not None:
        connector_indexes: dict[UUID, int] = {}
        for connector_id in (
            existing_connectors.artifact_store.connector_id,
            existing_connectors.container_registry.connector_id,
        ):
            if connector_id in connector_indexes:
                continue
            connector_indexes[connector_id] = len(service_connectors)
            service_connectors.append(connector_id)
        artifact_store_connector_kwargs = {
            "service_connector_index": connector_indexes[
                existing_connectors.artifact_store.connector_id
            ],
            "service_connector_resource_id": (
                existing_connectors.artifact_store.resource_id
            ),
        }
        container_registry_connector_kwargs = {
            "service_connector_index": connector_indexes[
                existing_connectors.container_registry.connector_id
            ],
            "service_connector_resource_id": (
                existing_connectors.container_registry.resource_id
            ),
        }

    stack_request = StackRequest(
        name=name,
        labels=merged_labels,
        components={
            StackComponentType.ORCHESTRATOR: [
                ComponentInfo(
                    flavor=MODAL_ORCHESTRATOR_FLAVOR,
                    configuration=_build_component_configuration(
                        {},
                        overrides=component_overrides,
                        target=StackComponentTarget.ORCHESTRATOR,
                    ),
                )
            ],
            StackComponentType.ARTIFACT_STORE: [
                ComponentInfo(
                    flavor=_artifact_store_flavor(provider),
                    **artifact_store_connector_kwargs,
                    configuration=_build_component_configuration(
                        {"path": artifact_store_uri},
                        overrides=component_overrides,
                        target=StackComponentTarget.ARTIFACT_STORE,
                    ),
                )
            ],
            StackComponentType.CONTAINER_REGISTRY: [
                ComponentInfo(
                    flavor=_container_registry_flavor(provider),
                    **container_registry_connector_kwargs,
                    configuration=_build_component_configuration(
                        _modal_container_registry_configuration(
                            spec.container_registry,
                            provider,
                        ),
                        overrides=component_overrides,
                        target=StackComponentTarget.CONTAINER_REGISTRY,
                    ),
                )
            ],
        },
        service_connectors=service_connectors,
    )
    assert stack_request.components is not None
    _add_remote_sandbox_component_info(
        stack_request.components,
        sandbox_flavor=sandbox_flavor,
        component_overrides=component_overrides,
    )
    return stack_request


def _get_required_stack_component(
    stack_model: Any,
    component_type: StackComponentType,
) -> Any:
    """Return the single component of a required stack type from a stack model."""
    raw_components = getattr(stack_model, "components", None)
    if not isinstance(raw_components, Mapping):
        raise KitaruStateError(
            "Unable to inspect components from the created remote stack."
        )

    components = raw_components.get(component_type, [])
    if len(components) != 1:
        raise KitaruStateError(
            "Created remote stack is missing the expected "
            f"{component_type.value} component."
        )
    return components[0]


def _extract_remote_stack_components(
    stack_model: Any,
    *,
    require_connector_metadata: bool = True,
    connector_required_component_types: frozenset[StackComponentType] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...], bool]:
    """Extract created component and connector names from a hydrated stack."""
    required_components = (
        (StackComponentType.ORCHESTRATOR, "orchestrator"),
        (StackComponentType.ARTIFACT_STORE, "artifact_store"),
        (StackComponentType.CONTAINER_REGISTRY, "container_registry"),
    )
    if not require_connector_metadata:
        connector_required_component_types = frozenset()
    if connector_required_component_types is None:
        connector_required_component_types = (
            frozenset(component_type for component_type, _ in required_components)
            if require_connector_metadata
            else frozenset()
        )
    component_labels: list[str] = []
    connector_names: list[str] = []
    seen_connector_names: set[str] = set()
    missing_connector_metadata = False

    def _collect_component(
        component: Any,
        *,
        kind: _StackComponentKind,
        require_connector_metadata: bool,
    ) -> None:
        nonlocal missing_connector_metadata
        component_name = str(getattr(component, "name", "")).strip()
        if not component_name:
            raise KitaruStateError(
                "Unable to inspect components from the created remote stack."
            )
        component_labels.append(_format_stack_component_label(component_name, kind))

        connector = getattr(component, "connector", None)
        if connector is None:
            if require_connector_metadata:
                missing_connector_metadata = True
            return
        connector_name = str(getattr(connector, "name", "")).strip()
        if not connector_name:
            if require_connector_metadata:
                missing_connector_metadata = True
            return
        if connector_name not in seen_connector_names:
            seen_connector_names.add(connector_name)
            connector_names.append(connector_name)

    for component_type, kind in required_components:
        _collect_component(
            _get_required_stack_component(stack_model, component_type),
            kind=kind,
            require_connector_metadata=component_type
            in connector_required_component_types,
        )

    for sandbox_component in _stack_component_models_for_type(
        stack_model,
        StackComponentType.SANDBOX,
    ):
        _collect_component(
            sandbox_component,
            kind=_sandbox_components.SANDBOX_COMPONENT_KIND,
            require_connector_metadata=False,
        )

    return tuple(component_labels), tuple(connector_names), missing_connector_metadata


def _stack_type_display_name(stack_type: StackType) -> str:
    """Render a user-facing stack-type label for errors and status messages."""
    return {
        StackType.LOCAL: "local",
        StackType.KUBERNETES: "Kubernetes",
        StackType.VERTEX: "Vertex",
        StackType.SAGEMAKER: "SageMaker",
        StackType.AZUREML: "AzureML",
        StackType.MODAL: "Modal",
    }.get(stack_type, str(stack_type))


def _create_remote_stack_operation(
    name: str,
    *,
    stack_type: StackType,
    connector_spec: _ResolvedConnectorSpec | None,
    stack_request: StackRequest,
    resource_summary: dict[str, str],
    connector_required_component_types: frozenset[StackComponentType] | None = None,
    activate: bool = True,
    verify: bool = True,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create a remote stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    client = client_factory()

    if any(
        stack_model.name == selector for stack_model in _iter_available_stacks(client)
    ):
        raise KitaruStateError(_stack_name_collision_message(selector))

    previous_active_stack = str(client.active_stack_model.name) if activate else None
    stack_label = _stack_type_display_name(stack_type)

    try:
        _prevalidate_stack_request_components(stack_request)
    except Exception as exc:
        if (
            stack_type == StackType.MODAL
            and _exception_chain_has_missing_modal_dependency(exc)
        ):
            raise KitaruUsageError(_MODAL_INSTALL_HINT) from exc
        raise

    if connector_spec is not None:
        try:
            client.create_service_connector(
                name=selector,
                connector_type=connector_spec.connector_info.type,
                resource_type=connector_spec.verify_resource_type,
                auth_method=connector_spec.connector_info.auth_method,
                configuration=dict(connector_spec.connector_info.configuration),
                verify=verify,
                list_resources=False,
                register=False,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to prepare {stack_label} stack '{selector}': {exc}"
            ) from exc

    try:
        client._validate_stack_configuration(stack_request)
    except (ValueError, ValidationError) as exc:
        if (
            stack_type == StackType.MODAL
            and _exception_chain_has_missing_modal_dependency(exc)
        ):
            raise KitaruUsageError(_MODAL_INSTALL_HINT) from exc
        raise KitaruUsageError(
            f"Invalid {stack_label} stack configuration for '{selector}'. "
            f"ZenML rejected the final stack request after Kitaru prevalidated "
            f"the component defaults: {exc}"
        ) from exc
    except Exception as exc:
        if (
            stack_type == StackType.MODAL
            and _exception_chain_has_missing_modal_dependency(exc)
        ):
            raise KitaruUsageError(_MODAL_INSTALL_HINT) from exc
        raise KitaruBackendError(
            f"Failed to validate {stack_label} stack '{selector}': {exc}"
        ) from exc

    try:
        created_stack = client.zen_store.create_stack(stack=stack_request)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to create {stack_label} stack '{selector}'. ZenML rolled back "
            "any partially created components and service connectors. Original "
            f"error: {exc}"
        ) from exc

    require_connector_metadata = bool(stack_request.service_connectors)
    components_created, service_connectors_created, missing_connector_metadata = (
        _extract_remote_stack_components(
            created_stack,
            require_connector_metadata=require_connector_metadata,
            connector_required_component_types=connector_required_component_types,
        )
    )
    if missing_connector_metadata:
        try:
            refreshed_stack = client.get_stack(created_stack.id, hydrate=True)
        except Exception:
            refreshed_stack = None
        if refreshed_stack is not None:
            components_created, service_connectors_created, _ = (
                _extract_remote_stack_components(
                    refreshed_stack,
                    require_connector_metadata=require_connector_metadata,
                    connector_required_component_types=connector_required_component_types,
                )
            )

    if activate:
        try:
            client.activate_stack(created_stack.id)
        except Exception as exc:
            raise KitaruBackendError(
                f"Created {stack_label} stack '{selector}' but failed to activate "
                "it. The stack was created successfully and remains available; "
                f"run 'kitaru stack use {selector}' to activate it manually. "
                f"Original error: {exc}"
            ) from exc
        active_stack_id = str(created_stack.id)
    else:
        active_stack_id = str(client.active_stack_model.id)

    return _StackCreateResult(
        stack=_stack_info_from_model(created_stack, active_stack_id=active_stack_id),
        previous_active_stack=previous_active_stack,
        components_created=components_created,
        stack_type=stack_type.value,
        service_connectors_created=service_connectors_created,
        resources=resource_summary,
    )


def _normalize_stack_selector(name_or_id: str) -> str:
    """Validate and normalize a stack selector provided by a user."""
    normalized_selector = name_or_id.strip()
    if not normalized_selector:
        raise KitaruUsageError("Stack name or ID cannot be empty.")

    return normalized_selector


def _stack_name_collision_message(name: str) -> str:
    """Return the user-facing message for stack-name collisions."""
    return (
        f'A stack named "{name}" already exists. To activate it, run '
        f"'kitaru stack use {name}'."
    )


def _component_collision_message(
    name: str,
    component_type: StackComponentType,
) -> str:
    """Return the user-facing message for stack component collisions."""
    return (
        f"Cannot create stack '{name}' because a {component_type.value} named "
        f"'{name}' already exists. Kitaru always creates fresh stack "
        "components and never reuses existing ones."
    )


def _model_label_matches(model: Any, *, key: str, value: str) -> bool:
    """Return whether a ZenML model label has the expected value."""
    raw_labels = getattr(model, "labels", None)
    if not isinstance(raw_labels, Mapping):
        return False

    raw_value = raw_labels.get(key)
    if raw_value is None:
        return False

    return str(raw_value).strip().lower() == value


def _stack_is_managed(stack_model: Any) -> bool:
    """Return whether a stack carries Kitaru's managed-stack label."""
    return _model_label_matches(
        stack_model,
        key=_STACK_MANAGED_LABEL_KEY,
        value=_STACK_MANAGED_LABEL_VALUE,
    )


def _stack_reuses_service_connectors(stack_model: Any) -> bool:
    """Return whether a stack reused pre-existing service connectors."""
    return _model_label_matches(
        stack_model,
        key=_STACK_REUSED_SERVICE_CONNECTORS_LABEL_KEY,
        value=_STACK_REUSED_SERVICE_CONNECTORS_LABEL_VALUE,
    )


def _format_stack_component_label(
    name: str,
    kind: _StackComponentKind,
) -> str:
    """Format one stack component for user-facing structured output."""
    return f"{name} ({kind})"


def _delete_stack_components_best_effort(
    client: Client,
    components: list[_StackComponent],
) -> str | None:
    """Best-effort cleanup for stack components created during a failed create."""
    cleanup_errors: list[str] = []
    component_types: dict[_StackComponentKind, StackComponentType] = {
        "orchestrator": StackComponentType.ORCHESTRATOR,
        "artifact_store": StackComponentType.ARTIFACT_STORE,
        "container_registry": StackComponentType.CONTAINER_REGISTRY,
        _sandbox_components.SANDBOX_COMPONENT_KIND: StackComponentType.SANDBOX,
    }

    for component in reversed(components):
        try:
            component_type = component_types[component.kind]
        except KeyError as exc:  # pragma: no cover - defensive type guard
            raise KitaruStateError(
                f"Unsupported stack component kind: {component.kind}"
            ) from exc
        try:
            client.delete_stack_component(component.component_id, component_type)
        except Exception as exc:  # pragma: no cover - cleanup failure path
            cleanup_errors.append(
                f"{_format_stack_component_label(component.name, component.kind)}: "
                f"{exc}"
            )

    if not cleanup_errors:
        return None

    return "Cleanup also failed for: " + "; ".join(cleanup_errors)


def _list_stack_entries(
    *,
    client_factory: Callable[[], Any] = Client,
) -> list[_StackListEntry]:
    """List stacks with active + managed metadata for structured output."""
    client = client_factory()
    active_stack_id = str(client.active_stack_model.id)

    return [
        _StackListEntry(
            stack=_stack_info_from_model(
                stack_model,
                active_stack_id=active_stack_id,
            ),
            is_managed=_stack_is_managed(stack_model),
        )
        for stack_model in _iter_available_stacks(client)
    ]


def _normalize_stack_detail_value(value: Any) -> str | None:
    """Normalize optional component metadata values for stack inspection."""
    if value is None:
        return None
    normalized_value = str(value).strip()
    return normalized_value or None


_RECURSIVE_DELETE_COMPONENT_TYPES: tuple[
    tuple[StackComponentType, _StackComponentKind], ...
] = (
    (StackComponentType.ORCHESTRATOR, "orchestrator"),
    (StackComponentType.ARTIFACT_STORE, "artifact_store"),
    (StackComponentType.CONTAINER_REGISTRY, "container_registry"),
    _sandbox_components.SANDBOX_RECURSIVE_DELETE_ENTRY,
)


def _stack_component_models_for_type(
    stack_model: Any,
    component_type: StackComponentType,
) -> tuple[Any, ...]:
    """Return normalized component models for one stack-component type."""
    raw_components = getattr(stack_model, "components", None)
    if not isinstance(raw_components, Mapping):
        return ()

    component_models = raw_components.get(component_type, ())
    if component_models is None:
        return ()
    if isinstance(component_models, Iterable) and not isinstance(
        component_models,
        (str, bytes),
    ):
        return tuple(component_models)
    return (component_models,)


def _iter_stack_component_models(stack_model: Any) -> Iterator[Any]:
    """Iterate all component models attached to a stack."""
    raw_components = getattr(stack_model, "components", None)
    if not isinstance(raw_components, Mapping):
        return

    for component_models in raw_components.values():
        if component_models is None:
            continue
        if isinstance(component_models, Iterable) and not isinstance(
            component_models,
            (str, bytes),
        ):
            yield from component_models
            continue
        yield component_models


def _recursive_delete_component_labels(
    client: Client,
    stack_model: Any,
) -> tuple[str, ...]:
    """Return labels for recursively deleted unshared managed components."""
    deletable_components: list[str] = []

    for component_type, component_kind in _RECURSIVE_DELETE_COMPONENT_TYPES:
        for component_model in _stack_component_models_for_type(
            stack_model,
            component_type,
        ):
            component_id = getattr(component_model, "id", None)
            if component_id is None:
                continue

            component_name = _normalize_stack_detail_value(
                getattr(component_model, "name", None)
            )
            if component_name is None:
                continue

            try:
                stacks = client.list_stacks(component_id=component_id, size=2, page=1)
            except Exception:
                continue

            if isinstance(stacks, Iterable) and not isinstance(stacks, (str, bytes)):
                matching_stacks = tuple(stacks)
            else:
                continue

            if len(matching_stacks) == 1 and str(
                getattr(matching_stacks[0], "id", "")
            ) == str(getattr(stack_model, "id", "")):
                deletable_components.append(
                    _format_stack_component_label(component_name, component_kind)
                )

    return tuple(deletable_components)


def _linked_service_connector_selectors_for_stack(
    stack_model: Any,
    *,
    require_complete_metadata: bool = False,
) -> tuple[str, ...] | None:
    """Extract linked service connector selectors from a hydrated stack."""
    selectors: list[str] = []
    seen_selectors: set[str] = set()

    for component_model in _iter_stack_component_models(stack_model):
        connector = getattr(component_model, "connector", None)
        if connector is None:
            continue

        for raw_selector in (
            getattr(connector, "id", None),
            getattr(connector, "name", None),
        ):
            selector = _normalize_stack_detail_value(raw_selector)
            if selector is None or selector in seen_selectors:
                continue
            seen_selectors.add(selector)
            selectors.append(selector)
            break
        else:
            if require_complete_metadata:
                return None

    return tuple(selectors)


def _resolve_service_connector_selectors(
    client: Client,
    connector_selectors: tuple[str, ...],
) -> tuple[str, ...]:
    """Resolve connector selectors to canonical delete selectors when possible."""
    resolved_selectors: list[str] = []
    seen_selectors: set[str] = set()

    for selector in connector_selectors:
        try:
            UUID(selector)
        except (TypeError, ValueError, AttributeError):
            connector_models = client.list_service_connectors(
                name=selector,
                page=1,
                size=2,
                hydrate=True,
            )
        else:
            connector_models = client.list_service_connectors(
                id=selector,
                page=1,
                size=2,
                hydrate=True,
            )

        if isinstance(connector_models, Iterable) and not isinstance(
            connector_models,
            (str, bytes),
        ):
            matching_connectors = tuple(connector_models)
        else:
            matching_connectors = ()

        if not matching_connectors:
            if selector not in seen_selectors:
                seen_selectors.add(selector)
                resolved_selectors.append(selector)
            continue

        for connector_model in matching_connectors:
            resolved_selector = _normalize_stack_detail_value(
                getattr(connector_model, "id", None)
            ) or _normalize_stack_detail_value(getattr(connector_model, "name", None))
            if resolved_selector is None or resolved_selector in seen_selectors:
                continue
            seen_selectors.add(resolved_selector)
            resolved_selectors.append(resolved_selector)

    return tuple(resolved_selectors)


def _delete_unshared_service_connectors_best_effort(
    client: Client,
    connector_selectors: tuple[str, ...],
) -> None:
    """Delete unshared service connectors after a successful stack delete."""
    if not connector_selectors:
        return

    try:
        resolved_selectors = _resolve_service_connector_selectors(
            client,
            connector_selectors,
        )
        if not resolved_selectors:
            return

        remaining_connector_selectors: set[str] = set()
        for stack_model in _iter_available_stacks(client):
            remaining_stack = client.get_stack(
                getattr(stack_model, "id", None),
                allow_name_prefix_match=False,
                hydrate=True,
            )
            remaining_stack_selectors = _linked_service_connector_selectors_for_stack(
                remaining_stack,
                require_complete_metadata=True,
            )
            if remaining_stack_selectors is None:
                return

            remaining_connector_selectors.update(
                _resolve_service_connector_selectors(
                    client,
                    remaining_stack_selectors,
                )
            )
    except Exception:
        return

    for selector in resolved_selectors:
        if selector in remaining_connector_selectors:
            continue
        try:
            client.delete_service_connector(selector)
        except Exception:
            continue


def _resolve_stack_for_show(client: Client, selector: str) -> Any:
    """Resolve a stack selector for `stack show`, preferring exact ID matches."""
    id_match: Any | None = None
    name_match: Any | None = None

    for stack_model in _iter_available_stacks(client):
        if str(getattr(stack_model, "id", "")).strip() == selector:
            id_match = stack_model
        if str(getattr(stack_model, "name", "")).strip() == selector:
            name_match = stack_model
        if id_match and name_match:
            break

    resolved_stack = id_match or name_match
    if resolved_stack is None:
        raise KitaruStateError(f"Stack '{selector}' not found.")
    return resolved_stack


def _stack_component_details_from_model(
    component_type: StackComponentType | None,
    component: Any,
    *,
    purpose: str | None = None,
) -> StackComponentDetails:
    """Translate one hydrated ZenML stack component into Kitaru vocabulary."""
    component_name = (
        _normalize_stack_detail_value(getattr(component, "name", None)) or "<unnamed>"
    )
    raw_flavor = getattr(component, "flavor", None)
    # Hydrated ZenML components return a FlavorResponse object for `.flavor`;
    # extract just the name string to avoid dumping the full response repr.
    if hasattr(raw_flavor, "name"):
        raw_flavor = raw_flavor.name
    backend = _normalize_stack_detail_value(raw_flavor)
    configuration = getattr(component, "configuration", None)
    component_configuration = (
        configuration if isinstance(configuration, Mapping) else {}
    )
    connector = getattr(component, "connector", None)
    connector_configuration_raw = getattr(connector, "configuration", None)
    connector_configuration = (
        connector_configuration_raw
        if isinstance(connector_configuration_raw, Mapping)
        else {}
    )

    if component_type == StackComponentType.ORCHESTRATOR:
        if backend == GCP_VERTEX_ORCHESTRATOR_FLAVOR:
            details: list[tuple[str, str]] = []
            location = _normalize_stack_detail_value(
                component_configuration.get("location")
            )
            if location is not None:
                details.append(("location", location))

            return StackComponentDetails(
                role="runner",
                name=component_name,
                backend=backend,
                details=tuple(details),
            )

        if backend == AWS_SAGEMAKER_ORCHESTRATOR_FLAVOR:
            details: list[tuple[str, str]] = []
            region = _normalize_stack_detail_value(
                connector_configuration.get("region")
            )
            if region is None:
                region = _normalize_stack_detail_value(
                    component_configuration.get("region")
                )
            if region is not None:
                details.append(("region", region))

            execution_role = _normalize_stack_detail_value(
                component_configuration.get("execution_role")
            )
            if execution_role is not None:
                details.append(("execution_role", execution_role))

            return StackComponentDetails(
                role="runner",
                name=component_name,
                backend=backend,
                details=tuple(details),
            )

        if backend == MODAL_ORCHESTRATOR_FLAVOR:
            details: list[tuple[str, str]] = []
            for field_name in ("region", "cloud", "modal_environment", "timeout"):
                value = _normalize_stack_detail_value(
                    component_configuration.get(field_name)
                )
                if value is not None:
                    details.append((field_name, value))

            return StackComponentDetails(
                role="runner",
                name=component_name,
                backend=backend,
                details=tuple(details),
            )

        if backend == AZUREML_ORCHESTRATOR_FLAVOR:
            details: list[tuple[str, str]] = []
            subscription_id = _normalize_stack_detail_value(
                connector_configuration.get("subscription_id")
            )
            if subscription_id is None:
                subscription_id = _normalize_stack_detail_value(
                    component_configuration.get("subscription_id")
                )
            if subscription_id is not None:
                details.append(("subscription_id", subscription_id))

            resource_group = _normalize_stack_detail_value(
                component_configuration.get("resource_group")
            )
            if resource_group is None:
                resource_group = _normalize_stack_detail_value(
                    connector_configuration.get("resource_group")
                )
            if resource_group is not None:
                details.append(("resource_group", resource_group))

            workspace = _normalize_stack_detail_value(
                component_configuration.get("workspace")
            )
            if workspace is not None:
                details.append(("workspace", workspace))

            location = _normalize_stack_detail_value(
                component_configuration.get("location")
            )
            if location is None:
                location = _normalize_stack_detail_value(
                    component_configuration.get("region")
                )
            if location is not None:
                details.append(("location", location))

            return StackComponentDetails(
                role="runner",
                name=component_name,
                backend=backend,
                details=tuple(details),
            )

        details: list[tuple[str, str]] = []
        cluster = next(
            (
                value
                for value in (
                    _normalize_stack_detail_value(
                        getattr(component, "service_connector_resource_id", None)
                    ),
                    _normalize_stack_detail_value(
                        getattr(component, "connector_resource_id", None)
                    ),
                    _normalize_stack_detail_value(
                        getattr(component, "resource_id", None)
                    ),
                )
                if value is not None
            ),
            None,
        )
        if cluster is not None:
            details.append(("cluster", cluster))

        region = _normalize_stack_detail_value(connector_configuration.get("region"))
        if region is None:
            region = _normalize_stack_detail_value(
                component_configuration.get("region")
            )
        if region is not None:
            details.append(("region", region))

        namespace = _normalize_stack_detail_value(
            component_configuration.get("kubernetes_namespace")
        )
        if namespace is not None:
            details.append(("namespace", namespace))

        return StackComponentDetails(
            role="runner",
            name=component_name,
            backend=backend,
            details=tuple(details),
        )

    if component_type == StackComponentType.ARTIFACT_STORE:
        details: list[tuple[str, str]] = []
        location = _normalize_stack_detail_value(component_configuration.get("path"))
        if location is not None:
            details.append(("location", location))

        return StackComponentDetails(
            role="storage",
            name=component_name,
            backend=backend,
            details=tuple(details),
        )

    if component_type == StackComponentType.CONTAINER_REGISTRY:
        details: list[tuple[str, str]] = []
        location = _normalize_stack_detail_value(component_configuration.get("uri"))
        default_repository = _normalize_stack_detail_value(
            component_configuration.get("default_repository")
        )
        if location is not None and default_repository is not None:
            location = f"{location.rstrip('/')}/{default_repository.lstrip('/')}"
        if location is not None:
            details.append(("location", location))

        return StackComponentDetails(
            role="image_registry",
            name=component_name,
            backend=backend,
            details=tuple(details),
        )

    if component_type == StackComponentType.SANDBOX:
        return StackComponentDetails(
            role=_sandbox_components.SANDBOX_COMPONENT_KIND,
            name=component_name,
            backend=backend,
        )

    normalized_purpose = _normalize_stack_detail_value(
        purpose
        if purpose is not None
        else (component_type.value if component_type is not None else None)
    )
    return StackComponentDetails(
        role="additional_component",
        name=component_name,
        backend=backend,
        purpose=normalized_purpose,
    )


def _stack_component_details_from_stack_model(
    stack_model: Any,
    *,
    selector: str,
) -> tuple[StackComponentDetails, ...]:
    """Translate hydrated stack component metadata into Kitaru details."""
    raw_components = getattr(stack_model, "components", None)
    if not isinstance(raw_components, Mapping):
        raise KitaruStateError(
            f"Stack '{selector}' returned malformed component metadata."
        )

    normalized_components: dict[StackComponentType, list[Any]] = {}
    ordered_components: list[StackComponentDetails] = []
    for raw_component_type, raw_component_models in raw_components.items():
        purpose = _normalize_stack_detail_value(
            getattr(raw_component_type, "value", raw_component_type)
        )
        try:
            component_type = (
                raw_component_type
                if isinstance(raw_component_type, StackComponentType)
                else StackComponentType(str(raw_component_type))
            )
        except ValueError:
            component_type = None

        if isinstance(raw_component_models, Iterable) and not isinstance(
            raw_component_models,
            (str, bytes, Mapping),
        ):
            component_models = list(raw_component_models)
        else:
            component_models = [raw_component_models]

        if component_type is None:
            for component_model in component_models:
                ordered_components.append(
                    _stack_component_details_from_model(
                        None,
                        component_model,
                        purpose=purpose,
                    )
                )
            continue

        normalized_components.setdefault(component_type, []).extend(component_models)

    for core_component_type in (
        StackComponentType.ORCHESTRATOR,
        StackComponentType.ARTIFACT_STORE,
        StackComponentType.CONTAINER_REGISTRY,
        StackComponentType.SANDBOX,
    ):
        for component_model in normalized_components.pop(core_component_type, []):
            ordered_components.append(
                _stack_component_details_from_model(
                    core_component_type,
                    component_model,
                )
            )

    for component_type in sorted(normalized_components, key=lambda item: item.value):
        for component_model in normalized_components[component_type]:
            ordered_components.append(
                _stack_component_details_from_model(component_type, component_model)
            )

    return tuple(ordered_components)


def _infer_stack_details_type(
    components: tuple[StackComponentDetails, ...],
) -> _StackShowType:
    """Infer a user-facing stack type from translated stack components."""
    if any(
        component.role == "runner"
        and component.backend == GCP_VERTEX_ORCHESTRATOR_FLAVOR
        for component in components
    ):
        return "vertex"

    if any(
        component.role == "runner"
        and component.backend == AWS_SAGEMAKER_ORCHESTRATOR_FLAVOR
        for component in components
    ):
        return "sagemaker"

    if any(
        component.role == "runner" and component.backend == AZUREML_ORCHESTRATOR_FLAVOR
        for component in components
    ):
        return "azureml"

    if any(
        component.role == "runner" and component.backend == MODAL_ORCHESTRATOR_FLAVOR
        for component in components
    ):
        return "modal"

    if any(
        component.role == "runner" and component.backend == "kubernetes"
        for component in components
    ):
        return "kubernetes"

    if components and all(
        component.role
        in {"runner", "storage", _sandbox_components.SANDBOX_COMPONENT_KIND}
        for component in components
    ):
        backends = {
            component.backend
            for component in components
            if component.backend is not None
        }
        if backends.issubset({"local"}):
            return "local"

    return "custom"


def classify_stack_model_deployment_type(
    stack_model: Any,
    *,
    selector: str = "<resolved stack>",
) -> _StackShowType:
    """Classify an already-hydrated stack model without resolving it again."""
    component_details = _stack_component_details_from_stack_model(
        stack_model,
        selector=selector,
    )
    if not component_details or all(
        component.backend is None for component in component_details
    ):
        raise KitaruStateError("Stack components did not include backend metadata.")
    return _infer_stack_details_type(component_details)


def classify_stack_deployment_type(
    name_or_id: str | None = None,
    *,
    client_factory: Callable[[], Any] = Client,
) -> _StackShowType:
    """Classify a stack using the same component translation as stack show.

    Args:
        name_or_id: Optional stack selector. When omitted, the active stack is
            classified.
        client_factory: Factory for a ZenML client.

    Returns:
        Low-cardinality stack deployment type.
    """
    client = client_factory()
    if name_or_id is None:
        resolved_stack = client.active_stack_model
        selector = (
            _normalize_stack_detail_value(getattr(resolved_stack, "id", None))
            or _normalize_stack_detail_value(getattr(resolved_stack, "name", None))
            or "<active stack>"
        )
    else:
        selector = _normalize_stack_selector(name_or_id)
        resolved_stack = _resolve_stack_for_show(client, selector)

    try:
        hydrated_stack = client.get_stack(resolved_stack.id, hydrate=True)
    except Exception as exc:
        raise KitaruBackendError(
            f"Unable to classify stack deployment type for '{selector}'."
        ) from exc

    return classify_stack_model_deployment_type(hydrated_stack, selector=selector)


def _show_stack_operation(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> StackDetails:
    """Inspect one stack and translate its component metadata for CLI display."""
    selector = _normalize_stack_selector(name_or_id)
    client = client_factory()
    resolved_stack = _resolve_stack_for_show(client, selector)

    try:
        hydrated_stack = client.get_stack(resolved_stack.id, hydrate=True)
    except Exception as exc:
        raise KitaruBackendError(
            f"Unable to inspect stack '{selector}': {exc}"
        ) from exc

    active_stack_id = str(client.active_stack_model.id)
    stack = _stack_info_from_model(hydrated_stack, active_stack_id=active_stack_id)
    is_managed = _stack_is_managed(hydrated_stack)
    component_details = _stack_component_details_from_stack_model(
        hydrated_stack,
        selector=selector,
    )
    return StackDetails(
        stack=stack,
        is_managed=is_managed,
        stack_type=_infer_stack_details_type(component_details),
        components=component_details,
    )


def _create_kubernetes_stack_operation(
    name: str,
    *,
    spec: KubernetesStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create a Kubernetes-backed stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    connector_spec = _resolve_kubernetes_connector_spec(spec)
    stack_request = _build_kubernetes_stack_request(
        selector,
        spec=spec,
        connector_spec=connector_spec,
        labels=labels,
        component_overrides=component_overrides,
        sandbox_flavor=sandbox_flavor,
    )
    return _create_remote_stack_operation(
        selector,
        stack_type=StackType.KUBERNETES,
        connector_spec=connector_spec,
        stack_request=stack_request,
        resource_summary={
            "provider": spec.provider.value,
            "cluster": spec.cluster,
            "region": spec.region,
            "namespace": spec.namespace,
            "artifact_store": spec.artifact_store,
            "container_registry": spec.container_registry,
            **({"sandbox": sandbox_flavor} if sandbox_flavor is not None else {}),
        },
        activate=activate,
        verify=spec.verify,
        client_factory=client_factory,
    )


def _create_vertex_stack_operation(
    name: str,
    *,
    spec: VertexStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create a Vertex AI-backed stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    connector_spec = _resolve_gcp_connector_spec(
        container_registry=spec.container_registry,
        credentials=spec.credentials,
    )
    stack_request = _build_vertex_stack_request(
        selector,
        spec=spec,
        connector_spec=connector_spec,
        labels=labels,
        component_overrides=component_overrides,
        sandbox_flavor=sandbox_flavor,
    )
    return _create_remote_stack_operation(
        selector,
        stack_type=StackType.VERTEX,
        connector_spec=connector_spec,
        stack_request=stack_request,
        resource_summary={
            "provider": CloudProvider.GCP.value,
            "region": spec.region,
            "artifact_store": spec.artifact_store,
            "container_registry": spec.container_registry,
            **({"sandbox": sandbox_flavor} if sandbox_flavor is not None else {}),
        },
        activate=activate,
        verify=spec.verify,
        client_factory=client_factory,
    )


def _create_sagemaker_stack_operation(
    name: str,
    *,
    spec: SagemakerStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create a SageMaker-backed stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    connector_spec = _resolve_aws_connector_spec(
        region=spec.region,
        credentials=spec.credentials,
    )
    stack_request = _build_sagemaker_stack_request(
        selector,
        spec=spec,
        connector_spec=connector_spec,
        labels=labels,
        component_overrides=component_overrides,
        sandbox_flavor=sandbox_flavor,
    )
    return _create_remote_stack_operation(
        selector,
        stack_type=StackType.SAGEMAKER,
        connector_spec=connector_spec,
        stack_request=stack_request,
        resource_summary={
            "provider": CloudProvider.AWS.value,
            "region": spec.region,
            "artifact_store": spec.artifact_store,
            "container_registry": spec.container_registry,
            "execution_role": spec.execution_role,
            **({"sandbox": sandbox_flavor} if sandbox_flavor is not None else {}),
        },
        activate=activate,
        verify=spec.verify,
        client_factory=client_factory,
    )


def _create_azureml_stack_operation(
    name: str,
    *,
    spec: AzureMLStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create an AzureML-backed stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    connector_spec = _resolve_azure_connector_spec(
        subscription_id=spec.subscription_id,
        credentials=spec.credentials,
    )
    stack_request = _build_azureml_stack_request(
        selector,
        spec=spec,
        connector_spec=connector_spec,
        labels=labels,
        component_overrides=component_overrides,
        sandbox_flavor=sandbox_flavor,
    )
    resource_summary = {
        "provider": CloudProvider.AZURE.value,
        "subscription_id": spec.subscription_id,
        "resource_group": spec.resource_group,
        "workspace": spec.workspace,
        "artifact_store": spec.artifact_store,
        "container_registry": spec.container_registry,
    }
    if spec.region is not None:
        resource_summary["region"] = spec.region
    if sandbox_flavor is not None:
        resource_summary["sandbox"] = sandbox_flavor
    return _create_remote_stack_operation(
        selector,
        stack_type=StackType.AZUREML,
        connector_spec=connector_spec,
        stack_request=stack_request,
        resource_summary=resource_summary,
        activate=activate,
        verify=spec.verify,
        client_factory=client_factory,
    )


def _create_modal_stack_operation(
    name: str,
    *,
    spec: ModalStackSpec,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    client_factory: Callable[[], Any] = Client,
) -> _StackCreateResult:
    """Create a Modal-backed stack via ZenML's one-shot stack API."""
    selector = _normalize_stack_selector(name)
    _require_modal_stack_support()
    provider = _infer_cloud_provider_from_artifact_store(spec.artifact_store)
    connector_spec = _resolve_modal_connector_spec(spec, provider=provider)
    existing_connectors: _ResolvedModalExistingConnectors | None = None
    operation_client_factory: Callable[[], Any] = client_factory
    if connector_spec is None:
        discovery_client = client_factory()
        existing_connectors = _resolve_modal_existing_connectors(
            spec,
            provider=provider,
            client=discovery_client,
        )

        def _discovered_client_factory() -> Any:
            return discovery_client

        operation_client_factory = _discovered_client_factory
    stack_request = _build_modal_stack_request(
        selector,
        spec=spec,
        connector_spec=connector_spec,
        existing_connectors=existing_connectors,
        labels=labels,
        component_overrides=component_overrides,
        sandbox_flavor=sandbox_flavor,
    )
    resource_summary = {
        "provider": provider.value,
        "artifact_store": spec.artifact_store,
        "container_registry": spec.container_registry,
    }
    if spec.region is not None:
        resource_summary["region"] = spec.region
    if spec.subscription_id is not None:
        resource_summary["subscription_id"] = spec.subscription_id
    if sandbox_flavor is not None:
        resource_summary["sandbox"] = sandbox_flavor
    return _create_remote_stack_operation(
        selector,
        stack_type=StackType.MODAL,
        connector_spec=connector_spec,
        stack_request=stack_request,
        resource_summary=resource_summary,
        connector_required_component_types=frozenset(
            {
                StackComponentType.ARTIFACT_STORE,
                StackComponentType.CONTAINER_REGISTRY,
            }
        ),
        activate=activate,
        verify=spec.verify,
        client_factory=operation_client_factory,
    )


def _create_stack_operation(
    name: str,
    *,
    stack_type: StackType = StackType.LOCAL,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    remote_spec: RemoteStackSpec | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str | None = None,
    operation_overrides: dict[StackType, Callable[..., _StackCreateResult]]
    | None = None,
) -> _StackCreateResult:
    """Create a stack by dispatching to the requested stack type flow."""
    dispatch: dict[StackType, Callable[..., _StackCreateResult]] = {
        StackType.LOCAL: _create_local_stack_operation,
        StackType.KUBERNETES: _create_kubernetes_stack_operation,
        StackType.VERTEX: _create_vertex_stack_operation,
        StackType.SAGEMAKER: _create_sagemaker_stack_operation,
        StackType.AZUREML: _create_azureml_stack_operation,
        StackType.MODAL: _create_modal_stack_operation,
    }
    if operation_overrides:
        dispatch.update(operation_overrides)

    if stack_type == StackType.LOCAL:
        if remote_spec is not None:
            raise KitaruUsageError("Local stacks do not accept remote stack specs.")
        local_kwargs: dict[str, Any] = {
            "activate": activate,
            "labels": labels,
            "sandbox_flavor": sandbox_flavor
            or _sandbox_components.LOCAL_SANDBOX_FLAVOR,
        }
        if component_overrides is not None:
            local_kwargs["component_overrides"] = component_overrides
        return dispatch[StackType.LOCAL](name, **local_kwargs)

    operation = dispatch.get(stack_type)
    if operation is None:
        raise KitaruUsageError(f"Unsupported stack type: {stack_type}")

    if remote_spec is None:
        display = _stack_type_display_name(stack_type)
        raise KitaruUsageError(
            f"{display} spec required for --type {stack_type.value}."
        )

    operation_kwargs: dict[str, Any] = {
        "spec": remote_spec,
        "activate": activate,
        "labels": labels,
    }
    if component_overrides is not None:
        operation_kwargs["component_overrides"] = component_overrides
    if sandbox_flavor is not None:
        operation_kwargs["sandbox_flavor"] = sandbox_flavor
    return operation(name, **operation_kwargs)


def _create_local_stack_operation(
    name: str,
    *,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    component_overrides: StackComponentConfigOverrides | None = None,
    sandbox_flavor: str = _sandbox_components.LOCAL_SANDBOX_FLAVOR,
    client_factory: Callable[[], Any] = Client,
    current_stack_getter: Callable[[], StackInfo] | None = None,
) -> _StackCreateResult:
    """Create a new local stack and return structured operation details."""
    selector = _normalize_stack_selector(name)
    client = client_factory()

    if any(
        stack_model.name == selector for stack_model in _iter_available_stacks(client)
    ):
        raise KitaruStateError(_stack_name_collision_message(selector))

    previous_active_stack = str(client.active_stack_model.name) if activate else None
    merged_labels = _merge_managed_labels(labels)
    overrides = component_overrides or StackComponentConfigOverrides()
    if overrides.container_registry:
        raise KitaruUsageError(
            "Local stacks do not create a container registry component, so "
            "`container_registry` overrides are not allowed."
        )
    orchestrator_configuration = _build_component_configuration(
        {},
        overrides=overrides,
        target=StackComponentTarget.ORCHESTRATOR,
    )
    artifact_store_configuration = _build_component_configuration(
        {},
        overrides=overrides,
        target=StackComponentTarget.ARTIFACT_STORE,
    )
    sandbox_configuration = _build_component_configuration(
        {},
        overrides=overrides,
        target=StackComponentTarget.SANDBOX,
    )

    created_components: list[_StackComponent] = []
    components_created = (
        _format_stack_component_label(selector, "orchestrator"),
        _format_stack_component_label(selector, "artifact_store"),
        _format_stack_component_label(
            selector,
            _sandbox_components.SANDBOX_COMPONENT_KIND,
        ),
    )

    _prevalidate_component_configuration(
        target=StackComponentTarget.ORCHESTRATOR,
        flavor=StackType.LOCAL.value,
        configuration=orchestrator_configuration,
    )
    _prevalidate_component_configuration(
        target=StackComponentTarget.ARTIFACT_STORE,
        flavor=StackType.LOCAL.value,
        configuration=artifact_store_configuration,
    )
    _prevalidate_component_configuration(
        target=StackComponentTarget.SANDBOX,
        flavor=sandbox_flavor,
        configuration=sandbox_configuration,
    )
    try:
        orchestrator = client.create_stack_component(
            name=selector,
            flavor="local",
            component_type=StackComponentType.ORCHESTRATOR,
            configuration=orchestrator_configuration,
        )
        created_components.append(
            _StackComponent(
                component_id=str(orchestrator.id),
                name=selector,
                kind="orchestrator",
            )
        )
    except EntityExistsError as exc:
        raise KitaruStateError(
            _component_collision_message(selector, StackComponentType.ORCHESTRATOR)
        ) from exc

    try:
        artifact_store = client.create_stack_component(
            name=selector,
            flavor="local",
            component_type=StackComponentType.ARTIFACT_STORE,
            configuration=artifact_store_configuration,
        )
        created_components.append(
            _StackComponent(
                component_id=str(artifact_store.id),
                name=selector,
                kind="artifact_store",
            )
        )
    except EntityExistsError as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = _component_collision_message(
            selector,
            StackComponentType.ARTIFACT_STORE,
        )
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruStateError(message) from exc
    except Exception as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = str(exc)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruBackendError(message) from exc

    try:
        sandbox = _sandbox_components.create_local_sandbox_component(
            client,
            name=selector,
            flavor=sandbox_flavor,
            configuration=sandbox_configuration,
        )
        created_components.append(
            _StackComponent(
                component_id=str(sandbox.id),
                name=selector,
                kind=_sandbox_components.SANDBOX_COMPONENT_KIND,
            )
        )
    except EntityExistsError as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = _component_collision_message(selector, StackComponentType.SANDBOX)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruStateError(message) from exc
    except Exception as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = str(exc)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruBackendError(message) from exc

    try:
        stack_model = client.create_stack(
            name=selector,
            components={
                StackComponentType.ORCHESTRATOR: [selector],
                StackComponentType.ARTIFACT_STORE: [selector],
                StackComponentType.SANDBOX: [selector],
            },
            labels=merged_labels,
        )
    except EntityExistsError as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = _stack_name_collision_message(selector)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruStateError(message) from exc
    except Exception as exc:
        cleanup_warning = _delete_stack_components_best_effort(
            client,
            created_components,
        )
        message = str(exc)
        if cleanup_warning:
            message = f"{message} {cleanup_warning}"
        raise KitaruBackendError(message) from exc

    if activate:
        try:
            client.activate_stack(selector)
        except Exception as exc:
            raise KitaruBackendError(
                f"Created stack '{selector}' but failed to activate it. The stack "
                "was created successfully and remains available; run "
                f"'kitaru stack use {selector}' to activate it manually. Original "
                f"error: {exc}"
            ) from exc
        active_stack_getter = (
            current_stack if current_stack_getter is None else current_stack_getter
        )
        stack = active_stack_getter()
    else:
        stack = _stack_info_from_model(
            stack_model,
            active_stack_id=str(client.active_stack_model.id),
        )

    return _StackCreateResult(
        stack=stack,
        previous_active_stack=previous_active_stack,
        components_created=components_created,
        stack_type=StackType.LOCAL.value,
    )


def _delete_stack_operation(
    name_or_id: str,
    *,
    recursive: bool = False,
    force: bool = False,
    client_factory: Callable[[], Any] = Client,
    current_stack_getter: Callable[[], StackInfo] | None = None,
) -> _StackDeleteResult:
    """Delete a stack and return structured operation details."""
    selector = _normalize_stack_selector(name_or_id)
    client = client_factory()
    try:
        target_stack = client.get_stack(
            selector,
            allow_name_prefix_match=False,
        )
    except Exception as exc:
        try:
            resolved_stack = _resolve_stack_for_show(client, selector)
        except KitaruStateError:
            raise
        except Exception as resolve_exc:
            raise KitaruBackendError(
                f"Unable to inspect stack '{selector}' before deletion: {resolve_exc}"
            ) from exc

        try:
            target_stack = client.get_stack(
                resolved_stack.id,
                allow_name_prefix_match=False,
            )
        except Exception as hydrate_exc:
            raise KitaruBackendError(
                f"Unable to inspect stack '{selector}' before deletion: {hydrate_exc}"
            ) from hydrate_exc
    active_stack = client.active_stack_model
    is_active = str(target_stack.id) == str(active_stack.id)

    if is_active and not force:
        raise KitaruStateError(
            "Cannot delete the active stack. Use '--force' to delete and fall "
            "back to the default stack, or switch first with 'kitaru stack use "
            "<other>'."
        )

    managed_recursive_delete = recursive and _stack_is_managed(target_stack)
    cleanup_service_connectors = (
        managed_recursive_delete and not _stack_reuses_service_connectors(target_stack)
    )
    components_deleted: tuple[str, ...] = ()
    connector_selectors: tuple[str, ...] = ()
    if managed_recursive_delete:
        components_deleted = _recursive_delete_component_labels(client, target_stack)
        if cleanup_service_connectors:
            connector_selectors = (
                _linked_service_connector_selectors_for_stack(target_stack) or ()
            )

    new_active_stack: str | None = None
    if is_active and force:
        try:
            client.activate_stack("default")
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to activate the default stack before deleting the active "
                f"stack '{selector}': {exc}"
            ) from exc
        active_stack_getter = (
            current_stack if current_stack_getter is None else current_stack_getter
        )
        new_active_stack = active_stack_getter().name

    try:
        client.delete_stack(target_stack.id, recursive=recursive)
    except Exception as exc:
        raise KitaruBackendError(f"Failed to delete stack '{selector}': {exc}") from exc
    if cleanup_service_connectors:
        _delete_unshared_service_connectors_best_effort(client, connector_selectors)

    return _StackDeleteResult(
        deleted_stack=str(target_stack.name),
        components_deleted=components_deleted,
        new_active_stack=new_active_stack,
        recursive=recursive,
    )


def _stack_info_from_model(
    stack_model: Any,
    *,
    active_stack_id: str | None,
) -> StackInfo:
    """Convert a runtime stack model to Kitaru's public stack shape."""
    try:
        stack_id_raw = stack_model.id
        stack_name_raw = stack_model.name
    except AttributeError as exc:
        raise KitaruStateError(
            "Unable to read stack information from the configured runtime."
        ) from exc

    stack_id = str(stack_id_raw).strip()
    stack_name = str(stack_name_raw).strip()
    if not stack_id or stack_id == "None" or not stack_name or stack_name == "None":
        raise KitaruStateError(
            "Unable to read stack information from the configured runtime."
        )

    return StackInfo(
        id=stack_id,
        name=stack_name,
        is_active=stack_id == active_stack_id,
    )


def _iter_available_stacks(client: Client) -> Iterable[Any]:
    """Return all available stacks from the runtime, including later pages."""
    first_page = client.list_stacks()
    if not isinstance(first_page, Iterable) or isinstance(first_page, (str, bytes)):
        raise KitaruStateError(
            "Unexpected stack list response from the configured runtime."
        )

    stack_models = list(first_page)

    total_pages_raw = getattr(first_page, "total_pages", 1)
    page_size_raw = getattr(first_page, "max_size", 1)
    try:
        total_pages = int(total_pages_raw)
    except (TypeError, ValueError):
        total_pages = 1

    try:
        page_size = int(page_size_raw)
    except (TypeError, ValueError):
        page_size = 1

    for page_number in range(2, total_pages + 1):
        page_result = client.list_stacks(page=page_number, size=page_size)
        if not isinstance(page_result, Iterable) or isinstance(
            page_result,
            (str, bytes),
        ):
            raise KitaruStateError(
                "Unexpected stack list response from the configured runtime."
            )
        stack_models.extend(page_result)

    return stack_models


def current_stack(
    *,
    client_factory: Callable[[], Any] = Client,
) -> StackInfo:
    """Return the currently active stack."""
    active_stack_model = client_factory().active_stack_model
    active_stack_id = str(active_stack_model.id)
    return _stack_info_from_model(
        active_stack_model,
        active_stack_id=active_stack_id,
    )


def list_stacks(
    *,
    list_stack_entries_fn: Callable[[], list[_StackListEntry]] | None = None,
) -> list[StackInfo]:
    """List stacks visible to the current user and mark the active one."""
    entries_getter = (
        _list_stack_entries if list_stack_entries_fn is None else list_stack_entries_fn
    )
    return [entry.stack for entry in entries_getter()]


def create_stack(
    name: str,
    *,
    activate: bool = True,
    labels: dict[str, str] | None = None,
    create_stack_operation: Callable[..., _StackCreateResult] | None = None,
) -> StackInfo:
    """Create a new local stack and optionally activate it."""
    operation = (
        _create_stack_operation
        if create_stack_operation is None
        else create_stack_operation
    )
    return operation(
        name,
        activate=activate,
        labels=labels,
    ).stack


def delete_stack(
    name_or_id: str,
    *,
    recursive: bool = False,
    force: bool = False,
    delete_stack_operation: Callable[..., _StackDeleteResult] | None = None,
) -> None:
    """Delete a stack and optionally its components."""
    operation = (
        _delete_stack_operation
        if delete_stack_operation is None
        else delete_stack_operation
    )
    operation(
        name_or_id,
        recursive=recursive,
        force=force,
    )


def use_stack(
    name_or_id: str,
    *,
    client_factory: Callable[[], Any] = Client,
) -> StackInfo:
    """Set the active stack and return the activated stack info.

    Activation returns the stack model that was just resolved and activated.
    Re-reading the active stack after activation can fail when the current
    process has a stale ``ZENML_ACTIVE_STACK_ID`` environment override.
    """
    selector = _normalize_stack_selector(name_or_id)
    client = client_factory()
    resolved_stack = _resolve_stack_for_show(client, selector)
    try:
        client.activate_stack(resolved_stack.id)
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to activate stack '{selector}': {exc}"
        ) from exc
    return _stack_info_from_model(
        resolved_stack,
        active_stack_id=str(resolved_stack.id),
    )
