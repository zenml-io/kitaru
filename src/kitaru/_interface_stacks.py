"""Shared stack request validation helpers for CLI and MCP surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from kitaru._config._stacks import (
    CloudProvider,
    KubernetesStackSpec,
    RemoteStackSpec,
    StackType,
    VertexStackSpec,
)

_CREATE_ALLOWED_STACK_TYPES = (
    StackType.LOCAL,
    StackType.KUBERNETES,
    StackType.VERTEX,
)
_DEFAULT_INTERFACE_STACK_TYPES = _CREATE_ALLOWED_STACK_TYPES
_REMOTE_STACK_TYPES = (StackType.KUBERNETES, StackType.VERTEX)
_FIELD_ORDER = (
    "artifact_store",
    "container_registry",
    "cluster",
    "region",
    "namespace",
    "credentials",
    "verify",
)
_REQUIRED_FIELDS: dict[StackType, tuple[str, ...]] = {
    StackType.KUBERNETES: (
        "artifact_store",
        "container_registry",
        "cluster",
        "region",
    ),
    StackType.VERTEX: (
        "artifact_store",
        "container_registry",
        "region",
    ),
}
_FIELD_ALLOWED_STACK_TYPES: dict[str, frozenset[StackType]] = {
    "artifact_store": frozenset(_REMOTE_STACK_TYPES),
    "container_registry": frozenset(_REMOTE_STACK_TYPES),
    "cluster": frozenset({StackType.KUBERNETES}),
    "region": frozenset(_REMOTE_STACK_TYPES),
    "namespace": frozenset({StackType.KUBERNETES}),
    "credentials": frozenset(_REMOTE_STACK_TYPES),
    "verify": frozenset(_REMOTE_STACK_TYPES),
}
_FIXED_PROVIDER_BY_STACK_TYPE = {
    StackType.VERTEX: CloudProvider.GCP,
    StackType.SAGEMAKER: CloudProvider.AWS,
    StackType.AZUREML: CloudProvider.AZURE,
}


@dataclass(frozen=True)
class StackOptionLabels:
    """Interface-specific labels used in stack validation errors."""

    stack_type_labels: Mapping[StackType, str]
    field_labels: Mapping[str, str]


CLI_STACK_OPTION_LABELS = StackOptionLabels(
    stack_type_labels={
        StackType.LOCAL: "--type local",
        StackType.KUBERNETES: "--type kubernetes",
        StackType.VERTEX: "--type vertex",
    },
    field_labels={
        "artifact_store": "--artifact-store",
        "container_registry": "--container-registry",
        "cluster": "--cluster",
        "region": "--region",
        "namespace": "--namespace",
        "credentials": "--credentials",
        "verify": "--no-verify",
    },
)

MCP_STACK_OPTION_LABELS = StackOptionLabels(
    stack_type_labels={
        StackType.LOCAL: '`stack_type="local"`',
        StackType.KUBERNETES: '`stack_type="kubernetes"`',
        StackType.VERTEX: '`stack_type="vertex"`',
    },
    field_labels={
        "artifact_store": "`artifact_store`",
        "container_registry": "`container_registry`",
        "cluster": "`cluster`",
        "region": "`region`",
        "namespace": "`namespace`",
        "credentials": "`credentials`",
        "verify": "`verify`",
        "stack_type": "`stack_type`",
    },
)


@dataclass(frozen=True)
class ManageStackCreateRequest:
    """Validated stack-create request shared by interface layers."""

    name: str
    activate: bool
    stack_type: StackType
    remote_spec: RemoteStackSpec | None = None


@dataclass(frozen=True)
class ManageStackDeleteRequest:
    """Validated stack-delete request shared by interface layers."""

    name: str
    recursive: bool
    force: bool


def normalize_optional_stack_string(value: str | None) -> str | None:
    """Normalize an optional stack input, treating blanks as omitted."""
    if value is None:
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _render_supported_stack_types(allowed_stack_types: tuple[StackType, ...]) -> str:
    """Render the supported stack types for a validation error."""
    values = [stack_type.value for stack_type in allowed_stack_types]
    if len(values) == 1:
        return f"'{values[0]}'"
    if len(values) == 2:
        return f"'{values[0]}' or '{values[1]}'"
    leading_values = ", ".join(f"'{value}'" for value in values[:-1])
    return f"{leading_values}, or '{values[-1]}'"


def normalize_stack_type(
    raw_type: str,
    *,
    allowed_stack_types: tuple[StackType, ...] = _DEFAULT_INTERFACE_STACK_TYPES,
) -> StackType:
    """Normalize a stack-type input into the internal enum."""
    normalized_type = raw_type.strip().lower()
    try:
        stack_type = StackType(normalized_type)
    except ValueError as exc:
        raise ValueError(
            f"Unsupported stack type: {raw_type}. Use "
            f"{_render_supported_stack_types(allowed_stack_types)}."
        ) from exc

    if stack_type not in allowed_stack_types:
        raise ValueError(
            f"Unsupported stack type: {raw_type}. Use "
            f"{_render_supported_stack_types(allowed_stack_types)}."
        )
    return stack_type


def infer_cloud_provider(artifact_store_uri: str) -> CloudProvider:
    """Infer the cloud provider from an artifact-store URI."""
    if artifact_store_uri.startswith("s3://"):
        return CloudProvider.AWS
    if artifact_store_uri.startswith("gs://"):
        return CloudProvider.GCP
    if artifact_store_uri.startswith(("az://", "abfs://", "abfss://")):
        return CloudProvider.AZURE
    raise ValueError(
        f"Cannot infer cloud provider from '{artifact_store_uri}'. "
        "Use an s3:// or gs:// URI."
    )


def _render_field_labels(field_names: list[str], *, labels: StackOptionLabels) -> str:
    """Render interface-specific labels for one or more stack fields."""
    return ", ".join(labels.field_labels[field_name] for field_name in field_names)


def _render_stack_type_requirement(
    allowed_stack_types: frozenset[StackType],
    *,
    labels: StackOptionLabels,
) -> str:
    """Render stack-type requirements for validation messages."""
    ordered_labels = [
        labels.stack_type_labels[stack_type]
        for stack_type in _CREATE_ALLOWED_STACK_TYPES
        if stack_type in allowed_stack_types and stack_type in labels.stack_type_labels
    ]
    if len(ordered_labels) == 1:
        return ordered_labels[0]
    if len(ordered_labels) == 2:
        return f"{ordered_labels[0]} or {ordered_labels[1]}"
    leading_values = ", ".join(ordered_labels[:-1])
    return f"{leading_values}, or {ordered_labels[-1]}"


def _validate_explicit_field_usage(
    *,
    stack_type: StackType,
    provided_fields: Mapping[str, bool],
    labels: StackOptionLabels,
) -> None:
    """Reject fields that were explicitly provided for the wrong stack type."""
    invalid_fields_by_allowed_types: dict[frozenset[StackType], list[str]] = {}
    for field_name in _FIELD_ORDER:
        if not provided_fields.get(field_name, False):
            continue
        allowed_stack_types = _FIELD_ALLOWED_STACK_TYPES[field_name]
        if stack_type in allowed_stack_types:
            continue
        invalid_fields_by_allowed_types.setdefault(allowed_stack_types, []).append(
            field_name
        )

    if not invalid_fields_by_allowed_types:
        return

    ordered_allowed_type_groups = sorted(
        invalid_fields_by_allowed_types,
        key=lambda group: (
            len(group),
            tuple(item.value for item in _CREATE_ALLOWED_STACK_TYPES if item in group),
        ),
    )
    allowed_stack_types = ordered_allowed_type_groups[0]
    option_label = (
        "Remote stack options"
        if allowed_stack_types == frozenset(_REMOTE_STACK_TYPES)
        else "Kubernetes-only options"
    )
    requirement_label = _render_stack_type_requirement(
        allowed_stack_types,
        labels=labels,
    )
    raise ValueError(
        f"{option_label} require {requirement_label}: "
        + _render_field_labels(
            invalid_fields_by_allowed_types[allowed_stack_types],
            labels=labels,
        )
    )


def build_remote_stack_spec(
    *,
    stack_type: StackType,
    artifact_store: str | None,
    container_registry: str | None,
    cluster: str | None,
    region: str | None,
    namespace: str | None,
    credentials: str | None,
    verify: bool,
    labels: StackOptionLabels,
) -> RemoteStackSpec | None:
    """Validate interface inputs and build a remote stack spec when needed."""
    provided_fields = {
        "artifact_store": artifact_store is not None,
        "container_registry": container_registry is not None,
        "cluster": cluster is not None,
        "region": region is not None,
        "namespace": namespace is not None,
        "credentials": credentials is not None,
        "verify": not verify,
    }
    _validate_explicit_field_usage(
        stack_type=stack_type,
        provided_fields=provided_fields,
        labels=labels,
    )

    if stack_type == StackType.LOCAL:
        return None

    normalized_artifact_store = normalize_optional_stack_string(artifact_store)
    normalized_container_registry = normalize_optional_stack_string(container_registry)
    normalized_cluster = normalize_optional_stack_string(cluster)
    normalized_region = normalize_optional_stack_string(region)
    normalized_namespace = normalize_optional_stack_string(namespace)
    normalized_credentials = normalize_optional_stack_string(credentials)

    normalized_required_values = {
        "artifact_store": normalized_artifact_store,
        "container_registry": normalized_container_registry,
        "cluster": normalized_cluster,
        "region": normalized_region,
    }
    missing_required_fields = [
        field_name
        for field_name in _REQUIRED_FIELDS[stack_type]
        if normalized_required_values[field_name] is None
    ]
    if missing_required_fields:
        raise ValueError(
            f"{labels.stack_type_labels[stack_type]} requires: "
            + _render_field_labels(missing_required_fields, labels=labels)
            + "."
        )

    assert normalized_artifact_store is not None
    assert normalized_container_registry is not None
    assert normalized_region is not None

    provider = infer_cloud_provider(normalized_artifact_store)
    fixed_provider = _FIXED_PROVIDER_BY_STACK_TYPE.get(stack_type)
    if fixed_provider is not None and provider != fixed_provider:
        if stack_type == StackType.VERTEX:
            raise ValueError(
                "Vertex stacks require a gs:// artifact store URI. "
                f"Received: '{normalized_artifact_store}'."
            )
        raise ValueError(
            f"{stack_type.value} stacks require a "
            f"{fixed_provider.value} artifact store."
        )

    if stack_type == StackType.KUBERNETES:
        if provider not in {CloudProvider.AWS, CloudProvider.GCP}:
            raise ValueError(
                f"Cannot infer cloud provider from '{normalized_artifact_store}'. "
                "Use an s3:// or gs:// URI."
            )
        assert normalized_cluster is not None
        return KubernetesStackSpec(
            provider=provider,
            artifact_store=normalized_artifact_store,
            container_registry=normalized_container_registry,
            cluster=normalized_cluster,
            region=normalized_region,
            namespace=normalized_namespace or "default",
            credentials=normalized_credentials,
            verify=verify,
        )

    if stack_type == StackType.VERTEX:
        return VertexStackSpec(
            artifact_store=normalized_artifact_store,
            container_registry=normalized_container_registry,
            region=normalized_region,
            credentials=normalized_credentials,
            verify=verify,
        )

    raise ValueError(f"Unsupported stack type: {stack_type.value}")


def build_stack_create_request(
    *,
    name: str,
    activate: bool,
    stack_type: str,
    artifact_store: str | None,
    container_registry: str | None,
    cluster: str | None,
    region: str | None,
    namespace: str | None,
    credentials: str | None,
    verify: bool,
    labels: StackOptionLabels,
    allowed_stack_types: tuple[StackType, ...] = _DEFAULT_INTERFACE_STACK_TYPES,
) -> ManageStackCreateRequest:
    """Validate create inputs and build a structured stack-create request."""
    normalized_stack_type = normalize_stack_type(
        stack_type,
        allowed_stack_types=allowed_stack_types,
    )
    return ManageStackCreateRequest(
        name=name,
        activate=activate,
        stack_type=normalized_stack_type,
        remote_spec=build_remote_stack_spec(
            stack_type=normalized_stack_type,
            artifact_store=artifact_store,
            container_registry=container_registry,
            cluster=cluster,
            region=region,
            namespace=namespace,
            credentials=credentials,
            verify=verify,
            labels=labels,
        ),
    )


def build_manage_stack_request(
    *,
    action: Literal["create", "delete"] | str,
    name: str,
    activate: bool,
    recursive: bool,
    force: bool,
    stack_type: str,
    artifact_store: str | None,
    container_registry: str | None,
    cluster: str | None,
    region: str | None,
    namespace: str | None,
    credentials: str | None,
    verify: bool,
) -> ManageStackCreateRequest | ManageStackDeleteRequest:
    """Validate MCP manage-stack inputs and build a structured request."""
    if action == "create":
        if recursive or force:
            raise ValueError(
                '`recursive` and `force` are only valid when action="delete".'
            )
        return build_stack_create_request(
            name=name,
            activate=activate,
            stack_type=stack_type,
            artifact_store=artifact_store,
            container_registry=container_registry,
            cluster=cluster,
            region=region,
            namespace=namespace,
            credentials=credentials,
            verify=verify,
            labels=MCP_STACK_OPTION_LABELS,
        )

    if action == "delete":
        if not activate:
            raise ValueError('`activate` is only valid when action="create".')

        normalized_stack_type = normalize_stack_type(
            stack_type,
            allowed_stack_types=_CREATE_ALLOWED_STACK_TYPES,
        )
        stack_create_only_fields = [
            field_name
            for field_name, is_provided in (
                ("stack_type", normalized_stack_type != StackType.LOCAL),
                ("artifact_store", artifact_store is not None),
                ("container_registry", container_registry is not None),
                ("cluster", cluster is not None),
                ("region", region is not None),
                ("namespace", namespace is not None),
                ("credentials", credentials is not None),
                ("verify", not verify),
            )
            if is_provided
        ]
        if stack_create_only_fields:
            raise ValueError(
                'Stack create options are only valid when action="create": '
                + _render_field_labels(
                    stack_create_only_fields,
                    labels=MCP_STACK_OPTION_LABELS,
                )
            )

        return ManageStackDeleteRequest(
            name=name,
            recursive=recursive,
            force=force,
        )

    raise ValueError('`action` must be "create" or "delete".')
