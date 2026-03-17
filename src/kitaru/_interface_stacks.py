"""Shared stack request validation helpers for CLI and MCP surfaces."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import yaml

from kitaru._config._stacks import (
    AzureMLStackSpec,
    CloudProvider,
    KubernetesStackSpec,
    RemoteStackSpec,
    SagemakerStackSpec,
    StackComponentConfigOverrides,
    StackComponentTarget,
    StackType,
    VertexStackSpec,
)

_CREATE_ALLOWED_STACK_TYPES = (
    StackType.LOCAL,
    StackType.KUBERNETES,
    StackType.VERTEX,
    StackType.SAGEMAKER,
    StackType.AZUREML,
)
_DEFAULT_INTERFACE_STACK_TYPES = _CREATE_ALLOWED_STACK_TYPES
_REMOTE_STACK_TYPES = (
    StackType.KUBERNETES,
    StackType.VERTEX,
    StackType.SAGEMAKER,
    StackType.AZUREML,
)
_FIELD_ORDER = (
    "artifact_store",
    "container_registry",
    "cluster",
    "region",
    "subscription_id",
    "resource_group",
    "workspace",
    "execution_role",
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
    StackType.SAGEMAKER: (
        "artifact_store",
        "container_registry",
        "region",
        "execution_role",
    ),
    StackType.AZUREML: (
        "artifact_store",
        "container_registry",
        "subscription_id",
        "resource_group",
        "workspace",
    ),
}
_FIELD_ALLOWED_STACK_TYPES: dict[str, frozenset[StackType]] = {
    "artifact_store": frozenset(_REMOTE_STACK_TYPES),
    "container_registry": frozenset(_REMOTE_STACK_TYPES),
    "cluster": frozenset({StackType.KUBERNETES}),
    "region": frozenset(_REMOTE_STACK_TYPES),
    "subscription_id": frozenset({StackType.AZUREML}),
    "resource_group": frozenset({StackType.AZUREML}),
    "workspace": frozenset({StackType.AZUREML}),
    "execution_role": frozenset({StackType.SAGEMAKER}),
    "namespace": frozenset({StackType.KUBERNETES}),
    "credentials": frozenset(_REMOTE_STACK_TYPES),
    "verify": frozenset(_REMOTE_STACK_TYPES),
}
_FIXED_PROVIDER_BY_STACK_TYPE = {
    StackType.VERTEX: CloudProvider.GCP,
    StackType.SAGEMAKER: CloudProvider.AWS,
    StackType.AZUREML: CloudProvider.AZURE,
}
_PROVIDER_URI_DESCRIPTIONS: dict[StackType, tuple[str, str]] = {
    StackType.VERTEX: ("Vertex", "a gs://"),
    StackType.SAGEMAKER: ("SageMaker", "an s3://"),
    StackType.AZUREML: ("AzureML", "an az://, abfs://, or abfss://"),
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
        StackType.SAGEMAKER: "--type sagemaker",
        StackType.AZUREML: "--type azureml",
    },
    field_labels={
        "artifact_store": "--artifact-store",
        "container_registry": "--container-registry",
        "cluster": "--cluster",
        "region": "--region",
        "subscription_id": "--subscription-id",
        "resource_group": "--resource-group",
        "workspace": "--workspace",
        "execution_role": "--execution-role",
        "namespace": "--namespace",
        "credentials": "--credentials",
        "verify": "--no-verify",
        "extra": "--extra",
        "async": "--async",
    },
)

MCP_STACK_OPTION_LABELS = StackOptionLabels(
    stack_type_labels={
        StackType.LOCAL: '`stack_type="local"`',
        StackType.KUBERNETES: '`stack_type="kubernetes"`',
        StackType.VERTEX: '`stack_type="vertex"`',
        StackType.SAGEMAKER: '`stack_type="sagemaker"`',
        StackType.AZUREML: '`stack_type="azureml"`',
    },
    field_labels={
        "artifact_store": "`artifact_store`",
        "container_registry": "`container_registry`",
        "cluster": "`cluster`",
        "region": "`region`",
        "subscription_id": "`subscription_id`",
        "resource_group": "`resource_group`",
        "workspace": "`workspace`",
        "execution_role": "`execution_role`",
        "namespace": "`namespace`",
        "credentials": "`credentials`",
        "verify": "`verify`",
        "extra": "`extra`",
        "async": "`async_mode`",
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
    component_overrides: StackComponentConfigOverrides = field(
        default_factory=StackComponentConfigOverrides
    )


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


def _render_oxford_comma_list(items: list[str]) -> str:
    """Render a list of items with Oxford comma formatting."""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} or {items[1]}"
    leading = ", ".join(items[:-1])
    return f"{leading}, or {items[-1]}"


def _render_supported_stack_types(allowed_stack_types: tuple[StackType, ...]) -> str:
    """Render the supported stack types for a validation error."""
    return _render_oxford_comma_list(
        [f"'{stack_type.value}'" for stack_type in allowed_stack_types]
    )


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
        "Use an s3://, gs://, az://, abfs://, or abfss:// URI."
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
    return _render_oxford_comma_list(ordered_labels)


def _option_group_label(allowed_stack_types: frozenset[StackType]) -> str:
    """Return a human-friendly label for a set of allowed stack types."""
    if allowed_stack_types == frozenset(_REMOTE_STACK_TYPES):
        return "Remote stack options"
    if len(allowed_stack_types) == 1:
        stack_type = next(iter(allowed_stack_types))
        display_name = {
            StackType.KUBERNETES: "Kubernetes",
            StackType.VERTEX: "Vertex",
            StackType.SAGEMAKER: "SageMaker",
            StackType.AZUREML: "AzureML",
        }.get(stack_type, stack_type.value.capitalize())
        return f"{display_name}-only options"
    return "Stack-specific options"


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
    requirement_label = _render_stack_type_requirement(
        allowed_stack_types,
        labels=labels,
    )
    raise ValueError(
        f"{_option_group_label(allowed_stack_types)} require {requirement_label}: "
        + _render_field_labels(
            invalid_fields_by_allowed_types[allowed_stack_types],
            labels=labels,
        )
    )


_LOCAL_COMPONENT_OVERRIDE_TARGETS = frozenset(
    {StackComponentTarget.ORCHESTRATOR, StackComponentTarget.ARTIFACT_STORE}
)
_REMOTE_COMPONENT_OVERRIDE_TARGETS = frozenset(StackComponentTarget)


def _deep_merge_config_values(base: Any, override: Any) -> Any:
    """Recursively merge two config values, preferring the later leaf value."""
    if isinstance(base, Mapping) and isinstance(override, Mapping):
        merged = {key: value for key, value in base.items()}
        for key, value in override.items():
            merged[key] = _deep_merge_config_values(merged.get(key), value)
        return merged
    return override


def _merge_nested_mapping(
    base: Mapping[str, Any], override: Mapping[str, Any]
) -> dict[str, Any]:
    """Merge two nested config mappings into a fresh dictionary."""
    merged = {key: value for key, value in base.items()}
    for key, value in override.items():
        if key in merged:
            merged[key] = _deep_merge_config_values(merged[key], value)
        else:
            merged[key] = value
    return merged


def merge_component_overrides(
    base: StackComponentConfigOverrides | None,
    override: StackComponentConfigOverrides | None,
) -> StackComponentConfigOverrides:
    """Merge two component-override payloads with later values winning."""
    base = base or StackComponentConfigOverrides()
    override = override or StackComponentConfigOverrides()
    return StackComponentConfigOverrides(
        orchestrator=_merge_nested_mapping(base.orchestrator, override.orchestrator),
        artifact_store=_merge_nested_mapping(
            base.artifact_store,
            override.artifact_store,
        ),
        container_registry=_merge_nested_mapping(
            base.container_registry,
            override.container_registry,
        ),
    )


def _validate_nested_override_mapping(
    raw: Mapping[str, Any],
    *,
    option_name: str,
) -> dict[str, Any]:
    """Validate a nested override mapping and normalize it to plain dicts."""
    normalized: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            raise ValueError(f"{option_name} keys must be strings.")
        if isinstance(value, Mapping):
            normalized[key] = _validate_nested_override_mapping(
                value,
                option_name=option_name,
            )
        else:
            normalized[key] = value
    return normalized


def _target_field_label(
    target: StackComponentTarget,
    *,
    labels: StackOptionLabels,
) -> str:
    """Render one component target as an interface-specific extra field label."""
    extra_label = labels.field_labels["extra"]
    return f"{extra_label} {target.value}.*"


def normalize_component_overrides_mapping(
    raw: Mapping[str, Any],
    *,
    labels: StackOptionLabels,
) -> StackComponentConfigOverrides:
    """Normalize YAML/MCP component overrides into the shared override model."""
    normalized: dict[str, dict[str, Any]] = {}
    valid_targets = {target.value: target for target in StackComponentTarget}
    for raw_target, raw_value in raw.items():
        if not isinstance(raw_target, str):
            raise ValueError(f"{labels.field_labels['extra']} keys must be strings.")
        target = valid_targets.get(raw_target)
        if target is None:
            expected = ", ".join(sorted(valid_targets))
            raise ValueError(
                f"Unsupported {labels.field_labels['extra']} target '{raw_target}'. "
                f"Use one of: {expected}."
            )
        if not isinstance(raw_value, Mapping):
            raise ValueError(
                f"{_target_field_label(target, labels=labels)} must be an object."
            )
        normalized[target.value] = _validate_nested_override_mapping(
            raw_value,
            option_name=_target_field_label(target, labels=labels),
        )
    return StackComponentConfigOverrides(**normalized)


def _assign_nested_override(
    root: dict[str, Any],
    path_parts: list[str],
    value: Any,
) -> None:
    """Assign a nested override path into a mutable mapping."""
    cursor = root
    for part in path_parts[:-1]:
        existing = cursor.get(part)
        if existing is None:
            existing = {}
            cursor[part] = existing
        elif not isinstance(existing, dict):
            raise ValueError(
                f"Cannot assign nested override through non-object field '{part}'."
            )
        cursor = existing
    cursor[path_parts[-1]] = value


def _parse_component_override_value(
    raw_value: str,
    *,
    labels: StackOptionLabels,
) -> Any:
    """Parse one CLI override value using YAML scalar/object semantics."""
    if raw_value == "":
        raise ValueError(
            f"Invalid {labels.field_labels['extra']} value. Use TARGET.FIELD=VALUE."
        )
    try:
        parsed = yaml.safe_load(raw_value)
    except yaml.YAMLError as exc:
        raise ValueError(
            f"Invalid {labels.field_labels['extra']} value '{raw_value}': {exc}"
        ) from exc
    return parsed


def parse_cli_component_overrides(
    assignments: Sequence[str],
    *,
    labels: StackOptionLabels,
) -> StackComponentConfigOverrides:
    """Parse repeatable CLI TARGET.FIELD=VALUE assignments into overrides."""
    merged = StackComponentConfigOverrides()
    valid_targets = {target.value: target for target in StackComponentTarget}
    for assignment in assignments:
        lhs, separator, rhs = assignment.partition("=")
        if not separator:
            raise ValueError(
                f"Invalid {labels.field_labels['extra']} value '{assignment}'. "
                "Use TARGET.FIELD=VALUE."
            )
        target_name, field_separator, raw_path = lhs.partition(".")
        if not field_separator or not target_name or not raw_path:
            raise ValueError(
                f"Invalid {labels.field_labels['extra']} value '{assignment}'. "
                "Use TARGET.FIELD=VALUE."
            )
        target = valid_targets.get(target_name)
        if target is None:
            expected = ", ".join(sorted(valid_targets))
            raise ValueError(
                f"Unsupported {labels.field_labels['extra']} target '{target_name}'. "
                f"Use one of: {expected}."
            )
        raw_path_parts = raw_path.split(".")
        if not raw_path_parts or any(not part for part in raw_path_parts):
            raise ValueError(
                f"Invalid {labels.field_labels['extra']} value '{assignment}'. "
                "Use TARGET.FIELD=VALUE."
            )
        override_mapping: dict[str, dict[str, Any]] = {target.value: {}}
        _assign_nested_override(
            override_mapping[target.value],
            raw_path_parts,
            _parse_component_override_value(rhs, labels=labels),
        )
        merged = merge_component_overrides(
            merged,
            StackComponentConfigOverrides(**override_mapping),
        )
    return merged


def apply_async_override(
    overrides: StackComponentConfigOverrides | None,
    *,
    async_enabled: bool,
) -> StackComponentConfigOverrides:
    """Inject `synchronous=false` unless the user already set it explicitly."""
    merged = overrides or StackComponentConfigOverrides()
    if not async_enabled or "synchronous" in merged.orchestrator:
        return merged
    return merge_component_overrides(
        merged,
        StackComponentConfigOverrides(orchestrator={"synchronous": False}),
    )


def validate_component_override_targets(
    stack_type: StackType,
    overrides: StackComponentConfigOverrides | None,
    *,
    labels: StackOptionLabels,
) -> None:
    """Validate that component override targets are legal for the stack type."""
    overrides = overrides or StackComponentConfigOverrides()
    allowed_targets = (
        _LOCAL_COMPONENT_OVERRIDE_TARGETS
        if stack_type == StackType.LOCAL
        else _REMOTE_COMPONENT_OVERRIDE_TARGETS
    )
    for target in StackComponentTarget:
        if not getattr(overrides, target.value):
            continue
        if target in allowed_targets:
            continue
        if target == StackComponentTarget.CONTAINER_REGISTRY:
            raise ValueError(
                f"{_target_field_label(target, labels=labels)} is not valid for "
                f"{labels.stack_type_labels[stack_type]} because local stacks do "
                "not create a container registry component."
            )
        raise ValueError(
            f"{_target_field_label(target, labels=labels)} is not valid for "
            f"{labels.stack_type_labels[stack_type]}."
        )


def build_remote_stack_spec(
    *,
    stack_type: StackType,
    artifact_store: str | None,
    container_registry: str | None,
    cluster: str | None,
    region: str | None,
    subscription_id: str | None,
    resource_group: str | None,
    workspace: str | None,
    execution_role: str | None,
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
        "subscription_id": subscription_id is not None,
        "resource_group": resource_group is not None,
        "workspace": workspace is not None,
        "execution_role": execution_role is not None,
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
    normalized_subscription_id = normalize_optional_stack_string(subscription_id)
    normalized_resource_group = normalize_optional_stack_string(resource_group)
    normalized_workspace = normalize_optional_stack_string(workspace)
    normalized_execution_role = normalize_optional_stack_string(execution_role)
    normalized_namespace = normalize_optional_stack_string(namespace)
    normalized_credentials = normalize_optional_stack_string(credentials)

    normalized_required_values = {
        "artifact_store": normalized_artifact_store,
        "container_registry": normalized_container_registry,
        "cluster": normalized_cluster,
        "region": normalized_region,
        "subscription_id": normalized_subscription_id,
        "resource_group": normalized_resource_group,
        "workspace": normalized_workspace,
        "execution_role": normalized_execution_role,
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

    provider = infer_cloud_provider(normalized_artifact_store)
    fixed_provider = _FIXED_PROVIDER_BY_STACK_TYPE.get(stack_type)
    if fixed_provider is not None and provider != fixed_provider:
        uri_desc = _PROVIDER_URI_DESCRIPTIONS.get(stack_type)
        if uri_desc is not None:
            display_name, uri_prefix = uri_desc
            raise ValueError(
                f"{display_name} stacks require {uri_prefix} "
                f"artifact store URI. Received: '{normalized_artifact_store}'."
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
        assert normalized_region is not None
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
        assert normalized_region is not None
        return VertexStackSpec(
            artifact_store=normalized_artifact_store,
            container_registry=normalized_container_registry,
            region=normalized_region,
            credentials=normalized_credentials,
            verify=verify,
        )

    if stack_type == StackType.SAGEMAKER:
        assert normalized_region is not None
        assert normalized_execution_role is not None
        return SagemakerStackSpec(
            artifact_store=normalized_artifact_store,
            container_registry=normalized_container_registry,
            region=normalized_region,
            execution_role=normalized_execution_role,
            credentials=normalized_credentials,
            verify=verify,
        )

    if stack_type == StackType.AZUREML:
        assert normalized_subscription_id is not None
        assert normalized_resource_group is not None
        assert normalized_workspace is not None
        return AzureMLStackSpec(
            artifact_store=normalized_artifact_store,
            container_registry=normalized_container_registry,
            subscription_id=normalized_subscription_id,
            resource_group=normalized_resource_group,
            workspace=normalized_workspace,
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
    subscription_id: str | None,
    resource_group: str | None,
    workspace: str | None,
    execution_role: str | None,
    namespace: str | None,
    credentials: str | None,
    verify: bool,
    component_overrides: StackComponentConfigOverrides | None = None,
    async_enabled: bool = False,
    labels: StackOptionLabels,
    allowed_stack_types: tuple[StackType, ...] = _DEFAULT_INTERFACE_STACK_TYPES,
) -> ManageStackCreateRequest:
    """Validate create inputs and build a structured stack-create request."""
    normalized_stack_type = normalize_stack_type(
        stack_type,
        allowed_stack_types=allowed_stack_types,
    )
    if async_enabled and normalized_stack_type == StackType.LOCAL:
        requirement = _render_stack_type_requirement(
            frozenset(_REMOTE_STACK_TYPES),
            labels=labels,
        )
        raise ValueError(f"{labels.field_labels['async']} requires {requirement}.")
    validate_component_override_targets(
        normalized_stack_type,
        component_overrides,
        labels=labels,
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
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace=workspace,
            execution_role=execution_role,
            namespace=namespace,
            credentials=credentials,
            verify=verify,
            labels=labels,
        ),
        component_overrides=apply_async_override(
            component_overrides,
            async_enabled=async_enabled,
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
    subscription_id: str | None,
    resource_group: str | None,
    workspace: str | None,
    execution_role: str | None,
    namespace: str | None,
    credentials: str | None,
    verify: bool,
    extra: Mapping[str, Any] | None = None,
    async_mode: bool = False,
) -> ManageStackCreateRequest | ManageStackDeleteRequest:
    """Validate MCP manage-stack inputs and build a structured request."""
    if action == "create":
        if recursive or force:
            raise ValueError(
                '`recursive` and `force` are only valid when action="delete".'
            )
        if extra is not None and not isinstance(extra, Mapping):
            raise ValueError("`extra` must be an object when provided.")
        return build_stack_create_request(
            name=name,
            activate=activate,
            stack_type=stack_type,
            artifact_store=artifact_store,
            container_registry=container_registry,
            cluster=cluster,
            region=region,
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace=workspace,
            execution_role=execution_role,
            namespace=namespace,
            credentials=credentials,
            verify=verify,
            component_overrides=(
                normalize_component_overrides_mapping(
                    extra, labels=MCP_STACK_OPTION_LABELS
                )
                if extra is not None
                else None
            ),
            async_enabled=async_mode,
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
                ("subscription_id", subscription_id is not None),
                ("resource_group", resource_group is not None),
                ("workspace", workspace is not None),
                ("execution_role", execution_role is not None),
                ("namespace", namespace is not None),
                ("credentials", credentials is not None),
                ("verify", not verify),
                ("extra", extra is not None),
                ("async", async_mode),
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
