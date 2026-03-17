"""Stack CLI commands."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from cyclopts import Parameter
from zenml.utils import yaml_utils

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru._interface_stacks import (
    CLI_STACK_OPTION_LABELS,
    normalize_optional_stack_string,
)
from kitaru._interface_stacks import (
    build_stack_create_request as _build_shared_stack_create_request,
)
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import StackInfo, StackType
from kitaru.inspection import (
    serialize_stack,
    serialize_stack_create_result,
    serialize_stack_delete_result,
    serialize_stack_details,
)

from . import stack_app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_json_items,
    _emit_snapshot,
    _exit_with_error,
    _facade_module,
    _print_success,
    _resolve_output_format,
)


@dataclass(frozen=True)
class _StackCreateInputs:
    """Normalized stack-create inputs before backend validation."""

    name: str | None = None
    type: str | None = None
    activate: bool | None = None
    artifact_store: str | None = None
    container_registry: str | None = None
    cluster: str | None = None
    region: str | None = None
    subscription_id: str | None = None
    resource_group: str | None = None
    workspace: str | None = None
    execution_role: str | None = None
    namespace: str | None = None
    credentials: str | None = None
    verify: bool | None = None


_STACK_CREATE_FILE_KEY_ALIASES = {
    "artifact-store": "artifact_store",
    "container-registry": "container_registry",
    "subscription-id": "subscription_id",
    "resource-group": "resource_group",
    "execution-role": "execution_role",
}
_STACK_CREATE_FILE_SUPPORTED_KEYS = {
    "name",
    "type",
    "activate",
    "artifact_store",
    "artifact-store",
    "container_registry",
    "container-registry",
    "cluster",
    "region",
    "subscription_id",
    "subscription-id",
    "resource_group",
    "resource-group",
    "workspace",
    "execution_role",
    "execution-role",
    "namespace",
    "credentials",
    "verify",
}
_STACK_CREATE_FILE_STRING_KEYS = {
    "name",
    "type",
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
}
_STACK_CREATE_FILE_BOOLEAN_KEYS = {
    "activate",
    "verify",
}


def _normalize_stack_create_file_mapping(
    raw: dict[str, Any],
    *,
    source: Path,
) -> _StackCreateInputs:
    """Validate and normalize a stack-create YAML mapping."""
    non_string_keys = [repr(key) for key in raw if not isinstance(key, str)]
    if non_string_keys:
        raise ValueError(
            f"Stack config file '{source}' can only use string keys: "
            + ", ".join(sorted(non_string_keys))
        )

    unknown_keys = sorted(
        key for key in raw if key not in _STACK_CREATE_FILE_SUPPORTED_KEYS
    )
    if unknown_keys:
        raise ValueError(
            f"Unsupported stack config keys in '{source}': " + ", ".join(unknown_keys)
        )

    normalized_values: dict[str, Any] = {}
    canonical_sources: dict[str, str] = {}
    for raw_key, value in raw.items():
        canonical_key = _STACK_CREATE_FILE_KEY_ALIASES.get(raw_key, raw_key)
        existing_source = canonical_sources.get(canonical_key)
        if existing_source is not None:
            raise ValueError(
                f"Stack config file '{source}' cannot define both "
                f"'{existing_source}' and '{raw_key}'."
            )

        if canonical_key in _STACK_CREATE_FILE_STRING_KEYS:
            if value is not None and not isinstance(value, str):
                raise ValueError(
                    f"Stack config key '{raw_key}' in '{source}' must be a string."
                )
        elif (
            canonical_key in _STACK_CREATE_FILE_BOOLEAN_KEYS
            and value is not None
            and not isinstance(value, bool)
        ):
            raise ValueError(
                f"Stack config key '{raw_key}' in '{source}' must be a boolean."
            )

        canonical_sources[canonical_key] = raw_key
        normalized_values[canonical_key] = value

    return _StackCreateInputs(**normalized_values)


def _load_stack_create_file(path: Path) -> _StackCreateInputs:
    """Load stack-create inputs from a YAML file."""
    try:
        raw = yaml_utils.read_yaml(str(path))
    except FileNotFoundError:
        raise ValueError(f"Stack config file not found: {path}") from None
    except Exception as exc:
        raise ValueError(f"Invalid YAML in stack config file '{path}': {exc}") from exc

    if raw is None:
        return _StackCreateInputs()
    if not isinstance(raw, dict):
        raise ValueError(
            f"Stack config file '{path}' must contain a top-level mapping."
        )

    return _normalize_stack_create_file_mapping(raw, source=path)


def _merge_stack_create_inputs(
    *,
    cli_inputs: _StackCreateInputs,
    file_inputs: _StackCreateInputs | None,
) -> _StackCreateInputs:
    """Merge CLI and YAML stack-create inputs with CLI precedence."""
    file_inputs = file_inputs or _StackCreateInputs()
    merged = {
        field.name: (
            cli_val
            if (cli_val := getattr(cli_inputs, field.name)) is not None
            else getattr(file_inputs, field.name)
        )
        for field in dataclasses.fields(_StackCreateInputs)
    }
    return _StackCreateInputs(**merged)


def _stack_list_rows(stacks: list[StackInfo]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru stack list`."""
    if not stacks:
        return [("Stacks", "none found")]

    return [
        (
            stack.name,
            f"{stack.id}{' (active)' if stack.is_active else ''}",
        )
        for stack in stacks
    ]


def _stack_create_detail_rows(result: Any) -> list[tuple[str, str]]:
    """Build optional detail rows for stack-create success output."""
    stack_type = getattr(result, "stack_type", StackType.LOCAL.value)
    if stack_type not in {
        StackType.KUBERNETES.value,
        StackType.VERTEX.value,
        StackType.SAGEMAKER.value,
        StackType.AZUREML.value,
    }:
        return []

    resources = getattr(result, "resources", None)
    if not isinstance(resources, dict):
        return []

    rows: list[tuple[str, str]] = []
    provider = resources.get("provider")
    if provider:
        rows.append(("Provider:", str(provider)))

    cluster = resources.get("cluster")
    region = resources.get("region")
    if stack_type == StackType.KUBERNETES.value and cluster:
        cluster_value = str(cluster)
        if region:
            cluster_value = f"{cluster_value} ({region})"
        rows.append(("Cluster:", cluster_value))
    elif stack_type in {StackType.VERTEX.value, StackType.SAGEMAKER.value} and region:
        rows.append(("Region:", str(region)))
    elif stack_type == StackType.AZUREML.value:
        subscription_id = resources.get("subscription_id")
        if subscription_id:
            rows.append(("Subscription:", str(subscription_id)))
        resource_group = resources.get("resource_group")
        if resource_group:
            rows.append(("Resource group:", str(resource_group)))
        workspace = resources.get("workspace")
        if workspace:
            rows.append(("Workspace:", str(workspace)))
        if region:
            rows.append(("Region:", str(region)))

    artifact_store = resources.get("artifact_store")
    if artifact_store:
        rows.append(("Artifacts:", str(artifact_store)))

    container_registry = resources.get("container_registry")
    if container_registry:
        rows.append(("Registry:", str(container_registry)))

    execution_role = resources.get("execution_role")
    if stack_type == StackType.SAGEMAKER.value and execution_role:
        rows.append(("Execution role:", str(execution_role)))

    return rows


def _current_stack_rows(stack: StackInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru stack current`."""
    return [
        ("Active stack", stack.name),
        ("Stack ID", stack.id),
    ]


def _format_stack_component_summary(component: Any) -> str:
    """Render one stack component for `kitaru stack show` text output."""
    summary = str(getattr(component, "name", "<unnamed>"))
    backend = getattr(component, "backend", None)
    if backend:
        summary += f" ({backend})"

    for key, value in getattr(component, "details", ()):
        summary += f"; {key.replace('_', ' ')}: {value}"

    purpose = getattr(component, "purpose", None)
    if purpose:
        summary += f"; purpose: {purpose}"

    return summary


def _stack_show_rows(details: Any) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru stack show`."""
    rows: list[tuple[str, str]] = [
        ("Name", details.stack.name),
        ("ID", details.stack.id),
        ("Type", str(details.stack_type)),
        ("Active", "yes" if details.stack.is_active else "no"),
        ("Managed", "yes" if getattr(details, "is_managed", False) else "no"),
    ]

    components = list(getattr(details, "components", ()))
    if not components:
        rows.append(("Components", "None reported"))
        return rows

    component_labels = {
        "runner": "Runner",
        "storage": "Storage",
        "image_registry": "Image registry",
        "additional_component": "Additional component",
    }
    label_counts: dict[str, int] = {}

    for component in components:
        base_label = component_labels.get(
            getattr(component, "role", "additional_component"),
            "Additional component",
        )
        label_counts[base_label] = label_counts.get(base_label, 0) + 1
        suffix = f" #{label_counts[base_label]}" if label_counts[base_label] > 1 else ""
        rows.append(
            (
                f"{base_label}{suffix}",
                _format_stack_component_summary(component),
            )
        )

    return rows


@stack_app.command
def list_(output: OutputFormatOption = "text") -> None:
    """List stacks visible to the current user."""
    command = "stack.list"
    output_format = _resolve_output_format(output)
    facade = _facade_module()

    def _list_stacks() -> tuple[list[StackInfo], list[Any] | None]:
        if output_format == CLIOutputFormat.JSON:
            stack_entries = facade._list_stack_entries()
            stacks = [entry.stack for entry in stack_entries]
        else:
            stacks = facade.get_available_stacks()
            stack_entries = None
        return stacks, stack_entries

    stacks, stack_entries = run_with_cli_error_boundary(
        _list_stacks,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        assert stack_entries is not None
        _emit_json_items(
            command,
            [
                serialize_stack(entry.stack, is_managed=entry.is_managed)
                for entry in stack_entries
            ],
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru stacks", _stack_list_rows(stacks))


@stack_app.command
def current(output: OutputFormatOption = "text") -> None:
    """Show the currently active stack."""
    command = "stack.current"
    output_format = _resolve_output_format(output)
    stack = run_with_cli_error_boundary(
        _facade_module().get_current_stack,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_stack(stack), output=output_format)
        return

    _emit_snapshot("Kitaru stack", _current_stack_rows(stack))


@stack_app.command
def show(
    name_or_id: Annotated[
        str,
        Parameter(help="Stack name or ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Show translated details for a stack by name or ID."""
    command = "stack.show"
    output_format = _resolve_output_format(output)
    details = run_with_cli_error_boundary(
        lambda: _facade_module()._show_stack_operation(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_stack_details(details),
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru stack", _stack_show_rows(details))


@stack_app.command
def use(
    stack: Annotated[
        str,
        Parameter(help="Stack name or ID to activate."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Use a stack as the active default by name or ID."""
    command = "stack.use"
    output_format = _resolve_output_format(output)
    selected_stack = run_with_cli_error_boundary(
        lambda: _facade_module().set_active_stack(stack),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_stack(selected_stack), output=output_format)
        return

    _print_success(
        f"Activated stack: {selected_stack.name}",
        detail=f"Stack ID: {selected_stack.id}",
    )


@stack_app.command
def create(
    name: Annotated[
        str | None,
        Parameter(help="Stack name. Required unless provided in --file."),
    ] = None,
    /,
    *,
    file: Annotated[
        Path | None,
        Parameter(
            help="Load stack configuration from a YAML file.",
            alias=["-f"],
        ),
    ] = None,
    no_activate: Annotated[
        bool | None,
        Parameter(help="Create without activating the stack."),
    ] = None,
    type: Annotated[
        str | None,
        Parameter(help="Stack type: local, kubernetes, vertex, sagemaker, or azureml."),
    ] = None,
    artifact_store: Annotated[
        str | None,
        Parameter(
            help=(
                "Artifact store URI for remote stacks "
                "(Kubernetes: s3:// or gs://; Vertex: gs://; SageMaker: s3://; "
                "AzureML: az://, abfs://, or abfss://)."
            )
        ),
    ] = None,
    container_registry: Annotated[
        str | None,
        Parameter(
            help=(
                "Container registry URI for Kubernetes, Vertex, SageMaker, or "
                "AzureML stacks."
            )
        ),
    ] = None,
    cluster: Annotated[
        str | None,
        Parameter(help="Kubernetes cluster name."),
    ] = None,
    region: Annotated[
        str | None,
        Parameter(
            help=(
                "Cloud region for Kubernetes, Vertex, SageMaker, or AzureML "
                "stacks. Optional for AzureML."
            )
        ),
    ] = None,
    subscription_id: Annotated[
        str | None,
        Parameter(help="Azure subscription ID for AzureML stacks."),
    ] = None,
    resource_group: Annotated[
        str | None,
        Parameter(help="Azure resource group for AzureML stacks."),
    ] = None,
    workspace: Annotated[
        str | None,
        Parameter(help="AzureML workspace name for AzureML stacks."),
    ] = None,
    execution_role: Annotated[
        str | None,
        Parameter(help="SageMaker execution role ARN."),
    ] = None,
    namespace: Annotated[
        str | None,
        Parameter(help="Kubernetes namespace (defaults to `default`)."),
    ] = None,
    credentials: Annotated[
        str | None,
        Parameter(
            help=(
                "Optional credentials reference for Kubernetes, Vertex, "
                "SageMaker, or AzureML stacks."
            )
        ),
    ] = None,
    no_verify: Annotated[
        bool | None,
        Parameter(
            help=(
                "Skip credential verification for Kubernetes, Vertex, "
                "SageMaker, or AzureML stacks."
            )
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Create a local, Kubernetes-backed, Vertex AI, SageMaker, or AzureML stack."""
    command = "stack.create"
    output_format = _resolve_output_format(output)

    def _create_stack() -> Any:
        file_inputs = _load_stack_create_file(file) if file is not None else None
        merged_inputs = _merge_stack_create_inputs(
            cli_inputs=_StackCreateInputs(
                name=name,
                type=type,
                activate=False if no_activate else None,
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
                verify=False if no_verify else None,
            ),
            file_inputs=file_inputs,
        )
        normalized_name = normalize_optional_stack_string(merged_inputs.name)
        if normalized_name is None:
            raise ValueError("Stack name or ID cannot be empty.")

        request = _build_shared_stack_create_request(
            name=normalized_name,
            activate=merged_inputs.activate
            if merged_inputs.activate is not None
            else True,
            stack_type=(
                merged_inputs.type
                if merged_inputs.type is not None
                else StackType.LOCAL.value
            ),
            artifact_store=merged_inputs.artifact_store,
            container_registry=merged_inputs.container_registry,
            cluster=merged_inputs.cluster,
            region=merged_inputs.region,
            subscription_id=merged_inputs.subscription_id,
            resource_group=merged_inputs.resource_group,
            workspace=merged_inputs.workspace,
            execution_role=merged_inputs.execution_role,
            namespace=merged_inputs.namespace,
            credentials=merged_inputs.credentials,
            verify=merged_inputs.verify if merged_inputs.verify is not None else True,
            labels=CLI_STACK_OPTION_LABELS,
        )
        return _facade_module()._create_stack_operation(
            request.name,
            stack_type=request.stack_type,
            activate=request.activate,
            remote_spec=request.remote_spec,
        )

    result = run_with_cli_error_boundary(
        _create_stack,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_stack_create_result(result),
            output=output_format,
        )
        return

    created_message = f"Created stack: {result.stack.name}"
    result_stack_type = getattr(result, "stack_type", StackType.LOCAL.value)
    if result_stack_type != StackType.LOCAL.value:
        created_message += f" ({result_stack_type})"
    _print_success(created_message)
    for label, value in _stack_create_detail_rows(result):
        print(f"{label:<12} {value}")
    if result.previous_active_stack is not None:
        print(f"Active stack: {result.previous_active_stack} → {result.stack.name}")


@stack_app.command
def delete(
    stack: Annotated[
        str,
        Parameter(help="Stack name or ID to delete."),
    ],
    recursive: Annotated[
        bool,
        Parameter(help="Delete the stack and any unshared managed components."),
    ] = False,
    force: Annotated[
        bool,
        Parameter(
            help=(
                "Allow deleting the active stack by falling back to the default stack."
            )
        ),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Delete a stack by name or ID."""
    command = "stack.delete"
    output_format = _resolve_output_format(output)
    result = run_with_cli_error_boundary(
        lambda: _facade_module()._delete_stack_operation(
            stack,
            recursive=recursive,
            force=force,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_stack_delete_result(result),
            output=output_format,
        )
        return

    _print_success(f"Deleted stack: {result.deleted_stack}")
    if result.components_deleted:
        print(f"Deleted components: {', '.join(result.components_deleted)}")
    if result.new_active_stack is not None:
        print(f"Active stack: {result.new_active_stack}")
