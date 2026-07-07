"""Stack CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from cyclopts import Parameter

from kitaru._config._active_stack_env import active_stack_env_override_warning
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru._interface_stacks import (
    CLI_STACK_OPTION_LABELS,
    _load_stack_create_file,
    _merge_stack_create_inputs,
    _StackCreateInputs,
    build_stack_create_request_from_inputs,
    execute_stack_create_request,
    parse_cli_component_overrides,
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
from ._dependencies import cli_dependencies
from ._helpers import (
    DEFAULT_LIST_PAGE,
    DEFAULT_LIST_SIZE,
    OutputFormatOption,
    PaginationPageOption,
    PaginationSizeOption,
    _emit_json_item,
    _emit_json_items,
    _emit_pagination_note,
    _emit_snapshot,
    _emit_warning,
    _exit_with_error,
    _paginate_items,
    _print_success,
    _resolve_output_format,
    _validate_pagination,
)


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
        StackType.MODAL.value,
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

    sandbox = resources.get("sandbox")
    if sandbox:
        rows.append(("Sandbox:", str(sandbox)))

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
        "sandbox": "Sandbox",
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
def list_(
    *,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List stacks visible to the current user."""
    command = "stack.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )
    deps = cli_dependencies()

    def _list_stacks() -> tuple[list[StackInfo], list[Any] | None]:
        if output_format == CLIOutputFormat.JSON:
            stack_entries = deps.list_stack_entries()
            stacks = [entry.stack for entry in stack_entries]
        else:
            stacks = deps.get_available_stacks()
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
        visible_entries = _paginate_items(stack_entries, page=page, size=size)
        _emit_json_items(
            command,
            [
                serialize_stack(entry.stack, is_managed=entry.is_managed)
                for entry in visible_entries
            ],
            output=output_format,
        )
        return

    visible_stacks = _paginate_items(stacks, page=page, size=size)
    if stacks and not visible_stacks:
        rows: list[tuple[str, str]] = [("Stacks", f"no items on page {page}")]
    else:
        rows = _stack_list_rows(visible_stacks)
    _emit_snapshot("Kitaru stacks", rows)
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(visible_stacks),
        total_count=len(stacks),
        output=output_format,
    )


@stack_app.command
def current(output: OutputFormatOption = "text") -> None:
    """Show the currently active stack."""
    command = "stack.current"
    output_format = _resolve_output_format(output)
    stack = run_with_cli_error_boundary(
        cli_dependencies().get_current_stack,
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
        lambda: cli_dependencies().show_stack_operation(name_or_id),
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
        lambda: cli_dependencies().set_active_stack(stack),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    warning = active_stack_env_override_warning(
        selected_stack_name=selected_stack.name,
        selected_stack_id=str(selected_stack.id),
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_stack(selected_stack), output=output_format)
        if warning is not None:
            message, detail = warning
            _emit_warning(message, output=output_format, detail=detail)
        return

    _print_success(
        f"Activated stack: {selected_stack.name}",
        detail=f"Stack ID: {selected_stack.id}",
    )
    if warning is not None:
        message, detail = warning
        _emit_warning(message, output=output_format, detail=detail)


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
        Parameter(
            help=(
                "Stack type: local, kubernetes, vertex, sagemaker, azureml, "
                "or modal. Modal creation requires `kitaru[modal]`."
            )
        ),
    ] = None,
    artifact_store: Annotated[
        str | None,
        Parameter(
            help=(
                "Artifact store URI for remote stacks. Modal accepts s3://, gs://, "
                "az://, abfs://, or abfss://; other stack types may require one "
                "provider-specific URI scheme."
            )
        ),
    ] = None,
    sandbox: Annotated[
        str | None,
        Parameter(
            help=(
                "Sandbox flavor to attach. Local stacks default to `local`; remote "
                "stacks attach a sandbox only when this option is provided. The "
                "flavor must be available in the active ZenML installation/server."
            )
        ),
    ] = None,
    container_registry: Annotated[
        str | None,
        Parameter(
            help=(
                "Container registry URI for Kubernetes, Vertex, SageMaker, "
                "AzureML, or Modal stacks."
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
                "Cloud provider region for Kubernetes, Vertex, SageMaker, "
                "AzureML, or credentialed Modal stack components. Optional for "
                "AzureML. For Modal, this is the cloud artifact/registry region "
                "where applicable; Modal placement uses "
                "--extra orchestrator.region=... instead."
            )
        ),
    ] = None,
    subscription_id: Annotated[
        str | None,
        Parameter(
            help=(
                "Azure subscription ID for AzureML stacks or credentialed "
                "Azure-backed Modal stack components."
            )
        ),
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
                "Optional cloud credentials reference for Kubernetes, Vertex, "
                "SageMaker, AzureML, or credentialed Modal stack components. "
                "Modal API credentials are separate: use "
                "--extra orchestrator.token_id=... and token_secret=... for "
                "Modal tokens."
            )
        ),
    ] = None,
    extra: Annotated[
        list[str] | None,
        Parameter(
            name=["--extra"],
            help=(
                "Advanced component defaults as TARGET.FIELD=VALUE. "
                "Valid targets: orchestrator, artifact_store, container_registry, "
                "sandbox. "
                "VALUE uses YAML parsing, so booleans, numbers, lists, and objects "
                "are accepted."
            ),
        ),
    ] = None,
    async_mode: Annotated[
        bool | None,
        Parameter(
            name=["--async"],
            help=(
                "Run remote stacks asynchronously by default "
                "(equivalent to `--extra orchestrator.synchronous=false`)."
            ),
        ),
    ] = None,
    no_verify: Annotated[
        bool | None,
        Parameter(
            help=(
                "Skip cloud connector verification for Kubernetes, Vertex, "
                "SageMaker, AzureML, or credentialed Modal stack components."
            )
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Create a local, Kubernetes, Vertex AI, SageMaker, AzureML, or Modal stack."""
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
                sandbox=sandbox,
                container_registry=container_registry,
                cluster=cluster,
                region=region,
                subscription_id=subscription_id,
                resource_group=resource_group,
                workspace=workspace,
                execution_role=execution_role,
                namespace=namespace,
                credentials=credentials,
                component_overrides=parse_cli_component_overrides(
                    extra,
                    labels=CLI_STACK_OPTION_LABELS,
                )
                if extra
                else None,
                async_mode=async_mode,
                verify=False if no_verify else None,
            ),
            file_inputs=file_inputs,
        )
        request = build_stack_create_request_from_inputs(
            inputs=merged_inputs,
            labels=CLI_STACK_OPTION_LABELS,
        )
        return execute_stack_create_request(
            request,
            create_stack_operation=cli_dependencies().create_stack_operation,
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
        lambda: cli_dependencies().delete_stack_operation(
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
