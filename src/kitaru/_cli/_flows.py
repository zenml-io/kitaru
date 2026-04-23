"""Flow deployment CLI commands."""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

from cyclopts import Parameter

from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    resolve_deployment_exclusive,
)
from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME
from kitaru._flow_loading import _load_deployable_flow_target
from kitaru._interface_deployments import (
    build_deployment_deploy_kwargs,
    resolve_deployment_selector,
    validate_deployment_selector,
)
from kitaru._interface_errors import (
    InterfaceErrorDetails,
    run_with_cli_error_boundary,
    translate_to_user_error,
)
from kitaru._interface_executions import (
    build_started_deployment_payload,
    flow_handle_exec_id,
    resolve_started_execution_details,
)
from kitaru.analytics import AnalyticsEvent, track
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import resolve_connection_config
from kitaru.errors import (
    KitaruDeploymentInputValuesError,
    KitaruUsageError,
    StackNotRemoteExecutable,
)
from kitaru.inspection import (
    serialize_deployment,
    serialize_flow_deployment_summary,
    serialize_log_entry,
)

from . import app, flow_app, flow_deployments_app
from ._executions import (
    _emit_control_message,
    _emit_json_log_event,
    _emit_log_entries,
    _follow_execution_logs,
    _parse_json_object,
)
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
    _emit_table,
    _exit_with_error,
    _facade_module,
    _format_table_timestamp,
    _format_timestamp,
    _paginate_items,
    _print_success,
    _resolve_output_format,
    _validate_pagination,
)

SERVER_ACCESS_TOKEN_COMMAND = "kitaru auth token"
SERVER_ACCESS_TOKEN_ENV = "KITARU_SERVER_ACCESS_TOKEN"
_LEGACY_REPOSITORY_DIRECTORY_NAME = ".zen"


def _translate_build_or_deploy_error(exc: Exception) -> InterfaceErrorDetails:
    """Add CLI-specific remediation guidance for build/deploy failures."""
    details = translate_to_user_error(exc)
    if isinstance(exc, StackNotRemoteExecutable):
        return InterfaceErrorDetails(
            message=(
                f"{details.message} Use `--stack <stack>` to select a stack the "
                "Kitaru server can execute remotely, or run `kitaru stack use "
                "<stack>` to change your active stack before retrying."
            ),
            error_type=details.error_type,
        )

    if isinstance(exc, KitaruDeploymentInputValuesError):
        return InterfaceErrorDetails(
            message=(
                f"{details.message} From the CLI, pass representative input values "
                'with `--input \'{"key": "value"}\'` (or `--input @inputs.json`).'
            ),
            error_type=details.error_type,
        )

    return details


def _project_marker_root(start: Path) -> Path | None:
    """Walk upward from a starting path to find a Kitaru project marker."""
    for parent in (start, *start.parents):
        for marker_name in (
            KITARU_REPOSITORY_DIRECTORY_NAME,
            _LEGACY_REPOSITORY_DIRECTORY_NAME,
        ):
            if (parent / marker_name).is_dir():
                return parent
    return None


def _ensure_deployment_project_initialized(target: str) -> None:
    """Fail early when build/deploy is launched outside an initialized project."""
    module_ref, separator, _ = target.partition(":")
    if separator != ":" or not module_ref:
        return

    if not module_ref.endswith(".py"):
        return

    candidate = Path(module_ref).expanduser()
    if not candidate.exists():
        return

    search_root = candidate.resolve().parent
    if _project_marker_root(search_root) is not None:
        return

    raise KitaruUsageError(
        "Building or deploying from source requires an initialized Kitaru project. "
        "Run `kitaru init` in the repository root, or `cd` into an initialized "
        "Kitaru project before using `kitaru build` or `kitaru deploy`."
    )


def _format_deployment_tags(tags: Mapping[str, bool]) -> str:
    """Render public deployment tags compactly for tables/snapshots."""
    if not tags:
        return "none"
    rendered = [
        f"{tag}*" if exclusive else tag for tag, exclusive in sorted(tags.items())
    ]
    return ", ".join(rendered)


def _deployment_rows(deployment: Any) -> list[tuple[str, str]]:
    """Build label/value rows for one deployment."""
    return [
        ("Flow", str(deployment.flow)),
        ("Version", f"v{deployment.version}"),
        ("Tags", _format_deployment_tags(deployment.tags)),
        ("Created", _format_timestamp(deployment.created_at)),
        ("Stack", deployment.stack or "not available"),
        ("Deployment ID", str(deployment.deployment_id)),
        ("Commit", deployment.commit_sha or "not available"),
        (
            "Dirty",
            str(deployment.commit_dirty)
            if deployment.commit_dirty is not None
            else "unknown",
        ),
        ("Image digest", deployment.image_digest or "not available"),
    ]


def _deployment_list_table(deployments: Sequence[Any]) -> list[list[str]]:
    """Build table rows for deployment lists."""
    return [
        [
            f"v{deployment.version}",
            _format_deployment_tags(deployment.tags),
            _format_table_timestamp(deployment.created_at),
            deployment.stack or "not set",
            str(deployment.deployment_id),
        ]
        for deployment in deployments
    ]


def _group_deployments_by_flow(deployments: Sequence[Any]) -> dict[str, list[Any]]:
    """Group deployment records/facades by flow name."""
    grouped: dict[str, list[Any]] = {}
    for deployment in deployments:
        grouped.setdefault(str(deployment.flow), []).append(deployment)
    return dict(sorted(grouped.items()))


def _flow_list_table(items: Sequence[dict[str, Any]]) -> list[list[str]]:
    """Build table rows for flow summaries."""
    rows: list[list[str]] = []
    for item in items:
        tag_names = sorted(item["tags"].keys())
        rows.append(
            [
                item["flow"],
                str(item["deployment_count"]),
                f"v{item['latest_version']}" if item["latest_version"] else "none",
                f"v{item['default_version']}" if item["default_version"] else "none",
                ", ".join(tag_names) if tag_names else "none",
            ]
        )
    return rows


def _selector_kind(*, version: int | None, tag: str | None) -> str:
    """Return a privacy-safe selector label for analytics and JSON output."""
    if version is not None:
        return "version"
    if tag == DEFAULT_DEPLOYMENT_TAG:
        return "default"
    return "tag"


def _build_deploy_kwargs(
    *,
    stack: str | None,
    cache: bool | None,
    retries: int | None,
    tags: Mapping[str, bool],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Build keyword arguments for `flow.deploy(...)` without noisy Nones."""
    return build_deployment_deploy_kwargs(
        stack=stack,
        cache=cache,
        retries=retries,
        tags=tags,
        inputs=inputs,
        input_label="`--input`",
    )


def _active_kitaru_server_url() -> str:
    try:
        resolved_connection = resolve_connection_config()
    except Exception as exc:
        raise KitaruUsageError(
            "No active Kitaru server connection could be read. Run `kitaru login` "
            "or set KITARU_SERVER_URL before generating a deployment curl command."
        ) from exc

    server_url = (resolved_connection.server_url or "").strip()
    if not server_url.startswith(("http://", "https://")):
        raise KitaruUsageError(
            "Deployment curl generation requires an active Kitaru server connection. "
            "Run `kitaru login` or set KITARU_SERVER_URL."
        )
    return server_url.rstrip("/")


def _deployment_invoke_url(*, server_url: str, deployment_id: str) -> str:
    encoded_id = quote(str(deployment_id), safe="")
    return f"{server_url}/api/v1/pipeline_snapshots/{encoded_id}/runs"


def _deployment_curl_body(inputs: Mapping[str, Any]) -> dict[str, Any]:
    if not inputs:
        return {}
    return {"run_configuration": {"parameters": dict(inputs)}}


def _shell_double_quote(value: str) -> str:
    """Double-quote a shell argument while preserving env-var expansion."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("`", "\\`")
    return f'"{escaped}"'


def _server_access_token_assignment() -> str:
    return f'{SERVER_ACCESS_TOKEN_ENV}="$({SERVER_ACCESS_TOKEN_COMMAND})"'


def _format_deployment_curl_command(
    *,
    invoke_url: str,
    body: Mapping[str, Any],
) -> str:
    """Format a copy-pasteable curl command without inlining real credentials."""
    body_json = json.dumps(body, separators=(",", ":"), sort_keys=True)
    auth_header = _shell_double_quote(
        f"Authorization: Bearer ${{{SERVER_ACCESS_TOKEN_ENV}}}"
    )
    curl_lines = "\n".join(
        [
            "curl -sS -X POST \\",
            f"  {shlex.quote(invoke_url)} \\",
            f"  -H {auth_header} \\",
            f"  -H {shlex.quote('Content-Type: application/json')} \\",
            f"  -H {shlex.quote('Accept: application/json')} \\",
            f"  -d {shlex.quote(body_json)}",
        ]
    )
    return f"{_server_access_token_assignment()}\n\n{curl_lines}"


def _deployment_curl_warning_lines(
    *,
    flow: str,
    tag: str | None,
    deployment_version: int,
) -> list[str]:
    if tag is None:
        return []
    return [
        f"Resolved {flow} tag {tag!r} to deployment version v{deployment_version}.",
        f"This command is pinned to v{deployment_version}. "
        "Regenerate it if you move the tag.",
    ]


def _deployment_curl_payload(
    *,
    flow: str,
    selector_version: int | None,
    selector_tag: str | None,
    deployment: Any,
    server_url: str,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    body = _deployment_curl_body(inputs)
    invoke_url = _deployment_invoke_url(
        server_url=server_url,
        deployment_id=str(deployment.deployment_id),
    )
    curl_command = _format_deployment_curl_command(invoke_url=invoke_url, body=body)
    warning_lines = _deployment_curl_warning_lines(
        flow=flow,
        tag=selector_tag,
        deployment_version=int(deployment.version),
    )
    payload: dict[str, Any] = {
        "flow": flow,
        "selector": {"version": selector_version, "tag": selector_tag},
        "resolved_deployment_version": deployment.version,
        "deployment_id": deployment.deployment_id,
        "server_url": server_url,
        "invoke_url": invoke_url,
        "token_env_var": SERVER_ACCESS_TOKEN_ENV,
        "token_command": SERVER_ACCESS_TOKEN_COMMAND,
        "request_body": body,
        "curl_command": curl_command,
    }
    if warning_lines:
        payload["warning_lines"] = warning_lines
        payload["warning"] = " ".join(warning_lines)
    return payload


def _resolve_latest_deployment_execution_id(
    *,
    client: Any,
    flow: str,
    deployment: Any,
) -> str:
    """Best-effort lookup of the latest execution associated with a deployment."""
    # executions.list returns desc:created, so the first match is the latest.
    # Intentionally do not cap this lookup: a deployment can still be valid even
    # when its latest execution is older than the first page of recent flow runs.
    executions = client.executions.list(flow=flow)
    for execution in executions:
        metadata = getattr(execution, "metadata", {}) or {}
        if not isinstance(metadata, Mapping):
            continue
        nested = metadata.get("kitaru_deployment")
        nested_mapping = nested if isinstance(nested, Mapping) else {}
        deployment_ids = {
            metadata.get("deployment_id"),
            metadata.get("kitaru_deployment_id"),
            nested_mapping.get("deployment_id"),
        }
        versions = {
            metadata.get("deployment_version"),
            metadata.get("kitaru_deployment_version"),
            nested_mapping.get("version"),
        }
        if deployment.deployment_id in deployment_ids:
            return str(execution.exec_id)
        if deployment.version in versions or str(deployment.version) in versions:
            return str(execution.exec_id)

    raise LookupError(
        "No execution logs could be resolved for deployment "
        f"'{flow}' v{deployment.version}. Pass --exec-id or use "
        "kitaru executions logs <exec-id>."
    )


@app.command
def build(
    target: Annotated[
        str,
        Parameter(help="Flow target `<module_or_file>:<flow_name>`."),
    ],
    *,
    input_: Annotated[
        str | None,
        Parameter(
            alias="--input",
            help="Deployment-time default flow inputs as JSON or `@file`.",
        ),
    ] = None,
    stack: Annotated[str | None, Parameter(help="Optional stack override.")] = None,
    cache: Annotated[bool | None, Parameter(help="Optional cache override.")] = None,
    retries: Annotated[int | None, Parameter(help="Optional retry override.")] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Build an immutable deployment version from a flow target."""
    command = "build"
    output_format = _resolve_output_format(output)

    def _build_deployment() -> Any:
        _ensure_deployment_project_initialized(target)
        inputs = _parse_json_object(input_, option_name="--input", allow_file=True)
        deploy_kwargs = _build_deploy_kwargs(
            stack=stack,
            cache=cache,
            retries=retries,
            tags={},
            inputs=inputs,
        )
        flow_target = _load_deployable_flow_target(
            target,
            module_name_prefix="kitaru_cli_deploy_",
        )
        deployment = flow_target.deploy(**deploy_kwargs)
        track(
            AnalyticsEvent.DEPLOYMENT_BUILT,
            {"command": command, "has_input": bool(inputs)},
        )
        return deployment

    deployment = run_with_cli_error_boundary(
        _build_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        translator=_translate_build_or_deploy_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_deployment(deployment), output=output_format)
        return

    _emit_snapshot("Kitaru deployment", _deployment_rows(deployment))


@app.command
def deploy(
    target: Annotated[
        str,
        Parameter(help="Flow target `<module_or_file>:<flow_name>`."),
    ],
    *,
    tag: Annotated[
        str,
        Parameter(
            help=(
                "Single routing tag to attach at deploy time. Use `kitaru flow "
                "tag` to add or move tags later."
            )
        ),
    ] = DEFAULT_DEPLOYMENT_TAG,
    exclusive: Annotated[
        bool, Parameter(help="Make this deploy-time tag exclusive.")
    ] = False,
    input_: Annotated[
        str | None,
        Parameter(
            alias="--input",
            help="Representative deployment-time flow inputs as JSON or `@file`.",
        ),
    ] = None,
    stack: Annotated[str | None, Parameter(help="Optional stack override.")] = None,
    cache: Annotated[bool | None, Parameter(help="Optional cache override.")] = None,
    retries: Annotated[int | None, Parameter(help="Optional retry override.")] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Deploy a new flow version and attach one routing tag.

    `kitaru deploy` attaches exactly one routing tag at deploy time. To add or
    move more tags later, use `kitaru flow tag`.
    """
    command = "deploy"
    output_format = _resolve_output_format(output)

    def _deploy_flow() -> Any:
        _ensure_deployment_project_initialized(target)
        inputs = _parse_json_object(input_, option_name="--input", allow_file=True)
        _, normalized_tag = validate_deployment_selector(
            tag=tag,
            require_one=True,
        )
        assert normalized_tag is not None  # guaranteed by require_one=True
        resolved_tags = {
            normalized_tag: resolve_deployment_exclusive(normalized_tag, exclusive)
        }
        deploy_kwargs = _build_deploy_kwargs(
            stack=stack,
            cache=cache,
            retries=retries,
            tags=resolved_tags,
            inputs=inputs,
        )
        flow_target = _load_deployable_flow_target(
            target,
            module_name_prefix="kitaru_cli_deploy_",
        )
        deployment = flow_target.deploy(**deploy_kwargs)
        track(
            AnalyticsEvent.DEPLOYMENT_DEPLOYED,
            {
                "command": command,
                "has_input": bool(inputs),
                "selector": _selector_kind(version=None, tag=normalized_tag),
                "exclusive": bool(resolved_tags[normalized_tag]),
            },
        )
        return deployment

    deployment = run_with_cli_error_boundary(
        _deploy_flow,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        translator=_translate_build_or_deploy_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_deployment(deployment), output=output_format)
        return

    _emit_snapshot("Kitaru deployment", _deployment_rows(deployment))


@app.command
def invoke(
    flow: Annotated[
        str,
        Parameter(help="Deployment-backed flow name."),
    ],
    *,
    version: Annotated[int | None, Parameter(help="Exact deployment version.")] = None,
    tag: Annotated[str | None, Parameter(help="Deployment tag selector.")] = None,
    input_: Annotated[
        str | None,
        Parameter(alias="--input", help="Invocation inputs as JSON or `@file`."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Invoke a deployed flow snapshot.

    If you omit `--version` and `--tag`, Kitaru tries the implicit `default`
    route. If that route is missing, invoke with an explicit tag/version or move
    `default` with `kitaru flow tag`.
    """
    command = "invoke"
    output_format = _resolve_output_format(output)

    def _invoke_deployment() -> tuple[Any, dict[str, Any], int | None, str | None]:
        inputs = _parse_json_object(input_, option_name="--input", allow_file=True)
        selector = resolve_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        client = _facade_module().KitaruClient()
        handle = client.deployments.invoke(
            flow=flow,
            version=selector.version,
            tag=selector.tag,
            selector_source=selector.source,
            inputs=inputs,
        )
        exec_id = flow_handle_exec_id(handle)
        details = resolve_started_execution_details(exec_id=exec_id, client=client)
        payload = build_started_deployment_payload(
            flow=flow,
            version=selector.version,
            tag=selector.tag,
            details=details,
        )
        track(
            AnalyticsEvent.DEPLOYMENT_INVOKED,
            {
                "command": command,
                "has_input": bool(inputs),
                "selector": _selector_kind(
                    version=selector.version,
                    tag=selector.tag,
                ),
            },
        )
        return handle, payload, selector.version, selector.tag

    handle, payload, resolved_version, resolved_tag = run_with_cli_error_boundary(
        _invoke_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, payload, output=output_format)
        return

    selector = (
        f"version v{resolved_version}"
        if resolved_version is not None
        else f"tag {resolved_tag!r}"
    )
    _print_success(
        f"Invoked deployment: {flow} ({selector})",
        detail=f"Execution: {flow_handle_exec_id(handle)}",
    )
    if payload.get("warning"):
        print(f"Warning: {payload['warning']}")


@flow_app.command
def list_(output: OutputFormatOption = "text") -> None:
    """List flows that have deployment versions."""
    command = "flow.list"
    output_format = _resolve_output_format(output)
    deployments = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().deployments.list(),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    items = [
        serialize_flow_deployment_summary(flow, grouped)
        for flow, grouped in _group_deployments_by_flow(deployments).items()
    ]

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(command, items, output=output_format)
        return

    _emit_table(
        "Kitaru flows",
        ["Flow", "Deployments", "Latest", "Default", "Tags"],
        _flow_list_table(items),
    )


@flow_app.command
def show(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    output: OutputFormatOption = "text",
) -> None:
    """Show deployment summary for one flow."""
    command = "flow.show"
    output_format = _resolve_output_format(output)

    def _show_flow() -> dict[str, Any]:
        deployments = _facade_module().KitaruClient().deployments.list(flow=flow)
        if not deployments:
            raise LookupError(f"No deployments found for flow {flow!r}.")
        return serialize_flow_deployment_summary(flow, deployments)

    item = run_with_cli_error_boundary(
        _show_flow,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, item, output=output_format)
        return

    _emit_snapshot(
        "Kitaru flow",
        [
            ("Flow", item["flow"]),
            ("Deployments", str(item["deployment_count"])),
            ("Latest", f"v{item['latest_version']}"),
            (
                "Default",
                f"v{item['default_version']}" if item["default_version"] else "none",
            ),
            ("Tags", ", ".join(sorted(item["tags"].keys())) or "none"),
        ],
    )


@flow_deployments_app.command
def list__(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    *,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List deployment versions for one flow."""
    command = "flow.deployments.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )
    deployments = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().deployments.list(flow=flow),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    page_items = _paginate_items(deployments, page=page, size=size)

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_deployment(deployment) for deployment in page_items],
            output=output_format,
        )
        return

    _emit_table(
        "Kitaru deployments",
        ["Version", "Tags", "Created", "Stack", "Deployment ID"],
        _deployment_list_table(page_items),
    )
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(page_items),
        total_count=len(deployments),
        output=output_format,
    )


@flow_deployments_app.command
def show__(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    *,
    version: Annotated[int | None, Parameter(help="Exact deployment version.")] = None,
    tag: Annotated[str | None, Parameter(help="Deployment tag selector.")] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Show one deployment version."""
    command = "flow.deployments.show"
    output_format = _resolve_output_format(output)

    def _show_deployment() -> Any:
        resolved_version, resolved_tag = validate_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        return (
            _facade_module()
            .KitaruClient()
            .deployments.get(
                flow=flow,
                version=resolved_version,
                tag=resolved_tag,
            )
        )

    deployment = run_with_cli_error_boundary(
        _show_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_deployment(deployment), output=output_format)
        return

    _emit_snapshot("Kitaru deployment", _deployment_rows(deployment))


@flow_deployments_app.command
def curl(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    *,
    version: Annotated[int | None, Parameter(help="Exact deployment version.")] = None,
    tag: Annotated[str | None, Parameter(help="Deployment tag selector.")] = None,
    input_: Annotated[
        str | None,
        Parameter(alias="--input", help="Invocation inputs as JSON or `@file`."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Generate a curl command that starts a deployment execution."""
    command = "flow.deployments.curl"
    output_format = _resolve_output_format(output)

    def _generate_curl() -> dict[str, Any]:
        inputs = _parse_json_object(input_, option_name="--input", allow_file=True)
        resolved_version, resolved_tag = validate_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        client = _facade_module().KitaruClient()
        deployment = client.deployments.get(
            flow=flow,
            version=resolved_version,
            tag=resolved_tag,
        )
        client.deployments._ensure_deployment_server_runnable(
            deployment,
            operation="curl",
        )
        server_url = _active_kitaru_server_url()
        payload = _deployment_curl_payload(
            flow=flow,
            selector_version=resolved_version,
            selector_tag=resolved_tag,
            deployment=deployment,
            server_url=server_url,
            inputs=inputs,
        )
        track(
            AnalyticsEvent.DEPLOYMENT_CURL_GENERATED,
            {
                "command": command,
                "has_input": bool(inputs),
                "selector": _selector_kind(version=resolved_version, tag=resolved_tag),
            },
        )
        return payload

    payload = run_with_cli_error_boundary(
        _generate_curl,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, payload, output=output_format)
        return

    warning_lines = payload.get("warning_lines") or []
    for line in warning_lines:
        print(f"# {line}")
    if warning_lines:
        print()
    print(payload["curl_command"])


@flow_deployments_app.command
def logs(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    *,
    version: Annotated[int | None, Parameter(help="Exact deployment version.")] = None,
    tag: Annotated[str | None, Parameter(help="Deployment tag selector.")] = None,
    exec_id: Annotated[
        str | None,
        Parameter(help="Explicit execution ID to read logs from."),
    ] = None,
    checkpoint: Annotated[
        str | None,
        Parameter(help="Optional checkpoint function name to filter by."),
    ] = None,
    source: Annotated[str, Parameter(help='Log source (default: "step").')] = "step",
    limit: Annotated[
        int | None, Parameter(help="Maximum total log entries to return.")
    ] = None,
    follow: Annotated[bool, Parameter(help="Stream until terminal status.")] = False,
    interval: Annotated[float, Parameter(help="Polling interval in seconds.")] = 3.0,
    grouped: Annotated[bool, Parameter(help="Group output by checkpoint.")] = False,
    output: OutputFormatOption = "text",
    verbosity: Annotated[
        int,
        Parameter(alias=["-v"], count=True, help="Increase log verbosity."),
    ] = 0,
) -> None:
    """Fetch runtime logs for an execution associated with a deployment."""
    command = "flow.deployments.logs"
    output_format = _resolve_output_format(output)

    if grouped and output_format == CLIOutputFormat.JSON:
        _exit_with_error(
            command,
            "`--grouped` cannot be combined with `--output json`.",
            output=output_format,
        )
    if checkpoint and source.strip().lower() == "runner":
        _exit_with_error(
            command,
            "`--checkpoint` cannot be combined with `--source runner`.",
            output=output_format,
        )
    if interval <= 0:
        _exit_with_error(command, "`--interval` must be > 0.", output=output_format)

    def _resolve_logs_target() -> tuple[Any, str]:
        client = _facade_module().KitaruClient()
        if exec_id:
            return client, exec_id

        resolved_version, resolved_tag = validate_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        deployment = client.deployments.get(
            flow=flow,
            version=resolved_version,
            tag=resolved_tag,
        )
        resolved_exec_id = _resolve_latest_deployment_execution_id(
            client=client,
            flow=flow,
            deployment=deployment,
        )
        return client, resolved_exec_id

    client, resolved_exec_id = run_with_cli_error_boundary(
        _resolve_logs_target,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    verbosity = min(verbosity, 2)
    if follow:
        try:
            exit_code = run_with_cli_error_boundary(
                lambda: _follow_execution_logs(
                    client=client,
                    exec_id=resolved_exec_id,
                    checkpoint=checkpoint,
                    source=source,
                    limit=limit,
                    output=output_format,
                    grouped=grouped,
                    verbosity=verbosity,
                    interval=interval,
                    command=command,
                ),
                command=command,
                output=output_format,
                exit_with_error=_exit_with_error,
            )
        except KeyboardInterrupt:
            if output_format == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "interrupted",
                    {"message": "Log follow interrupted"},
                    command=command,
                )
            else:
                _emit_control_message("[Log follow interrupted]", output=output_format)
            raise SystemExit(1) from None
        raise SystemExit(exit_code)

    entries = run_with_cli_error_boundary(
        lambda: client.executions.logs(
            resolved_exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_log_entry(entry) for entry in entries],
            output=output_format,
        )
        return

    if not entries:
        print(f"No log entries found for execution {resolved_exec_id}.")
        print("The execution may still be starting, or step logging may be disabled.")
        return

    _emit_log_entries(
        entries,
        output=output_format,
        grouped=grouped,
        verbosity=verbosity,
    )


@flow_deployments_app.command
def delete(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    *,
    version: Annotated[int, Parameter(help="Exact deployment version to delete.")],
    output: OutputFormatOption = "text",
) -> None:
    """Delete one deployment version when no exclusive tag protects it."""
    command = "flow.deployments.delete"
    output_format = _resolve_output_format(output)

    def _delete_deployment() -> dict[str, Any]:
        _facade_module().KitaruClient().deployments.delete(flow=flow, version=version)
        track(
            AnalyticsEvent.DEPLOYMENT_DELETED,
            {"command": command, "selector": "version"},
        )
        return {"flow": flow, "version": version, "deleted": True}

    item = run_with_cli_error_boundary(
        _delete_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, item, output=output_format)
        return

    _print_success(f"Deleted deployment: {flow} v{version}")


@flow_app.command
def tag(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    tag: Annotated[
        str,
        Parameter(help="Public deployment tag to attach or move after deploy time."),
    ],
    *,
    version: Annotated[int, Parameter(help="Exact deployment version to tag.")],
    exclusive: Annotated[
        bool, Parameter(help="Move this tag away from older versions.")
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Attach or move a public tag on one deployment version.

    Use this after `kitaru deploy` when you want to add another route or move an
    existing one without creating a new deployment version.
    """
    command = "flow.tag"
    output_format = _resolve_output_format(output)

    def _tag_deployment() -> Any:
        deployment = (
            _facade_module()
            .KitaruClient()
            .deployments.tag(
                flow=flow,
                version=version,
                tag=tag,
                exclusive=exclusive,
            )
        )
        track(
            AnalyticsEvent.DEPLOYMENT_TAGGED,
            {"command": command, "selector": "version", "exclusive": exclusive},
        )
        return deployment

    deployment = run_with_cli_error_boundary(
        _tag_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_deployment(deployment), output=output_format)
        return

    _emit_snapshot("Kitaru deployment", _deployment_rows(deployment))


@flow_app.command
def untag(
    flow: Annotated[str, Parameter(help="Deployment-backed flow name.")],
    tag: Annotated[str, Parameter(help="Public deployment tag to remove.")],
    *,
    version: Annotated[int, Parameter(help="Exact deployment version to untag.")],
    output: OutputFormatOption = "text",
) -> None:
    """Remove a public tag from one deployment version."""
    command = "flow.untag"
    output_format = _resolve_output_format(output)

    def _untag_deployment() -> Any:
        deployment = (
            _facade_module()
            .KitaruClient()
            .deployments.untag(
                flow=flow,
                version=version,
                tag=tag,
            )
        )
        track(
            AnalyticsEvent.DEPLOYMENT_UNTAGGED,
            {"command": command, "selector": "version"},
        )
        return deployment

    deployment = run_with_cli_error_boundary(
        _untag_deployment,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_deployment(deployment), output=output_format)
        return

    _emit_snapshot("Kitaru deployment", _deployment_rows(deployment))
