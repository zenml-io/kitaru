"""Kitaru command-line interface compatibility facade."""

from __future__ import annotations

import sys
import time

from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.exceptions import EntityExistsError, ZenKeyError
from zenml.login.credentials_store import get_credentials_store
from zenml.zen_server.deploy.deployer import LocalServerDeployer

from kitaru._cli import (
    _UNKNOWN_VERSION,
    app,
    executions_app,
    log_store_app,
    main,
    model_app,
    secrets_app,
    stack_app,
)
from kitaru._cli._executions import (
    _auto_detect_single_pending_wait,
    _checkpoint_summary,
    _collect_interactive_wait_candidates,
    _emit_control_message,
    _emit_empty_logs_message,
    _emit_json_log_event,
    _emit_log_entries,
    _execution_list_table,
    _execution_rows,
    _follow_execution_logs,
    _format_log_entry,
    _format_log_timestamp,
    _InteractiveWaitCandidate,
    _log_entry_dedup_key,
    _parse_json_object,
    _parse_json_value,
    _prompt_interactive_action,
    _prompt_interactive_value,
    _render_interactive_wait_candidate,
    _run_interactive_input_flow,
    _status_label,
    cancel_,
    get_,
    input_,
    list____,
    logs_,
    replay_,
    resume_,
    retry_,
)
from kitaru._cli._helpers import (
    OutputFormatOption,
    SnapshotSection,
    _emit_json_item,
    _emit_json_items,
    _emit_snapshot,
    _emit_snapshot_sections,
    _exit_with_error,
    _format_timestamp,
    _is_input_interactive,
    _is_interactive,
    _print_success,
    _print_warning,
    _render_plain_snapshot,
    _render_plain_snapshot_sections,
    _render_rich_snapshot,
    _render_rich_snapshot_sections,
    _resolve_output_format,
    _value_style,
)
from kitaru._cli._init import init
from kitaru._cli._models import _model_rows, list___, register
from kitaru._cli._secrets import (
    _SECRET_KEY_PATTERN,
    _list_accessible_secrets,
    _parse_secret_assignments,
    _resolve_secret_exact,
    _secret_list_rows,
    _secret_show_rows,
    _secret_visibility,
    delete_,
    list__,
    set_,
    show_,
)
from kitaru._cli._stacks import (
    _STACK_CREATE_FILE_BOOLEAN_KEYS,
    _STACK_CREATE_FILE_KEY_ALIASES,
    _STACK_CREATE_FILE_STRING_KEYS,
    _STACK_CREATE_FILE_SUPPORTED_KEYS,
    _current_stack_rows,
    _format_stack_component_summary,
    _load_stack_create_file,
    _merge_stack_create_inputs,
    _normalize_stack_create_file_mapping,
    _stack_create_detail_rows,
    _stack_list_rows,
    _stack_show_rows,
    _StackCreateInputs,
    create,
    current,
    delete,
    list_,
    show,
    use,
)
from kitaru._cli._status import (
    LogoutResult,
    _clear_persisted_store_configuration,
    _describe_local_server,
    _ensure_no_auth_environment_overrides,
    _environment_rows,
    _get_connected_server_url,
    _info_rows,
    _log_store_detail,
    _log_store_payload,
    _log_store_rows,
    _logout_current_connection,
    _logout_result_message,
    _logout_result_payload,
    _status_rows,
    info,
    login,
    logout,
    reset,
    set,
    show__,
    status,
)
from kitaru._env import KITARU_REPOSITORY_DIRECTORY_NAME
from kitaru._local_server import (
    LocalServerConnectionResult,
    LocalServerStopResult,
    start_or_connect_local_server,
    stop_registered_local_server,
)
from kitaru._version import resolve_installed_version
from kitaru.client import Execution, ExecutionStatus, KitaruClient, LogEntry
from kitaru.config import (
    _create_stack_operation,
    _delete_stack_operation,
    _list_stack_entries,
    _show_stack_operation,
    list_model_aliases,
    login_to_server,
    register_model_alias,
    reset_global_log_store,
    resolve_log_store,
    set_global_log_store,
)
from kitaru.config import current_stack as get_current_stack
from kitaru.config import list_stacks as get_available_stacks
from kitaru.config import use_stack as set_active_stack
from kitaru.inspection import RuntimeSnapshot
from kitaru.inspection import build_runtime_snapshot as _build_runtime_snapshot
from kitaru.inspection import combine_warnings as _combine_warnings
from kitaru.inspection import (
    connected_to_local_server_safe as _connected_to_local_server,
)
from kitaru.inspection import log_store_mismatch_details as _log_store_mismatch_details

app.version = _UNKNOWN_VERSION


def _sdk_version() -> str:
    """Resolve the installed SDK version lazily."""
    return resolve_installed_version()


def _apply_runtime_version() -> None:
    """Populate the CLI app version just before command dispatch."""
    app.version = _sdk_version()


_MULTI_TOKEN_COMMANDS: frozenset[str] = frozenset(
    {"executions", "secrets", "log-store", "stack", "model"}
)


def cli() -> None:
    """Entry point for the `kitaru` console script."""
    from kitaru.analytics import AnalyticsEvent, set_source, track

    set_source("cli")
    # Touch zen_store to mark GlobalConfiguration as initialized before
    # any analytics calls.  Without this, AnalyticsContext.__enter__()
    # sees is_initialized=False and silently skips all tracking.
    GlobalConfiguration().zen_store  # noqa: B018
    args = sys.argv[1:]
    if not args:
        command = "help"
    elif len(args) >= 2 and args[0] in _MULTI_TOKEN_COMMANDS:
        command = f"{args[0]} {args[1]}"
    else:
        command = args[0]
    track(AnalyticsEvent.CLI_INVOKED, {"command": command})
    _apply_runtime_version()
    app()


__all__ = [
    "KITARU_REPOSITORY_DIRECTORY_NAME",
    "_SECRET_KEY_PATTERN",
    "_STACK_CREATE_FILE_BOOLEAN_KEYS",
    "_STACK_CREATE_FILE_KEY_ALIASES",
    "_STACK_CREATE_FILE_STRING_KEYS",
    "_STACK_CREATE_FILE_SUPPORTED_KEYS",
    "_UNKNOWN_VERSION",
    "Client",
    "EntityExistsError",
    "Execution",
    "ExecutionStatus",
    "GlobalConfiguration",
    "KitaruClient",
    "LocalServerConnectionResult",
    "LocalServerDeployer",
    "LocalServerStopResult",
    "LogEntry",
    "LogoutResult",
    "OutputFormatOption",
    "RuntimeSnapshot",
    "SnapshotSection",
    "ZenKeyError",
    "_InteractiveWaitCandidate",
    "_StackCreateInputs",
    "_apply_runtime_version",
    "_auto_detect_single_pending_wait",
    "_build_runtime_snapshot",
    "_checkpoint_summary",
    "_clear_persisted_store_configuration",
    "_collect_interactive_wait_candidates",
    "_combine_warnings",
    "_connected_to_local_server",
    "_create_stack_operation",
    "_current_stack_rows",
    "_delete_stack_operation",
    "_describe_local_server",
    "_emit_control_message",
    "_emit_empty_logs_message",
    "_emit_json_item",
    "_emit_json_items",
    "_emit_json_log_event",
    "_emit_log_entries",
    "_emit_snapshot",
    "_emit_snapshot_sections",
    "_ensure_no_auth_environment_overrides",
    "_environment_rows",
    "_execution_list_table",
    "_execution_rows",
    "_exit_with_error",
    "_follow_execution_logs",
    "_format_log_entry",
    "_format_log_timestamp",
    "_format_stack_component_summary",
    "_format_timestamp",
    "_get_connected_server_url",
    "_info_rows",
    "_is_input_interactive",
    "_is_interactive",
    "_list_accessible_secrets",
    "_list_stack_entries",
    "_load_stack_create_file",
    "_log_entry_dedup_key",
    "_log_store_detail",
    "_log_store_mismatch_details",
    "_log_store_payload",
    "_log_store_rows",
    "_logout_current_connection",
    "_logout_result_message",
    "_logout_result_payload",
    "_merge_stack_create_inputs",
    "_model_rows",
    "_normalize_stack_create_file_mapping",
    "_parse_json_object",
    "_parse_json_value",
    "_parse_secret_assignments",
    "_print_success",
    "_print_warning",
    "_prompt_interactive_action",
    "_prompt_interactive_value",
    "_render_interactive_wait_candidate",
    "_render_plain_snapshot",
    "_render_plain_snapshot_sections",
    "_render_rich_snapshot",
    "_render_rich_snapshot_sections",
    "_resolve_output_format",
    "_resolve_secret_exact",
    "_run_interactive_input_flow",
    "_sdk_version",
    "_secret_list_rows",
    "_secret_show_rows",
    "_secret_visibility",
    "_show_stack_operation",
    "_stack_create_detail_rows",
    "_stack_list_rows",
    "_stack_show_rows",
    "_status_label",
    "_status_rows",
    "_value_style",
    "app",
    "cancel_",
    "cli",
    "create",
    "current",
    "delete",
    "delete_",
    "executions_app",
    "get_",
    "get_available_stacks",
    "get_credentials_store",
    "get_current_stack",
    "info",
    "init",
    "input_",
    "list_",
    "list__",
    "list___",
    "list____",
    "list_model_aliases",
    "log_store_app",
    "login",
    "login_to_server",
    "logout",
    "logs_",
    "main",
    "model_app",
    "register",
    "register_model_alias",
    "replay_",
    "reset",
    "reset_global_log_store",
    "resolve_installed_version",
    "resolve_log_store",
    "resume_",
    "retry_",
    "secrets_app",
    "set",
    "set_",
    "set_active_stack",
    "set_global_log_store",
    "show",
    "show_",
    "show__",
    "stack_app",
    "start_or_connect_local_server",
    "status",
    "stop_registered_local_server",
    "time",
    "use",
]
