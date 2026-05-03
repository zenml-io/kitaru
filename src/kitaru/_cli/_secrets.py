"""Secret-management CLI commands."""

from __future__ import annotations

from typing import Annotated, Any

from cyclopts import Parameter
from zenml.exceptions import EntityExistsError
from zenml.models import SecretResponse

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.inspection import serialize_secret_detail, serialize_secret_summary
from kitaru.secrets import _SECRET_KEY_PATTERN

from . import secrets_app
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
    _exit_with_error,
    _paginate_items,
    _print_success,
    _resolve_output_format,
    _validate_pagination,
)


def _parse_secret_assignments(raw_assignments: list[str]) -> dict[str, str]:
    """Parse `--KEY=value` style assignment tokens into a dictionary."""
    if not raw_assignments:
        raise ValueError(
            "Provide at least one secret assignment (for example "
            "`--OPENAI_API_KEY=sk-...`)."
        )

    parsed: dict[str, str] = {}
    idx = 0
    while idx < len(raw_assignments):
        token = raw_assignments[idx]

        if token == "--":
            idx += 1
            continue

        if not token.startswith("--"):
            raise ValueError(
                f"Invalid secret assignment `{token}`. Use `--KEY=value` format."
            )

        key_part = token[2:]
        if not key_part:
            raise ValueError("Secret key cannot be empty.")

        if "=" in key_part:
            key, value = key_part.split("=", 1)
        else:
            if idx + 1 >= len(raw_assignments):
                raise ValueError(
                    f"Missing value for secret key `{key_part}`. Use `--KEY=value`."
                )
            key = key_part
            value = raw_assignments[idx + 1]
            if value == "--" or value.startswith("--"):
                raise ValueError(
                    f"Missing value for secret key `{key}`. Use `--KEY=value`."
                )
            idx += 1

        if not _SECRET_KEY_PATTERN.fullmatch(key):
            raise ValueError(
                "Invalid secret key "
                f"`{key}`. Use env-var style names "
                "(letters, numbers, underscores; cannot start with a number)."
            )

        if value == "":
            raise ValueError(f"Secret value for key `{key}` cannot be empty.")

        if key in parsed:
            raise ValueError(f"Duplicate secret key `{key}` in one command.")

        parsed[key] = value
        idx += 1

    if not parsed:
        raise ValueError(
            "Provide at least one secret assignment (for example "
            "`--OPENAI_API_KEY=sk-...`)."
        )

    return parsed


def _list_accessible_secrets(client: Any) -> list[SecretResponse]:
    """List all accessible secrets across all pages."""
    first_page = client.list_secrets(page=1)
    secrets = list(first_page.items)

    for page_number in range(2, first_page.total_pages + 1):
        page = client.list_secrets(page=page_number, size=first_page.max_size)
        secrets.extend(page.items)

    return secrets


def _secret_show_rows(
    secret: SecretResponse,
    *,
    show_values: bool,
) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru secrets show`."""
    keys = sorted(secret.values.keys())
    rows: list[tuple[str, str]] = [
        ("Name", secret.name),
        ("Secret ID", str(secret.id)),
        ("Visibility", "private" if secret.private else "public"),
        ("Keys", ", ".join(keys) if keys else "none"),
        ("Missing values", "yes" if secret.has_missing_values else "no"),
    ]

    if show_values and keys:
        for key in keys:
            value = secret.secret_values.get(key, "unavailable")
            rows.append((f"Value ({key})", value))

    return rows


def _order_secrets(secrets: list[SecretResponse]) -> list[SecretResponse]:
    """Return secrets in deterministic CLI display order."""
    return sorted(secrets, key=lambda secret: (secret.name.lower(), str(secret.id)))


def _secret_list_rows(secrets: list[SecretResponse]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru secrets list`."""
    if not secrets:
        return [("Secrets", "none found")]

    return [
        (
            secret.name,
            f"{secret.id} ({'private' if secret.private else 'public'})",
        )
        for secret in secrets
    ]


@secrets_app.command
def set_(
    name: Annotated[
        str,
        Parameter(help="Secret name."),
    ],
    assignments: Annotated[
        list[str],
        Parameter(
            help="One or more secret assignments in `--KEY=value` form.",
            allow_leading_hyphen=True,
        ),
    ],
    private: Annotated[
        bool,
        Parameter(help="Create a private secret instead of the default public secret."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Set a public secret by default, or create it privately with --private."""
    command = "secrets.set"
    output_format = _resolve_output_format(output)
    deps = cli_dependencies()

    def _set_secret() -> tuple[SecretResponse, str, int]:
        parsed_assignments = _parse_secret_assignments(assignments)
        client = deps.zenml_client()

        try:
            secret = client.create_secret(
                name=name,
                values=parsed_assignments,
                private=private,
            )
            action = "Created"
        except EntityExistsError:
            existing_secret = deps.resolve_secret_exact(client, name)
            secret = client.update_secret(
                name_id_or_prefix=existing_secret.id,
                add_or_update_values=parsed_assignments,
            )
            action = "Updated"
        return secret, action, len(parsed_assignments)

    secret, action, key_count = run_with_cli_error_boundary(
        _set_secret,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.SECRET_UPSERTED,
        {
            "operation": action.lower(),
            "key_count": key_count,
        },
    )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_secret_summary(secret)
        payload["result"] = action.lower()
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(
        f"{action} secret: {secret.name}",
        detail=f"Secret ID: {secret.id}",
    )


@secrets_app.command
def show_(
    name_or_id: Annotated[
        str,
        Parameter(help="Secret name or ID."),
    ],
    show_values: Annotated[
        bool,
        Parameter(help="Display raw secret values in command output."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Show a secret with metadata and optional raw values."""
    command = "secrets.show"
    output_format = _resolve_output_format(output)
    deps = cli_dependencies()
    secret = run_with_cli_error_boundary(
        lambda: deps.resolve_secret_exact(deps.zenml_client(), name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_secret_detail(secret, show_values=show_values),
            output=output_format,
        )
        return

    _emit_snapshot(
        "Kitaru secret",
        _secret_show_rows(secret, show_values=show_values),
    )


@secrets_app.command
def list__(
    *,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List all secrets visible to the current user context."""
    command = "secrets.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )
    secrets = run_with_cli_error_boundary(
        lambda: _list_accessible_secrets(cli_dependencies().zenml_client()),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    ordered = _order_secrets(secrets)
    visible_secrets = _paginate_items(ordered, page=page, size=size)

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_secret_summary(secret) for secret in visible_secrets],
            output=output_format,
        )
        return

    if ordered and not visible_secrets:
        rows: list[tuple[str, str]] = [("Secrets", f"no items on page {page}")]
    else:
        rows = _secret_list_rows(visible_secrets)
    _emit_snapshot("Kitaru secrets", rows)
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(visible_secrets),
        total_count=len(ordered),
        output=output_format,
    )


@secrets_app.command
def delete_(
    name_or_id: Annotated[
        str,
        Parameter(help="Secret name or ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Delete a secret by exact name or exact ID."""
    command = "secrets.delete"
    output_format = _resolve_output_format(output)
    deps = cli_dependencies()

    def _delete_secret() -> SecretResponse:
        client = deps.zenml_client()
        secret = deps.resolve_secret_exact(client, name_or_id)
        client.delete_secret(name_id_or_prefix=str(secret.id))
        return secret

    secret = run_with_cli_error_boundary(
        _delete_secret,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_secret_summary(secret)
        payload["result"] = "deleted"
        _emit_json_item(command, payload, output=output_format)
        return

    _print_success(
        f"Deleted secret: {secret.name}",
        detail=f"Secret ID: {secret.id}",
    )
