#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Connection read and management handlers."""

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.connections import (
    ConnectionCreate,
    ConnectionListRequest,
    ConnectionReadRequest,
    ConnectionsManageRequest,
    ConnectionUpdate,
)
from kitaru.mcp.redaction import MASKED, is_credential_name, redact_data
from kitaru.mcp.references import resolve_connection
from kitaru.mcp.tools.params import build_list_params
from kitaru.mcp.tools.registry import build_page_data


async def handle_connection_read(
    state: MCPServerState, request: ConnectionReadRequest
) -> object:
    """Execute one bounded connection read."""
    if isinstance(request, ConnectionListRequest):
        params = build_list_params(ConnectionListParams, request, with_filter=False)
        if request.provider is not None:
            params.filter = FilterCondition(
                field="provider", op=FilterOp.EQ, value=request.provider
            )
        page = await state.client.connections.list(params)
        return build_page_data(page, request.size, PageData[ConnectionResponse])
    return await resolve_connection(state.client, request.reference)


async def handle_connections_manage(
    state: MCPServerState, request: ConnectionsManageRequest
) -> object:
    """Perform one connection mutation."""
    if isinstance(request, ConnectionCreate):
        return await state.client.connections.create(
            ConnectionCreateRequest(
                name=request.name,
                provider=request.provider,
                env=request.env,
                secrets=request.secrets,
                default=request.default,
            ),
            idempotency_key=request.idempotency_key,
        )
    if isinstance(request, ConnectionUpdate):
        values = request.model_dump(
            include={"env", "secrets", "default"}, exclude_unset=True
        )
        if any(
            value is not None
            for value in (request.env, request.secrets, request.expected_updated)
        ):
            if request.env:
                _refuse_sent_credentials(request.env)
            # Read right before the write; a change landing between the two
            # is still missed, which only a check in the server could close.
            stored = await state.client.connections.get(request.connection_id)
            _check_unchanged(stored, request.expected_updated)
            if request.env:
                values["env"] = _restore_shown_values(stored.env, request.env)
            if request.secrets is not None:
                _check_dropped_secrets(
                    stored.secret_keys, request.secrets, request.remove_secrets or []
                )
        return await state.client.connections.update(
            request.connection_id, ConnectionUpdateRequest.model_validate(values)
        )
    return await state.client.connections.update(
        request.connection_id, ConnectionUpdateRequest(default=True)
    )


def _check_dropped_secrets(
    stored: Iterable[str], sent: Mapping[str, object], removed: Iterable[str]
) -> None:
    """Refuse an update whose `secrets` would delete a stored secret unasked."""
    stored, removed = set(stored), set(removed)
    if unknown := sorted(removed - stored):
        raise MCPToolError(
            "invalid_arguments",
            f"`remove_secrets` names {', '.join(unknown)}, which the connection "
            "does not store.",
            details={"secret_keys": sorted(stored)},
        )
    if dropped := sorted(stored - set(sent) - removed):
        raise MCPToolError(
            "invalid_arguments",
            "`secrets` replaces every stored secret, so this update would delete "
            f"{', '.join(dropped)}.",
            details={"secret_keys": sorted(stored)},
            recovery="Send each of them with its value in `secrets`, or list it "
            "in `remove_secrets` to delete it on purpose.",
        )


def _check_unchanged(
    stored: ConnectionResponse, expected_updated: datetime | None
) -> None:
    """Refuse an update built on a read older than the stored connection."""
    if expected_updated is None:
        # Validation requires it; a request built without validation must not
        # skip the check.
        raise MCPToolError(
            "invalid_arguments",
            "An update of `env` or `secrets` needs `expected_updated`.",
            recovery="Send the connection's `updated` time from "
            "`kitaru_connection_read` as `expected_updated`.",
        )
    if _as_utc(stored.updated) != _as_utc(expected_updated):
        raise MCPToolError(
            "conflict",
            "The connection changed after it was read.",
            recovery="Read it again with `kitaru_connection_read`, apply the "
            "change to what it shows now, and send its new `updated` time as "
            "`expected_updated`.",
        )


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _refuse_sent_credentials(env: Mapping[str, str]) -> None:
    sent = sorted(key for key in env if is_credential_name(key) and env[key] != MASKED)
    if sent:
        # Validation refuses these; a request built without it must neither
        # store the credential nor lose the change without a word.
        raise MCPToolError(
            "invalid_arguments",
            f"`env` keys {', '.join(sent)} name credentials and take only "
            f"`{MASKED}`, which keeps the stored value.",
            recovery="Put a new credential in `secrets`; on update, `secrets` "
            "replaces every stored secret.",
        )


def _restore_shown_values(
    stored: Mapping[str, str], env: Mapping[str, str]
) -> dict[str, str]:
    """Return `env` with each value sent as a read shows it set to the stored one."""
    missing = sorted(
        key for key in env if is_credential_name(key) and key not in stored
    )
    if missing:
        raise MCPToolError(
            "invalid_arguments",
            f"`env` sends {', '.join(missing)} as `{MASKED}`, but the connection "
            "stores no such key in `env`.",
            recovery="Put a new credential in `secrets`; on update, `secrets` "
            "replaces every stored secret.",
        )
    return {
        key: stored[key]
        if key in stored and value == redact_data(stored[key], key=key)
        else value
        for key, value in env.items()
    }
