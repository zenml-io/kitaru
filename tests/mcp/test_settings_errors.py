#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Settings precedence, stable error mapping, and redaction tests."""

import json
from typing import Any

import httpx
import pytest
from mcp.types import TextContent
from pydantic import ValidationError

from kitaru.analytics.source import AnalyticsSource
from kitaru.client.exceptions import APIError
from kitaru.mcp import connection as mcp_connection
from kitaru.mcp import server as mcp_server
from kitaru.mcp.connection import ConnectionConfigurationError, MCPConnection
from kitaru.mcp.errors import (
    MCPOutputValidationError,
    MCPToolError,
    map_exception,
    protocol_result,
)
from kitaru.mcp.models.common import ToolResult
from kitaru.mcp.settings import CapabilityMode, MCPSettings


def test_protocol_preserves_model_mapping_keys() -> None:
    """Model conversion must not erase distinct keys before redaction."""

    class MappingResult(ToolResult):
        data: Any = None

    result = protocol_result(MappingResult(ok=True, data={1: "first", "1": "second"}))
    structured = result.structured_content
    assert structured is not None
    assert len(structured["data"]) == 2
    assert set(structured["data"].values()) == {"first", "second"}
    content = result.content[0]
    assert isinstance(content, TextContent)
    assert json.loads(content.text) == structured


def test_protocol_contains_model_serialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed model conversion returns a safe MCP error, never a raw cause."""

    def fail_dump(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise ValueError("Bearer raw-secret")

    monkeypatch.setattr(ToolResult, "model_dump", fail_dump)
    result = protocol_result(ToolResult(ok=True, data={"value": "raw-secret"}))
    assert result.is_error is True
    structured = result.structured_content
    assert structured is not None
    assert structured["ok"] is False
    assert structured["error"]["code"] == "internal_error"
    assert "raw-secret" not in json.dumps(structured)
    content = result.content[0]
    assert isinstance(content, TextContent)
    assert json.loads(content.text) == structured


def test_protocol_contains_deep_model_data() -> None:
    """Model serialization and JSON encoding both stay within safe bounds."""
    value: Any = {"password": "deep-secret"}
    for _ in range(3000):
        value = {"nested": value}
    envelope = ToolResult.model_construct(ok=True, data=value)
    result = protocol_result(envelope)
    assert result.structured_content is not None
    content = result.content[0]
    assert isinstance(content, TextContent)
    assert json.loads(content.text) == result.structured_content
    assert "deep-secret" not in content.text


def test_settings_default_and_explicit_over_environment_precedence() -> None:
    assert MCPSettings().mode is CapabilityMode.READ_ONLY
    settings = MCPSettings.from_environment(
        {
            "KITARU_MCP_MODE": "standard",
            "KITARU_MCP_TIMEOUT": "10",
            "KITARU_MCP_DEBUG": "true",
        },
        mode="destructive",
        timeout=20,
    )
    assert settings.mode is CapabilityMode.DESTRUCTIVE
    assert settings.timeout == 20
    assert settings.debug is True
    assert (
        MCPSettings.from_environment(
            {"KITARU_API_URL": "https://api.example"}
        ).server_url
        == "https://api.example"
    )
    assert (
        MCPSettings.from_environment(
            {
                "KITARU_API_URL": "https://api.example",
                "KITARU_MCP_SERVER": "https://mcp.example",
            }
        ).server_url
        == "https://mcp.example"
    )


def test_settings_reject_nonfinite_and_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="must not be blank"):
        MCPSettings(server_url="  \t")
    with pytest.raises(ValueError, match="must not be blank"):
        MCPSettings.from_environment({"KITARU_MCP_SERVER": ""})
    with pytest.raises(ValueError, match="must not be blank"):
        MCPSettings.from_environment(
            {"KITARU_MCP_SERVER": "https://valid.example"}, server_url=""
        )
    with pytest.raises(ValueError, match="finite"):
        MCPSettings(timeout=float("inf"))
    with pytest.raises(ValueError):
        MCPSettings(max_concurrency=0)


@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    [
        (APIError(400, "bad"), "invalid_arguments", False),
        (APIError(401, "bad"), "authentication_failed", False),
        (APIError(403, "bad"), "permission_denied", False),
        (APIError(404, "bad"), "not_found", False),
        (APIError(409, "conflict"), "conflict", False),
        (APIError(429, "slow down"), "rate_limited", True),
        (APIError(500, "failed"), "remote_failed", True),
        (httpx.ConnectError("offline"), "network_error", True),
        (httpx.ReadTimeout("slow"), "timeout", True),
        (RuntimeError("secret"), "internal_error", False),
    ],
)
def test_complete_expected_error_categories(
    error: BaseException, code: str, retryable: bool
) -> None:
    mapped = map_exception(error)
    assert mapped.code == code
    assert mapped.retryable is retryable


def test_input_and_output_validation_map_to_distinct_contracts() -> None:
    validation_error = _get_validation_error()
    caller_input = map_exception(validation_error)
    remote_output = map_exception(MCPOutputValidationError(validation_error))
    assert caller_input.code == "invalid_arguments"
    assert caller_input.message == "Tool arguments are invalid."
    assert remote_output.code == "internal_error"
    assert remote_output.message == "The Kitaru response failed MCP output validation."


def test_remote_error_body_is_not_returned_to_the_model() -> None:
    mapped = map_exception(APIError(500, "Bearer top-secret arbitrary response"))
    assert "top-secret" not in mapped.message
    assert mapped.message == "The Kitaru server failed the request."


def _get_validation_error() -> ValidationError:
    with pytest.raises(ValidationError) as error:
        MCPSettings.model_validate({"max_concurrency": 0})
    return error.value


def test_tool_error_keeps_bounded_recovery_contract() -> None:
    mapped = map_exception(
        MCPToolError(
            "partial_failure",
            "One step failed",
            details={"completed": 1},
            recovery="Inspect the returned receipt.",
        )
    )
    assert mapped.details == {"completed": 1}
    assert mapped.recovery == "Inspect the returned receipt."


def test_connection_requires_explicit_url_and_prefers_environment_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ConnectionConfigurationError, match="No Kitaru server"):
        mcp_server.resolve_connection(MCPSettings())
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_secret")
    connection = mcp_server.resolve_connection(
        MCPSettings(server_url="https://example.test/root/")
    )
    assert connection.server_url == "https://example.test/root"
    assert connection.credential_source == "environment"
    assert connection.api_key == "KITKEY_secret"
    assert connection.credential_store is None


def test_connection_uses_store_keyed_by_fixed_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []
    stored = object()

    class FakeStore:
        def get(self, url: str) -> object:
            requested.append(url)
            return stored

    fake_store = FakeStore()
    monkeypatch.delenv("KITARU_API_KEY", raising=False)
    monkeypatch.setattr(mcp_connection, "CredentialStore", lambda: fake_store)
    connection = mcp_connection.resolve_connection("https://example.test/root/")
    assert requested == ["https://example.test/root"]
    assert connection.credential_source == "stored"
    assert connection.api_key is None
    assert connection.credential_store is fake_store


def test_connection_rejects_malformed_port() -> None:
    with pytest.raises(ConnectionConfigurationError, match="invalid port"):
        mcp_connection.resolve_connection("https://example.test:not-a-port")


@pytest.mark.parametrize("option", ["--context", "--retries"])
def test_removed_connection_options_are_rejected(option: str) -> None:
    with pytest.raises(SystemExit):
        mcp_server._parse_arguments([option, "value"])


def test_process_client_disables_transport_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_client(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(mcp_server, "KitaruAPIClient", fake_client)
    connection = MCPConnection(
        server_url="https://example.test",
        credential_source="environment",
        api_key="KITKEY_secret",
        credential_store=None,
    )
    mcp_server._build_client(MCPSettings(), connection)
    assert captured == {
        "base_url": "https://example.test",
        "api_key": "KITKEY_secret",
        "credential_store": None,
        "timeout": 30.0,
        "retries": 0,
        "pool_size": 20,
        "analytics_source": AnalyticsSource.MCP,
    }
