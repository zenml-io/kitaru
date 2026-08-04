#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for neutral persisted connection configuration."""

import json

import pytest

from kitaru.analytics.source import CLIENT_HEADER, AnalyticsSource
from kitaru.client.connection import (
    ConfigStore,
    ConnectionConfigurationError,
    LocalConfig,
    ResolvedCredential,
    build_api_client,
    resolve_credential,
    resolve_target,
)
from kitaru.client.credential_store import CredentialStore


def test_neutral_store_preserves_schema_and_resolution_precedence(
    tmp_path, monkeypatch
) -> None:
    """The extracted store reads the version-1 document without CLI imports."""
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "active_context": "Prod",
                "contexts": {
                    "Prod": {"server_url": "https://active.example.com"},
                    "Other": {"server_url": "https://other.example.com"},
                },
                "cli": {"machine_mode": True},
            }
        ),
        encoding="utf-8",
    )
    store = ConfigStore(path)
    config = store.load()
    assert config == LocalConfig.model_validate_json(path.read_text(encoding="utf-8"))
    assert config.schema_version == 1
    assert config.cli.machine_mode is True

    monkeypatch.setenv("KITARU_API_URL", "https://env.example.com/")
    assert resolve_target(store, context_name="Other").server_url == (
        "https://other.example.com"
    )
    assert resolve_target(store).source == "environment"
    monkeypatch.delenv("KITARU_API_URL")
    assert resolve_target(store).source == "active_context"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("server", ""),
        ("server", "  \t"),
        ("context", ""),
        ("context", "  \t"),
    ],
)
def test_blank_explicit_target_never_falls_through(
    tmp_path, monkeypatch, field: str, value: str
) -> None:
    """Reject an explicit blank instead of selecting environment or active context."""
    monkeypatch.setenv("KITARU_API_URL", "https://environment.example.com")
    with pytest.raises(ConnectionConfigurationError) as captured:
        resolve_target(
            ConfigStore(tmp_path / "config.json"),
            explicit_server=value if field == "server" else None,
            context_name=value if field == "context" else None,
        )
    assert captured.value.kind == "invalid_arguments"


def test_credential_precedence_is_environment_then_stored(
    tmp_path, monkeypatch
) -> None:
    """Neutral resolution keeps the established shared-login precedence."""
    server_url = "https://example.com"
    store = CredentialStore(tmp_path / "credentials.json")
    store.set_api_key(server_url, "KITKEY_stored")
    monkeypatch.setenv("KITARU_API_KEY", "KITKEY_environment")

    environment = resolve_credential(server_url, store)
    assert environment.source == "environment"
    assert environment.api_key == "KITKEY_environment"

    monkeypatch.delenv("KITARU_API_KEY")
    stored = resolve_credential(server_url, store)
    assert stored.source == "stored"
    assert stored.stored is not None


async def test_client_builder_forwards_mcp_source(tmp_path) -> None:
    """Frontend callers can additively select MCP identity headers."""
    client = build_api_client(
        "https://example.com",
        ResolvedCredential("none"),
        CredentialStore(tmp_path / "credentials.json"),
        30.0,
        source=AnalyticsSource.MCP,
    )
    try:
        assert client._http.headers["User-Agent"].startswith("kitaru-mcp/")
        assert client._http.headers[CLIENT_HEADER].startswith("kitaru-mcp/")
    finally:
        await client.close()


def test_neutral_errors_are_bounded_and_do_not_echo_invalid_input(tmp_path) -> None:
    """Validation details cap issue count and omit raw configuration values."""
    path = tmp_path / "config.json"
    secret = "KITKEY_must-not-appear"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "contexts": {
                    "Prod": {
                        "server_url": "https://example.com",
                        **{f"unknown_{index}": secret for index in range(20)},
                    }
                },
                "unknown": secret,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConnectionConfigurationError) as captured:
        ConfigStore(path).load()

    error = captured.value
    assert error.kind == "invalid_configuration"
    assert isinstance(error.details, list)
    assert len(error.details) <= 10
    assert secret not in repr(error.details)
