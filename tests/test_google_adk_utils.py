"""Validation helper tests for the Google ADK adapter."""

from __future__ import annotations

import importlib

import pytest
from pydantic import BaseModel, SecretStr

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruUsageError


def _utils(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    return importlib.import_module("kitaru.adapters.google_adk._utils")


def _serialization(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    return importlib.import_module("kitaru.adapters.google_adk._serialization")


def test_valid_checkpoint_strategies(monkeypatch: pytest.MonkeyPatch) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_checkpoint_strategy("runner_call") == "runner_call"
    assert utils.validate_checkpoint_strategy("calls") == "calls"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("turn", "runner_call"),
        ("interaction", "another adapter"),
        ("model_call", "checkpoint_strategy='calls'"),
        ("wat", "Expected one of"),
    ],
)
def test_invalid_checkpoint_strategy_messages(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    message: str,
) -> None:
    utils = _utils(monkeypatch)

    with pytest.raises(KitaruUsageError, match=message):
        utils.validate_checkpoint_strategy(value)


def test_checkpoint_config_rejects_isolated_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        utils.validate_checkpoint_config(
            {"runtime": "isolated"},
            context="run_checkpoint_config",
        )


def test_checkpoint_config_validates_cache_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_checkpoint_config(
        {"runtime": "inline", "cache": True, "retries": 1},
        context="run_checkpoint_config",
    ) == {"runtime": "inline", "cache": True, "retries": 1}

    with pytest.raises(KitaruUsageError, match="cache must be a boolean"):
        utils.validate_checkpoint_config({"cache": "yes"}, context="config")


def test_cache_identity_distinguishes_redacted_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialization = _serialization(monkeypatch)

    alice = {"api_token": "alice", "query": "cats"}
    bob = {"api_token": "bob", "query": "cats"}

    assert serialization.to_json_safe(alice) == serialization.to_json_safe(bob)
    assert serialization.to_cache_identity(alice) != serialization.to_cache_identity(
        bob
    )
    assert "alice" not in repr(serialization.to_cache_identity(alice))


class SecretRequest(BaseModel):
    api_token: str
    prompt: str


class SecretStrRequest(BaseModel):
    api_token: SecretStr
    prompt: str


class OpaqueSecretRequest:
    def __init__(self, api_token: str) -> None:
        self.api_token = api_token

    def __repr__(self) -> str:
        return f"OpaqueSecretRequest(api_token={self.api_token!r})"


def test_cache_identity_hashes_secret_fields_after_model_serialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialization = _serialization(monkeypatch)

    alice = SecretRequest(api_token="alice", prompt="cats")
    bob = SecretRequest(api_token="bob", prompt="cats")
    identity = serialization.to_cache_identity(alice)

    assert identity != serialization.to_cache_identity(bob)
    assert "alice" not in repr(identity)


def test_cache_identity_distinguishes_secretstr_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialization = _serialization(monkeypatch)

    alice = SecretStrRequest(api_token=SecretStr("alice"), prompt="cats")
    bob = SecretStrRequest(api_token=SecretStr("bob"), prompt="cats")
    identity = serialization.to_cache_identity(alice)

    assert identity != serialization.to_cache_identity(bob)
    assert "alice" not in repr(identity)


def test_cache_identity_does_not_expose_opaque_object_repr_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialization = _serialization(monkeypatch)

    identity = serialization.to_cache_identity(
        {"llm_request": OpaqueSecretRequest("alice")}
    )

    assert "alice" not in repr(identity)
    assert "api_token" not in repr(identity)
    assert "repr_sha256" in repr(identity)


def test_secret_digest_is_stable_for_equivalent_opaque_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serialization = _serialization(monkeypatch)

    alice_a = {"api_token": OpaqueSecretRequest("alice")}
    alice_b = {"api_token": OpaqueSecretRequest("alice")}
    bob = {"api_token": OpaqueSecretRequest("bob")}

    assert serialization.to_cache_identity(alice_a) == serialization.to_cache_identity(
        alice_b
    )
    assert serialization.to_cache_identity(alice_a) != serialization.to_cache_identity(
        bob
    )


def test_tool_checkpoint_overrides_validate_tool_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_tool_checkpoint_overrides(
        {"search": False, "lookup": {"type": "tool_call"}},
        context="tool_checkpoint_config_by_name",
    ) == {"search": False, "lookup": {"type": "tool_call"}}

    with pytest.raises(KitaruUsageError, match="non-empty tool name"):
        utils.validate_tool_checkpoint_overrides(
            {"": {}},
            context="tool_checkpoint_config_by_name",
        )
