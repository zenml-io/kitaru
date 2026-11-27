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
"""Property tests for CLI and MCP credential redaction."""

import json
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from pydantic import BaseModel

from kitaru.api_models.v1.session import TokenUsage
from kitaru.cli.redaction import redact_data as cli_redact
from kitaru.mcp.redaction import redact_data as mcp_redact

_json = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=30),
    lambda c: (
        st.lists(c, max_size=4) | st.dictionaries(st.text(max_size=12), c, max_size=4)
    ),
    max_leaves=12,
)
_secret_keys = st.sampled_from(
    [
        "api_key",
        "apiKey",
        "x-api-key",
        "API_KEY",
        "token",
        "tokens",
        "access_tokens",
        "refreshTokens",
        "password",
        "passwords",
        "secret",
        "secrets",
        "authorization",
        "credentials",
    ]
)

_inline_secret = st.builds(
    lambda prefix, secret: f"before {prefix}{secret} after",
    st.sampled_from(["KITKEY_", "ZENPROKEY_", "Bearer "]),
    st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
)
_idempotent_roots = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.text(max_size=30)
    | _inline_secret
    | st.just("***"),
    lambda children: (
        st.lists(children, max_size=4)
        | st.dictionaries(st.text(max_size=12), children, max_size=4)
    ),
    max_leaves=12,
)


@given(
    key=_secret_keys,
    secret=st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
    rest=_json,
)
def test_secret_under_secret_key_never_survives(
    key: str, secret: str, rest: Any
) -> None:
    # The generated filler can coincidentally contain the same hex string, which would
    # fail the assertion without any secret having escaped its own key.
    assume(secret not in json.dumps(rest))
    output = cli_redact({key: secret, "other": rest})
    assert output[key] == "***"
    assert secret not in json.dumps(output)


@given(
    prefix=st.sampled_from(["KITKEY_", "ZENPROKEY_", "Bearer "]),
    secret=st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
)
def test_inline_marker_never_survives(prefix: str, secret: str) -> None:
    output = cli_redact({"note": f"use {prefix}{secret} now"})
    assert output == {"note": f"use {prefix}*** now"}


def test_camel_and_hyphen_secret_keys_are_masked() -> None:
    out = cli_redact({"apiKey": "0123456789", "x-api-key": "0123456789"})
    assert out == {"apiKey": "***", "x-api-key": "***"}


def test_distinct_keys_are_preserved() -> None:
    assert len(cli_redact({1: "a", "1": "b"})) == 2


def test_plural_secret_keys_are_masked() -> None:
    out = cli_redact({"secrets": {"a": "s5"}, "tokens": "s2"})
    assert out["secrets"] == "***" and out["tokens"] == "***"


def test_deep_nesting_does_not_recurse_out() -> None:
    value: dict[str, Any] = {}
    cursor = value
    for _ in range(3000):
        cursor["k"] = {}
        cursor = cursor["k"]
    json.dumps(cli_redact(value))


@given(
    value=st.dictionaries(
        st.text(max_size=12)
        | st.integers(-5, 5)
        | st.booleans()
        | st.tuples(st.integers(-5, 5)),
        _json,
        max_size=8,
    )
)
def test_mixed_keys_survive_json_and_repeated_redaction(value: Any) -> None:
    output = cli_redact(value)
    assert len(json.loads(json.dumps(output))) == len(value)
    assert cli_redact(output) == output


@given(value=_idempotent_roots)
def test_root_shapes_are_idempotent_and_inputs_are_unchanged(value: Any) -> None:
    original = deepcopy(value)

    output = cli_redact(value)

    assert cli_redact(output) == output
    assert value == original
    json.dumps(output)


def test_cli_and_mcp_export_the_shared_redactor() -> None:
    """Keep the single property corpus valid for both public aliases."""
    assert cli_redact is mcp_redact


@pytest.mark.parametrize("key", [1, True, (1,)])
@pytest.mark.parametrize("reverse", [False, True])
def test_key_aliases_reserve_existing_strings(key: Any, reverse: bool) -> None:
    entries = [(key, "native"), (str(key), "string"), (f"{key} [2]", "reserved")]
    value = dict(reversed(entries) if reverse else entries)
    output = json.loads(json.dumps(cli_redact({"nested": [value]})))["nested"][0]
    assert len(output) == 3
    assert output[str(key)] == "string"
    assert output[f"{key} [2]"] == "reserved"
    assert set(output.values()) == {"native", "string", "reserved"}
    assert list(value.items()) == list(reversed(entries) if reverse else entries)


def test_cycles_are_bounded_but_shared_values_remain_visible() -> None:
    mapping: dict[str, Any] = {}
    sequence: list[Any] = [mapping]
    mapping["cycle"] = sequence
    shared = {"visible": 1, "password": "hidden"}
    output = cli_redact({"cycle": mapping, "first": shared, "second": shared})
    assert "hidden" not in json.dumps(output)
    assert output["first"] == output["second"] == {"visible": 1, "password": "***"}
    assert "cycle" in output["cycle"]
    assert cli_redact(output) == output
    assert sequence[0] is mapping and mapping["cycle"] is sequence


@pytest.mark.parametrize("depth", [63, 64, 65])
def test_depth_boundary_is_bounded(depth: int) -> None:
    value: Any = "visible"
    for _ in range(depth):
        value = [value]
    output = cli_redact(value)
    serialized = json.dumps(output)
    assert ("visible" in serialized) == (depth < 64)
    assert len(serialized) < 200
    assert cli_redact(output) == output


def test_model_normalization_keeps_keys_and_supported_scalars() -> None:
    class Values(BaseModel):
        values: dict[Any, Any]

    class Label(Enum):
        VALUE = "value"

    value = Values(
        values={
            1: "native",
            "1": "string",
            "enum": Label.VALUE,
            "date": date(2026, 8, 31),
            "datetime": datetime(2026, 8, 31, 12),
            "uuid": UUID(int=1),
            "path": Path("/tmp/example"),
            "decimal": Decimal("0.1250"),
            "tuple": (1, 2),
            "set": {3},
        }
    )
    output = json.loads(json.dumps(cli_redact(value)))["values"]
    assert len(output) == len(value.values)
    assert output["1"] == "string"
    assert output["enum"] == "value"
    assert output["date"] == "2026-08-31"
    assert output["datetime"] == "2026-08-31T12:00:00"
    assert output["uuid"] == str(UUID(int=1))
    assert output["path"] == "/tmp/example"
    assert output["decimal"] == "0.1250"
    assert output["tuple"] == [1, 2] and output["set"] == [3]


@pytest.mark.parametrize(
    "usage",
    [
        None,
        0,
        17,
        {},
        {
            "input_tokens": 12,
            "output_tokens": None,
            "cached_input_tokens": 3,
            "cache_write_input_tokens": 2,
            "reasoning_tokens": 4,
        },
        {"input_tokens": 120.5, "cached_input_tokens": 0.0, "output_tokens": 40.2},
        # Counts read back from settings, as at the top level.
        {"input_tokens": "4096", "output_tokens": 12},
    ],
)
def test_numeric_token_usage_is_preserved(usage: Any) -> None:
    assert cli_redact({"tokens": usage}) == {"tokens": usage}


@pytest.mark.parametrize(
    "secret",
    [
        "credential-value",
        ["credential-value"],
        True,
        False,
        1.5,
        {"input_tokens": "credential-value"},
        {"input_tokens": True},
        {"input_tokens": 1, "access_token": "credential-value"},
        {"unknown": 1},
    ],
)
def test_secret_bearing_tokens_are_masked(secret: Any) -> None:
    assert cli_redact({"tokens": secret}) == {"tokens": "***"}


@pytest.mark.parametrize("key", list(TokenUsage.model_fields))
@given(count=st.none() | st.integers() | st.floats(allow_nan=False))
def test_numeric_usage_fields_are_preserved(key: str, count: Any) -> None:
    assert cli_redact({key: count}) == {key: count}
    assert cli_redact({key: "4096"}) == {key: "4096"}
    for other in ("credential-value", "4096 ", "12.5", "\u0664\u0660", True):
        assert cli_redact({key: other}) == {key: "***"}


def test_mean_token_usage_survives_beside_masked_credentials() -> None:
    per_call = {"input_tokens": 120.5, "output_tokens": 40.2}
    out = cli_redact(
        {
            "candidates": [{"tokens_per_call": per_call}],
            "access_token": "credential-value",
        }
    )
    assert out == {"candidates": [{"tokens_per_call": per_call}], "access_token": "***"}


def test_credential_status_fields_are_preserved() -> None:
    value = {"credential_stored": True, "credential_status": "configured"}
    assert cli_redact(value) == value


def test_provider_token_counts_and_model_settings_survive_beside_credentials() -> None:
    """Provider usage counts and max_tokens stay visible while secrets stay masked."""
    payload = {
        "usage": {
            "input_tokens": 10,
            "cache_read_tokens": 5,
            "total_tokens": 15,
            "request_tokens": 3,
            "completion_tokens": 7,
            "prompt_tokens": 8,
            "cache_creation_input_tokens": 12,
        },
        "model_settings": {
            "max_tokens": 1024,
            "max-tokens": 2048,
            "cache-read-tokens": 4,
            "MAX_TOKENS": 4096,
        },
        "api_key": "x",
        "access_token": "y",
    }
    assert cli_redact(payload) == {
        "usage": {
            "input_tokens": 10,
            "cache_read_tokens": 5,
            "total_tokens": 15,
            "request_tokens": 3,
            "completion_tokens": 7,
            "prompt_tokens": 8,
            "cache_creation_input_tokens": 12,
        },
        "model_settings": {
            "max_tokens": 1024,
            "max-tokens": 2048,
            "cache-read-tokens": 4,
            "MAX_TOKENS": 4096,
        },
        "api_key": "***",
        "access_token": "***",
    }


@pytest.mark.parametrize(
    "key",
    [
        "cache_read_tokens",
        "total_tokens",
        "request_tokens",
        "max_tokens",
        "completion_tokens",
        "prompt_tokens",
        "cache_creation_input_tokens",
        "cache-read-tokens",
        "max-tokens",
        "MAX_TOKENS",
    ],
)
@given(count=st.none() | st.integers())
def test_plural_token_count_keys_allow_only_whole_numbers_and_none(
    key: str, count: int | None
) -> None:
    assert cli_redact({key: count}) == {key: count}


@pytest.mark.parametrize(
    "key",
    ["cache_read_tokens", "max_tokens", "cache-read-tokens", "max-tokens"],
)
def test_plural_token_count_keys_preserve_digit_strings(key: str) -> None:
    assert cli_redact({key: "1024"}) == {key: "1024"}


@pytest.mark.parametrize(
    "key",
    ["cache_read_tokens", "max_tokens", "cache-read-tokens", "max-tokens"],
)
@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        "secret",
        "KITKEY_abc",
        "1024 ",
        "12.5",
        "\u0664\u0660",
        1.5,
        [1024],
        {"count": 1024},
        object(),
    ],
)
def test_plural_token_count_keys_mask_non_count_values(key: str, value: Any) -> None:
    assert cli_redact({key: value}) == {key: "***"}


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        "secret",
        "KITKEY_abc",
        "1024",
        1.5,
        [1024],
        {"count": 1024},
        object(),
    ],
)
def test_other_plural_token_keys_mask_strings_and_non_whole_values(value: Any) -> None:
    assert cli_redact({"request_tokens": value}) == {"request_tokens": "***"}


@pytest.mark.parametrize(
    "key",
    [
        "access_tokens",
        "access-tokens",
        "accessTokens",
        "oauth_access_tokens",
        "refresh_tokens",
        "refresh-tokens",
        "refreshTokens",
        "id_tokens",
        "id-tokens",
        "auth_tokens",
        "auth-tokens",
        "session_tokens",
        "bearer_tokens",
        "csrf_tokens",
        "api_tokens",
        "secret_tokens",
        "oauth_tokens",
        "security_tokens",
        "ACCESS_TOKENS",
        "_tokens",
        "-tokens",
    ],
)
@pytest.mark.parametrize("value", [0, 123, 456, None, True, "secret"])
def test_explicit_credential_plural_token_keys_always_mask(
    key: str, value: Any
) -> None:
    assert cli_redact({key: value}) == {key: "***"}


@pytest.mark.parametrize(
    "key",
    [
        "token",
        "github_token",
        "access_token",
        "api_token",
        "bearer_token",
        "refresh_token",
        "max_token",
        "x-auth-token",
    ],
)
@pytest.mark.parametrize("value", [0, 789, None, True, "secret"])
def test_singular_token_keys_always_mask_even_for_integers(
    key: str, value: Any
) -> None:
    assert cli_redact({key: value}) == {key: "***"}


def test_nested_provider_payloads_preserve_counts_and_mask_credentials() -> None:
    payload = {
        "nodes": [
            {
                "usage": {
                    "cache_read_tokens": 5,
                    "cache_creation_input_tokens": None,
                    "total_tokens": 25,
                },
                "settings": {"max-tokens": 512, "max_tokens": "secret"},
                "auth": {
                    "access_tokens": 123,
                    "refresh_tokens": 456,
                    "github_token": 789,
                    "cache_read_tokens": "KITKEY_abc",
                },
            }
        ]
    }
    assert cli_redact(payload) == {
        "nodes": [
            {
                "usage": {
                    "cache_read_tokens": 5,
                    "cache_creation_input_tokens": None,
                    "total_tokens": 25,
                },
                "settings": {"max-tokens": 512, "max_tokens": "***"},
                "auth": {
                    "access_tokens": "***",
                    "refresh_tokens": "***",
                    "github_token": "***",
                    "cache_read_tokens": "***",
                },
            }
        ]
    }
