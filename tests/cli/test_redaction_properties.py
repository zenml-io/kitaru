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

from kitaru.cli.redaction import redact_data as cli_redact
from kitaru.mcp.redaction import redact_data as mcp_redact

_REDACTORS = [pytest.param(cli_redact, id="cli"), pytest.param(mcp_redact, id="mcp")]

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


@pytest.mark.parametrize("redact_data", _REDACTORS)
@given(value=_json)
def test_never_raises_and_is_json(redact_data: Any, value: Any) -> None:
    json.dumps(redact_data(value))


@pytest.mark.parametrize("redact_data", _REDACTORS)
@given(
    key=_secret_keys,
    secret=st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
    rest=_json,
)
def test_secret_under_secret_key_never_survives(
    redact_data: Any, key: str, secret: str, rest: Any
) -> None:
    # The generated filler can coincidentally contain the same hex string, which would
    # fail the assertion without any secret having escaped its own key.
    assume(secret not in json.dumps(rest))
    out = json.dumps(redact_data({key: secret, "other": rest}))
    assert secret not in out


@pytest.mark.parametrize("redact_data", _REDACTORS)
@given(
    prefix=st.sampled_from(["KITKEY_", "Bearer "]),
    secret=st.text(min_size=8, max_size=20, alphabet="abcdef0123456789"),
)
def test_inline_marker_never_survives(
    redact_data: Any, prefix: str, secret: str
) -> None:
    out = json.dumps(redact_data({"note": f"use {prefix}{secret} now"}))
    assert secret not in out


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_camel_and_hyphen_secret_keys_are_masked(redact_data: Any) -> None:
    out = redact_data({"apiKey": "0123456789", "x-api-key": "0123456789"})
    assert out == {"apiKey": "***", "x-api-key": "***"}


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_distinct_keys_are_preserved(redact_data: Any) -> None:
    assert len(redact_data({1: "a", "1": "b"})) == 2


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_plural_secret_keys_are_masked(redact_data: Any) -> None:
    out = redact_data({"secrets": {"a": "s5"}, "tokens": "s2"})
    assert out["secrets"] == "***" and out["tokens"] == "***"


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_deep_nesting_does_not_recurse_out(redact_data: Any) -> None:
    value: dict[str, Any] = {}
    cursor = value
    for _ in range(3000):
        cursor["k"] = {}
        cursor = cursor["k"]
    json.dumps(redact_data(value))


@pytest.mark.parametrize("redact_data", _REDACTORS)
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
def test_mixed_keys_survive_json_and_repeated_redaction(
    redact_data: Any, value: Any
) -> None:
    output = redact_data(value)
    assert len(json.loads(json.dumps(output))) == len(value)
    assert redact_data(output) == output


@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.parametrize("key", [1, True, (1,)])
@pytest.mark.parametrize("reverse", [False, True])
def test_key_aliases_reserve_existing_strings(
    redact_data: Any, key: Any, reverse: bool
) -> None:
    entries = [(key, "native"), (str(key), "string"), (f"{key} [2]", "reserved")]
    value = dict(reversed(entries) if reverse else entries)
    output = json.loads(json.dumps(redact_data({"nested": [value]})))["nested"][0]
    assert len(output) == 3
    assert output[str(key)] == "string"
    assert output[f"{key} [2]"] == "reserved"
    assert set(output.values()) == {"native", "string", "reserved"}
    assert list(value.items()) == list(reversed(entries) if reverse else entries)


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_cycles_are_bounded_but_shared_values_remain_visible(redact_data: Any) -> None:
    mapping: dict[str, Any] = {}
    sequence: list[Any] = [mapping]
    mapping["cycle"] = sequence
    shared = {"visible": 1, "password": "hidden"}
    output = redact_data({"cycle": mapping, "first": shared, "second": shared})
    assert "hidden" not in json.dumps(output)
    assert output["first"] == output["second"] == {"visible": 1, "password": "***"}
    assert "cycle" in output["cycle"]
    assert sequence[0] is mapping and mapping["cycle"] is sequence


@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.parametrize("depth", [63, 64, 65])
def test_depth_boundary_is_bounded(redact_data: Any, depth: int) -> None:
    value: Any = "visible"
    for _ in range(depth):
        value = [value]
    serialized = json.dumps(redact_data(value))
    assert ("visible" in serialized) == (depth < 64)
    assert len(serialized) < 200


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_model_normalization_keeps_keys_and_supported_scalars(redact_data: Any) -> None:
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
    output = json.loads(json.dumps(redact_data(value)))["values"]
    assert len(output) == len(value.values)
    assert output["1"] == "string"
    assert output["enum"] == "value"
    assert output["date"] == "2026-08-31"
    assert output["datetime"] == "2026-08-31T12:00:00"
    assert output["uuid"] == str(UUID(int=1))
    assert output["path"] == "/tmp/example"
    assert output["decimal"] == "0.1250"
    assert output["tuple"] == [1, 2] and output["set"] == [3]


@pytest.mark.parametrize("redact_data", _REDACTORS)
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
            "reasoning_tokens": 4,
        },
    ],
)
def test_numeric_token_usage_is_preserved(redact_data: Any, usage: Any) -> None:
    assert redact_data({"tokens": usage}) == {"tokens": usage}


@pytest.mark.parametrize("redact_data", _REDACTORS)
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
def test_secret_bearing_tokens_are_masked(redact_data: Any, secret: Any) -> None:
    assert redact_data({"tokens": secret}) == {"tokens": "***"}


@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.parametrize(
    "key", ["input_tokens", "output_tokens", "cached_input_tokens", "reasoning_tokens"]
)
@given(count=st.none() | st.integers())
def test_numeric_usage_fields_are_preserved(
    redact_data: Any, key: str, count: Any
) -> None:
    assert redact_data({key: count}) == {key: count}
    assert redact_data({key: "credential-value"}) == {key: "***"}
    assert redact_data({key: True}) == {key: "***"}


@pytest.mark.parametrize("redact_data", _REDACTORS)
def test_credential_status_fields_are_preserved(redact_data: Any) -> None:
    value = {"credential_stored": True, "credential_status": "configured"}
    assert redact_data(value) == value
