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
from typing import Any

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from kitaru.cli.redaction import redact_data as cli_redact
from kitaru.mcp.redaction import redact_data as mcp_redact

_REDACTORS = [pytest.param(cli_redact, id="cli"), pytest.param(mcp_redact, id="mcp")]
_KNOWN = "#906"

_json = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=30),
    lambda c: (
        st.lists(c, max_size=4) | st.dictionaries(st.text(max_size=12), c, max_size=4)
    ),
    max_leaves=12,
)
# NEW-FINDING-8: "apiKey" and "x-api-key" are omitted here because only the CLI regex
# spells the separator as `[_-]?`; the MCP copy accepts `_` only, so it leaves those two
# key names untouched. `test_camel_and_hyphen_secret_keys_are_masked` pins that gap.
_secret_keys = st.sampled_from(
    ["api_key", "token", "password", "secret", "authorization"]
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


@pytest.mark.parametrize(
    "redact_data",
    [
        pytest.param(cli_redact, id="cli"),
        pytest.param(
            mcp_redact,
            id="mcp",
            marks=pytest.mark.xfail(strict=True, reason="NEW-FINDING-8"),
        ),
    ],
)
def test_camel_and_hyphen_secret_keys_are_masked(redact_data: Any) -> None:
    out = redact_data({"apiKey": "0123456789", "x-api-key": "0123456789"})
    assert out == {"apiKey": "***", "x-api-key": "***"}


@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.xfail(strict=True, reason=_KNOWN)
def test_distinct_keys_are_preserved(redact_data: Any) -> None:
    assert len(redact_data({1: "a", "1": "b"})) == 2


# Whether plural forms count as secret keys is an open question in #906; strict xfail
# forces the decision when the issue is resolved.
@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.xfail(strict=True, reason=_KNOWN)
def test_plural_secret_keys_are_masked(redact_data: Any) -> None:
    out = redact_data({"secrets": {"a": "s5"}, "tokens": "s2"})
    assert out["secrets"] == "***" and out["tokens"] == "***"


@pytest.mark.parametrize("redact_data", _REDACTORS)
@pytest.mark.xfail(strict=True, reason=_KNOWN)
def test_deep_nesting_does_not_recurse_out(redact_data: Any) -> None:
    value: dict[str, Any] = {}
    cursor = value
    for _ in range(3000):
        cursor["k"] = {}
        cursor = cursor["k"]
    json.dumps(redact_data(value))
