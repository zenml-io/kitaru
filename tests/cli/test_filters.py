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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Actionable CLI --filter JSON parsing and validation diagnostics."""

import json
import uuid
from contextlib import asynccontextmanager

import pytest

from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context
from kitaru.cli.filters import parse_filter_option
from kitaru.cli.output import CLIError


def test_parse_filter_option_accepts_conditions_and_combinators() -> None:
    """Valid leaf conditions and nested combinators parse into API models."""
    condition = parse_filter_option('{"field":"status","op":"eq","value":"failed"}')
    assert isinstance(condition, FilterCondition)
    assert condition.field == "status"
    assert condition.op is FilterOp.EQ
    assert condition.value == "failed"

    combined = parse_filter_option(
        '{"and":['
        '{"field":"status","op":"in","value":["completed","failed"]},'
        '{"not":{"field":"cost","op":"is_null"}}'
        "]}"
    )
    assert isinstance(combined, AndFilter)
    assert len(combined.and_) == 2


@pytest.mark.parametrize(
    ("raw", "fragments"),
    [
        ("not json", ("--filter is not valid JSON:", "line 1")),
        ("{", ("--filter is not valid JSON:",)),
        ("[1, 2]", ("--filter: expected a JSON object, got array.",)),
        ("true", ("--filter: expected a JSON object, got boolean.",)),
        ("null", ("--filter: expected a JSON object, got null.",)),
        ("{}", ("--filter: filter object cannot be empty.",)),
        (
            '{"field":"status","op":"gte","value":"failed"}',
            (
                "--filter: unknown operator 'gte'.",
                "eq, ne, lt, le, gt, ge, in, is_null, startswith, endswith, contains",
            ),
        ),
        (
            '{"and":[{"field":"status","op":"gte","value":"failed"}]}',
            (
                "--filter (and[0]): unknown operator 'gte'.",
                "eq, ne, lt, le, gt, ge, in, is_null, startswith, endswith, contains",
            ),
        ),
        (
            '{"or":[{"not":{"field":"cost","op":"approx","value":1}}]}',
            ("--filter (or[0].not): unknown operator 'approx'.",),
        ),
        (
            '{"field":"metadata.agent_release","op":"eq","value":"v1"}',
            (
                "--filter: invalid field name 'metadata.agent_release'.",
                "^[a-z][a-z0-9_]*$",
            ),
        ),
        (
            '{"field":"Status","op":"eq","value":"failed"}',
            ("--filter: invalid field name 'Status'.", "^[a-z][a-z0-9_]*$"),
        ),
        (
            '{"field":123,"op":"eq","value":"failed"}',
            ("--filter: 'field' must be a string, got number.",),
        ),
        (
            '{"field":"status","op":123,"value":"failed"}',
            ("--filter: 'op' must be a string, got number.", "eq, ne"),
        ),
        (
            '{"field":"status"}',
            ("--filter: filter condition is missing required key 'op'.",),
        ),
        (
            '{"op":"eq","value":"failed"}',
            ("--filter: filter condition is missing required key 'field'.",),
        ),
        (
            '{"field":"status","op":"eq","value":"failed","extra":1}',
            ("--filter: unknown key(s) in filter condition: 'extra'.",),
        ),
        (
            '{"foo":"bar"}',
            ("--filter: unrecognized filter key(s): 'foo'.",),
        ),
        (
            '{"and":[],"or":[]}',
            ("--filter: filter object cannot combine 'and', 'or'",),
        ),
        (
            '{"and":[{"field":"status","op":"eq","value":"failed"}],"field":"status"}',
            ("--filter: 'and' filter does not allow extra key(s): 'field'.",),
        ),
        (
            '{"and":[]}',
            ("--filter: 'and' must contain at least one filter.",),
        ),
        (
            '{"or":{"field":"status","op":"eq","value":"failed"}}',
            ("--filter: 'or' must be a non-empty JSON array of filters.",),
        ),
        (
            '{"not":[]}',
            ("--filter (not): expected a JSON object, got array.",),
        ),
    ],
)
def test_parse_filter_option_reports_actionable_diagnostics(
    raw: str, fragments: tuple[str, ...]
) -> None:
    """Every rejected filter explains what failed and what syntax is valid."""
    with pytest.raises(CLIError) as raised:
        parse_filter_option(raw)

    assert raised.value.kind == "invalid_arguments"
    for fragment in fragments:
        assert fragment in raised.value.message


@pytest.mark.parametrize(
    "argv",
    [
        [
            "session",
            "list",
            "--filter",
            '{"and":[{"field":"status","op":"gte","value":"failed"}]}',
        ],
        [
            "session",
            "nodes",
            str(uuid.uuid4()),
            "--filter",
            '{"and":[{"field":"node_type","op":"gte","value":"tool_call"}]}',
        ],
    ],
)
def test_cli_filter_errors_report_nested_operator_without_masking(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
) -> None:
    """List commands surface nested filter diagnostics instead of a generic error."""

    class UnusedClient:
        @property
        def sessions(self):
            raise AssertionError("network call should not happen on invalid --filter")

    @asynccontextmanager
    async def fake_open_client():
        yield UnusedClient()

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    assert app_module.main([*argv, "--output", "json"]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert "and[0]" in error["message"]
    assert "unknown operator 'gte'" in error["message"]
    assert (
        "eq, ne, lt, le, gt, ge, in, is_null, startswith, endswith, contains"
        in (error["message"])
    )
