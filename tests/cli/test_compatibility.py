#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Registration description compatibility and shared pagination aliases."""

import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.cli.test_registration import StubClient, StubModel

from kitaru.cli import app as app_module
from kitaru.cli import command_registry, invocation_context
from kitaru.cli.agents import load_agent_register_spec, load_agent_version_spec
from kitaru.cli.schema import describe_schema


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"parent": {"description": "agent"}}, "agent"),
        ({"description": "agent"}, "agent"),
        ({"description": "agent", "parent": {}}, "agent"),
        ({"description": "agent", "parent": {"description": "agent"}}, "agent"),
        ({"description": None}, None),
        ({"description": None, "parent": {"description": None}}, None),
        ({"parent": {"description": None}}, None),
        ({}, None),
    ],
)
def test_register_spec_descriptions_remain_independent(tmp_path, fields, expected):
    """Both parent forms preserve a distinct version description."""
    path = tmp_path / "agent.json"
    path.write_text(
        json.dumps(
            {
                **fields,
                "version": {"description": "version", "run_spec": {"command": "run"}},
            }
        )
    )
    parent, version = load_agent_register_spec("demo", path)
    assert parent.description == expected
    assert version.description == "version"


@pytest.mark.parametrize(
    "fields",
    [
        {"description": "agent", "parent": {"description": "other"}},
        {"description": None, "parent": {"description": "other"}},
        {"description": "agent", "parent": {"description": None}},
        {"descriptions": "typo"},
        {"parent": {"descriptions": "typo"}},
        {"version": {"descriptions": "typo", "run_spec": {"command": "run"}}},
    ],
)
def test_invalid_register_spec_never_opens_client(
    tmp_path, monkeypatch, capsys, fields
):
    """Ambiguous descriptions and misspellings fail before any remote work."""
    path = tmp_path / "agent.json"
    path.write_text(json.dumps({"version": {"run_spec": {"command": "run"}}, **fields}))

    def fail_client():
        pytest.fail("invalid spec opened a remote client")

    monkeypatch.setattr(invocation_context, "open_asset_client", fail_client)
    assert (
        app_module.main(["agent", "register", "demo", "--spec", str(path), "--machine"])
        == 2
    )
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)["error"]
    assert error["kind"] == "invalid_arguments"
    if "description" in fields:
        assert "parent.description" in error["message"]
        assert "description" in error["hint"]


def test_register_spec_forwards_both_dtos(tmp_path, monkeypatch, capsys):
    """The shorthand reaches the agent DTO without overwriting the version DTO."""
    client = StubClient()
    path = tmp_path / "agent.json"
    path.write_text(
        json.dumps(
            {
                "description": "agent",
                "version": {"description": "version", "run_spec": {"command": "run"}},
            }
        )
    )

    @asynccontextmanager
    async def open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", open_client)
    assert (
        app_module.main(["agent", "register", "demo", "--spec", str(path), "--machine"])
        == 0
    )
    assert client.agents.created_requests[0].description == "agent"
    assert client.agents.version_requests[0][1].description == "version"
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_version_only_spec_description_keeps_its_meaning(tmp_path: Path):
    """A version-only document still interprets description as the version text."""
    path = tmp_path / "version.json"
    path.write_text('{"description":"version","run_spec":{"command":"run"}}')
    assert load_agent_version_spec(path).description == "version"


@pytest.mark.parametrize(
    "option", ["--description", "--version-description", "--command"]
)
def test_spec_direct_options_have_actionable_hint(
    tmp_path, monkeypatch, capsys, option
):
    """Direct values cannot silently override spec values."""
    path = tmp_path / "agent.json"
    path.write_text('{"version":{"run_spec":{"command":"run"}}}')

    def fail_client():
        pytest.fail("conflicting options opened a remote client")

    monkeypatch.setattr(invocation_context, "open_asset_client", fail_client)
    assert (
        app_module.main(
            [
                "agent",
                "register",
                "demo",
                "--spec",
                str(path),
                option,
                "value",
                "--machine",
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)["error"]
    assert "parent.description" in error["hint"]
    assert "version.description" in error["hint"]


@pytest.mark.parametrize("command", ["agent", "session"])
@pytest.mark.parametrize("option", ["--size", "--limit"])
def test_page_size_alias_preserves_one_page(command, option, monkeypatch, capsys):
    """The alias forwards page size, cursor, ordering, and session selectors."""
    calls = []
    agent_id = uuid.uuid4()

    async def list_page(params):
        calls.append(params)
        return SimpleNamespace(items=[StubModel("item")], next_cursor="next-page")

    resource = SimpleNamespace(list=list_page)
    client = SimpleNamespace(agents=resource, sessions=resource)

    @asynccontextmanager
    async def open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", open_client)
    argv = [
        command,
        "list",
        option,
        "7",
        "--cursor",
        "previous-page",
        "--sort",
        "created:asc",
        "--machine",
    ]
    if command == "session":
        argv.extend(["--agent", str(agent_id), "--tag", "review"])
    assert app_module.main(argv) == 0
    assert len(calls) == 1
    params = calls[0]
    assert (params.size, params.cursor, params.sort) == (
        7,
        "previous-page",
        "created:asc",
    )
    if command == "session":
        conditions = params.filter.model_dump(mode="json", by_alias=True)["and"]
        assert {item["field"]: item["value"] for item in conditions} == {
            "agent_id": str(agent_id),
            "tag": "review",
        }
    result = json.loads(capsys.readouterr().out)
    assert result["page"] == {"limit": 7, "next_cursor": "next-page", "truncated": True}
    assert result["count"] == 1


@pytest.mark.parametrize(
    "args",
    [
        ["--limit", "0"],
        ["--limit", "1001"],
        ["--size", "2", "--limit", "3"],
        ["--limit", "2", "--size", "2"],
    ],
)
def test_invalid_page_size_is_structured_before_network(args, monkeypatch, capsys):
    """Bounds and duplicate scalar aliases fail without remote access."""

    def fail_client():
        pytest.fail("invalid size opened a remote client")

    monkeypatch.setattr(invocation_context, "open_asset_client", fail_client)
    assert app_module.main(["session", "list", *args, "--machine"]) == 2
    assert json.loads(capsys.readouterr().err)["error"]["kind"] == "invalid_arguments"


def test_every_shared_list_size_advertises_alias(capsys):
    """All collection and version commands inherit the same alias metadata."""
    for spec in command_registry.FUNCTION_SPECS.values():
        for parameter in spec.parameters:
            if parameter.name.startswith("--size"):
                assert parameter.name == "--size/--limit", spec.command
                assert "20" in parameter.description
                assert "1-1000" in parameter.description
    [session] = describe_schema(("session", "list"))
    assert "one page" in session["description"]
    assert app_module.main(["session", "list", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "--limit" in help_text
    assert "--size" in help_text


@pytest.mark.parametrize(
    ("args", "expected"), [([], 20), (["--limit", "1"], 1), (["--limit", "1000"], 1000)]
)
def test_page_size_default_and_valid_bounds(args, expected, monkeypatch, capsys):
    """The compatibility spelling retains the shared default and inclusive bounds."""
    calls = []

    async def list_page(params):
        calls.append(params.size)
        return SimpleNamespace(items=[], next_cursor=None)

    @asynccontextmanager
    async def open_client():
        yield SimpleNamespace(agents=SimpleNamespace(list=list_page))

    monkeypatch.setattr(invocation_context, "open_asset_client", open_client)
    assert app_module.main(["agent", "list", *args, "--machine"]) == 0
    assert calls == [expected]
    assert json.loads(capsys.readouterr().out)["page"] == {
        "limit": expected,
        "next_cursor": None,
        "truncated": False,
    }


def test_registration_help_describes_shorthand_only_for_parent_registration(capsys):
    """Registration help makes the separate parent and version documents clear."""
    assert app_module.main(["agent", "register", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "parent.description" in help_text
    assert "version.description" in help_text
    assert "shorthand" in help_text
    assert app_module.main(["agent", "version", "register", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "Version-only" in help_text
    assert "shorthand" not in help_text
