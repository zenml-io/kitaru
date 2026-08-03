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
"""Offline command discovery metadata."""

import pytest

from kitaru.cli import app as _registered_app
from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.schema import describe_schema


def test_top_level_schema_includes_completed_stage_one_slices() -> None:
    """Discovery exposes every completed Stage 1 command group."""
    _ = _registered_app
    roots = {item["name"] for item in describe_schema()}
    assert roots == {
        "agent",
        "config",
        "context",
        "doctor",
        "evaluator",
        "importer",
        "info",
        "job",
        "login",
        "logout",
        "schema",
        "status",
        "version",
        "worker",
    }
    descriptions = {item["name"]: item["description"] for item in describe_schema()}
    assert descriptions["agent"] == "Register and inspect agents."
    assert descriptions["importer"] == "Develop, register, and inspect importers."
    assert descriptions["evaluator"] == "Develop, register, and inspect evaluators."


def test_command_schema_contains_behavior_and_error_contracts() -> None:
    """Leaf descriptions include parameters, side effects, and stable exits."""
    [login] = describe_schema(("login",))
    assert login["command"] == "login"
    assert login["mutating"] is True
    assert login["streams"] is False
    assert login["side_effects"]["writes_local_config"] is True
    assert {error["kind"]: error["exit_code"] for error in login["errors"]}[
        "authentication_failed"
    ] == 3
    assert any(
        parameter["name"] == "--api-key-stdin" for parameter in login["parameters"]
    )

    [agent_register] = describe_schema(("agent", "register"))
    assert agent_register["mutating"] is True
    assert agent_register["side_effects"]["creates_remote_state"] is True
    assert any(
        parameter["name"] == "--entrypoint"
        for parameter in agent_register["parameters"]
    )

    [importer_scaffold] = describe_schema(("importer", "scaffold"))
    assert importer_scaffold["side_effects"]["writes_local_file"] is True
    assert importer_scaffold["side_effects"]["creates_remote_state"] is False

    [worker_start] = describe_schema(("worker", "start"))
    assert worker_start["streams"] is True
    assert worker_start["output_modes"] == ["auto", "text", "json", "jsonl"]
    assert worker_start["side_effects"]["executes_local_code"] is True

    [job_watch] = describe_schema(("job", "watch"))
    assert job_watch["streams"] is True
    assert job_watch["output_modes"] == ["auto", "text", "json", "jsonl"]
    assert {error["kind"]: error["exit_code"] for error in job_watch["errors"]}[
        "remote_canceled"
    ] == 9

    with pytest.raises(CLIError, match="Unknown command path"):
        describe_schema(("session", "import"))


def test_help_exposes_only_the_schema_command_surface(capsys) -> None:
    """Cyclopts does not invent option aliases omitted from command metadata."""
    assert app_module.main(["agent", "register", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "NAME --name" not in help_text
    assert "--empty-env" not in help_text
    assert "JSONL is streaming-only" in help_text

    assert app_module.main(["context", "remove", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "NAME --name" not in help_text
    assert "--no-force" not in help_text

    [job_get] = describe_schema(("job", "get"))
    assert job_get["side_effects"]["reads_local_file"] is True
