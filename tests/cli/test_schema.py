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

from kitaru.cli import app as _registered_app
from kitaru.cli import app as app_module
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
        "evaluation",
        "evaluator",
        "importer",
        "info",
        "job",
        "login",
        "logout",
        "schema",
        "session",
        "status",
        "version",
        "worker",
    }
    descriptions = {item["name"]: item["description"] for item in describe_schema()}
    assert descriptions["agent"] == "Register and inspect agents."
    assert descriptions["evaluation"] == "Inspect stored evaluations."
    assert descriptions["importer"] == "Develop, register, and inspect importers."
    assert descriptions["evaluator"] == "Develop, register, and inspect evaluators."
    assert descriptions["session"] == "Import and inspect sessions and their nodes."


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

    session_commands = {item["command"]: item for item in describe_schema(("session",))}
    assert set(session_commands) == {
        "session.evaluate",
        "session.get",
        "session.import",
        "session.list",
        "session.nodes",
    }
    assert all(
        command["read_only"]
        for name, command in session_commands.items()
        if name not in {"session.evaluate", "session.import"}
    )
    session_import = session_commands["session.import"]
    assert session_import["read_only"] is False
    assert session_import["streams"] is True
    assert session_import["output_modes"] == ["auto", "text", "json", "jsonl"]
    assert session_import["side_effects"]["reads_local_file"] is True
    assert session_import["side_effects"]["uploads_data"] is True
    assert session_import["side_effects"]["creates_remote_state"] is True
    import_parameters = {
        parameter["name"]: parameter for parameter in session_import["parameters"]
    }
    assert import_parameters["FILE"]["required"] is True
    assert import_parameters["--importer"]["required"] is True
    assert import_parameters["--agent"]["required"] is True
    assert {"--params", "--media-type", "--wait", "--interval", "--timeout"} <= set(
        import_parameters
    )
    import_errors = {error["kind"] for error in session_import["errors"]}
    assert {
        "partial_failure",
        "timeout",
        "remote_failed",
        "remote_canceled",
    } <= import_errors
    session_evaluate = session_commands["session.evaluate"]
    assert session_evaluate["read_only"] is False
    assert session_evaluate["streams"] is True
    assert session_evaluate["output_modes"] == ["auto", "text", "json", "jsonl"]
    assert session_evaluate["side_effects"]["reads_local_file"] is True
    assert session_evaluate["side_effects"]["uploads_data"] is False
    assert session_evaluate["side_effects"]["creates_remote_state"] is True
    evaluate_parameters = {
        parameter["name"]: parameter for parameter in session_evaluate["parameters"]
    }
    assert evaluate_parameters["SESSION"]["required"] is False
    assert evaluate_parameters["--sessions-file"]["required"] is False
    assert evaluate_parameters["--evaluator"]["required"] is True
    assert {"--evaluator-params", "--wait", "--interval", "--timeout"} <= set(
        evaluate_parameters
    )
    evaluate_errors = {error["kind"] for error in session_evaluate["errors"]}
    assert {"timeout", "remote_failed", "remote_canceled"} <= evaluate_errors

    node_parameters = {
        parameter["name"]
        for parameter in session_commands["session.nodes"]["parameters"]
    }
    assert "--include-payloads" in node_parameters
    assert "--filter" not in node_parameters
    assert "--sort" not in node_parameters
    session_list_errors = {
        error["kind"] for error in session_commands["session.list"]["errors"]
    }
    session_get_errors = {
        error["kind"] for error in session_commands["session.get"]["errors"]
    }
    assert "not_found" not in session_list_errors
    assert "not_found" in session_get_errors
    assert "conflict" not in session_list_errors | session_get_errors

    evaluation_commands = {
        item["command"]: item for item in describe_schema(("evaluation",))
    }
    assert set(evaluation_commands) == {"evaluation.get", "evaluation.list"}
    assert all(command["read_only"] for command in evaluation_commands.values())
    assert all(
        not command["side_effects"]["mutates_remote_state"]
        for command in evaluation_commands.values()
    )


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
