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

import inspect
import re

from kitaru.cli import app as _registered_app
from kitaru.cli import app as app_module
from kitaru.cli.schema import ParameterSpec, describe_schema


def _normalize_parameter_name(parameter: ParameterSpec) -> str:
    """Normalize a public schema parameter to its handler parameter name."""
    name = parameter.name
    if parameter.kind == "option":
        name = next(part for part in name.split("/") if part.startswith("--"))
        name = name.removeprefix("--")
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def test_handler_parameters_match_command_schema() -> None:
    """Every command-local handler parameter has exactly one schema entry."""
    for function, spec in app_module._FUNCTION_SPECS.items():
        local_parameters = spec.parameters[len(app_module._GLOBAL_PARAMETERS) :]
        schema_names = [_normalize_parameter_name(item) for item in local_parameters]
        handler_names = list(inspect.signature(function).parameters)
        if "all_sessions" in handler_names:
            schema_names = [
                "all_sessions" if name == "all" else name for name in schema_names
            ]
        if spec.path == ("worker", "start"):
            # The singular public --claim and --selector options intentionally
            # map to the plural handler parameters that receive their
            # repeatable values.
            plural = {"claim": "claims", "selector": "selectors"}
            schema_names = [plural.get(name, name) for name in schema_names]
        assert len(schema_names) == len(set(schema_names)), spec.command
        assert set(schema_names) == set(handler_names), spec.command


def test_top_level_schema_includes_completed_stage_one_slices() -> None:
    """Discovery exposes every completed Stage 1 command group."""
    _ = _registered_app
    roots = {item["name"] for item in describe_schema()}
    assert roots == {
        "agent",
        "annotation",
        "cohort",
        "config",
        "doctor",
        "evaluation",
        "evaluator",
        "experiment",
        "importer",
        "info",
        "investigation",
        "job",
        "login",
        "local",
        "logout",
        "replay",
        "schema",
        "session",
        "status",
        "version",
        "worker",
    }
    descriptions = {item["name"]: item["description"] for item in describe_schema()}
    assert descriptions["agent"] == "Register and inspect agents."
    assert descriptions["annotation"] == "Create and manage session annotations."
    assert (
        descriptions["cohort"]
        == "Manage cohort namespaces and immutable membership versions."
    )
    assert descriptions["evaluation"] == "Inspect stored evaluations."
    assert (
        descriptions["experiment"]
        == "Configure experiments and manage asynchronous runs."
    )
    assert descriptions["importer"] == "Develop, register, and inspect importers."
    assert descriptions["investigation"] == (
        "Create investigations and review their linked sessions."
    )
    assert descriptions["evaluator"] == "Develop, register, and inspect evaluators."
    assert descriptions["session"] == "Import and inspect sessions and their nodes."
    assert descriptions["replay"] == "Create and inspect standalone replays."


def test_command_schema_contains_behavior_and_error_contracts() -> None:
    """Leaf descriptions include parameters, side effects, and stable exits."""
    [login] = describe_schema(("login",))
    assert login["command"] == "login"
    assert login["mutating"] is True
    assert login["streams"] is False
    assert login["offline"] is False
    assert login["side_effects"]["writes_local_config"] is True
    assert {error["kind"]: error["exit_code"] for error in login["errors"]}[
        "authentication_failed"
    ] == 3
    assert any(
        parameter["name"] == "--api-key-stdin" for parameter in login["parameters"]
    )
    assert any(parameter["name"] == "--upgrade" for parameter in login["parameters"])

    [local_logs] = describe_schema(("local", "logs"))
    assert local_logs["streams"] is True
    assert local_logs["side_effects"]["executes_local_code"] is True

    [agent_register] = describe_schema(("agent", "register"))
    assert agent_register["mutating"] is True
    assert agent_register["side_effects"]["creates_remote_state"] is True
    names = {parameter["name"] for parameter in agent_register["parameters"]}
    assert "--command" in names
    assert "--entrypoint" not in names

    [importer_scaffold] = describe_schema(("importer", "scaffold"))
    assert importer_scaffold["side_effects"]["writes_local_file"] is True
    assert importer_scaffold["side_effects"]["creates_remote_state"] is False

    [version] = describe_schema(("version",))
    assert version["offline"] is True

    [worker_start] = describe_schema(("worker", "start"))
    assert worker_start["streams"] is True
    assert worker_start["output_modes"] == ["auto", "text", "json", "jsonl"]
    assert worker_start["side_effects"]["executes_local_code"] is True

    replay_commands = {item["command"]: item for item in describe_schema(("replay",))}
    assert set(replay_commands) == {"replay.create", "replay.get", "replay.list"}
    replay_create = replay_commands["replay.create"]
    assert replay_create["read_only"] is False
    assert replay_create["side_effects"]["creates_remote_state"] is True
    assert replay_create["idempotency"] == "non_idempotent_replay_created_per_request"
    policy = next(
        parameter
        for parameter in replay_create["parameters"]
        if parameter["name"] == "--tool-policy"
    )
    assert "server default" in policy["description"]
    assert "live tools" in policy["description"]

    [annotation_create] = describe_schema(("annotation", "create"))
    selector = next(
        parameter
        for parameter in annotation_create["parameters"]
        if parameter["name"] == "--selector"
    )
    assert selector["description"] == "Optional node, JSON Pointer, or span selector."

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

    [experiment_export] = describe_schema(("experiment", "export"))
    export_parameters = {
        parameter["name"] for parameter in experiment_export["parameters"]
    }
    assert {
        "--omit-content",
        "--environment-mode",
        "--include-source",
        "--exclude-source",
    } <= export_parameters
    assert not any(
        component in parameter
        for parameter in export_parameters
        for component in ("taskset-only", "harness-only", "evaluator-only", "append")
    )

    evaluation_commands = {
        item["command"]: item for item in describe_schema(("evaluation",))
    }
    assert set(evaluation_commands) == {"evaluation.get", "evaluation.list"}
    assert all(command["read_only"] for command in evaluation_commands.values())
    assert all(
        not command["side_effects"]["mutates_remote_state"]
        for command in evaluation_commands.values()
    )


def test_cohort_schema_describes_exact_and_destructive_commands() -> None:
    """Cohort discovery exposes only the implemented SDK-backed surface."""
    commands = {item["command"]: item for item in describe_schema(("cohort",))}
    assert set(commands) == {
        "cohort.create",
        "cohort.delete",
        "cohort.get",
        "cohort.list",
        "cohort.update",
        "cohort.version.create",
        "cohort.version.delete",
        "cohort.version.get",
        "cohort.version.list",
        "cohort.version.update",
    }

    assert commands["cohort.create"]["idempotency"] == ("non_idempotent_remote_create")
    assert commands["cohort.version.create"]["idempotency"] == (
        "non_idempotent_server_assigned_version"
    )
    parameters = {
        parameter["name"]: parameter
        for parameter in commands["cohort.version.create"]["parameters"]
    }
    assert parameters["--baseline"]["type"] == "UUID"
    assert parameters["--baseline"]["required"] is False
    assert commands["cohort.update"]["idempotency"] == "idempotent replacement"
    assert commands["cohort.version.update"]["idempotency"] == (
        "idempotent replacement"
    )
    for command_name in ("cohort.delete", "cohort.version.delete"):
        command = commands[command_name]
        assert command["read_only"] is False
        assert command["side_effects"]["deletes_remote_state"] is True
        assert command["side_effects"]["mutates_remote_state"] is True
        assert command["idempotency"] == "not_found after first removal"
        force = {parameter["name"]: parameter for parameter in command["parameters"]}[
            "--force"
        ]
        assert force["type"] == "boolean"

    cohort_delete = commands["cohort.delete"]
    assert "all of its versions" in cohort_delete["description"]
    version_list_parameters = {
        parameter["name"] for parameter in commands["cohort.version.list"]["parameters"]
    }
    assert "--filter" not in version_list_parameters
    assert {"--size", "--cursor", "--sort"} <= version_list_parameters
    version_reference = {
        parameter["name"]: parameter
        for parameter in commands["cohort.version.get"]["parameters"]
    }["VERSION"]
    assert version_reference["type"] == "UUID|COHORT@VERSION"
    assert all(command["streams"] is False for command in commands.values())
    assert all(
        command["output_modes"] == ["auto", "text", "json"]
        for command in commands.values()
    )
    assert not any(
        deferred in command_name
        for command_name in commands
        for deferred in ("members", "membership", "open", "experiment", "run")
    )


def test_experiment_schema_describes_crud_and_run_lifecycle() -> None:
    """Experiment discovery exposes exact CRUD and SDK-backed run commands."""
    commands = {item["command"]: item for item in describe_schema(("experiment",))}
    assert set(commands) == {
        "experiment.create",
        "experiment.delete",
        "experiment.export",
        "experiment.get",
        "experiment.list",
        "experiment.run.cancel",
        "experiment.run.delete",
        "experiment.run.get",
        "experiment.run.jobs",
        "experiment.run.list",
        "experiment.run.start",
        "experiment.run.watch",
        "experiment.update",
    }

    create = commands["experiment.create"]
    assert create["read_only"] is False
    assert create["side_effects"]["creates_remote_state"] is True
    assert create["idempotency"] == "non_idempotent_remote_create"
    create_parameters = {
        parameter["name"]: parameter for parameter in create["parameters"]
    }
    assert create_parameters["--evaluator"]["required"] is True
    assert create_parameters["--evaluator"]["type"] == "reference[]"
    assert create_parameters["--evaluator-params"]["required"] is False
    assert {"--override", "--tool-policy"} <= set(create_parameters)

    update = commands["experiment.update"]
    assert update["read_only"] is False
    assert update["side_effects"]["mutates_remote_state"] is True
    assert update["idempotency"] == "idempotent replacement"
    update_parameters = {
        parameter["name"]: parameter for parameter in update["parameters"]
    }
    assert {
        "--clear-description",
        "--clear-override",
        "--evaluator",
        "--evaluator-params",
        "--tool-policy",
    } <= set(update_parameters)
    assert "--clear-tool-policy" not in update_parameters
    assert update_parameters["--evaluator"]["required"] is False

    delete = commands["experiment.delete"]
    assert delete["read_only"] is False
    assert delete["side_effects"]["mutates_remote_state"] is True
    assert delete["side_effects"]["deletes_remote_state"] is True
    assert delete["idempotency"] == "not_found after first removal"
    force = {parameter["name"]: parameter for parameter in delete["parameters"]}[
        "--force"
    ]
    assert force["type"] == "boolean"

    export = commands["experiment.export"]
    assert export["side_effects"]["reads_local_file"] is True
    assert export["side_effects"]["writes_local_file"] is True
    export_parameters = {
        parameter["name"]: parameter for parameter in export["parameters"]
    }
    assert export_parameters["--primary-reward"]["required"] is True
    assert export_parameters["--dry-run"]["required"] is False

    start = commands["experiment.run.start"]
    assert start["read_only"] is False
    assert start["streams"] is True
    assert start["side_effects"]["creates_remote_state"] is True
    assert start["idempotency"] == "non_idempotent_run_created_per_request"
    assert start["output_modes"] == ["auto", "text", "json", "jsonl"]
    start_parameters = {
        parameter["name"]: parameter for parameter in start["parameters"]
    }
    assert start_parameters["--cohort-version"]["required"] is True
    assert start_parameters["--agent"]["required"] is True
    assert (
        start_parameters["--wait"]["description"] == "Wait for remote work settlement."
    )
    start_errors = {error["kind"]: error["exit_code"] for error in start["errors"]}
    assert start_errors["timeout"] == 7
    assert start_errors["remote_failed"] == 8
    assert start_errors["remote_canceled"] == 9

    watch = commands["experiment.run.watch"]
    assert watch["read_only"] is True
    assert watch["streams"] is True
    assert watch["output_modes"] == ["auto", "text", "json", "jsonl"]

    cancel = commands["experiment.run.cancel"]
    assert cancel["idempotency"] == "server_rejects_settled_runs"
    assert cancel["side_effects"]["mutates_remote_state"] is True

    run_delete = commands["experiment.run.delete"]
    assert run_delete["side_effects"]["deletes_remote_state"] is True
    assert "jobs and tasks" in run_delete["description"]
    assert run_delete["idempotency"] == "not_found after first removal"

    finite_commands = set(commands) - {
        "experiment.run.start",
        "experiment.run.watch",
    }
    assert all(commands[name]["streams"] is False for name in finite_commands)
    assert all(
        commands[name]["output_modes"] == ["auto", "text", "json"]
        for name in finite_commands
    )
    assert not any(
        deferred in command_name
        for command_name in commands
        for deferred in ("compare", "ci", "review", "open")
    )


def test_help_exposes_only_the_schema_command_surface(capsys) -> None:
    """Cyclopts does not invent option aliases omitted from command metadata."""
    assert app_module.main(["agent", "register", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "NAME --name" not in help_text
    assert "--empty-env" not in help_text
    assert "JSONL is streaming-only" in help_text

    assert app_module.main(["session", "list", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "Items per page" in help_text
    assert "Cursor from the previous page" in help_text
    assert "created:asc or created:desc" in help_text
    assert "JSON filter expression" in help_text

    [job_get] = describe_schema(("job", "get"))
    assert job_get["side_effects"]["reads_local_file"] is True
