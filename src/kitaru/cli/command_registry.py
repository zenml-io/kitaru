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
"""Cyclopts applications and the metadata registry shared by every command."""

import inspect
from collections.abc import Callable
from typing import Annotated, Any, TypeVar, get_args, get_origin

from cyclopts import App, Parameter

from kitaru.cli import diagnostics
from kitaru.cli.schema import (
    GROUP_DESCRIPTIONS,
    CommandSpec,
    ParameterSpec,
    SideEffect,
    register_spec,
)

F = TypeVar("F", bound=Callable[..., Any])

app = App(
    name="kitaru",
    help="Connect to, inspect, and configure Kitaru.",
    version=diagnostics.package_version,
    result_action="return_value",
    print_error=False,
    help_on_error=False,
    default_parameter=Parameter(negative=False),
)
config_app = App(
    name="config",
    help=GROUP_DESCRIPTIONS["config"],
    default_parameter=Parameter(negative=False),
)
agent_app = App(
    name="agent",
    help=GROUP_DESCRIPTIONS["agent"],
    default_parameter=Parameter(negative=False),
)
agent_version_app = App(
    name="version",
    help="Register and inspect agent versions.",
    default_parameter=Parameter(negative=False),
)
cohort_app = App(
    name="cohort",
    help=GROUP_DESCRIPTIONS["cohort"],
    default_parameter=Parameter(negative=False),
)
cohort_version_app = App(
    name="version",
    help="Create and manage immutable cohort membership versions.",
    default_parameter=Parameter(negative=False),
)
connection_app = App(
    name="connection",
    help=GROUP_DESCRIPTIONS["connection"],
    default_parameter=Parameter(negative=False),
)
annotation_app = App(
    name="annotation",
    help=GROUP_DESCRIPTIONS["annotation"],
    default_parameter=Parameter(negative=False),
)
insight_app = App(
    name="insight",
    help=GROUP_DESCRIPTIONS["insight"],
    default_parameter=Parameter(negative=False),
)
investigation_app = App(
    name="investigation",
    help=GROUP_DESCRIPTIONS["investigation"],
    default_parameter=Parameter(negative=False),
)
investigation_session_app = App(
    name="session",
    help="List and settle sessions linked to an investigation.",
    default_parameter=Parameter(negative=False),
)
experiment_app = App(
    name="experiment",
    help=GROUP_DESCRIPTIONS["experiment"],
    default_parameter=Parameter(negative=False),
)
experiment_run_app = App(
    name="run",
    help="Start, inspect, watch, cancel, and delete experiment runs.",
    default_parameter=Parameter(negative=False),
)
import_app = App(
    name="import",
    help=GROUP_DESCRIPTIONS["import"],
    default_parameter=Parameter(negative=False),
)
importer_app = App(
    name="importer",
    help=GROUP_DESCRIPTIONS["importer"],
    default_parameter=Parameter(negative=False),
)
importer_version_app = App(
    name="version",
    help="Register and inspect importer versions.",
    default_parameter=Parameter(negative=False),
)
evaluator_app = App(
    name="evaluator",
    help=GROUP_DESCRIPTIONS["evaluator"],
    default_parameter=Parameter(negative=False),
)
evaluator_version_app = App(
    name="version",
    help="Register and inspect evaluator versions.",
    default_parameter=Parameter(negative=False),
)
analyzer_app = App(
    name="analyzer",
    help=GROUP_DESCRIPTIONS["analyzer"],
    default_parameter=Parameter(negative=False),
)
analyzer_version_app = App(
    name="version",
    help="Register and inspect analyzer versions.",
    default_parameter=Parameter(negative=False),
)
session_app = App(
    name="session",
    help=GROUP_DESCRIPTIONS["session"],
    default_parameter=Parameter(negative=False),
)
evaluation_app = App(
    name="evaluation",
    help=GROUP_DESCRIPTIONS["evaluation"],
    default_parameter=Parameter(negative=False),
)
worker_app = App(
    name="worker",
    help=GROUP_DESCRIPTIONS["worker"],
    default_parameter=Parameter(negative=False),
)
job_app = App(
    name="job",
    help=GROUP_DESCRIPTIONS["job"],
    default_parameter=Parameter(negative=False),
)
replay_app = App(
    name="replay",
    help=GROUP_DESCRIPTIONS["replay"],
    default_parameter=Parameter(negative=False),
)
local_app = App(
    name="local",
    help=GROUP_DESCRIPTIONS["local"],
    default_parameter=Parameter(negative=False),
)
agent_app.command(agent_version_app, name="version")
cohort_app.command(cohort_version_app, name="version")
investigation_app.command(investigation_session_app, name="session")
experiment_app.command(experiment_run_app, name="run")
importer_app.command(importer_version_app, name="version")
evaluator_app.command(evaluator_version_app, name="version")
analyzer_app.command(analyzer_version_app, name="version")
app.command(config_app, name="config")
app.command(agent_app, name="agent")
app.command(analyzer_app, name="analyzer")
app.command(annotation_app, name="annotation")
app.command(cohort_app, name="cohort")
app.command(connection_app, name="connection")
app.command(experiment_app, name="experiment")
app.command(import_app, name="import")
app.command(importer_app, name="importer")
app.command(insight_app, name="insight")
app.command(investigation_app, name="investigation")
app.command(evaluator_app, name="evaluator")
app.command(session_app, name="session")
app.command(evaluation_app, name="evaluation")
app.command(worker_app, name="worker")
app.command(job_app, name="job")
app.command(replay_app, name="replay")
app.command(local_app, name="local")

FUNCTION_SPECS: dict[Callable[..., Any], CommandSpec] = {}


GLOBAL_PARAMETERS = (
    ParameterSpec(
        "--output/-o", "auto|text|json|jsonl", "option", False, "Serialization mode."
    ),
    ParameterSpec(
        "--machine/--no-machine", "boolean", "option", False, "Terminal rendering mode."
    ),
    ParameterSpec("--server", "URL", "option", False, "Server URL override."),
    ParameterSpec(
        "--non-interactive", "boolean", "option", False, "Disable interaction."
    ),
    ParameterSpec(
        "--no-browser", "boolean", "option", False, "Disable browser launch."
    ),
    ParameterSpec(
        "--request-timeout", "positive float", "option", False, "HTTP timeout."
    ),
    ParameterSpec("--debug", "boolean", "option", False, "Redacted debug diagnostics."),
    ParameterSpec("--traceback", "boolean", "option", False, "Redacted traceback."),
)


def build_spec(
    path: tuple[str, ...],
    description: str,
    *,
    parameters: tuple[ParameterSpec, ...] = (),
    read_only: bool = True,
    side_effects: tuple[SideEffect, ...] = (),
    idempotency: str = "read_only",
    interaction: str = "none",
    errors: tuple[str, ...] = ("invalid_arguments", "internal_error"),
    offline: bool = False,
    streams: bool = False,
) -> CommandSpec:
    """Create one leaf contract including shared global options."""
    if "invalid_arguments" not in errors:
        errors = ("invalid_arguments", *errors)
    if "interrupted" not in errors:
        errors = (*errors, "interrupted")
    if not offline and "reads_local_file" not in side_effects:
        side_effects = ("reads_local_file", *side_effects)
    return CommandSpec(
        path=path,
        description=description,
        parameters=GLOBAL_PARAMETERS + parameters,
        read_only=read_only,
        side_effects=side_effects,
        idempotency=idempotency,
        interaction=interaction,
        streams=streams,
        error_kinds=errors,
        output_modes=("auto", "text", "json", "jsonl")
        if streams
        else ("auto", "text", "json"),
        offline=offline,
    )


def register_command(target: App, spec: CommandSpec) -> Callable[[F], F]:
    """Register a command and the metadata that powers offline schema."""
    register_spec(spec)

    def decorator(function: F) -> F:
        _add_parameter_help(function, spec)
        FUNCTION_SPECS[function] = spec
        target.command(function, name=spec.path[-1], help=spec.description)
        return function

    return decorator


def _add_parameter_help(function: F, spec: CommandSpec) -> None:
    """Attach registered parameter descriptions to Cyclopts annotations."""
    signature = inspect.signature(function)
    command_parameters = spec.parameters[len(GLOBAL_PARAMETERS) :]
    positional_specs = iter(
        parameter for parameter in command_parameters if parameter.kind == "argument"
    )
    option_specs = {
        parameter.name.split("/")[0].removeprefix("--").replace("-", "_"): parameter
        for parameter in command_parameters
        if parameter.kind == "option"
    }
    for name, parameter in signature.parameters.items():
        parameter_spec = option_specs.get(name)
        if parameter_spec is None and parameter.kind in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL,
        }:
            parameter_spec = next(positional_specs, None)
        if parameter_spec is None:
            continue
        annotation = parameter.annotation
        if annotation is inspect.Parameter.empty:
            annotation = str
        if get_origin(annotation) is Annotated:
            base, *metadata = get_args(annotation)
            annotation = Annotated[
                base, *metadata, Parameter(help=parameter_spec.description)
            ]
        else:
            annotation = Annotated[
                annotation, Parameter(help=parameter_spec.description)
            ]
        function.__annotations__[name] = annotation


COLLECTION_READ_ERRORS = (
    "invalid_arguments",
    "invalid_configuration",
    "authentication_failed",
    "network_error",
    "internal_error",
)
UUID_READ_ERRORS = (*COLLECTION_READ_ERRORS, "not_found")
ASSET_READ_ERRORS = (*UUID_READ_ERRORS, "conflict")
ASSET_WRITE_ERRORS = (*ASSET_READ_ERRORS, "partial_failure")
JOB_WAIT_ERRORS = ("timeout", "remote_failed", "remote_canceled")
LIST_PARAMETERS = (
    ParameterSpec("--size", "integer", "option", False, "Items per page (1-1000)."),
    ParameterSpec(
        "--cursor", "string", "option", False, "Cursor from the previous page."
    ),
    ParameterSpec(
        "--sort",
        "field:direction",
        "option",
        False,
        "Sort by created:asc or created:desc.",
    ),
    ParameterSpec(
        "--filter", "JSON", "option", False, "Advanced JSON filter expression."
    ),
)
VERSION_LIST_PARAMETERS = LIST_PARAMETERS[:-1]
WAIT_PARAMETERS = (
    ParameterSpec(
        "--wait", "boolean", "option", False, "Wait for remote work settlement."
    ),
    ParameterSpec(
        "--interval",
        "positive float",
        "option",
        False,
        "Polling interval; requires --wait.",
    ),
    ParameterSpec(
        "--timeout",
        "positive float",
        "option",
        False,
        "Local wait timeout; requires --wait.",
    ),
)
IDEMPOTENCY_KEY_PARAMETER = ParameterSpec(
    "--idempotency-key",
    "string",
    "option",
    False,
    "Retrying this exact invocation with the same key returns the original "
    "result instead of acting twice.",
)
