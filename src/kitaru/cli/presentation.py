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
"""Human-only presentation metadata for CLI results."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

HumanFormatter = Callable[[Any], str]


@dataclass(frozen=True, slots=True)
class HumanField:
    """One selected field in a human list or detail view."""

    key: str
    label: str
    min_console_width: int = 0
    formatter: HumanFormatter | None = None
    no_wrap: bool = False


@dataclass(frozen=True, slots=True)
class HumanSection:
    """A group of fields in a human detail view."""

    title: str
    fields: tuple[HumanField, ...]


@dataclass(frozen=True, slots=True)
class HumanView:
    """Curated human presentation for one command result."""

    title: str
    fields: tuple[HumanField, ...]
    sections: tuple[HumanSection, ...] = ()
    empty_message: str = "No results found."
    renderer: Literal["default", "doctor"] = "default"


def _count(value: Any) -> str:
    """Format a collection as its item count."""
    if value is None:
        return "0"
    try:
        return str(len(value))
    except TypeError:
        return str(value)


def _progress(value: Any) -> str:
    """Format experiment-run progress as completed over total."""
    if not isinstance(value, dict):
        return str(value)
    completed = value.get("completed", 0)
    failed = value.get("failed", 0)
    canceled = value.get("canceled", 0)
    total = value.get("total", 0)
    settled = completed + failed + canceled
    return f"{settled}/{total}"


def _runtime(value: Any) -> str:
    """Format a worker runtime as a short platform summary."""
    if not isinstance(value, dict):
        return str(value)
    parts = [value.get("platform"), value.get("python_version")]
    return " / ".join(str(part) for part in parts if part) or "-"


def _scope(value: Any) -> str:
    """Format worker task kinds from its scope."""
    if not isinstance(value, dict):
        return str(value)
    kinds = value.get("kinds")
    if not kinds:
        return "all"
    return ", ".join(str(kind) for kind in kinds)


def _verdict(value: Any) -> str:
    """Format a nullable evaluation verdict."""
    if value is True:
        return "pass"
    if value is False:
        return "fail"
    return "-"


_ID = HumanField("id", "ID", no_wrap=True)
_CREATED = HumanField("created", "Created", 110)
_UPDATED = HumanField("updated", "Updated")
_NAME = HumanField("name", "Name")
_STATUS = HumanField("status", "Status")
_DESCRIPTION = HumanField("description", "Description")
_METADATA = HumanField("metadata", "Metadata")

_ASSET_FIELDS = (
    _NAME,
    HumanField("latest_version", "Latest"),
    _ID,
    _CREATED,
)
_ASSET_SECTIONS = (
    HumanSection("Summary", (_NAME, _DESCRIPTION, _ID)),
    HumanSection("Versions", (HumanField("latest_version", "Latest version"),)),
    HumanSection("Timing", (_CREATED, _UPDATED)),
    HumanSection("Metadata", (_METADATA,)),
)
_VERSION_FIELDS = (
    HumanField("version", "Version"),
    HumanField("display_version", "Display"),
    _ID,
    _CREATED,
)
_VERSION_SECTIONS = (
    HumanSection(
        "Summary",
        (
            HumanField("version", "Version"),
            HumanField("display_version", "Display version"),
            _DESCRIPTION,
            _ID,
        ),
    ),
    HumanSection("Timing", (_CREATED, _UPDATED)),
    HumanSection(
        "Configuration",
        (
            HumanField("run_spec", "Run spec"),
            HumanField("capabilities", "Capabilities"),
            HumanField("source", "Source"),
        ),
    ),
)
_JOB_SECTIONS = (
    HumanSection(
        "Summary",
        (
            HumanField("operation", "Operation"),
            HumanField("terminal", "Terminal"),
            HumanField("job.status|run.status|status", "Status"),
            HumanField("job.id|run.id|id", "ID", no_wrap=True),
        ),
    ),
    HumanSection(
        "Progress",
        (
            HumanField("run.progress|summary|stats", "Progress"),
            HumanField("job.error|run.error|error", "Error"),
        ),
    ),
    HumanSection(
        "Request",
        (
            HumanField("experiment", "Experiment"),
            HumanField("cohort_version", "Cohort version"),
            HumanField("agent|agent_version", "Agent"),
            HumanField("importer", "Importer"),
            HumanField("session_ids", "Sessions"),
            HumanField("evaluators", "Evaluators"),
        ),
    ),
)
_REGISTRATION_SECTIONS = (
    HumanSection(
        "Registered",
        (
            HumanField("agent.name|importer.name|evaluator.name", "Name"),
            HumanField(
                "agent.id|importer.id|evaluator.id",
                "Parent ID",
                no_wrap=True,
            ),
            HumanField("version.version", "Version"),
            HumanField("version.display_version", "Display version"),
            HumanField("version.id", "Version ID", no_wrap=True),
        ),
    ),
    HumanSection("Phases", (HumanField("phases", "Phases"),)),
)


def _view(
    title: str,
    fields: tuple[HumanField, ...],
    sections: tuple[HumanSection, ...] = (),
    *,
    empty: str | None = None,
) -> HumanView:
    """Create one view with a command-specific empty message."""
    return HumanView(
        title=title,
        fields=fields,
        sections=sections,
        empty_message=empty or f"No {title.lower()} found.",
    )


_VIEWS: dict[str, HumanView] = {
    "status": HumanView(
        title="Status",
        fields=(),
        sections=(
            HumanSection(
                "Connection",
                (
                    HumanField("server_url", "Server"),
                    HumanField("server_source", "Source"),
                    HumanField("context", "Context"),
                ),
            ),
            HumanSection(
                "Health",
                (
                    HumanField("authentication", "Authentication"),
                    HumanField("compatibility", "Compatibility"),
                    HumanField("live_worker_count", "Live workers"),
                    HumanField("dashboard_url", "Dashboard"),
                ),
            ),
            HumanSection(
                "Server",
                (
                    HumanField("server.version", "Version"),
                    HumanField("server.auth_scheme", "Authentication scheme"),
                ),
            ),
        ),
    ),
    "info": HumanView(
        title="Runtime information",
        fields=(),
        sections=(
            HumanSection(
                "Local runtime",
                (
                    HumanField("kitaru_version", "Kitaru"),
                    HumanField("python_version", "Python"),
                    HumanField("python_implementation", "Implementation"),
                    HumanField("platform", "Platform"),
                    HumanField("executable", "Executable"),
                ),
            ),
            HumanSection(
                "Server",
                (
                    HumanField("server_url", "URL"),
                    HumanField("server_source", "Source"),
                    HumanField("context", "Context"),
                    HumanField("server.version", "Version"),
                    HumanField("compatibility", "Compatibility"),
                ),
            ),
        ),
    ),
    "doctor": HumanView(
        title="Doctor",
        fields=(),
        renderer="doctor",
    ),
    "agent.list": _view("Agents", _ASSET_FIELDS, _ASSET_SECTIONS),
    "agent.get": _view("Agent", _ASSET_FIELDS, _ASSET_SECTIONS),
    "agent.register": _view("Agent", (), _REGISTRATION_SECTIONS),
    "agent.version.list": _view("Agent versions", _VERSION_FIELDS, _VERSION_SECTIONS),
    "agent.version.get": _view("Agent version", _VERSION_FIELDS, _VERSION_SECTIONS),
    "agent.version.register": _view("Agent version", (), _REGISTRATION_SECTIONS),
    "importer.list": _view(
        "Importers",
        (
            _NAME,
            HumanField("provider", "Provider"),
            HumanField("latest_version", "Latest"),
            _ID,
            _CREATED,
        ),
        _ASSET_SECTIONS,
    ),
    "importer.get": _view("Importer", _ASSET_FIELDS, _ASSET_SECTIONS),
    "importer.register": _view("Importer", (), _REGISTRATION_SECTIONS),
    "importer.version.list": _view(
        "Importer versions", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "importer.version.get": _view(
        "Importer version", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "importer.version.register": _view("Importer version", (), _REGISTRATION_SECTIONS),
    "evaluator.list": _view("Evaluators", _ASSET_FIELDS, _ASSET_SECTIONS),
    "evaluator.get": _view("Evaluator", _ASSET_FIELDS, _ASSET_SECTIONS),
    "evaluator.register": _view("Evaluator", (), _REGISTRATION_SECTIONS),
    "evaluator.version.list": _view(
        "Evaluator versions", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "evaluator.version.get": _view(
        "Evaluator version", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "evaluator.version.register": _view(
        "Evaluator version", (), _REGISTRATION_SECTIONS
    ),
    "cohort.list": _view(
        "Cohorts",
        (
            _NAME,
            HumanField("latest_version", "Latest"),
            HumanField("agent_id", "Agent", 120),
            _ID,
            _CREATED,
        ),
        _ASSET_SECTIONS,
    ),
    "cohort.get": _view("Cohort", _ASSET_FIELDS, _ASSET_SECTIONS),
    "cohort.create": _view("Cohort", _ASSET_FIELDS, _ASSET_SECTIONS),
    "cohort.update": _view("Cohort", _ASSET_FIELDS, _ASSET_SECTIONS),
    "cohort.version.list": _view(
        "Cohort versions",
        (
            HumanField("version", "Version"),
            HumanField("display_version", "Display"),
            HumanField("session_count", "Sessions"),
            _ID,
            _CREATED,
        ),
        _VERSION_SECTIONS,
    ),
    "cohort.version.get": _view("Cohort version", _VERSION_FIELDS, _VERSION_SECTIONS),
    "cohort.version.create": _view(
        "Cohort version", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "cohort.version.update": _view(
        "Cohort version", _VERSION_FIELDS, _VERSION_SECTIONS
    ),
    "experiment.list": _view(
        "Experiments",
        (
            _NAME,
            HumanField("evaluators", "Evaluators", 100, _count),
            HumanField("tool_policy", "Tool policy", 150),
            _ID,
            _CREATED,
        ),
        _ASSET_SECTIONS,
    ),
    "experiment.get": _view("Experiment", _ASSET_FIELDS, _ASSET_SECTIONS),
    "experiment.create": _view("Experiment", _ASSET_FIELDS, _ASSET_SECTIONS),
    "experiment.update": _view("Experiment", _ASSET_FIELDS, _ASSET_SECTIONS),
    "experiment.run.list": _view(
        "Experiment runs",
        (
            _STATUS,
            HumanField("number", "Run"),
            HumanField("progress", "Progress", 90, _progress),
            _ID,
            _CREATED,
        ),
    ),
    "experiment.run.get": _view(
        "Experiment run",
        (
            _STATUS,
            HumanField("number", "Run"),
            HumanField("progress", "Progress", 90, _progress),
            _ID,
        ),
        (
            HumanSection(
                "Summary",
                (
                    _STATUS,
                    HumanField("number", "Run"),
                    HumanField("progress", "Progress", formatter=_progress),
                    _ID,
                ),
            ),
            HumanSection(
                "Timing",
                (
                    HumanField("started_at", "Started"),
                    HumanField("ended_at", "Ended"),
                    _CREATED,
                ),
            ),
            HumanSection(
                "Configuration",
                (
                    HumanField("experiment_id", "Experiment ID"),
                    HumanField("cohort_version_id", "Cohort version ID"),
                    HumanField("agent_version_id", "Agent version ID"),
                    HumanField("evaluate_baselines", "Evaluate baselines"),
                ),
            ),
            HumanSection("Error", (HumanField("error", "Error"),)),
        ),
    ),
    "experiment.run.jobs": _view(
        "Run jobs",
        (
            _STATUS,
            _ID,
            HumanField("started_at", "Started", 110),
            HumanField("ended_at", "Ended", 150),
            HumanField("error", "Error", 190),
        ),
    ),
    "session.list": _view(
        "Sessions",
        (
            _STATUS,
            _NAME,
            _ID,
            HumanField("origin", "Origin", 105),
            HumanField("provider", "Provider", 125),
            HumanField("llm_call_count", "LLM calls", 150),
            HumanField("tool_call_count", "Tool calls", 165),
            HumanField("cost", "Cost", 175),
            _CREATED,
        ),
    ),
    "session.get": _view(
        "Session",
        (_STATUS, _NAME, _ID),
        (
            HumanSection(
                "Summary",
                (
                    _STATUS,
                    _NAME,
                    _ID,
                    HumanField("origin", "Origin"),
                    HumanField("external_id", "External ID"),
                ),
            ),
            HumanSection(
                "Timing",
                (
                    HumanField("started_at", "Started"),
                    HumanField("ended_at", "Ended"),
                    _CREATED,
                    _UPDATED,
                ),
            ),
            HumanSection(
                "Agent and source",
                (
                    HumanField("agent_id", "Agent ID"),
                    HumanField("agent_version_id", "Agent version ID"),
                    HumanField("provider", "Provider"),
                    HumanField("framework", "Framework"),
                    HumanField("adapter_version", "Adapter version"),
                ),
            ),
            HumanSection(
                "Usage",
                (
                    HumanField("llm_call_count", "LLM calls"),
                    HumanField("tool_call_count", "Tool calls"),
                    HumanField("tokens", "Tokens"),
                    HumanField("cost", "Cost"),
                ),
            ),
            HumanSection(
                "Payload",
                (
                    HumanField("inputs", "Inputs"),
                    HumanField("outputs", "Outputs"),
                    HumanField("expected", "Expected"),
                ),
            ),
            HumanSection("Error", (HumanField("error", "Error"),)),
            HumanSection("Metadata", (_METADATA,)),
        ),
    ),
    "session.nodes": _view(
        "Session nodes",
        (
            HumanField("index", "#"),
            HumanField("node_type", "Type"),
            _NAME,
            _STATUS,
            HumanField("model|tool_name", "Model / tool", 120),
            HumanField("cost", "Cost", 155),
            _ID,
        ),
    ),
    "evaluation.list": _view(
        "Evaluations",
        (
            HumanField("passed", "Verdict", formatter=_verdict),
            _NAME,
            HumanField("score|value", "Result"),
            HumanField("evaluator_name", "Evaluator", 110),
            HumanField("session_id", "Session", 145),
            _ID,
            _CREATED,
        ),
    ),
    "evaluation.get": _view(
        "Evaluation",
        (
            HumanField("passed", "Verdict", formatter=_verdict),
            _NAME,
            HumanField("score|value", "Result"),
            _ID,
        ),
        (
            HumanSection(
                "Result",
                (
                    HumanField("passed", "Verdict", formatter=_verdict),
                    _NAME,
                    HumanField("data_type", "Data type"),
                    HumanField("score", "Score"),
                    HumanField("value", "Value"),
                    HumanField("explanation", "Explanation"),
                ),
            ),
            HumanSection(
                "Source",
                (
                    HumanField("session_id", "Session ID"),
                    HumanField("evaluator_name", "Evaluator"),
                    HumanField("evaluator_version", "Evaluator version"),
                    HumanField("evaluator_version_id", "Evaluator version ID"),
                ),
            ),
            HumanSection("Identity and timing", (_ID, _CREATED, _UPDATED)),
        ),
    ),
    "worker.list": _view(
        "Workers",
        (
            _STATUS,
            _NAME,
            _ID,
            HumanField("last_seen_at", "Last seen", 105),
            HumanField("scope", "Kinds", 130, _scope),
            HumanField("runtime", "Runtime", 160, _runtime),
        ),
    ),
    "worker.get": _view(
        "Worker",
        (HumanField("live", "Live"), _NAME, _ID),
        (
            HumanSection(
                "Summary",
                (
                    HumanField("live", "Live"),
                    _NAME,
                    _ID,
                    HumanField("last_seen_at", "Last seen"),
                ),
            ),
            HumanSection(
                "Scope", (HumanField("scope", "Task kinds", formatter=_scope),)
            ),
            HumanSection("Runtime", (HumanField("runtime", "Runtime"),)),
            HumanSection("Metadata", (_METADATA,)),
        ),
    ),
    "job.get": _view(
        "Job",
        (_STATUS, _ID),
        (
            HumanSection(
                "Summary",
                (
                    _STATUS,
                    _ID,
                    HumanField("cancel_requested_at", "Cancellation requested"),
                ),
            ),
            HumanSection(
                "Timing",
                (
                    HumanField("started_at", "Started"),
                    HumanField("ended_at", "Ended"),
                    _CREATED,
                    _UPDATED,
                ),
            ),
            HumanSection("Error", (HumanField("error", "Error"),)),
            HumanSection("Tasks", (HumanField("tasks", "Tasks"),)),
        ),
    ),
    "job.watch": _view("Job", (_STATUS, _ID), _JOB_SECTIONS),
    "job.cancel": _view("Job", (_STATUS, _ID), _JOB_SECTIONS),
    "session.import": _view("Import", (), _JOB_SECTIONS),
    "session.evaluate": _view("Evaluation job", (), _JOB_SECTIONS),
    "experiment.run.start": _view("Experiment run", (), _JOB_SECTIONS),
    "experiment.run.watch": _view("Experiment run", (), _JOB_SECTIONS),
    "config.list": _view(
        "Configuration",
        (
            HumanField("key", "Key"),
            HumanField("value", "Value"),
            HumanField("source", "Source"),
        ),
    ),
    "context.list": _view(
        "Contexts",
        (
            _NAME,
            HumanField("server", "Server"),
            HumanField("active", "Active"),
            HumanField("credential_stored", "Credential"),
        ),
    ),
    "schema": _view(
        "Commands",
        (
            HumanField("command|name", "Command"),
            HumanField("description", "Description"),
            HumanField("read_only", "Read only", 140),
        ),
    ),
}


def get_human_view(command: str) -> HumanView | None:
    """Return a curated view for a dotted command path."""
    return _VIEWS.get(command)
