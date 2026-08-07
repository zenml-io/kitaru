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
"""Analytics event property and user trait builders."""

from datetime import datetime
from typing import Any

from kitaru.analytics.events import AccountOrigin
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.annotation import Annotation
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PluginKind, PluginSource
from kitaru.server.domain.replay_config import ReplayOverride
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import EvaluationTask, ImportTask, Task


def _duration_properties(
    started_at: datetime | None, ended_at: datetime | None
) -> dict[str, Any]:
    """Build the duration property, empty when either timestamp is unset.

    Args:
        started_at: Start time.
        ended_at: End time.

    Returns:
        Duration property.
    """
    if started_at is None or ended_at is None:
        return {}
    return {"duration_seconds": (ended_at - started_at).total_seconds()}


def build_account_traits(account: Account, origin: AccountOrigin) -> dict[str, Any]:
    """Build the traits of an account.

    Args:
        account: Account the traits describe.
        origin: Where the account was created.

    Returns:
        User traits.
    """
    traits: dict[str, Any] = {
        "is_service_account": account.is_service_account,
        "account_origin": origin.value,
    }
    if account.email is not None:
        traits["email"] = account.email
    return traits


def build_session_completed_properties(session: Session) -> dict[str, Any]:
    """Build the properties of a session's transition to a terminal status.

    Args:
        session: Session that transitioned to a terminal status.

    Returns:
        Event properties.
    """
    properties: dict[str, Any] = {
        "origin": session.origin.value,
        "status": session.status.value,
        **_duration_properties(session.started_at, session.ended_at),
    }
    if session.tokens is not None:
        if session.tokens.input_tokens is not None:
            properties["input_tokens"] = session.tokens.input_tokens
        if session.tokens.output_tokens is not None:
            properties["output_tokens"] = session.tokens.output_tokens
        if session.tokens.cached_input_tokens is not None:
            properties["cached_input_tokens"] = session.tokens.cached_input_tokens
        if session.tokens.reasoning_tokens is not None:
            properties["reasoning_tokens"] = session.tokens.reasoning_tokens
    return properties


def build_import_completed_properties(task: ImportTask) -> dict[str, Any]:
    """Build the properties of an import task's transition to a terminal status.

    Args:
        task: Import task that transitioned to a terminal status.

    Returns:
        Event properties.
    """
    properties: dict[str, Any] = {
        "status": task.status.value,
        **_duration_properties(task.started_at, task.ended_at),
    }
    if isinstance(task.result, dict) and isinstance(task.result.get("created"), int):
        properties["session_count"] = task.result["created"]
    return properties


def build_evaluation_completed_properties(task: EvaluationTask) -> dict[str, Any]:
    """Build the properties of an evaluation task's transition to a terminal status.

    Args:
        task: Evaluation task that transitioned to a terminal status.

    Returns:
        Event properties.
    """
    return {
        "status": task.status.value,
        **_duration_properties(task.started_at, task.ended_at),
    }


def build_job_completed_properties(job: Job, tasks: list[Task]) -> dict[str, Any]:
    """Build the properties of a job's settlement to a terminal status.

    Args:
        job: Job that settled.
        tasks: Every task of the job, in creation order.

    Returns:
        Event properties.
    """
    return {
        "kind": job.kind.value,
        "status": job.status.value,
        "task_count": len(tasks),
        "task_kinds": sorted({task.kind.value for task in tasks}),
        **_duration_properties(job.started_at, job.ended_at),
    }


def build_replay_created_properties(override: ReplayOverride | None) -> dict[str, Any]:
    """Build the properties naming which override kinds a replay sets.

    Args:
        override: Replay override, if any.

    Returns:
        Event properties.
    """
    return {
        "model_override": override is not None and override.model is not None,
        "system_prompt_override": (
            override is not None and override.system_prompt is not None
        ),
        "prompt_override": override is not None and override.prompt is not None,
        "model_params_override": (
            override is not None and override.model_params is not None
        ),
    }


def build_experiment_created_properties(
    evaluator_count: int, tool_override_count: int
) -> dict[str, Any]:
    """Build the properties of an experiment creation.

    Args:
        evaluator_count: Evaluators attached to the experiment.
        tool_override_count: Tools overridden by the replay config.

    Returns:
        Event properties.
    """
    return {
        "evaluator_count": evaluator_count,
        "tool_override_count": tool_override_count,
    }


def build_experiment_run_completed_properties(
    run: ExperimentRun, replay_count: int
) -> dict[str, Any]:
    """Build the properties of an experiment run's finalization.

    Args:
        run: Finalized experiment run.
        replay_count: Replays of the run.

    Returns:
        Event properties.
    """
    return {
        "status": run.status.value,
        "replay_count": replay_count,
        **_duration_properties(run.started_at, run.ended_at),
    }


def build_cohort_version_created_properties(session_count: int) -> dict[str, Any]:
    """Build the properties of a cohort version creation.

    Args:
        session_count: Sessions in the version.

    Returns:
        Event properties.
    """
    return {"session_count": session_count}


def build_agent_version_created_properties(version: AgentVersion) -> dict[str, Any]:
    """Build the properties of an agent version creation.

    Args:
        version: Created agent version.

    Returns:
        Event properties.
    """
    return {
        "version": version.version,
        "runnable": version.run_spec is not None,
        "tool_count": len(version.capabilities.tools),
        "mcp_server_count": len(version.capabilities.mcp_servers),
        "skill_count": len(version.capabilities.skills),
    }


def build_investigation_created_properties(
    investigation: Investigation,
) -> dict[str, Any]:
    """Build the properties of an investigation creation.

    Args:
        investigation: Created investigation.

    Returns:
        Event properties.
    """
    return {
        "question_count": len(investigation.questions),
        "session_count": investigation.total_sessions,
    }


def build_annotation_created_properties(annotation: Annotation) -> dict[str, Any]:
    """Build the properties of an annotation creation.

    Args:
        annotation: Stored annotation.

    Returns:
        Event properties.
    """
    return {
        "investigation_answer": annotation.investigation_session_id is not None,
        "has_selector": annotation.selector is not None,
    }


def build_plugin_registered_properties(
    kind: PluginKind, source: PluginSource
) -> dict[str, Any]:
    """Build the properties of a plugin version registration.

    Args:
        kind: Plugin kind.
        source: Plugin source.

    Returns:
        Event properties.
    """
    return {"kind": kind.value, "plugin_source": source.type}
