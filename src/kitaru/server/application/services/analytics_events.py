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
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.annotation import Annotation
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginSource
from kitaru.server.domain.replay_config import ReplayConfig
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import EvaluationTask, ImportTask, Task
from kitaru.server.domain.worker import Worker


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
    if account.external_id is not None:
        traits["control_plane_user_id"] = account.external_id
    return traits


def build_account_created_properties(
    account: Account, origin: AccountOrigin
) -> dict[str, Any]:
    """Build the properties of an account creation.

    Args:
        account: Created account.
        origin: Where the account was created.

    Returns:
        Event properties.
    """
    return {
        "account_origin": origin.value,
        "is_service_account": account.is_service_account,
    }


def build_user_enriched_properties(account: Account) -> dict[str, Any]:
    """Build the properties of a user finishing the onboarding survey.

    Args:
        account: Account that finished the survey.

    Returns:
        Event properties.
    """
    properties: dict[str, Any] = {**account.metadata, "name": account.name}
    if account.email is not None:
        properties["email"] = account.email
    return properties


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
        "llm_call_count": session.llm_call_count,
        "tool_call_count": session.tool_call_count,
        **_duration_properties(session.started_at, session.ended_at),
    }
    if session.framework is not None:
        properties["framework"] = session.framework
    if session.adapter_version is not None:
        properties["adapter_version"] = session.adapter_version
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


def _plugin_properties(plugin: Plugin | None) -> dict[str, Any]:
    """Build the plugin properties, empty without a plugin.

    Args:
        plugin: Plugin the task ran.

    Returns:
        Plugin properties.
    """
    if plugin is None:
        return {}
    # Ownerless plugins are the builtin ones, so only their names are safe to
    # report.
    properties: dict[str, Any] = {
        "plugin": plugin.name if plugin.owner_id is None else "custom"
    }
    if plugin.provider is not None:
        properties["provider"] = plugin.provider
    return properties


def build_import_completed_properties(
    task: ImportTask, plugin: Plugin | None
) -> dict[str, Any]:
    """Build the properties of an import task's transition to a terminal status.

    Args:
        task: Import task that transitioned to a terminal status.
        plugin: Importer plugin the task ran.

    Returns:
        Event properties.
    """
    properties: dict[str, Any] = {
        "status": task.status.value,
        **_plugin_properties(plugin),
        **_duration_properties(task.started_at, task.ended_at),
    }
    if isinstance(task.result, dict) and isinstance(task.result.get("created"), int):
        properties["session_count"] = task.result["created"]
    return properties


def build_evaluation_completed_properties(
    task: EvaluationTask, plugin: Plugin | None
) -> dict[str, Any]:
    """Build the properties of an evaluation task's transition to a terminal status.

    Args:
        task: Evaluation task that transitioned to a terminal status.
        plugin: Evaluator plugin the task ran.

    Returns:
        Event properties.
    """
    return {
        "status": task.status.value,
        **_plugin_properties(plugin),
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
        "successful_task_count": sum(
            1 for task in tasks if task.status == TaskStatus.COMPLETED
        ),
        "task_kinds": sorted({task.kind.value for task in tasks}),
        **_duration_properties(job.started_at, job.ended_at),
    }


def _replay_config_properties(config: ReplayConfig) -> dict[str, Any]:
    """Build the properties describing a replay config.

    Args:
        config: Replay config.

    Returns:
        Replay config properties.
    """
    override = config.override
    return {
        "model_override": override is not None and override.model is not None,
        "system_prompt_override": (
            override is not None and override.system_prompt is not None
        ),
        "prompt_override": override is not None and override.prompt is not None,
        "model_params_override": (
            override is not None and override.model_params is not None
        ),
        "tool_policy_default": config.tool_policy.default.type,
        "tool_override_count": len(config.tool_policy.tools),
        "tool_override_types": sorted(
            {tool.type for tool in config.tool_policy.tools.values()}
        ),
        "evaluator_count": len(config.evaluators),
    }


def build_replay_created_properties(config: ReplayConfig) -> dict[str, Any]:
    """Build the properties of a replay creation.

    Args:
        config: Replay config of the replay.

    Returns:
        Event properties.
    """
    return _replay_config_properties(config)


def build_experiment_created_properties(config: ReplayConfig) -> dict[str, Any]:
    """Build the properties of an experiment creation.

    Args:
        config: Replay config of the experiment.

    Returns:
        Event properties.
    """
    return _replay_config_properties(config)


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
    return {"session_count": investigation.total_sessions}


def build_insight_created_properties(insight: Insight) -> dict[str, Any]:
    """Build the properties of an insight creation.

    Args:
        insight: Created insight.

    Returns:
        Event properties.
    """
    return {"insight_type": insight.data.type}


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


def build_worker_registered_properties(worker: Worker) -> dict[str, Any]:
    """Build the properties of a worker registration.

    Args:
        worker: Registered worker.

    Returns:
        Event properties.
    """
    properties = {"worker_platform": worker.runtime.platform}
    if worker_os := worker.runtime.os:
        properties["worker_os"] = worker_os

    return properties


def build_plugin_version_registered_properties(
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
