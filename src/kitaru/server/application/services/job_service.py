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
"""Job use cases."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import SecretStr

from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.secret_repository import (
    SecretRepository,
)
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.worker_repository import (
    WorkerRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import CohortSessionsFilter
from kitaru.server.application.models.jobs import (
    JobFilter,
    JobUpdate,
    ReplayCreate,
    SessionRunCreate,
)
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
)
from kitaru.server.application.services.worker_liveness import (
    warn_if_no_live_worker,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    MissingRunImage,
    NoRunnableAgentVersion,
)
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment_run import (
    TERMINAL_RUN_STATUSES,
    ExperimentRunStatus,
)
from kitaru.server.domain.job import (
    TERMINAL_JOB_STATUSES,
    InvalidJob,
    InvalidJobTransition,
    InvalidToolLookup,
    Job,
    JobActive,
    JobKind,
    JobKindMismatch,
    JobMissingResultSession,
    JobNotStandalone,
    JobSpec,
    JobStatus,
    Replay,
    SessionRun,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    ReplayConfig,
    ScoringResult,
    ToolPolicyConfig,
    effective_inputs,
)
from kitaru.server.domain.replay_diff import (
    ReplayDiff,
    compute_diff_summary,
    compute_replay_diff,
)
from kitaru.server.domain.session import SessionStatus
from kitaru.server.domain.session_node import SessionNode

# Page size for resolving every member session of a cohort.
_MEMBER_RESOLUTION_PAGE_SIZE = 1000


class JobService:
    """Job use cases."""

    def __init__(
        self,
        repository: JobRepository,
        replay_config_repository: ReplayConfigRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
        session_node_repository: SessionNodeRepository,
        experiment_run_repository: ExperimentRunRepository,
        experiment_repository: ExperimentRepository,
        cohort_repository: CohortRepository,
        secret_repository: SecretRepository,
        worker_repository: WorkerRepository,
        heartbeat_timeout_seconds: int,
        max_attempts: int,
        worker_liveness_timeout_seconds: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Job repository.
            replay_config_repository: Replay config repository.
            session_repository: Session repository.
            agent_version_repository: Agent version repository.
            session_node_repository: Session node repository.
            experiment_run_repository: Experiment run repository.
            experiment_repository: Experiment repository.
            cohort_repository: Cohort repository.
            secret_repository: Secret repository.
            worker_repository: Worker repository.
            heartbeat_timeout_seconds: Seconds after which a heartbeat
                counts as lost.
            max_attempts: Attempt count at which a stale job times out.
            worker_liveness_timeout_seconds: Seconds after which a worker
                counts as dead.
        """
        self._repository = repository
        self._replay_config_repository = replay_config_repository
        self._session_repository = session_repository
        self._agent_version_repository = agent_version_repository
        self._session_node_repository = session_node_repository
        self._experiment_run_repository = experiment_run_repository
        self._experiment_repository = experiment_repository
        self._cohort_repository = cohort_repository
        self._secret_repository = secret_repository
        self._worker_repository = worker_repository
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._max_attempts = max_attempts
        self._worker_liveness_timeout_seconds = worker_liveness_timeout_seconds

    def _stale_before(self) -> datetime:
        """Compute the heartbeat staleness threshold.

        Returns:
            Time before which a heartbeat counts as lost.
        """
        return datetime.now(UTC) - timedelta(seconds=self._heartbeat_timeout_seconds)

    async def _resolve_agent_version(
        self, agent_id: uuid.UUID, version_id: uuid.UUID | None
    ) -> AgentVersion:
        """Resolve the agent version a job executes.

        Args:
            agent_id: Id of the original session's agent.
            version_id: Explicit version id, ``None`` resolves the latest
                runnable version.

        Raises:
            NoRunnableAgentVersion: The agent has no runnable version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidJob: The explicit version belongs to another agent.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Resolved agent version.
        """
        if version_id is None:
            version = await self._agent_version_repository.get_latest_runnable(agent_id)
            if version is None:
                raise NoRunnableAgentVersion(agent_id)
            return version
        version = await self._agent_version_repository.get(version_id)
        if version.agent_id != agent_id:
            raise InvalidJob(
                f"Agent version {version_id} does not belong to agent {agent_id}"
            )
        if version.run_spec is None:
            raise AgentVersionNotRunnable(version_id)
        return version

    async def create_replay(
        self, command: ReplayCreate, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig]:
        """Create a standalone replay of one session.

        The inline config is normalized into a replay config row. The tool
        policy defaults to a history policy scoped to the original session.

        Args:
            command: Replay create command.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has the original session id.
            InvalidJob: The original session is in progress or the
                explicit version belongs to another agent.
            InvalidReplayConfig: A history policy scopes to a cohort.
            NoRunnableAgentVersion: The session's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Created job and its replay config.
        """
        session = await self._session_repository.get(command.original_session_id)
        if session.status is SessionStatus.IN_PROGRESS:
            raise InvalidJob(f"Session {session.id} is in progress")
        version = await self._resolve_agent_version(
            session.agent_id, command.agent_version_id
        )
        config = ReplayConfig(
            owner_id=actor.account.id,
            override=command.override,
            tool_policy=command.tool_policy
            or ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=command.scoring_policy,
        )
        config.check_standalone()
        config = await self._replay_config_repository.create(config)
        assert version.run_spec is not None
        job = Replay(
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
            execution_target=version.run_spec.default_execution_target,
        )
        job = await self._repository.create(job)
        assert isinstance(job, Replay)
        return job, config

    async def create_session_run(
        self, command: SessionRunCreate, actor: AuthContext
    ) -> SessionRun:
        """Create a session run of one agent version.

        For a pool target a warning is logged when no live worker serves
        the agent.

        Args:
            command: Session run create command.
            actor: Caller context.

        Raises:
            NoRunnableAgentVersion: The agent has no runnable version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidJob: The explicit version belongs to another agent.
            AgentVersionNotRunnable: The explicit version has no run spec.
            MissingRunImage: An on demand run resolves to a version without
                an image.

        Returns:
            Created session run.
        """
        _ = actor
        if command.agent_id is not None:
            version = await self._resolve_agent_version(
                command.agent_id, command.agent_version_id
            )
        else:
            assert command.agent_version_id is not None
            version = await self._agent_version_repository.get(command.agent_version_id)
            if version.run_spec is None:
                raise AgentVersionNotRunnable(version.id)
        assert version.run_spec is not None
        target = command.execution_target or version.run_spec.default_execution_target
        if target is ExecutionTarget.ON_DEMAND and version.run_spec.image is None:
            raise MissingRunImage(version.id)
        if target is ExecutionTarget.POOL:
            await warn_if_no_live_worker(
                self._worker_repository,
                version.agent_id,
                self._worker_liveness_timeout_seconds,
            )
        job = SessionRun(
            agent_version_id=version.id,
            inputs=command.inputs,
            name=command.name,
            execution_target=target,
        )
        job = await self._repository.create(job)
        assert isinstance(job, SessionRun)
        return job

    async def _config_for(self, job: Job) -> ReplayConfig | None:
        """Load the replay config of a job.

        Args:
            job: Stored job.

        Returns:
            Replay config, ``None`` for session runs.
        """
        if isinstance(job, Replay):
            return await self._replay_config_repository.get(job.replay_config_id)
        return None

    async def _with_configs(
        self, jobs: list[Job]
    ) -> list[tuple[Job, ReplayConfig | None]]:
        """Pair jobs with their replay configs.

        Args:
            jobs: Stored jobs.

        Returns:
            Jobs with their replay configs, ``None`` for session runs.
        """
        configs = await self._replay_config_repository.get_many(
            [job.replay_config_id for job in jobs if isinstance(job, Replay)]
        )
        return [
            (
                job,
                configs[job.replay_config_id] if isinstance(job, Replay) else None,
            )
            for job in jobs
        ]

    async def get_job(
        self, job_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Get a job by id, reporting lost heartbeats as pending.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        job = job.with_staleness(self._stale_before(), self._max_attempts)
        return job, await self._config_for(job)

    async def list_jobs(
        self, job_filter: JobFilter, actor: AuthContext
    ) -> tuple[list[tuple[Job, ReplayConfig | None]], int]:
        """List jobs matching a filter, reporting lost heartbeats.

        Args:
            job_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching jobs with their replay configs and the
            total match count.
        """
        _ = actor
        stale_before = self._stale_before()
        job_filter = job_filter.model_copy(
            update={"stale_before": stale_before, "max_attempts": self._max_attempts}
        )
        jobs, total = await self._repository.query(job_filter)
        jobs = [job.with_staleness(stale_before, self._max_attempts) for job in jobs]
        return await self._with_configs(jobs), total

    async def get_spec(self, job_id: uuid.UUID, actor: AuthContext) -> JobSpec:
        """Resolve the spec a runner executes a job with.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            AgentVersionNotRunnable: The stamped agent version has no run
                spec.

        Returns:
            Resolved job spec.
        """
        _ = actor
        job = await self._repository.get(job_id)
        version = await self._agent_version_repository.get(job.agent_version_id)
        if version.run_spec is None:
            raise AgentVersionNotRunnable(version.id)
        secret_env: dict[str, SecretStr] = {}
        for secret_id in version.run_spec.secret_ids:
            secret = await self._secret_repository.get(secret_id)
            secret_env.update(secret.values)
        if isinstance(job, SessionRun):
            return JobSpec(
                job_id=job.id,
                kind=JobKind.SESSION_RUN,
                inputs=job.inputs,
                run_spec=version.run_spec,
                secret_env=secret_env,
                name=job.name,
            )
        assert isinstance(job, Replay)
        config = await self._replay_config_repository.get(job.replay_config_id)
        session = await self._session_repository.get(job.original_session_id)
        score_baselines = True
        if job.experiment_run_id is not None:
            run = await self._experiment_run_repository.get(job.experiment_run_id)
            score_baselines = run.score_baselines
        return JobSpec(
            job_id=job.id,
            kind=JobKind.REPLAY,
            inputs=effective_inputs(session.inputs, config.override),
            override=config.override,
            tool_policy=config.tool_policy,
            scoring_policy=config.scoring_policy,
            score_baselines=score_baselines,
            run_spec=version.run_spec,
            secret_env=secret_env,
            original_session_id=session.id,
        )

    async def _compute_summary(
        self, job: Replay, scores: dict[str, float]
    ) -> dict[str, Any]:
        """Compute the diff summary stored on a completing job.

        Args:
            job: Job with a linked result session.
            scores: Scores reported by the runner.

        Returns:
            Diff summary.
        """
        assert job.result_session_id is not None
        original = await self._session_repository.get(job.original_session_id)
        result = await self._session_repository.get(job.result_session_id)
        original_nodes = await self._session_node_repository.list_for_session(
            original.id, include_payloads=True
        )
        result_nodes = await self._session_node_repository.list_for_session(
            result.id, include_payloads=True
        )
        return compute_diff_summary(
            scores, original, result, original_nodes, result_nodes
        )

    async def update_job(
        self, job_id: uuid.UUID, command: JobUpdate, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Transition a job through the runner status updates.

        Completing a replay stores the scoring result and the computed
        diff summary. The transition that makes the last job of a run
        terminal also finalizes the run.

        Args:
            job_id: Id of the job.
            command: Job update command.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            InvalidJobTransition: The transition is illegal.
            JobMissingResultSession: Completing without a linked result
                session.
            InvalidJob: Completing a replay without a scoring result,
                completing a session run with one, or failing without an
                error.

        Returns:
            Updated job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if command.status is JobStatus.RUNNING:
            job.start()
        elif command.status is JobStatus.COMPLETED:
            if job.status is not JobStatus.RUNNING:
                raise InvalidJobTransition(job.id, job.status, JobStatus.COMPLETED)
            if job.result_session_id is None:
                raise JobMissingResultSession(job.id)
            if isinstance(job, SessionRun):
                if (
                    command.passed is not None
                    or command.score is not None
                    or command.scores is not None
                ):
                    raise InvalidJob(
                        "Completing a session run rejects passed, score, and scores"
                    )
                job.complete()
            else:
                assert isinstance(job, Replay)
                if (
                    command.passed is None
                    or command.score is None
                    or command.scores is None
                ):
                    raise InvalidJob(
                        "Completing a replay requires passed, score, and scores"
                    )
                diff = await self._compute_summary(job, command.scores)
                job.complete(
                    ScoringResult(
                        passed=command.passed,
                        score=command.score,
                        scores=command.scores,
                    ),
                    diff,
                )
        elif command.status is JobStatus.FAILED:
            if command.error is None:
                raise InvalidJob("Failing a job requires an error")
            job.fail(command.error)
        elif command.status is JobStatus.TIMED_OUT:
            if command.error is None:
                raise InvalidJob("Timing out a job requires an error")
            job.time_out(command.error)
        elif command.status is JobStatus.CANCELED:
            job.cancel()
        else:
            raise InvalidJobTransition(job.id, job.status, command.status)
        config = await self._config_for(job)
        job = await self._repository.update(job)
        if (
            isinstance(job, Replay)
            and job.experiment_run_id is not None
            and job.status in TERMINAL_JOB_STATUSES
        ):
            await finalize_run_if_drained(
                self._experiment_run_repository,
                self._repository,
                self._session_repository,
                job.experiment_run_id,
            )
        return job, config

    async def heartbeat_job(
        self, job_id: uuid.UUID, actor: AuthContext
    ) -> tuple[JobStatus, bool]:
        """Record a worker heartbeat on a job.

        Terminal jobs record nothing and report the stop flag, so the
        worker abandons the job.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobNotActive: The job is pending.

        Returns:
            Job status and whether the worker should stop working on
            the job.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if job.status in TERMINAL_JOB_STATUSES:
            return job.status, True
        job.heartbeat()
        await self._repository.update(job)
        if not isinstance(job, Replay) or job.experiment_run_id is None:
            return job.status, False
        run = await self._experiment_run_repository.get(job.experiment_run_id)
        return job.status, run.status is ExperimentRunStatus.CANCELING

    async def claim_job(
        self, job_id: uuid.UUID, worker_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Claim a standalone job for a worker.

        A stale claim or start is requeued or timed out first, so a job
        whose worker died is claimable again. The claim bumps the
        worker's last seen time.

        Args:
            job_id: Id of the job.
            worker_id: Id of the claiming worker.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            WorkerNotFound: No worker has the claiming worker id.
            JobNotStandalone: The job belongs to an experiment run.
            InvalidJobTransition: The job is not pending after the
                staleness resolution.

        Returns:
            Claimed job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        await self._worker_repository.get(worker_id)
        if not job.standalone:
            raise JobNotStandalone(job.id)
        resolved = job.with_staleness(self._stale_before(), self._max_attempts)
        if resolved is not job:
            resolved = await self._repository.update(resolved)
        resolved.claim(worker_id)
        job = await self._repository.update(resolved)
        await self._worker_repository.touch(worker_id, datetime.now(UTC))
        return job, await self._config_for(job)

    async def claim_jobs(
        self,
        worker_id: uuid.UUID,
        max_jobs: int,
        agent_ids: list[uuid.UUID] | None,
        experiment_run_id: uuid.UUID | None,
        actor: AuthContext,
    ) -> list[tuple[Job, ReplayConfig | None]]:
        """Atomically claim pending jobs within a scope for a worker.

        Stale claimed or running jobs in scope are requeued or timed out
        first, and the claim bumps the worker's last seen time. An
        unscoped claim yields only pool-target work. With an experiment
        run id the first claim moves a pending run to running, canceling
        and terminal runs yield no jobs, and an empty claim finalizes the
        run when every job is already terminal.

        Args:
            worker_id: Id of the claiming worker.
            max_jobs: Maximum number of jobs to claim.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has the claiming worker id.
            ExperimentRunNotFound: No experiment run has the scoped run
                id.

        Returns:
            Claimed jobs with their replay configs.
        """
        _ = actor
        await self._worker_repository.get(worker_id)
        await self._worker_repository.touch(worker_id, datetime.now(UTC))
        run = None
        if experiment_run_id is not None:
            run = await self._experiment_run_repository.get(experiment_run_id)
            if (
                run.status is ExperimentRunStatus.CANCELING
                or run.status in TERMINAL_RUN_STATUSES
            ):
                return []
        await self._repository.requeue_stale(
            self._stale_before(),
            self._max_attempts,
            agent_ids=agent_ids,
            experiment_run_id=experiment_run_id,
        )
        jobs = await self._repository.claim_pending(
            worker_id,
            max_jobs,
            agent_ids=agent_ids,
            experiment_run_id=experiment_run_id,
        )
        if run is not None:
            if jobs and run.status is ExperimentRunStatus.PENDING:
                run.start()
                await self._experiment_run_repository.update(run)
            if not jobs:
                # The requeue may have timed out the run's last job, which
                # leaves no transition that would finalize the run.
                await finalize_run_if_drained(
                    self._experiment_run_repository,
                    self._repository,
                    self._session_repository,
                    run.id,
                )
        return await self._with_configs(jobs)

    async def release_job(
        self, job_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Requeue a claimed or running job for another attempt.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            InvalidJobTransition: The job is not claimed or running.

        Returns:
            Requeued job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        job.requeue()
        job = await self._repository.update(job)
        return job, await self._config_for(job)

    async def retry_job(
        self, job_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Requeue a finished standalone job for another attempt.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobNotStandalone: The job belongs to an experiment run.
            InvalidJobTransition: The job is not failed, timed out,
                or canceled.

        Returns:
            Requeued job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if not job.standalone:
            raise JobNotStandalone(job.id)
        job = job.with_staleness(self._stale_before(), self._max_attempts)
        job.retry()
        job = await self._repository.update(job)
        return job, await self._config_for(job)

    async def delete_job(self, job_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a standalone job.

        Deletes the job's config when nothing else references it.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobNotStandalone: The job belongs to an experiment run.
            JobActive: The job is claimed or running.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if not job.standalone:
            raise JobNotStandalone(job.id)
        resolved = job.with_staleness(self._stale_before(), self._max_attempts)
        if resolved.status in (JobStatus.CLAIMED, JobStatus.RUNNING):
            raise JobActive(job.id)
        await self._repository.delete(job.id)
        if isinstance(job, Replay):
            await self._replay_config_repository.delete_if_unreferenced(
                job.replay_config_id
            )

    async def _resolve_cohort_session_ids(
        self, cohort_id: uuid.UUID
    ) -> list[uuid.UUID]:
        """Resolve every member session id of a cohort across all pages.

        Args:
            cohort_id: Id of the cohort.

        Returns:
            Member session ids in position order.
        """
        session_ids: list[uuid.UUID] = []
        page = 1
        while True:
            batch, total = await self._cohort_repository.query_sessions(
                cohort_id,
                CohortSessionsFilter(page=page, page_size=_MEMBER_RESOLUTION_PAGE_SIZE),
            )
            session_ids.extend(session.id for session in batch)
            if len(session_ids) >= total or not batch:
                return session_ids
            page += 1

    async def tool_lookup(
        self,
        job_id: uuid.UUID,
        tool_name: str,
        inputs: Any,
        cache_key: str,
        actor: AuthContext,
    ) -> SessionNode | None:
        """Resolve a history tool policy lookup within its scope.

        Args:
            job_id: Id of the job.
            tool_name: Name of the called tool.
            inputs: Tool call inputs.
            cache_key: Cache key claimed by the caller.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobKindMismatch: The job is a session run.
            InvalidToolLookup: The cache key does not match or the tool
                resolves to no history policy.
            InvalidReplayConfig: A standalone job scopes to a cohort.

        Returns:
            Most recent matching tool call node, ``None`` on a miss.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if not isinstance(job, Replay):
            raise JobKindMismatch(job.id, JobKind.REPLAY)
        if tool_call_cache_key(tool_name, inputs) != cache_key:
            raise InvalidToolLookup("Cache key does not match the tool name and inputs")
        config = await self._replay_config_repository.get(job.replay_config_id)
        policy = config.tool_policy.tools.get(tool_name, config.tool_policy.default)
        if not isinstance(policy, HistoryPolicy):
            raise InvalidToolLookup(f"Tool '{tool_name}' resolves to no history policy")
        if policy.scope is HistoryScope.ORIGINAL_SESSION:
            return await self._session_node_repository.find_tool_result(
                cache_key,
                session_ids=[job.original_session_id],
                agent_id=None,
            )
        if policy.scope is HistoryScope.COHORT:
            if job.experiment_run_id is None:
                raise InvalidReplayConfig(
                    "Standalone replays cannot use history scope 'cohort'"
                )
            run = await self._experiment_run_repository.get(job.experiment_run_id)
            experiment = await self._experiment_repository.get(run.experiment_id)
            session_ids = await self._resolve_cohort_session_ids(experiment.cohort_id)
            return await self._session_node_repository.find_tool_result(
                cache_key, session_ids=session_ids, agent_id=None
            )
        session = await self._session_repository.get(job.original_session_id)
        return await self._session_node_repository.find_tool_result(
            cache_key, session_ids=None, agent_id=session.agent_id
        )

    async def compute_diff(self, job_id: uuid.UUID, actor: AuthContext) -> ReplayDiff:
        """Compute the full diff between a job's sessions.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobKindMismatch: The job is a session run.
            JobMissingResultSession: The job has no result session.

        Returns:
            Full replay diff.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if not isinstance(job, Replay):
            raise JobKindMismatch(job.id, JobKind.REPLAY)
        if job.result_session_id is None:
            raise JobMissingResultSession(job.id)
        config = await self._replay_config_repository.get(job.replay_config_id)
        original = await self._session_repository.get(job.original_session_id)
        result = await self._session_repository.get(job.result_session_id)
        original_nodes = await self._session_node_repository.list_for_session(
            original.id, include_payloads=True
        )
        result_nodes = await self._session_node_repository.list_for_session(
            result.id, include_payloads=True
        )
        return compute_replay_diff(
            job, config.override, original, result, original_nodes, result_nodes
        )
