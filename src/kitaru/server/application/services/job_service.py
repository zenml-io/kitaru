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
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Generic, NamedTuple, TypeVar

from pydantic import SecretStr

from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.interfaces.agent_repository import (
    AgentRepository,
)
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import (
    BlobRepository,
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
from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
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
    ImportCreate,
    JobFilter,
    JobUpdate,
    ReplayCreate,
    SessionRunCreate,
)
from kitaru.server.application.services.plugin_resolution import (
    resolve_plugin,
)
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
)
from kitaru.server.application.services.scorer_resolution import (
    resolve_registry_scorer,
    validate_scoring_policy,
)
from kitaru.server.application.services.worker_liveness import (
    warn_if_no_live_worker,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
    AgentVersionNotRunnable,
    MissingRunImage,
    NoRunnableAgentVersion,
    RunSpec,
)
from kitaru.server.domain.base import DomainError
from kitaru.server.domain.blob import BlobNotFound
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment_run import (
    TERMINAL_RUN_STATUSES,
    ExperimentRunStatus,
)
from kitaru.server.domain.job import (
    TERMINAL_JOB_STATUSES,
    Import,
    ImporterSpec,
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
    PayloadSpec,
    PluginSpec,
    Replay,
    Score,
    ScorerSpec,
    SessionRun,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginVersion,
    PluginVersionIdNotFound,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    InvalidReplayConfig,
    RegistryScorerConfig,
    ReplayConfig,
    ReplayConfigNotFound,
    ScorerConfig,
    ToolPolicyConfig,
    effective_inputs,
)
from kitaru.server.domain.replay_diff import (
    ReplayDiff,
    compute_diff_summary,
    compute_replay_diff,
)
from kitaru.server.domain.secret import Secret, SecretNotFound
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionProvider,
    SessionStatus,
)
from kitaru.server.domain.session_node import SessionNode

# Page size for resolving every member session of a cohort.
_MEMBER_RESOLUTION_PAGE_SIZE = 1000

# Entries each referent cache holds before evicting the oldest.
_REFERENT_CACHE_SIZE = 1024

T = TypeVar("T")


class _ReferentCache(Generic[T]):
    """Bounded cache of spec referents by id."""

    def __init__(self, max_entries: int) -> None:
        """Initialize the cache.

        Args:
            max_entries: Entries held before the oldest is evicted.
        """
        self._entries: OrderedDict[uuid.UUID, T] = OrderedDict()
        self._max_entries = max_entries

    def take(
        self, entry_ids: set[uuid.UUID]
    ) -> tuple[dict[uuid.UUID, T], list[uuid.UUID]]:
        """Split requested ids into cached entries and ids still to load.

        Args:
            entry_ids: Ids of the requested entries.

        Returns:
            Cached entries keyed by id and the ids left to load.
        """
        cached: dict[uuid.UUID, T] = {}
        missing: list[uuid.UUID] = []
        for entry_id in entry_ids:
            entry = self._entries.get(entry_id)
            if entry is None:
                missing.append(entry_id)
            else:
                cached[entry_id] = entry
        return cached, missing

    def put(self, entries: Mapping[uuid.UUID, T]) -> None:
        """Store entries, evicting the oldest beyond the maximum.

        Args:
            entries: Entries keyed by id.
        """
        self._entries.update(entries)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


# Referents cached across requests. Agent versions freeze their run spec
# once a job references them, plugin versions are append-only, and blob
# content is addressed by its hash.
_AGENT_VERSION_CACHE: _ReferentCache[AgentVersion] = _ReferentCache(
    _REFERENT_CACHE_SIZE
)
_PLUGIN_VERSION_CACHE: _ReferentCache[PluginVersion] = _ReferentCache(
    _REFERENT_CACHE_SIZE
)
_PLUGIN_CACHE: _ReferentCache[Plugin] = _ReferentCache(_REFERENT_CACHE_SIZE)
_BLOB_HASH_CACHE: _ReferentCache[str] = _ReferentCache(_REFERENT_CACHE_SIZE)


async def _cached(
    cache: _ReferentCache[T],
    entry_ids: set[uuid.UUID],
    loader: Callable[[list[uuid.UUID]], Awaitable[dict[uuid.UUID, T]]],
) -> dict[uuid.UUID, T]:
    """Resolve entries from a cache, loading and caching the rest.

    Args:
        cache: Cache holding the entries.
        entry_ids: Ids of the requested entries.
        loader: Batch loader for the ids the cache misses.

    Returns:
        Resolved entries keyed by id, missing ids omitted.
    """
    resolved, missing = cache.take(entry_ids)
    if missing:
        loaded = await loader(missing)
        cache.put(loaded)
        resolved.update(loaded)
    return resolved


class _SpecReferents(NamedTuple):
    """Preloaded referents of a job spec batch."""

    agent_versions: dict[uuid.UUID, AgentVersion]
    replay_configs: dict[uuid.UUID, ReplayConfig]
    sessions: dict[uuid.UUID, Session]
    secrets: dict[uuid.UUID, Secret]
    plugin_versions: dict[uuid.UUID, PluginVersion]
    plugins: dict[uuid.UUID, Plugin]
    blob_hashes: dict[uuid.UUID, str]


def _importer_provider(plugin: Plugin) -> SessionProvider:
    """Resolve the session provider an importer plugin writes.

    Args:
        plugin: Importer plugin.

    Raises:
        InvalidJob: The plugin carries no provider or one no session can
            carry.

    Returns:
        Session provider.
    """
    if plugin.provider is None:
        raise InvalidJob(f"Importer '{plugin.name}' carries no provider")
    try:
        return SessionProvider(plugin.provider)
    except ValueError as exc:
        raise InvalidJob(
            f"Importer '{plugin.name}' reads from unknown provider '{plugin.provider}'"
        ) from exc


def _settled_score_parent(job: Job) -> uuid.UUID | None:
    """Return the replay a terminal score job settles.

    Args:
        job: Stored job.

    Returns:
        Id of the parent replay, ``None`` when nothing settles.
    """
    if (
        isinstance(job, Score)
        and job.parent_job_id is not None
        and job.status in TERMINAL_JOB_STATUSES
    ):
        return job.parent_job_id
    return None


def _run_environment(
    version_id: uuid.UUID, referents: _SpecReferents
) -> tuple[RunSpec, dict[str, SecretStr]]:
    """Resolve the run spec and secret environment of an agent version.

    Args:
        version_id: Id of the agent version.
        referents: Preloaded referents.

    Raises:
        AgentVersionNotFound: No agent version has this id.
        AgentVersionNotRunnable: The agent version has no run spec.
        SecretNotFound: No secret has a run spec secret id.

    Returns:
        Run spec and resolved secret environment.
    """
    version = referents.agent_versions.get(version_id)
    if version is None:
        raise AgentVersionNotFound(version_id)
    if version.run_spec is None:
        raise AgentVersionNotRunnable(version.id)
    secret_env: dict[str, SecretStr] = {}
    for secret_id in version.run_spec.secret_ids:
        secret = referents.secrets.get(secret_id)
        if secret is None:
            raise SecretNotFound(secret_id)
        secret_env.update(secret.values)
    return version.run_spec, secret_env


def _plugin_spec(version: PluginVersion, referents: _SpecReferents) -> PluginSpec:
    """Resolve the code reference of a plugin version.

    Args:
        version: Pinned plugin version.
        referents: Preloaded referents.

    Raises:
        BlobNotFound: No blob has the plugin version's blob id.

    Returns:
        Plugin spec.
    """
    sha256 = referents.blob_hashes.get(version.blob_id)
    if sha256 is None:
        raise BlobNotFound(version.blob_id)
    return PluginSpec(
        format=version.format,
        entrypoint=version.entrypoint,
        blob_id=version.blob_id,
        sha256=sha256,
    )


def _score_spec(job: Score, referents: _SpecReferents) -> JobSpec:
    """Build the spec a worker runs a score job with.

    Args:
        job: Stored score job.
        referents: Preloaded referents.

    Raises:
        PluginVersionIdNotFound: No plugin version has the pinned id.
        BlobNotFound: No blob has the plugin version's blob id.
        AgentVersionNotFound: No agent version has the stamped id.
        AgentVersionNotRunnable: The stamped agent version has no run
            spec.

    Returns:
        Resolved job spec.
    """
    plugin = None
    run_spec = None
    secret_env: dict[str, SecretStr] = {}
    if job.plugin_version_id is not None:
        version = referents.plugin_versions.get(job.plugin_version_id)
        if version is None:
            raise PluginVersionIdNotFound(job.plugin_version_id)
        plugin = _plugin_spec(version, referents)
    else:
        assert job.agent_version_id is not None
        run_spec, secret_env = _run_environment(job.agent_version_id, referents)
    return JobSpec(
        job_id=job.id,
        kind=JobKind.SCORE,
        scorer=ScorerSpec(
            config=job.scorer_config,
            plugin=plugin,
            input_session_id=job.input_session_id,
        ),
        run_spec=run_spec,
        secret_env=secret_env,
        input_session_id=job.input_session_id,
    )


def _import_spec(job: Import, referents: _SpecReferents) -> JobSpec:
    """Build the spec a worker runs an import job with.

    Args:
        job: Stored import job.
        referents: Preloaded referents.

    Raises:
        PluginVersionIdNotFound: No plugin version has the pinned id.
        PluginNotFound: No plugin has the pinned version's plugin id.
        InvalidJob: The importer reads from no provider an imported
            session can carry.
        BlobNotFound: No blob has the plugin version's blob id or the
            payload blob id.

    Returns:
        Resolved job spec.
    """
    version = referents.plugin_versions.get(job.plugin_version_id)
    if version is None:
        raise PluginVersionIdNotFound(job.plugin_version_id)
    plugin = referents.plugins.get(version.plugin_id)
    if plugin is None:
        raise PluginNotFound(version.plugin_id)
    payload_sha256 = referents.blob_hashes.get(job.payload_blob_id)
    if payload_sha256 is None:
        raise BlobNotFound(job.payload_blob_id)
    return JobSpec(
        job_id=job.id,
        kind=JobKind.IMPORT,
        importer=ImporterSpec(
            plugin=_plugin_spec(version, referents),
            payload=PayloadSpec(blob_id=job.payload_blob_id, sha256=payload_sha256),
            provider=_importer_provider(plugin),
            agent_id=job.agent_id,
            params=job.inputs or {},
        ),
    )


def _session_run_spec(job: SessionRun, referents: _SpecReferents) -> JobSpec:
    """Build the spec a worker runs a session run with.

    Args:
        job: Stored session run.
        referents: Preloaded referents.

    Raises:
        AgentVersionNotFound: No agent version has the stamped id.
        AgentVersionNotRunnable: The stamped agent version has no run
            spec.

    Returns:
        Resolved job spec.
    """
    assert job.agent_version_id is not None
    run_spec, secret_env = _run_environment(job.agent_version_id, referents)
    return JobSpec(
        job_id=job.id,
        kind=JobKind.SESSION_RUN,
        inputs=job.inputs,
        run_spec=run_spec,
        secret_env=secret_env,
        name=job.name,
    )


def _replay_spec(job: Replay, referents: _SpecReferents) -> JobSpec:
    """Build the spec a worker runs a replay with.

    Args:
        job: Stored replay.
        referents: Preloaded referents.

    Raises:
        AgentVersionNotFound: No agent version has the stamped id.
        AgentVersionNotRunnable: The stamped agent version has no run
            spec.
        ReplayConfigNotFound: No replay config has the stamped id.
        SessionNotFound: No session has the input session id.

    Returns:
        Resolved job spec.
    """
    assert job.agent_version_id is not None
    run_spec, secret_env = _run_environment(job.agent_version_id, referents)
    config = referents.replay_configs.get(job.replay_config_id)
    if config is None:
        raise ReplayConfigNotFound(job.replay_config_id)
    session = referents.sessions.get(job.input_session_id)
    if session is None:
        raise SessionNotFound(job.input_session_id)
    return JobSpec(
        job_id=job.id,
        kind=JobKind.REPLAY,
        inputs=effective_inputs(session.inputs, config.override),
        override=config.override,
        tool_policy=config.tool_policy,
        run_spec=run_spec,
        secret_env=secret_env,
        input_session_id=session.id,
    )


def _build_spec(job: Job, referents: _SpecReferents) -> JobSpec:
    """Build the spec a runner executes a job with from preloaded referents.

    Args:
        job: Stored job.
        referents: Preloaded referents.

    Raises:
        DomainError: A referent of the job no longer resolves.

    Returns:
        Resolved job spec.
    """
    if isinstance(job, Score):
        return _score_spec(job, referents)
    if isinstance(job, Import):
        return _import_spec(job, referents)
    if isinstance(job, SessionRun):
        return _session_run_spec(job, referents)
    assert isinstance(job, Replay)
    return _replay_spec(job, referents)


class JobService:
    """Job use cases."""

    def __init__(
        self,
        repository: JobRepository,
        replay_config_repository: ReplayConfigRepository,
        session_repository: SessionRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        session_node_repository: SessionNodeRepository,
        experiment_run_repository: ExperimentRunRepository,
        experiment_repository: ExperimentRepository,
        cohort_repository: CohortRepository,
        secret_repository: SecretRepository,
        worker_repository: WorkerRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        heartbeat_timeout_seconds: int,
        max_attempts: int,
        worker_liveness_timeout_seconds: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Job repository.
            replay_config_repository: Replay config repository.
            session_repository: Session repository.
            agent_repository: Agent repository.
            agent_version_repository: Agent version repository.
            session_node_repository: Session node repository.
            experiment_run_repository: Experiment run repository.
            experiment_repository: Experiment repository.
            cohort_repository: Cohort repository.
            secret_repository: Secret repository.
            worker_repository: Worker repository.
            plugin_repository: Plugin repository.
            blob_repository: Blob repository.
            heartbeat_timeout_seconds: Seconds after which a heartbeat
                counts as lost.
            max_attempts: Attempt count at which a stale job times out.
            worker_liveness_timeout_seconds: Seconds after which a worker
                counts as dead.
        """
        self._repository = repository
        self._replay_config_repository = replay_config_repository
        self._session_repository = session_repository
        self._agent_repository = agent_repository
        self._agent_version_repository = agent_version_repository
        self._session_node_repository = session_node_repository
        self._experiment_run_repository = experiment_run_repository
        self._experiment_repository = experiment_repository
        self._cohort_repository = cohort_repository
        self._secret_repository = secret_repository
        self._worker_repository = worker_repository
        self._plugin_repository = plugin_repository
        self._blob_repository = blob_repository
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
            SessionNotFound: No session has the input session id.
            InvalidJob: The input session is in progress or the explicit
                version belongs to another agent.
            InvalidReplayConfig: A history policy scopes to a cohort.
            NoRunnableAgentVersion: The session's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            AgentVersionNotRunnable: The explicit version has no run spec.
            PluginNameNotFound: No scorer has a configured name.
            PluginVersionNotFound: A scorer has no configured version.

        Returns:
            Created job and its replay config.
        """
        session = await self._session_repository.get(command.input_session_id)
        if session.status is SessionStatus.IN_PROGRESS:
            raise InvalidJob(f"Session {session.id} is in progress")
        version = await self._resolve_agent_version(
            session.agent_id, command.agent_version_id
        )
        await validate_scoring_policy(self._plugin_repository, command.scoring_policy)
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
            input_session_id=session.id,
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

    async def create_import(self, command: ImportCreate, actor: AuthContext) -> Import:
        """Create an import of one payload blob.

        The named importer resolves to a plugin version that the job
        pins, so a later registration does not change what runs.

        Args:
            command: Import create command.
            actor: Caller context.

        Raises:
            PluginNameNotFound: No importer has the configured name.
            PluginVersionNotFound: The importer has no such version.
            InvalidJob: The importer reads from no provider an imported
                session can carry.
            AgentNotFound: No agent has the configured id.
            BlobNotFound: No blob has the payload blob id.

        Returns:
            Created import.
        """
        _ = actor
        plugin, version = await resolve_plugin(
            self._plugin_repository,
            PluginKind.IMPORTER,
            command.importer,
            command.version,
        )
        _importer_provider(plugin)
        agent = await self._agent_repository.get(command.agent_id)
        if not await self._blob_repository.get_hashes([command.payload_blob_id]):
            raise BlobNotFound(command.payload_blob_id)
        job = Import(
            plugin_version_id=version.id,
            payload_blob_id=command.payload_blob_id,
            agent_id=agent.id,
            inputs=command.params,
            execution_target=ExecutionTarget.POOL,
        )
        job = await self._repository.create(job)
        assert isinstance(job, Import)
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

    async def _load_referents(self, jobs: list[Job]) -> _SpecReferents:
        """Preload every row the specs of a job batch read.

        One query per related table serves the whole batch. Referents that
        cannot change while a job references them come from the process
        caches.

        Args:
            jobs: Stored jobs.

        Returns:
            Preloaded referents.
        """
        agent_versions = await _cached(
            _AGENT_VERSION_CACHE,
            {job.agent_version_id for job in jobs if job.agent_version_id is not None},
            self._agent_version_repository.get_many,
        )
        plugin_versions = await _cached(
            _PLUGIN_VERSION_CACHE,
            {
                job.plugin_version_id
                for job in jobs
                if isinstance(job, Score | Import) and job.plugin_version_id is not None
            },
            self._plugin_repository.get_versions_by_ids,
        )
        replay_configs = await self._replay_config_repository.get_many(
            [job.replay_config_id for job in jobs if isinstance(job, Replay)]
        )
        sessions = await self._session_repository.get_many(
            [job.input_session_id for job in jobs if isinstance(job, Replay)]
        )
        secret_ids: set[uuid.UUID] = set()
        for version in agent_versions.values():
            if version.run_spec is not None:
                secret_ids.update(version.run_spec.secret_ids)
        secrets = await self._secret_repository.get_many(list(secret_ids))
        plugin_ids = {
            plugin_versions[job.plugin_version_id].plugin_id
            for job in jobs
            if isinstance(job, Import) and job.plugin_version_id in plugin_versions
        }
        plugins = await _cached(
            _PLUGIN_CACHE, plugin_ids, self._plugin_repository.get_many
        )
        blob_ids = {version.blob_id for version in plugin_versions.values()}
        blob_ids.update(job.payload_blob_id for job in jobs if isinstance(job, Import))
        blob_hashes = await _cached(
            _BLOB_HASH_CACHE, blob_ids, self._blob_repository.get_hashes
        )
        return _SpecReferents(
            agent_versions=agent_versions,
            replay_configs=replay_configs,
            sessions=sessions,
            secrets=secrets,
            plugin_versions=plugin_versions,
            plugins=plugins,
            blob_hashes=blob_hashes,
        )

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
        return _build_spec(job, await self._load_referents([job]))

    async def _compute_summary(
        self, job: Replay, scores: dict[str, float]
    ) -> dict[str, Any]:
        """Compute the diff summary stored on a completing job.

        Args:
            job: Job with a linked result session.
            scores: Scores of the replayed session.

        Returns:
            Diff summary.
        """
        assert job.result_session_id is not None
        original = await self._session_repository.get(job.input_session_id)
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

    async def _score_baselines(self, job: Replay) -> bool:
        """Report whether a replay also scores its input session.

        Args:
            job: Stored replay.

        Returns:
            The run's flag for run replays, ``True`` for standalone ones.
        """
        if job.experiment_run_id is None:
            return True
        run = await self._experiment_run_repository.get(job.experiment_run_id)
        return run.score_baselines

    def _score_child(
        self,
        job: Replay,
        config: ScorerConfig,
        version: PluginVersion | None,
        input_session_id: uuid.UUID,
    ) -> Score:
        """Build one score job of a replay's fan-out.

        Args:
            job: Replay handing over to its score jobs.
            config: Scorer configuration.
            version: Resolved plugin version, ``None`` for source
                scorers.
            input_session_id: Id of the session to score.

        Returns:
            Score job.
        """
        if version is None:
            return Score(
                parent_job_id=job.id,
                input_session_id=input_session_id,
                scorer_config=config,
                agent_version_id=job.agent_version_id,
                execution_target=job.execution_target,
            )
        return Score(
            parent_job_id=job.id,
            input_session_id=input_session_id,
            plugin_version_id=version.id,
            scorer_config=config.model_copy(update={"version": version.version}),
            execution_target=job.execution_target,
        )

    async def _fan_out_scores(self, job: Replay, config: ReplayConfig) -> None:
        """Create one score job per scorer of a replay's policy.

        Registry scorers pin the resolved plugin version. Scorers missing
        from the input session's scores also get a baseline score job on
        that session when the run asks for baselines.

        Args:
            job: Replay that entered scoring.
            config: Replay config of the job.

        Raises:
            PluginNameNotFound: No scorer has a configured name.
            PluginVersionNotFound: A scorer has no configured version.
        """
        assert job.result_session_id is not None
        versions: dict[str, PluginVersion | None] = {}
        for scorer in config.scoring_policy.scorers:
            versions[scorer.name] = (
                await resolve_registry_scorer(self._plugin_repository, scorer)
                if isinstance(scorer, RegistryScorerConfig)
                else None
            )
        children: list[Job] = [
            self._score_child(job, scorer, versions[scorer.name], job.result_session_id)
            for scorer in config.scoring_policy.scorers
        ]
        if await self._score_baselines(job):
            original = await self._session_repository.get(job.input_session_id)
            children.extend(
                self._score_child(job, scorer, versions[scorer.name], original.id)
                for scorer in config.scoring_policy.scorers
                if scorer.name not in original.scores
            )
        await self._repository.create_many(children)

    async def _cancel_children(self, parent_job_id: uuid.UUID) -> None:
        """Cancel every non-terminal job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.
        """
        for child in await self._repository.list_children(parent_job_id):
            if child.status not in TERMINAL_JOB_STATUSES:
                child.cancel()
                await self._repository.update(child)

    async def _merge_scores(
        self, session_id: uuid.UUID, scores: dict[str, float]
    ) -> None:
        """Merge scorer results into a session's scores map.

        Args:
            session_id: Id of the session.
            scores: Score values by scorer name.
        """
        if not scores:
            return
        session = await self._session_repository.get(session_id)
        session.merge_scores(scores)
        await self._session_repository.update(session)

    async def _aggregate_scores(self, parent_job_id: uuid.UUID) -> None:
        """Settle a scoring replay once its score jobs went terminal.

        A terminally failed score job fails the replay and cancels its
        siblings. All scores in, the baselines merge into the input
        session, the replay scores merge into the result session, and the
        policy verdict plus the diff summary land on the replay.

        Args:
            parent_job_id: Id of the parent replay.

        Raises:
            InvalidReplayConfig: The policy has a total scorer weight of
                0.
        """
        # Why: two children going terminal in overlapping transactions
        # would each read the other as non-terminal and neither would
        # settle the parent. The parent row lock serializes them, the
        # waiter re-reads the children after the holder commits.
        job = await self._repository.get(parent_job_id, for_update=True)
        if not isinstance(job, Replay) or job.status is not JobStatus.SCORING:
            return
        children = [
            child
            for child in await self._repository.list_children(parent_job_id)
            if isinstance(child, Score)
        ]
        failed = next(
            (
                child
                for child in children
                if child.status
                in (JobStatus.FAILED, JobStatus.TIMED_OUT, JobStatus.CANCELED)
            ),
            None,
        )
        if failed is not None:
            await self._cancel_children(parent_job_id)
            job.fail(f"Scorer '{failed.scorer_config.name}' did not complete")
        elif all(child.status is JobStatus.COMPLETED for child in children):
            assert job.result_session_id is not None
            baselines: dict[str, float] = {}
            replay_scores: dict[str, float] = {}
            for child in children:
                assert child.score is not None
                if child.input_session_id == job.result_session_id:
                    replay_scores[child.scorer_config.name] = child.score
                else:
                    baselines[child.scorer_config.name] = child.score
            await self._merge_scores(job.input_session_id, baselines)
            await self._merge_scores(job.result_session_id, replay_scores)
            config = await self._replay_config_repository.get(job.replay_config_id)
            diff = await self._compute_summary(job, replay_scores)
            job.complete(config.scoring_policy.evaluate(replay_scores), diff)
        else:
            return
        job = await self._repository.update(job)
        assert isinstance(job, Replay)
        if job.experiment_run_id is not None:
            await finalize_run_if_drained(
                self._experiment_run_repository,
                self._repository,
                self._session_repository,
                job.experiment_run_id,
            )

    async def _settle_score(self, job: Job) -> None:
        """Settle the replay of a job that went terminal as a score job.

        Args:
            job: Stored job.

        Raises:
            InvalidReplayConfig: The policy has a total scorer weight of
                0.
        """
        parent_job_id = _settled_score_parent(job)
        if parent_job_id is not None:
            await self._aggregate_scores(parent_job_id)

    async def _apply_status(self, job: Job, command: JobUpdate) -> None:
        """Apply the requested status transition to a job.

        Args:
            job: Stored job.
            command: Job update command.

        Raises:
            InvalidJobTransition: The transition is illegal.
            JobKindMismatch: Only a replay enters scoring.
            InvalidJob: Failing without an error, or completing a replay.
        """
        if command.status is JobStatus.RUNNING:
            job.start()
        elif command.status is JobStatus.SCORING:
            if not isinstance(job, Replay):
                raise JobKindMismatch(job.id, JobKind.REPLAY)
            job.enter_scoring()
        elif command.status is JobStatus.COMPLETED:
            if not isinstance(job, SessionRun | Score | Import):
                raise InvalidJob("Replays complete once their score jobs finish")
            job.complete()
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
            assert command.status is not None
            raise InvalidJobTransition(job.id, job.status, command.status)

    async def update_job(
        self, job_id: uuid.UUID, command: JobUpdate, actor: AuthContext
    ) -> tuple[Job, ReplayConfig | None]:
        """Transition a job through the runner status updates.

        A replay entering scoring fans out one score job per scorer, and
        each score job going terminal settles its replay. The transition
        that makes the last job of a run terminal also finalizes the run.

        Args:
            job_id: Id of the job.
            command: Job update command.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            InvalidJobTransition: The transition is illegal.
            JobKindMismatch: Only a replay enters scoring.
            JobMissingResultSession: Entering scoring or completing
                without a linked result session.
            JobMissingScore: Completing a score job without a score.
            JobMissingStats: Completing an import job without stats.
            InvalidJob: Recording a score or stats on another kind,
                completing a replay, failing without an error, or
                updating none of the status, score, and stats.

        Returns:
            Updated job and its replay config.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if command.score is not None:
            if not isinstance(job, Score):
                raise InvalidJob("Only score jobs record a score")
            job.record_score(command.score)
        elif command.stats is not None:
            if not isinstance(job, Import):
                raise InvalidJob("Only import jobs record stats")
            job.record_stats(command.stats)
        elif command.status is None:
            raise InvalidJob("Updating a job requires a status, a score, or stats")
        scoring = job.status is JobStatus.SCORING
        if command.status is not None:
            await self._apply_status(job, command)
        config = await self._config_for(job)
        job = await self._repository.update(job)
        if isinstance(job, Replay):
            if job.status is JobStatus.SCORING:
                assert config is not None
                await self._fan_out_scores(job, config)
            elif scoring and job.status is JobStatus.CANCELED:
                await self._cancel_children(job.id)
        await self._settle_score(job)
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

    async def heartbeat_worker(
        self, worker_id: uuid.UUID, job_ids: list[uuid.UUID], actor: AuthContext
    ) -> list[uuid.UUID]:
        """Record one worker heartbeat on the jobs it reports as in flight.

        The heartbeat reaches only claimed or running jobs the worker owns
        and bumps the worker's last seen time.

        Args:
            worker_id: Id of the heartbeating worker.
            job_ids: Ids of the jobs the worker reports.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has this id.

        Returns:
            Reported job ids the worker should stop working on.
        """
        _ = actor
        await self._worker_repository.get(worker_id)
        now = datetime.now(UTC)
        reached = await self._repository.heartbeat_many(worker_id, job_ids, now)
        await self._worker_repository.touch(worker_id, now)
        canceling: set[uuid.UUID] = set()
        run_ids = {
            job.experiment_run_id
            for job in reached
            if isinstance(job, Replay) and job.experiment_run_id is not None
        }
        for run_id in run_ids:
            run = await self._experiment_run_repository.get(run_id)
            if run.status is not ExperimentRunStatus.CANCELING:
                continue
            canceling.update(
                job.id
                for job in reached
                if isinstance(job, Replay) and job.experiment_run_id == run_id
            )
        reached_ids = {job.id for job in reached}
        return [
            job_id
            for job_id in job_ids
            if job_id not in reached_ids or job_id in canceling
        ]

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
        parent_job_id: uuid.UUID | None,
        actor: AuthContext,
    ) -> list[tuple[Job, ReplayConfig | None, JobSpec]]:
        """Atomically claim pending jobs within a scope for a worker.

        Stale claimed or running jobs in scope are requeued or timed out
        first, and a score job timing out settles its replay. The claim
        bumps the worker's last seen time. An unscoped claim yields only
        pool-target work. With an experiment run id the first claim moves
        a pending run to running, canceling and terminal runs yield no
        jobs, and an empty claim finalizes the run when every job is
        already terminal. A claimed job whose spec does not resolve fails
        and drops out of the result.

        Args:
            worker_id: Id of the claiming worker.
            max_jobs: Maximum number of jobs to claim.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.
            actor: Caller context.

        Raises:
            WorkerNotFound: No worker has the claiming worker id.
            ExperimentRunNotFound: No experiment run has the scoped run
                id.

        Returns:
            Claimed jobs with their replay configs and specs.
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
        resolved = await self._repository.requeue_stale(
            self._stale_before(),
            self._max_attempts,
            agent_ids=agent_ids,
            experiment_run_id=experiment_run_id,
            parent_job_id=parent_job_id,
        )
        parent_job_ids = {
            parent_job_id
            for job in resolved
            if (parent_job_id := _settled_score_parent(job)) is not None
        }
        for parent_job_id in parent_job_ids:
            await self._aggregate_scores(parent_job_id)
        jobs = await self._repository.claim_pending(
            worker_id,
            max_jobs,
            agent_ids=agent_ids,
            experiment_run_id=experiment_run_id,
            parent_job_id=parent_job_id,
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
        return await self._with_specs(jobs)

    async def _fail_claimed(self, job: Job, error: str) -> None:
        """Fail a claimed job and settle whatever waits on it.

        Args:
            job: Claimed job.
            error: Error message.
        """
        job.fail(error)
        job = await self._repository.update(job)
        if isinstance(job, Score):
            await self._settle_score(job)
        elif isinstance(job, Replay) and job.experiment_run_id is not None:
            await finalize_run_if_drained(
                self._experiment_run_repository,
                self._repository,
                self._session_repository,
                job.experiment_run_id,
            )

    async def _with_specs(
        self, jobs: list[Job]
    ) -> list[tuple[Job, ReplayConfig | None, JobSpec]]:
        """Pair claimed jobs with their replay configs and specs.

        A job whose referents no longer resolve fails with the resolution
        error instead of failing the whole claim.

        Args:
            jobs: Claimed jobs.

        Returns:
            Jobs with their replay configs and specs.
        """
        referents = await self._load_referents(jobs)
        claimed: list[tuple[Job, ReplayConfig | None, JobSpec]] = []
        for job in jobs:
            try:
                spec = _build_spec(job, referents)
            except DomainError as exc:
                await self._fail_claimed(job, f"Failed to resolve the job spec: {exc}")
                continue
            config = (
                referents.replay_configs.get(job.replay_config_id)
                if isinstance(job, Replay)
                else None
            )
            claimed.append((job, config, spec))
        return claimed

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

        A replay drops its score jobs, so the next scoring transition
        fans out again.

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
        if isinstance(job, Replay):
            await self._repository.delete_children(job.id)
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
                session_ids=[job.input_session_id],
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
        session = await self._session_repository.get(job.input_session_id)
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
        original = await self._session_repository.get(job.input_session_id)
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
