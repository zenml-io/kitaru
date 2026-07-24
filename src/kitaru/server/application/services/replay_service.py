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
"""Replay use cases."""

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
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
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
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohorts import CohortSessionsFilter
from kitaru.server.application.models.replays import (
    ReplayCreate,
    ReplayFilter,
    ReplayUpdate,
)
from kitaru.server.application.services.run_finalization import (
    finalize_run_if_drained,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
)
from kitaru.server.domain.experiment_run import ExperimentRunStatus
from kitaru.server.domain.replay import (
    TERMINAL_REPLAY_STATUSES,
    InvalidReplay,
    InvalidReplayTransition,
    InvalidToolLookup,
    Replay,
    ReplayMissingResultSession,
    ReplaySpec,
    ReplayStatus,
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


class ReplayService:
    """Replay use cases."""

    def __init__(
        self,
        repository: ReplayRepository,
        replay_config_repository: ReplayConfigRepository,
        session_repository: SessionRepository,
        agent_version_repository: AgentVersionRepository,
        session_node_repository: SessionNodeRepository,
        experiment_run_repository: ExperimentRunRepository,
        experiment_repository: ExperimentRepository,
        cohort_repository: CohortRepository,
        secret_repository: SecretRepository,
        heartbeat_timeout_seconds: int,
        max_attempts: int,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Replay repository.
            replay_config_repository: Replay config repository.
            session_repository: Session repository.
            agent_version_repository: Agent version repository.
            session_node_repository: Session node repository.
            experiment_run_repository: Experiment run repository.
            experiment_repository: Experiment repository.
            cohort_repository: Cohort repository.
            secret_repository: Secret repository.
            heartbeat_timeout_seconds: Seconds after which a heartbeat
                counts as lost.
            max_attempts: Attempt count at which a stale replay times out.
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
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._max_attempts = max_attempts

    def _stale_before(self) -> datetime:
        """Compute the heartbeat staleness threshold.

        Returns:
            Time before which a heartbeat counts as lost.
        """
        return datetime.now(UTC) - timedelta(seconds=self._heartbeat_timeout_seconds)

    async def _resolve_agent_version(
        self, agent_id: uuid.UUID, version_id: uuid.UUID | None
    ) -> AgentVersion:
        """Resolve the agent version a replay executes.

        Args:
            agent_id: Id of the original session's agent.
            version_id: Explicit version id, ``None`` resolves the latest
                runnable version.

        Raises:
            NoRunnableAgentVersion: The agent has no runnable version.
            AgentVersionNotFound: No agent version has the explicit id.
            InvalidReplay: The explicit version belongs to another agent.
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
            raise InvalidReplay(
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
            InvalidReplay: The original session is in progress or the
                explicit version belongs to another agent.
            InvalidReplayConfig: A history policy scopes to a cohort.
            NoRunnableAgentVersion: The session's agent has no runnable
                version.
            AgentVersionNotFound: No agent version has the explicit id.
            AgentVersionNotRunnable: The explicit version has no run spec.

        Returns:
            Created replay and its replay config.
        """
        session = await self._session_repository.get(command.original_session_id)
        if session.status is SessionStatus.IN_PROGRESS:
            raise InvalidReplay(f"Session {session.id} is in progress")
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
        replay = Replay(
            replay_config_id=config.id,
            agent_version_id=version.id,
            original_session_id=session.id,
        )
        return await self._repository.create(replay), config

    async def get_replay(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig]:
        """Get a replay by id, reporting lost heartbeats as pending.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay and its replay config.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        replay = replay.with_staleness(self._stale_before(), self._max_attempts)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        return replay, config

    async def list_replays(
        self, replay_filter: ReplayFilter, actor: AuthContext
    ) -> tuple[list[tuple[Replay, ReplayConfig]], int]:
        """List replays matching a filter, reporting lost heartbeats.

        Args:
            replay_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching replays with their replay configs and the
            total match count.
        """
        _ = actor
        replays, total = await self._repository.query(replay_filter)
        stale_before = self._stale_before()
        replays = [
            replay.with_staleness(stale_before, self._max_attempts)
            for replay in replays
        ]
        configs = await self._replay_config_repository.get_many(
            [replay.replay_config_id for replay in replays]
        )
        return [(replay, configs[replay.replay_config_id]) for replay in replays], total

    async def get_spec(self, replay_id: uuid.UUID, actor: AuthContext) -> ReplaySpec:
        """Resolve the spec a runner executes a replay with.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            AgentVersionNotRunnable: The stamped agent version has no run
                spec.
            SecretNotFound: A run spec secret was deleted.

        Returns:
            Resolved replay spec.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        version = await self._agent_version_repository.get(replay.agent_version_id)
        if version.run_spec is None:
            raise AgentVersionNotRunnable(version.id)
        session = await self._session_repository.get(replay.original_session_id)
        score_baselines = True
        if replay.experiment_run_id is not None:
            run = await self._experiment_run_repository.get(replay.experiment_run_id)
            score_baselines = run.score_baselines
        secret_env: dict[str, SecretStr] = {}
        for secret_id in version.run_spec.secret_ids:
            secret = await self._secret_repository.get(secret_id)
            secret_env.update(secret.values)
        return ReplaySpec(
            replay_id=replay.id,
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
        self, replay: Replay, scores: dict[str, float]
    ) -> dict[str, Any]:
        """Compute the diff summary stored on a completing replay.

        Args:
            replay: Replay with a linked result session.
            scores: Scores reported by the runner.

        Returns:
            Diff summary.
        """
        assert replay.result_session_id is not None
        original = await self._session_repository.get(replay.original_session_id)
        result = await self._session_repository.get(replay.result_session_id)
        original_nodes = await self._session_node_repository.list_for_session(
            original.id, include_payloads=True
        )
        result_nodes = await self._session_node_repository.list_for_session(
            result.id, include_payloads=True
        )
        return compute_diff_summary(
            scores, original, result, original_nodes, result_nodes
        )

    async def update_replay(
        self, replay_id: uuid.UUID, command: ReplayUpdate, actor: AuthContext
    ) -> tuple[Replay, ReplayConfig]:
        """Transition a replay through the runner status updates.

        Completing stores the scoring result and the computed diff summary.
        The transition that makes the last replay of a run terminal also
        finalizes the run.

        Args:
            replay_id: Id of the replay.
            command: Replay update command.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            InvalidReplayTransition: The transition is illegal.
            ReplayMissingResultSession: Completing without a linked result
                session.
            InvalidReplay: Completing without a scoring result or failing
                without an error.

        Returns:
            Updated replay and its replay config.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        if command.status is ReplayStatus.RUNNING:
            replay.start()
        elif command.status is ReplayStatus.COMPLETED:
            if replay.status is not ReplayStatus.RUNNING:
                raise InvalidReplayTransition(
                    replay.id, replay.status, ReplayStatus.COMPLETED
                )
            if replay.result_session_id is None:
                raise ReplayMissingResultSession(replay.id)
            if (
                command.passed is None
                or command.score is None
                or command.scores is None
            ):
                raise InvalidReplay(
                    "Completing a replay requires passed, score, and scores"
                )
            diff = await self._compute_summary(replay, command.scores)
            replay.complete(
                ScoringResult(
                    passed=command.passed,
                    score=command.score,
                    scores=command.scores,
                ),
                diff,
            )
        elif command.status is ReplayStatus.FAILED:
            if command.error is None:
                raise InvalidReplay("Failing a replay requires an error")
            replay.fail(command.error)
        elif command.status is ReplayStatus.TIMED_OUT:
            if command.error is None:
                raise InvalidReplay("Timing out a replay requires an error")
            replay.time_out(command.error)
        elif command.status is ReplayStatus.CANCELED:
            replay.cancel()
        else:
            raise InvalidReplayTransition(replay.id, replay.status, command.status)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        replay = await self._repository.update(replay)
        if (
            replay.experiment_run_id is not None
            and replay.status in TERMINAL_REPLAY_STATUSES
        ):
            await finalize_run_if_drained(
                self._experiment_run_repository,
                self._repository,
                self._session_repository,
                replay.experiment_run_id,
            )
        return replay, config

    async def heartbeat_replay(self, replay_id: uuid.UUID, actor: AuthContext) -> bool:
        """Record a worker heartbeat on a replay.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            ReplayNotActive: The replay is not claimed, running, or
                canceled.

        Returns:
            ``True`` when the replay was canceled or its run is canceling.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        if replay.status is ReplayStatus.CANCELED:
            return True
        replay.heartbeat()
        await self._repository.update(replay)
        if replay.experiment_run_id is None:
            return False
        run = await self._experiment_run_repository.get(replay.experiment_run_id)
        return run.status is ExperimentRunStatus.CANCELING

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
        replay_id: uuid.UUID,
        tool_name: str,
        inputs: Any,
        cache_key: str,
        actor: AuthContext,
    ) -> SessionNode | None:
        """Resolve a history tool policy lookup within its scope.

        Args:
            replay_id: Id of the replay.
            tool_name: Name of the called tool.
            inputs: Tool call inputs.
            cache_key: Cache key claimed by the caller.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            InvalidToolLookup: The cache key does not match or the tool
                resolves to no history policy.
            InvalidReplayConfig: A standalone replay scopes to a cohort.

        Returns:
            Most recent matching tool call node, ``None`` on a miss.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        if tool_call_cache_key(tool_name, inputs) != cache_key:
            raise InvalidToolLookup("Cache key does not match the tool name and inputs")
        config = await self._replay_config_repository.get(replay.replay_config_id)
        policy = config.tool_policy.tools.get(tool_name, config.tool_policy.default)
        if not isinstance(policy, HistoryPolicy):
            raise InvalidToolLookup(f"Tool '{tool_name}' resolves to no history policy")
        if policy.scope is HistoryScope.ORIGINAL_SESSION:
            return await self._session_node_repository.find_tool_result(
                cache_key,
                session_ids=[replay.original_session_id],
                agent_id=None,
            )
        if policy.scope is HistoryScope.COHORT:
            if replay.experiment_run_id is None:
                raise InvalidReplayConfig(
                    "Standalone replays cannot use history scope 'cohort'"
                )
            run = await self._experiment_run_repository.get(replay.experiment_run_id)
            experiment = await self._experiment_repository.get(run.experiment_id)
            session_ids = await self._resolve_cohort_session_ids(experiment.cohort_id)
            return await self._session_node_repository.find_tool_result(
                cache_key, session_ids=session_ids, agent_id=None
            )
        session = await self._session_repository.get(replay.original_session_id)
        return await self._session_node_repository.find_tool_result(
            cache_key, session_ids=None, agent_id=session.agent_id
        )

    async def compute_diff(
        self, replay_id: uuid.UUID, actor: AuthContext
    ) -> ReplayDiff:
        """Compute the full diff between a replay's sessions.

        Args:
            replay_id: Id of the replay.
            actor: Caller context.

        Raises:
            ReplayNotFound: No replay has this id.
            ReplayMissingResultSession: The replay has no result session.

        Returns:
            Full replay diff.
        """
        _ = actor
        replay = await self._repository.get(replay_id)
        if replay.result_session_id is None:
            raise ReplayMissingResultSession(replay.id)
        config = await self._replay_config_repository.get(replay.replay_config_id)
        original = await self._session_repository.get(replay.original_session_id)
        result = await self._session_repository.get(replay.result_session_id)
        original_nodes = await self._session_node_repository.list_for_session(
            original.id, include_payloads=True
        )
        result_nodes = await self._session_node_repository.list_for_session(
            result.id, include_payloads=True
        )
        return compute_replay_diff(
            replay, config.override, original, result, original_nodes, result_nodes
        )
