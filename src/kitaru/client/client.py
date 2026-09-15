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
"""Kitaru client."""

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import TypeVar

from pydantic import BaseModel

from kitaru.api_models.v1.agent import (
    AgentCreateRequest,
    AgentListParams,
    AgentResponse,
)
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionResponse,
    RunSpec,
)
from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.experiment import ExperimentListParams, ExperimentResponse
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunCreateRequest,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.replay import (
    BaselineEvaluationMode,
    ReplayCreateRequest,
    ReplayResponse,
    ReplayStatus,
)
from kitaru.api_models.v1.replay_config import (
    ReplayOverride,
    ToolPolicy,
)
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionListParams,
    SessionResponse,
)
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.client.api_client import KitaruAPIClient, validate_idempotency_key
from kitaru.client.exceptions import (
    AgentRegistrationError,
    KitaruClientError,
    NotFoundError,
)

TERMINAL_REPLAY_STATUSES = frozenset(
    {ReplayStatus.COMPLETED, ReplayStatus.FAILED, ReplayStatus.CANCELED}
)
TERMINAL_EXPERIMENT_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)


NamedT = TypeVar("NamedT", AgentResponse, ExperimentResponse)
ListParamsT = TypeVar("ListParamsT", AgentListParams, ExperimentListParams)
StatusT = TypeVar("StatusT", ReplayResponse, ExperimentRunResponse)


class AgentRegistrationResult(BaseModel):
    """Result of creating an agent and its initial version."""

    agent: AgentResponse
    version: AgentVersionResponse


class KitaruClient:
    """Kitaru client."""

    def __init__(self, api_client: KitaruAPIClient | None = None) -> None:
        """Initialize the client.

        Args:
            api_client: API client used to send requests.
        """
        self._api_client = api_client or KitaruAPIClient()

    @property
    def api(self) -> KitaruAPIClient:
        """API client with one method per endpoint.

        Returns:
            API client with one method per endpoint.
        """
        return self._api_client

    async def close(self) -> None:
        """Close the underlying API client."""
        await self._api_client.close()

    async def __aenter__(self) -> "KitaruClient":
        """Enter the context manager.

        Returns:
            The client.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the client.

        Args:
            exc_type: Exception type.
            exc: Exception instance.
            traceback: Exception traceback.
        """
        await self.close()

    async def get_agent(self, agent: uuid.UUID | str) -> AgentResponse:
        """Get an agent by id or name.

        Args:
            agent: Id or name of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing agent.

        Returns:
            Stored agent.
        """
        if isinstance(agent, uuid.UUID):
            return await self._api_client.agents.get(agent)
        return await self._get_by_name(
            "agent", agent, AgentListParams, self._api_client.agents.list
        )

    async def register_agent(
        self,
        name: str,
        run_spec: RunSpec,
        *,
        description: str | None = None,
        display_version: str | None = None,
        version_description: str | None = None,
        capabilities: AgentCapabilities | None = None,
        agent_idempotency_key: str | None = None,
        version_idempotency_key: str | None = None,
    ) -> AgentRegistrationResult:
        """Create an agent and its initial version.

        The operation sends separate agent and version requests. If the agent is
        created but the initial version request raises an ordinary exception,
        ``AgentRegistrationError`` records the created agent, exact version
        request, and version idempotency key. Cancellation still propagates,
        with the agent id and version idempotency key attached as an exception
        note. The method does not roll back the agent or start a fresh version
        request with a different idempotency key.

        Args:
            name: New agent name.
            run_spec: Run spec for the initial version.
            description: Agent description.
            display_version: Human-readable designator for the initial version.
            version_description: Initial version description.
            capabilities: Initial version capabilities.
            agent_idempotency_key: Idempotency key for agent creation.
            version_idempotency_key: Idempotency key for version creation.

        Raises:
            ValueError: A request field is invalid or the two idempotency keys
                are equal after normalization.
            APIError: An idempotency key is invalid or agent creation failed.
            AgentRegistrationError: The agent was created but the initial
                version request raised an ordinary exception.

        Returns:
            Created agent and initial version.
        """
        agent_request = AgentCreateRequest(name=name, description=description)
        version_request = AgentVersionCreateRequest(
            display_version=display_version,
            description=version_description,
            run_spec=run_spec,
            capabilities=capabilities,
        )
        # Both keys must be valid and distinct before either write is sent.
        agent_idempotency_key = validate_idempotency_key(
            agent_idempotency_key, "agent_idempotency_key"
        )
        version_idempotency_key = validate_idempotency_key(
            version_idempotency_key, "version_idempotency_key"
        ) or str(uuid.uuid4())
        if agent_idempotency_key == version_idempotency_key:
            raise ValueError(
                "agent_idempotency_key and version_idempotency_key must differ."
            )

        agent = await self._api_client.agents.create(
            agent_request,
            idempotency_key=agent_idempotency_key,
        )
        try:
            version = await self._api_client.agents.create_version(
                agent.id,
                version_request,
                idempotency_key=version_idempotency_key,
            )
        except asyncio.CancelledError as error:
            error.add_note(
                f"Agent {agent.id} was created before registration was canceled. "
                f"The initial-version request used idempotency key "
                f"{version_idempotency_key!r}."
            )
            raise
        except Exception as error:
            raise AgentRegistrationError(
                agent=agent,
                version_request=version_request,
                version_idempotency_key=version_idempotency_key,
                cause=error,
            ) from error
        agent = agent.model_copy(update={"latest_version": version.version})
        return AgentRegistrationResult(agent=agent, version=version)

    async def register_agent_version(
        self,
        agent: uuid.UUID | str,
        run_spec: RunSpec,
        *,
        display_version: str | None = None,
        description: str | None = None,
        capabilities: AgentCapabilities | None = None,
        idempotency_key: str | None = None,
    ) -> AgentVersionResponse:
        """Create the next version of an existing agent.

        Args:
            agent: Id or exact name of the agent.
            run_spec: Run spec for the new version.
            display_version: Human-readable version designator.
            description: Version description.
            capabilities: Version capabilities.
            idempotency_key: Idempotency key for version creation.

        Raises:
            ValueError: A request field is invalid.
            APIError: The idempotency key, agent lookup, or version creation
                failed.

        Returns:
            Created agent version.
        """
        version_request = AgentVersionCreateRequest(
            display_version=display_version,
            description=description,
            run_spec=run_spec,
            capabilities=capabilities,
        )
        agent_id = (
            agent if isinstance(agent, uuid.UUID) else (await self.get_agent(agent)).id
        )
        return await self._api_client.agents.create_version(
            agent_id, version_request, idempotency_key=idempotency_key
        )

    def list_agents(self) -> AsyncIterator[AgentResponse]:
        """Iterate over all agents.

        Returns:
            Async iterator over every agent.
        """
        return self._api_client.agents.iter()

    async def get_session(self, session_id: uuid.UUID) -> SessionDetailResponse:
        """Get a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing session.

        Returns:
            Stored session.
        """
        return await self._api_client.sessions.get(session_id)

    async def list_sessions(
        self, agent: uuid.UUID | str | None = None
    ) -> AsyncIterator[SessionResponse]:
        """Iterate over sessions, optionally scoped to one agent.

        Args:
            agent: Id or name of the agent to scope to.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every matching session.
        """
        params = SessionListParams()
        if agent is not None:
            agent_id = (await self.get_agent(agent)).id
            params = SessionListParams(
                filter=FilterCondition(
                    field="agent_id", op=FilterOp.EQ, value=str(agent_id)
                )
            )
        async for session in self._api_client.sessions.iter(params):
            yield session

    def list_session_nodes(
        self, session_id: uuid.UUID
    ) -> AsyncIterator[SessionNodeResponse]:
        """Iterate over the nodes of a session in index order.

        Args:
            session_id: Id of the session.

        Returns:
            Async iterator over every node.
        """
        return self._api_client.sessions.iter_nodes(session_id)

    async def replay(
        self,
        session_id: uuid.UUID,
        evaluators: list[EvaluatorConfig],
        agent_version_id: uuid.UUID | None = None,
        override: ReplayOverride | None = None,
        tool_policy: ToolPolicy | None = None,
        baseline_evaluation_mode: BaselineEvaluationMode = (
            BaselineEvaluationMode.IF_MISSING
        ),
        wait: bool = True,
        timeout: float | None = None,
    ) -> ReplayResponse:
        """Replay a session and optionally wait for it to finish.

        Args:
            session_id: Id of the session to replay.
            evaluators: Evaluators run against the result session.
            agent_version_id: Agent version to replay with, the session's
                recorded version when unset.
            override: Override to apply.
            tool_policy: Tool policy to apply.
            baseline_evaluation_mode: How to score the baseline session.
            wait: Whether to wait for the replay to reach a terminal status.
            timeout: Seconds to wait before giving up.

        Raises:
            APIError: The request failed, including 404 for a missing session
                or agent version.
            TimeoutError: The replay did not finish within the timeout.

        Returns:
            Replay, in a terminal status when waited for.
        """
        request = ReplayCreateRequest(
            baseline_session_id=session_id,
            agent_version_id=agent_version_id,
            override=override,
            tool_policy=tool_policy,
            evaluators=evaluators,
            baseline_evaluation_mode=baseline_evaluation_mode,
        )
        replay = await self._api_client.replays.create(request)
        if wait:
            replay = await self.wait_for_replay(replay.id, timeout=timeout)
        return replay

    async def get_replay(self, replay_id: uuid.UUID) -> ReplayResponse:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            APIError: The request failed, including 404 for a missing replay.

        Returns:
            Stored replay.
        """
        return await self._api_client.replays.get(replay_id)

    async def wait_for_replay(
        self,
        replay_id: uuid.UUID,
        timeout: float | None = None,
        poll_interval: float = 2.0,
    ) -> ReplayResponse:
        """Wait for a replay to reach a terminal status.

        Args:
            replay_id: Id of the replay.
            timeout: Seconds to wait before giving up.
            poll_interval: Seconds between status checks.

        Raises:
            APIError: The request failed.
            TimeoutError: The replay did not finish within the timeout.

        Returns:
            Replay in a terminal status.
        """
        return await self._wait_for_status(
            lambda: self._api_client.replays.get(replay_id),
            lambda replay: replay.status in TERMINAL_REPLAY_STATUSES,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    async def get_experiment(self, experiment: uuid.UUID | str) -> ExperimentResponse:
        """Get an experiment by id or name.

        Args:
            experiment: Id or name of the experiment.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Stored experiment.
        """
        if isinstance(experiment, uuid.UUID):
            return await self._api_client.experiments.get(experiment)
        return await self._get_by_name(
            "experiment",
            experiment,
            ExperimentListParams,
            self._api_client.experiments.list,
        )

    def list_experiments(self) -> AsyncIterator[ExperimentResponse]:
        """Iterate over all experiments.

        Returns:
            Async iterator over every experiment.
        """
        return self._api_client.experiments.iter()

    async def run_experiment(
        self,
        experiment: uuid.UUID | str,
        cohort_version_id: uuid.UUID,
        agent_version_id: uuid.UUID,
        baseline_evaluation_mode: BaselineEvaluationMode = (
            BaselineEvaluationMode.IF_MISSING
        ),
        wait: bool = True,
        timeout: float | None = None,
    ) -> ExperimentRunResponse:
        """Start an experiment run and optionally wait for it to finish.

        Args:
            experiment: Id or name of the experiment.
            cohort_version_id: Cohort version whose sessions are replayed.
            agent_version_id: Agent version to replay with.
            baseline_evaluation_mode: How to score each baseline session.
            wait: Whether to wait for the run to reach a terminal status.
            timeout: Seconds to wait before giving up.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment, cohort version, or agent version.
            TimeoutError: The run did not finish within the timeout.

        Returns:
            Experiment run, in a terminal status when waited for.
        """
        experiment_id = (await self.get_experiment(experiment)).id
        request = ExperimentRunCreateRequest(
            cohort_version_id=cohort_version_id,
            agent_version_id=agent_version_id,
            baseline_evaluation_mode=baseline_evaluation_mode,
        )
        run = await self._api_client.experiments.start_run(experiment_id, request)
        if wait:
            run = await self.wait_for_experiment_run(run.id, timeout=timeout)
        return run

    async def get_experiment_run(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Get an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: The request failed, including 404 for a missing run.

        Returns:
            Stored experiment run.
        """
        return await self._api_client.experiment_runs.get(run_id)

    async def wait_for_experiment_run(
        self,
        run_id: uuid.UUID,
        timeout: float | None = None,
        poll_interval: float = 2.0,
    ) -> ExperimentRunResponse:
        """Wait for an experiment run to reach a terminal status.

        Args:
            run_id: Id of the experiment run.
            timeout: Seconds to wait before giving up.
            poll_interval: Seconds between status checks.

        Raises:
            APIError: The request failed.
            TimeoutError: The run did not finish within the timeout.

        Returns:
            Experiment run in a terminal status.
        """
        return await self._wait_for_status(
            lambda: self._api_client.experiment_runs.get(run_id),
            lambda run: run.status in TERMINAL_EXPERIMENT_RUN_STATUSES,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    async def _get_by_name(
        self,
        kind: str,
        name: str,
        params_type: type[ListParamsT],
        load_page: Callable[[ListParamsT], Awaitable[Page[NamedT]]],
    ) -> NamedT:
        """Get one resource by its exact name.

        Args:
            kind: Resource kind used in error messages.
            name: Name of the resource.
            params_type: List params type of the resource.
            load_page: List method of the resource.

        Raises:
            NotFoundError: No resource has the name.
            KitaruClientError: More than one resource has the name.

        Returns:
            Matching resource.
        """
        params = params_type(
            size=2, filter=FilterCondition(field="name", op=FilterOp.EQ, value=name)
        )
        page = await load_page(params)
        matches = [item for item in page.items if item.name == name]
        if not matches:
            raise NotFoundError(404, f"{kind.title()} {name!r} was not found")
        if len(matches) > 1:
            raise KitaruClientError(f"More than one {kind} has the exact name {name!r}")
        return matches[0]

    async def _wait_for_status(
        self,
        load: Callable[[], Awaitable[StatusT]],
        is_terminal: Callable[[StatusT], bool],
        timeout: float | None,
        poll_interval: float,
    ) -> StatusT:
        """Poll a resource until it reaches a terminal status.

        Args:
            load: Loads the current resource state.
            is_terminal: Whether the resource reached a terminal status.
            timeout: Seconds to wait before giving up.
            poll_interval: Seconds between status checks.

        Raises:
            TimeoutError: The resource did not reach a terminal status within
                the timeout.

        Returns:
            Resource in a terminal status.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            resource = await load()
            if is_terminal(resource):
                return resource
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out after {timeout} seconds waiting for {resource.id}"
                )
            await asyncio.sleep(poll_interval)
