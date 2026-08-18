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
"""Synchronous Kitaru client."""

import asyncio
import threading
import uuid
from collections.abc import AsyncIterator, Coroutine, Iterator
from types import TracebackType
from typing import Any, TypeVar

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.experiment_run import ExperimentRunResponse
from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.client import KitaruClient

T = TypeVar("T")


class KitaruSyncClient:
    """Synchronous Kitaru client."""

    def __init__(self, api_client: KitaruAPIClient | None = None) -> None:
        """Initialize the client.

        Args:
            api_client: API client used to send requests.
        """
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._client = KitaruClient(api_client=api_client)

    @property
    def api(self) -> KitaruAPIClient:
        """API client with one method per endpoint.

        Returns:
            API client with one method per endpoint.
        """
        return self._client.api

    def _run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine on the event loop thread and wait for the result.

        Args:
            coro: Coroutine to run.

        Returns:
            Coroutine result.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def _iterate(self, iterator: AsyncIterator[T]) -> Iterator[T]:
        """Drain an async iterator on the event loop thread one item at a time.

        Args:
            iterator: Async iterator to drain.

        Returns:
            Iterator over the same items.
        """

        async def next_item() -> T:
            return await anext(iterator)

        while True:
            try:
                yield self._run(next_item())
            except StopAsyncIteration:
                return

    def close(self) -> None:
        """Close the underlying client and stop the event loop thread."""
        self._run(self._client.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    def __enter__(self) -> "KitaruSyncClient":
        """Enter the context manager.

        Returns:
            The client.
        """
        return self

    def __exit__(
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
        self.close()

    def get_agent(self, agent: uuid.UUID | str) -> AgentResponse:
        """Get an agent by id or name.

        Args:
            agent: Id or name of the agent.

        Raises:
            APIError: The request failed, including 404 for a missing agent.

        Returns:
            Stored agent.
        """
        return self._run(self._client.get_agent(agent))

    def list_agents(self) -> Iterator[AgentResponse]:
        """Iterate over all agents.

        Returns:
            Iterator over every agent.
        """
        return self._iterate(self._client.list_agents())

    def get_session(self, session_id: uuid.UUID) -> SessionResponse:
        """Get a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing session.

        Returns:
            Stored session.
        """
        return self._run(self._client.get_session(session_id))

    def list_sessions(
        self, agent: uuid.UUID | str | None = None
    ) -> Iterator[SessionResponse]:
        """Iterate over sessions, optionally scoped to one agent.

        Args:
            agent: Id or name of the agent to scope to.

        Raises:
            APIError: The request failed.

        Returns:
            Iterator over every matching session.
        """
        return self._iterate(self._client.list_sessions(agent))

    def list_session_nodes(
        self, session_id: uuid.UUID
    ) -> Iterator[SessionNodeResponse]:
        """Iterate over the nodes of a session in index order.

        Args:
            session_id: Id of the session.

        Returns:
            Iterator over every node.
        """
        return self._iterate(self._client.list_session_nodes(session_id))

    def replay(
        self,
        session_id: uuid.UUID,
        evaluators: list[EvaluatorConfig],
        agent_version_id: uuid.UUID | None = None,
        override: ReplayOverride | None = None,
        tool_policy: ToolPolicy | None = None,
        evaluate_baselines: bool = False,
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
            evaluate_baselines: Whether to also score the baseline session.
            wait: Whether to wait for the replay to reach a terminal status.
            timeout: Seconds to wait before giving up.

        Raises:
            APIError: The request failed, including 404 for a missing session
                or agent version.
            TimeoutError: The replay did not finish within the timeout.

        Returns:
            Replay, in a terminal status when waited for.
        """
        return self._run(
            self._client.replay(
                session_id,
                evaluators,
                agent_version_id=agent_version_id,
                override=override,
                tool_policy=tool_policy,
                evaluate_baselines=evaluate_baselines,
                wait=wait,
                timeout=timeout,
            )
        )

    def get_replay(self, replay_id: uuid.UUID) -> ReplayResponse:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            APIError: The request failed, including 404 for a missing replay.

        Returns:
            Stored replay.
        """
        return self._run(self._client.get_replay(replay_id))

    def wait_for_replay(
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
        return self._run(
            self._client.wait_for_replay(
                replay_id, timeout=timeout, poll_interval=poll_interval
            )
        )

    def get_experiment(self, experiment: uuid.UUID | str) -> ExperimentResponse:
        """Get an experiment by id or name.

        Args:
            experiment: Id or name of the experiment.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment.

        Returns:
            Stored experiment.
        """
        return self._run(self._client.get_experiment(experiment))

    def list_experiments(self) -> Iterator[ExperimentResponse]:
        """Iterate over all experiments.

        Returns:
            Iterator over every experiment.
        """
        return self._iterate(self._client.list_experiments())

    def run_experiment(
        self,
        experiment: uuid.UUID | str,
        cohort_version_id: uuid.UUID,
        agent_version_id: uuid.UUID,
        evaluate_baselines: bool = False,
        wait: bool = True,
        timeout: float | None = None,
    ) -> ExperimentRunResponse:
        """Start an experiment run and optionally wait for it to finish.

        Args:
            experiment: Id or name of the experiment.
            cohort_version_id: Cohort version whose sessions are replayed.
            agent_version_id: Agent version to replay with.
            evaluate_baselines: Whether to also score each baseline session.
            wait: Whether to wait for the run to reach a terminal status.
            timeout: Seconds to wait before giving up.

        Raises:
            APIError: The request failed, including 404 for a missing
                experiment, cohort version, or agent version.
            TimeoutError: The run did not finish within the timeout.

        Returns:
            Experiment run, in a terminal status when waited for.
        """
        return self._run(
            self._client.run_experiment(
                experiment,
                cohort_version_id,
                agent_version_id,
                evaluate_baselines=evaluate_baselines,
                wait=wait,
                timeout=timeout,
            )
        )

    def get_experiment_run(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Get an experiment run by id.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: The request failed, including 404 for a missing run.

        Returns:
            Stored experiment run.
        """
        return self._run(self._client.get_experiment_run(run_id))

    def wait_for_experiment_run(
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
        return self._run(
            self._client.wait_for_experiment_run(
                run_id, timeout=timeout, poll_interval=poll_interval
            )
        )
