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
"""Importer-backed adapter base class."""

import asyncio
import os
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from typing import Any, ClassVar, TypeVar

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.replay_config import PassthroughConfig
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.env import get_required_env
from kitaru.task.importer import (
    ImportedSession,
    Parser,
    SessionImportError,
    call_parser,
    ingest_session,
)

__all__ = ["ImporterBackedAdapter"]

T = TypeVar("T")


class ImporterBackedAdapter:
    """Base class for adapters importing provider traces of wrapped runs."""

    provider: str
    parser: Parser
    parser_params: ClassVar[dict[str, Any]] = {}

    def __init__(self, completeness_timeout: float = 120.0) -> None:
        """Initialize the adapter.

        Args:
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        self._completeness_timeout = completeness_timeout

    def trace(self) -> AbstractContextManager[str]:
        """Activate a provider trace and yield its external id.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError

    async def wait_until_complete(self, external_id: str) -> None:
        """Block until the provider has the finished trace.

        Args:
            external_id: Provider trace id.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError

    async def fetch(self, external_id: str) -> bytes:
        """Fetch the finished trace as payload bytes.

        Args:
            external_id: Provider trace id.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError

    def run(self, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run a function inside a provider trace and import the trace.

        Args:
            func: Function to run.
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's result.
        """
        asyncio.run(self._check_replay())
        agent_id = uuid.UUID(get_required_env("KITARU_AGENT_ID"))
        with self.trace() as external_id:
            result = func(*args, **kwargs)
        asyncio.run(self._import_trace(external_id, agent_id))
        return result

    async def run_async(
        self, func: Callable[..., Awaitable[T]], /, *args: Any, **kwargs: Any
    ) -> T:
        """Run an async function inside a provider trace and import the trace.

        Args:
            func: Function to run.
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's result.
        """
        await self._check_replay()
        agent_id = uuid.UUID(get_required_env("KITARU_AGENT_ID"))
        with self.trace() as external_id:
            result = await func(*args, **kwargs)
        await self._import_trace(external_id, agent_id)
        return result

    async def _check_replay(self) -> None:
        """Reject a replay config the adapter cannot apply.

        Raises:
            RuntimeError: The replay carries an override or a non-passthrough
                tool policy.
        """
        replay_value = os.environ.get("KITARU_REPLAY_ID")
        if replay_value is None:
            return
        async with KitaruAPIClient() as client:
            replay = await client.replays.get(uuid.UUID(replay_value))
        if replay.override is not None:
            raise RuntimeError(
                "Importer-backed adapters do not support replay overrides"
            )
        policies = [replay.tool_policy.default, *replay.tool_policy.tools.values()]
        if any(not isinstance(policy, PassthroughConfig) for policy in policies):
            raise RuntimeError(
                "Importer-backed adapters do not support replay tool policies"
            )

    async def _import_trace(self, external_id: str, agent_id: uuid.UUID) -> None:
        """Wait for the provider trace, then fetch, parse, and ingest it.

        Args:
            external_id: Provider trace id.
            agent_id: Agent the imported session is created under.

        Raises:
            SessionImportError: The parse yielded a failure or anything but
                exactly one session, or a session with the external id
                already exists.
        """
        try:
            async with asyncio.timeout(self._completeness_timeout):
                await self.wait_until_complete(external_id)
        except TimeoutError:
            async with KitaruAPIClient() as client:
                await self._create_timed_out_session(client, external_id, agent_id)
            return
        payload = await self.fetch(external_id)
        sessions: list[ImportedSession] = []
        for item in call_parser(self.parser, payload, self.parser_params):
            if isinstance(item, ImportFailure):
                raise SessionImportError(
                    f"Parser failed on trace {external_id}: {item.error}"
                )
            sessions.append(item)
        if len(sessions) != 1:
            raise SessionImportError(
                f"Parser yielded {len(sessions)} sessions for trace "
                f"{external_id}, expected exactly one"
            )
        async with KitaruAPIClient() as client:
            session = await ingest_session(client, sessions[0], agent_id, self.provider)
        if session is None:
            raise SessionImportError(
                f"A session with external id {sessions[0].external_id} already exists"
            )

    async def _create_timed_out_session(
        self, client: KitaruAPIClient, external_id: str, agent_id: uuid.UUID
    ) -> None:
        """Create a failed session for a trace that did not complete in time.

        Args:
            client: API client.
            external_id: Provider trace id.
            agent_id: Agent the failed session is created under.
        """
        request = SessionCreateRequest(
            agent_id=agent_id,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.FAILED,
            inputs=None,
            outputs=None,
            error=f"Provider trace did not complete within "
            f"{self._completeness_timeout} seconds",
            external_id=external_id,
            imported_from=self.provider,
        )
        await client.sessions.create(request)
