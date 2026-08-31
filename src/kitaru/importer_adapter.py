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
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from typing import Any, TypeVar

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.client.api_client import KitaruAPIClient
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

    def __init__(
        self,
        client: KitaruAPIClient,
        agent_id: uuid.UUID,
        completeness_timeout: float = 120.0,
    ) -> None:
        """Initialize the adapter.

        Args:
            client: API client.
            agent_id: Agent imported sessions are created under.
            completeness_timeout: Seconds to wait for the provider trace to
                complete.
        """
        self._client = client
        self._agent_id = agent_id
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
        with self.trace() as external_id:
            result = func(*args, **kwargs)
        asyncio.run(self._import_trace(external_id))
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
        with self.trace() as external_id:
            result = await func(*args, **kwargs)
        await self._import_trace(external_id)
        return result

    async def _import_trace(self, external_id: str) -> None:
        """Wait for the provider trace, then fetch, parse, and ingest it.

        Args:
            external_id: Provider trace id.

        Raises:
            SessionImportError: The parse yielded a failure or anything but
                exactly one session.
        """
        try:
            async with asyncio.timeout(self._completeness_timeout):
                await self.wait_until_complete(external_id)
        except TimeoutError:
            await self._create_timed_out_session(external_id)
            return
        payload = await self.fetch(external_id)
        items = list(call_parser(self.parser, payload, {}))
        for item in items:
            if isinstance(item, ImportFailure):
                raise SessionImportError(
                    f"Parser failed on trace {external_id}: {item.error}"
                )
        sessions = [item for item in items if isinstance(item, ImportedSession)]
        if len(sessions) != 1:
            raise SessionImportError(
                f"Parser yielded {len(sessions)} sessions for trace "
                f"{external_id}, expected exactly one"
            )
        await ingest_session(self._client, sessions[0], self._agent_id, self.provider)

    async def _create_timed_out_session(self, external_id: str) -> None:
        """Create a failed session for a trace that did not complete in time.

        Args:
            external_id: Provider trace id.
        """
        request = SessionCreateRequest(
            agent_id=self._agent_id,
            origin=SessionOrigin.IMPORTED,
            status=SessionStatus.FAILED,
            inputs=None,
            outputs=None,
            error=f"Provider trace did not complete within "
            f"{self._completeness_timeout} seconds",
            external_id=external_id,
            imported_from=self.provider,
        )
        await self._client.sessions.create(request)
