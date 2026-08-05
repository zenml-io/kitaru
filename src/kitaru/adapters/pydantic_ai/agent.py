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
"""Thin public PydanticAI agent wrapper."""

import os
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Generic, TypeVar

from kitaru.adapters.pydantic_ai.capability import _KitaruCapability
from pydantic_ai import AgentRun
from pydantic_ai.agent import AbstractAgent, WrapperAgent

AgentDepsT = TypeVar("AgentDepsT")
OutputDataT = TypeVar("OutputDataT")


class KitaruAgent(
    WrapperAgent[AgentDepsT, OutputDataT], Generic[AgentDepsT, OutputDataT]
):
    """Record and replay an existing PydanticAI agent through Kitaru."""

    def __init__(
        self,
        agent: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        api_url: str | None = None,
        api_key: str | None = None,
        session_name: str | None = None,
        batch_size: int = 20,
    ) -> None:
        """Initialize a transparent wrapper around an existing agent.

        Args:
            agent: PydanticAI agent to execute.
            agent_id: Optional Kitaru agent identifier. Task-bound runs infer
                this from the task's agent version.
            agent_version_id: Optional Kitaru agent-version identifier.
            api_url: Kitaru API URL, falling back to ``KITARU_API_URL``.
            api_key: Kitaru API key, falling back to ``KITARU_API_KEY``.
            session_name: Recorded name, falling back to
                ``KITARU_SESSION_NAME``.
            batch_size: Number of child nodes sent per upsert batch.

        Raises:
            ValueError: If the API URL is missing or batch size is invalid.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        resolved_url = api_url or os.environ.get("KITARU_API_URL")
        if not resolved_url:
            raise ValueError("KITARU_API_URL is not set")
        super().__init__(agent)
        self._capability = _KitaruCapability(
            agent_id=agent_id,
            agent_version_id=agent_version_id,
            api_url=resolved_url,
            api_key=api_key or os.environ.get("KITARU_API_KEY"),
            session_name=session_name or os.environ.get("KITARU_SESSION_NAME"),
            batch_size=batch_size,
        )

    @asynccontextmanager
    async def iter(
        self, *args: Any, **kwargs: Any
    ) -> AsyncGenerator[AgentRun[Any, Any]]:
        """Run the wrapped agent with Kitaru as an outermost capability."""
        capabilities = kwargs.pop("capabilities", None)
        kwargs["capabilities"] = [self._capability, *(capabilities or ())]
        async with super().iter(*args, **kwargs) as run:
            yield run
