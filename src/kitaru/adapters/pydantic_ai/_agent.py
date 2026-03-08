"""Agent wrapper for Kitaru's PydanticAI adapter."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from pydantic_ai import messages as _messages
from pydantic_ai import models
from pydantic_ai import usage as _usage
from pydantic_ai.agent import AbstractAgent, AgentRun, WrapperAgent
from pydantic_ai.agent.abstract import AgentMetadata, Instructions
from pydantic_ai.builtin_tools import AbstractBuiltinTool
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT, OutputSpec
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import AgentDepsT, BuiltinToolFunc, DeferredToolResults
from pydantic_ai.toolsets import AbstractToolset

from kitaru.errors import KitaruUsageError

from ._hitl import attach_hitl_metadata
from ._model import KitaruModel
from ._toolset import kitaruify_toolset
from ._tracking import tracker_scope


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    """Wrap a PydanticAI agent so internal calls become Kitaru child events."""

    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
    ) -> None:
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise KitaruUsageError(
                "Wrapped PydanticAI agents must define a model at construction time."
            )

        self._name = name or wrapped.name or "agent"
        self._model = KitaruModel(wrapped.model)

        for toolset in wrapped.toolsets:
            toolset.apply(attach_hitl_metadata)

        self._toolsets = [
            toolset.visit_and_replace(kitaruify_toolset) for toolset in wrapped.toolsets
        ]

    @property
    def name(self) -> str | None:
        """Return the wrapped agent name used in event IDs."""
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        """Set adapter-visible and wrapped-agent name values."""
        self._name = value
        self.wrapped.name = value

    @property
    def model(self) -> Model:
        """Return the adapter's model wrapper."""
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        """Return adapter-wrapped toolsets."""
        return self._toolsets

    @contextmanager
    def _kitaru_overrides(self) -> Iterator[None]:
        """Force wrapped agent runs to use adapter model/toolsets."""
        with super().override(model=self._model, toolsets=self._toolsets, tools=[]):
            yield

    @asynccontextmanager
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: Instructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: ModelSettings | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AbstractBuiltinTool | BuiltinToolFunc[AgentDepsT]]
        | None = None,
    ) -> AsyncIterator[AgentRun[AgentDepsT, Any]]:
        with self._kitaru_overrides(), tracker_scope(self._name):
            async with self.wrapped.iter(
                user_prompt=user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                model=model,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=toolsets,
                builtin_tools=builtin_tools,
            ) as run:
                yield run
