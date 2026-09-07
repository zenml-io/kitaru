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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Analyzer entrypoint for post-import insight generation."""

from collections.abc import AsyncIterable, AsyncIterator, Iterable

from kitaru.api_models.v1.insight import InsightInput
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.insights.generation import ModelGenerationConfig
from kitaru.insights.models import (
    MAX_NAME_LENGTH,
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru.insights.observability import (
    GenerationObserver,
    LangfuseGenerationObserver,
)
from kitaru.insights.pipeline import (
    InsightGenerationConfig,
    generate_insights_from_profile,
    validate_session,
)
from kitaru.insights.profiling import SessionProfiler
from kitaru.task.analyzer import SessionView


def _get_context(
    first: SessionView, *, agent_name: str | None
) -> InsightGenerationContext:
    """Derive the identity available on normalized analyzer sessions."""
    if first.session.import_id is None:
        raise ValueError("every post-import insight session must have an import ID")

    return InsightGenerationContext(
        agent_id=first.session.agent_id,
        agent_name=agent_name,
        source_import=SourceImportContext(
            import_id=first.session.import_id,
            provider=_get_provider(first.session.imported_from),
        ),
    )


def _get_provider(provider: str | None) -> str | None:
    """Retain one provider label only when it fits card metadata bounds."""
    if provider is None or not provider or len(provider) > MAX_NAME_LENGTH:
        return None
    try:
        provider.encode("utf-8")
    except UnicodeEncodeError:
        return None
    return provider


async def _iterate_session_views(
    sessions: Iterable[SessionView] | AsyncIterable[SessionView],
) -> AsyncIterator[SessionView]:
    """Yield either input form without retaining a view across the next fetch."""
    if isinstance(sessions, AsyncIterable):
        async for item in sessions:
            yield item
            del item
    else:
        for item in sessions:
            yield item
            del item


def _get_observer(enabled: bool) -> GenerationObserver | None:
    """Build best-effort telemetry from insight-specific configuration."""
    if not enabled:
        return None
    try:
        return LangfuseGenerationObserver()
    except Exception:
        return None


async def analyze_post_import_sessions(
    sessions: Iterable[SessionView] | AsyncIterable[SessionView],
    *,
    agent_name: str | None = None,
    model: str | None = None,
    observe: bool = False,
    source_session_count: int | None = None,
) -> list[InsightInput]:
    """Generate persistable insight cards from normalized imported sessions.

    Args:
        sessions: Analyzer session views for one agent and import.
        agent_name: Optional display name included in copied prompt context.
        model: Optional OpenAI model for the bounded analyst and editor calls.
        observe: Whether to emit metadata-only events to a dedicated Langfuse project.
        source_session_count: Optional eligible-source total for coverage accounting.

    Returns:
        Insight inputs ready for the analyzer task to persist.
    """
    context: InsightGenerationContext | None = None
    provider: str | None = None
    with SessionProfiler() as profiler:
        async for item in _iterate_session_views(sessions):
            if context is None:
                context = _get_context(item, agent_name=agent_name)
                provider = context.source_import.provider
            elif _get_provider(item.session.imported_from) != provider:
                provider = None
            normalized = SessionWithNodesResponse(
                session=item.session, nodes=item.nodes
            )
            validate_session(normalized, context=context)
            profiler.consume(normalized)
            del normalized, item
        profiling = profiler.finish(source_session_count=source_session_count)
    if context is None:
        return []
    context = context.model_copy(
        update={
            "source_import": context.source_import.model_copy(
                update={"provider": provider}
            )
        }
    )
    generator = None
    if model is not None:
        from kitaru.insights.openai_generator import OpenAIInsightGenerator

        generator = OpenAIInsightGenerator()
    result = await generate_insights_from_profile(
        profiling,
        context=context,
        config=InsightGenerationConfig(
            model=ModelGenerationConfig(model=model) if model is not None else None
        ),
        generator=generator,
        observer=_get_observer(observe),
    )
    return result.insights


__all__ = ["analyze_post_import_sessions"]
