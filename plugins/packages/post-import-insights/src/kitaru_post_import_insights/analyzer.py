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

from uuid import UUID

from kitaru.api_models.v1.insight import InsightInput
from kitaru.api_models.v1.session_node import SessionWithNodesResponse
from kitaru.client import KitaruAPIClient
from kitaru_post_import_insights.generation import ModelGenerationConfig
from kitaru_post_import_insights.models import (
    MAX_NAME_LENGTH,
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru_post_import_insights.pipeline import (
    InsightGenerationConfig,
    generate_insights_from_profile,
    validate_session,
)
from kitaru_post_import_insights.profiling import SessionProfiler


def _get_context(
    first: SessionWithNodesResponse, *, agent_name: str | None
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


async def analyze_post_import_sessions(
    session_ids: list[UUID],
    *,
    agent_name: str | None = None,
) -> list[InsightInput]:
    """Generate deterministic insight cards without provider credentials.

    Args:
        session_ids: IDs of imported sessions for one agent and import.
        agent_name: Optional display name included in copied prompt context.

    Returns:
        Insight inputs ready for the analyzer task to persist.
    """
    return await _analyze_sessions(session_ids, agent_name=agent_name, model=None)


async def analyze_openai_post_import_sessions(
    session_ids: list[UUID],
    *,
    model: str = "gpt-5.6-luna",
    agent_name: str | None = None,
) -> list[InsightInput]:
    """Generate insight cards selected and edited by OpenAI.

    Args:
        session_ids: IDs of imported sessions for one agent and import.
        model: OpenAI model for the bounded analyst and editor calls. Defaults
            to `gpt-5.6-luna`.
        agent_name: Optional display name included in copied prompt context.

    Returns:
        Insight inputs ready for the analyzer task to persist.

    Raises:
        MissingOpenAICredential: OPENAI_API_KEY is unavailable.
    """
    return await _analyze_sessions(
        session_ids,
        agent_name=agent_name,
        model=ModelGenerationConfig(model=model),
    )


async def _analyze_sessions(
    session_ids: list[UUID],
    *,
    agent_name: str | None,
    model: ModelGenerationConfig | None,
) -> list[InsightInput]:
    """Profile normalized sessions and generate cards using the selected analyzer."""
    if not session_ids:
        return []
    context: InsightGenerationContext | None = None
    provider: str | None = None
    with SessionProfiler() as profiler:
        async with KitaruAPIClient() as client:
            for session_id in session_ids:
                normalized = await client.sessions.get_with_nodes(session_id)
                if context is None:
                    context = _get_context(normalized, agent_name=agent_name)
                    provider = context.source_import.provider
                elif _get_provider(normalized.session.imported_from) != provider:
                    provider = None
                validate_session(normalized, context=context)
                profiler.consume(normalized)
                del normalized
        profiling = profiler.finish()
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
        from kitaru_post_import_insights.openai_generator import OpenAIInsightGenerator

        generator = OpenAIInsightGenerator()
    result = await generate_insights_from_profile(
        profiling,
        context=context,
        config=InsightGenerationConfig(model=model),
        generator=generator,
    )
    return result.insights


__all__ = ["analyze_openai_post_import_sessions", "analyze_post_import_sessions"]
