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
from kitaru.client.exceptions import KitaruClientError
from kitaru_post_import_insights.generation import ModelGenerationConfig
from kitaru_post_import_insights.models import (
    MAX_NAME_LENGTH,
    MAX_SERVER_URL_LENGTH,
    GenerationMode,
    InsightGenerationContext,
    SourceImportContext,
)
from kitaru_post_import_insights.pipeline import (
    InsightGenerationConfig,
    generate_insights_from_profile,
    validate_session,
)
from kitaru_post_import_insights.profiling import SessionProfiler

DEFAULT_OPENAI_MODEL = "gpt-5.6-luna"
DEFAULT_OPENAI_REASONING_EFFORT = "low"


def _get_context(
    first: SessionWithNodesResponse,
    *,
    agent_name: str | None,
    server_url: str | None,
) -> InsightGenerationContext:
    """Derive the identity available on the first normalized analyzer session."""
    import_id = first.session.import_id
    return InsightGenerationContext(
        agent_id=first.session.agent_id,
        agent_name=agent_name,
        source_import=(
            None
            if import_id is None
            else SourceImportContext(
                import_id=import_id,
                provider=_get_provider(first.session.imported_from),
            )
        ),
        server_url=server_url,
    )


def _narrow_source_import(
    source: SourceImportContext, item: SessionWithNodesResponse
) -> SourceImportContext | None:
    """Keep the source import only while every session shares it and its provider."""
    if item.session.import_id != source.import_id:
        return None
    if (
        source.provider is not None
        and _get_provider(item.session.imported_from) != source.provider
    ):
        return source.model_copy(update={"provider": None})
    return source


async def _lookup_agent_name(client: KitaruAPIClient, agent_id: UUID) -> str | None:
    """Fetch the agent's display name, or None when the lookup fails."""
    try:
        agent = await client.agents.get(agent_id)
    except KitaruClientError:
        # The name only decorates the copied prompt, so a missing agent, a
        # token without agent read access, or a transient server error must
        # not fail an analysis whose session reads already succeeded.
        return None
    return agent.name if 0 < len(agent.name) <= MAX_NAME_LENGTH else None


def _get_server_url(client: KitaruAPIClient) -> str | None:
    """Return the client's resolved server URL when it fits the context bounds."""
    server_url = client.base_url.rstrip("/")
    return server_url if 0 < len(server_url) <= MAX_SERVER_URL_LENGTH else None


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
        session_ids: IDs of sessions for one agent, recorded live or imported.
        agent_name: Optional display name included in copied prompt context.

    Returns:
        Insight inputs ready for the analyzer task to persist.
    """
    return await _analyze_sessions(session_ids, agent_name=agent_name, model=None)


async def analyze_openai_post_import_sessions(
    session_ids: list[UUID],
    *,
    model: str | None = None,
    agent_name: str | None = None,
) -> list[InsightInput]:
    """Generate insight cards selected and edited by OpenAI.

    Args:
        session_ids: IDs of sessions for one agent, recorded live or imported.
        model: OpenAI model for the bounded analyst and editor calls. Defaults
            to `gpt-5.6-luna` with low reasoning effort.
        agent_name: Optional display name included in copied prompt context.

    Returns:
        Insight inputs ready for the analyzer task to persist.

    Raises:
        MissingOpenAICredential: OPENAI_API_KEY is unavailable.
    """
    selected_model = DEFAULT_OPENAI_MODEL if model is None else model
    return await _analyze_sessions(
        session_ids,
        agent_name=agent_name,
        model=ModelGenerationConfig(
            model=selected_model,
            reasoning_effort=(
                DEFAULT_OPENAI_REASONING_EFFORT if model is None else None
            ),
        ),
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
    source_import: SourceImportContext | None = None
    with SessionProfiler() as profiler:
        async with KitaruAPIClient() as client:
            for session_id in session_ids:
                normalized = await client.sessions.get_with_nodes(session_id)
                if context is None:
                    context = _get_context(
                        normalized,
                        agent_name=agent_name,
                        server_url=_get_server_url(client),
                    )
                    source_import = context.source_import
                elif source_import is not None:
                    source_import = _narrow_source_import(source_import, normalized)
                validate_session(normalized, context=context)
                profiler.consume(normalized)
                del normalized
            if context is not None and agent_name is None:
                agent_name = await _lookup_agent_name(client, context.agent_id)
        profiling = profiler.finish()
    if context is None:
        return []
    context = context.model_copy(
        update={"agent_name": agent_name, "source_import": source_import}
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
    if model is not None and result.mode is GenerationMode.DETERMINISTIC_FALLBACK:
        # The deterministic analyzer already covers the no-model case, so a
        # model-backed run that could not use the model fails the task instead
        # of silently returning deterministic cards under the OpenAI analyzer.
        raise RuntimeError(
            "OpenAI insight generation could not use the model: "
            f"{result.diagnostics.fallback_reason or 'unknown reason'}"
        )
    return result.insights


__all__ = ["analyze_openai_post_import_sessions", "analyze_post_import_sessions"]
