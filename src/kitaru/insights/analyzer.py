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
from kitaru.insights.pipeline import InsightGenerationConfig, generate_insights
from kitaru.task.analyzer import SessionView


def _get_context(
    sessions: list[SessionView], *, agent_name: str | None
) -> InsightGenerationContext:
    """Derive the identity available on normalized analyzer sessions."""
    if not sessions:
        raise ValueError("post-import insight analysis requires at least one session")

    first = sessions[0].session
    if first.import_id is None:
        raise ValueError("every post-import insight session must have an import ID")

    provider = _get_provider(sessions)
    return InsightGenerationContext(
        agent_id=first.agent_id,
        agent_name=agent_name,
        source_import=SourceImportContext(import_id=first.import_id, provider=provider),
    )


def _get_provider(sessions: list[SessionView]) -> str | None:
    """Retain one provider label only when it fits card metadata bounds."""
    providers = {item.session.imported_from for item in sessions}
    if len(providers) != 1:
        return None
    provider = providers.pop()
    if provider is None or not provider or len(provider) > MAX_NAME_LENGTH:
        return None
    try:
        provider.encode("utf-8")
    except UnicodeEncodeError:
        return None
    return provider


def _get_observer(enabled: bool) -> GenerationObserver | None:
    """Build best-effort telemetry from insight-specific configuration."""
    if not enabled:
        return None
    try:
        return LangfuseGenerationObserver()
    except Exception:
        return None


async def analyze_post_import_sessions(
    sessions: list[SessionView],
    *,
    agent_name: str | None = None,
    model: str | None = None,
    observe: bool = False,
) -> list[InsightInput]:
    """Generate persistable insight cards from normalized imported sessions.

    Args:
        sessions: Analyzer session views for one agent and import.
        agent_name: Optional display name included in copied prompt context.
        model: Optional OpenAI model for the bounded analyst and editor calls.
        observe: Whether to emit metadata-only events to a dedicated Langfuse project.

    Returns:
        Insight inputs ready for the analyzer task to persist.
    """
    context = _get_context(sessions, agent_name=agent_name)
    normalized = [
        SessionWithNodesResponse(session=item.session, nodes=item.nodes)
        for item in sessions
    ]
    generator = None
    if model is not None:
        from kitaru.insights.openai_generator import OpenAIInsightGenerator

        generator = OpenAIInsightGenerator()
    result = await generate_insights(
        normalized,
        context=context,
        config=InsightGenerationConfig(
            model=ModelGenerationConfig(model=model) if model is not None else None
        ),
        generator=generator,
        observer=_get_observer(observe),
    )
    return result.insights


__all__ = ["analyze_post_import_sessions"]
