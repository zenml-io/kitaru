"""First-insight milestone analytics."""

from kitaru.analytics.events import AnalyticsEvent
from kitaru.analytics.source import analytics_event_context
from kitaru.server.application.interfaces.insight_repository import InsightRepository
from kitaru.server.application.services.analytics_events import (
    build_insight_created_properties,
)
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.insight import Insight


async def track_first_insight_generated(
    repository: InsightRepository,
    insights: list[Insight],
    owner: Account,
    analytics: ServerAnalytics | None,
) -> None:
    """Track a batch containing a human account's first insight.

    Args:
        repository: Insight repository with the batch already persisted.
        insights: Newly stored insights.
        owner: Account owning the insights.
        analytics: Analytics tracker, None skips tracking.
    """
    if analytics is None or not insights or owner.is_service_account:
        return
    first_id = await repository.find_first_id_by_owner(owner.id)
    first = next((insight for insight in insights if insight.id == first_id), None)
    if first is None:
        return
    # A worker may be reporting on behalf of a different account.
    with analytics_event_context(
        control_plane_user_id=owner.external_id, service_account=False
    ):
        analytics.track(
            owner.external_id or owner.id,
            AnalyticsEvent.FIRST_INSIGHT_GENERATED,
            {**build_insight_created_properties(first), "email": owner.email},
        )
