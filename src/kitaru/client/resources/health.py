"""Health resource."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class HealthResource:
    """Health check API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def get(self) -> dict[str, str]:
        """Check database-backed service readiness."""
        response = await self._client.request("GET", "/health", authenticate=False)
        return dict(response.json())

    async def live(self) -> dict[str, str]:
        """Check process liveness."""
        response = await self._client.request("GET", "/health/live", authenticate=False)
        return dict(response.json())
