"""Session run command resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.session_run import SessionRunCreateRequest

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SessionRunsResource:
    """Session run API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: SessionRunCreateRequest) -> JobResponse:
        response = await self._client.request(
            "POST",
            "/v1/session-runs",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())
