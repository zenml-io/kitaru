"""Import command resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.job import JobResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ImportsResource:
    """Import command API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        self._client = client

    async def create(self, request: ImportCreateRequest) -> JobResponse:
        response = await self._client.request(
            "POST",
            "/v1/imports",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())
