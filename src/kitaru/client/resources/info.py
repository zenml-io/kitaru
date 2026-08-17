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
"""Info SDK resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.info import ServerInfoResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class InfoResource:
    """Info API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def get(self) -> ServerInfoResponse:
        """Get the server info.

        Raises:
            APIError: The request failed.

        Returns:
            Server info.
        """
        response = await self._client.request("GET", "/api/v1/info", authenticate=False)
        return ServerInfoResponse.model_validate(response.json())
