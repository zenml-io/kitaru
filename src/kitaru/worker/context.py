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
"""Worker execution context."""

from kitaru.blob_cache import BlobCache
from kitaru.client.api_client import KitaruAPIClient


class ExecutionContext:
    """Shared runtime dependencies of job execution."""

    def __init__(
        self,
        client: KitaruAPIClient,
        blob_cache: BlobCache,
        payload_cache: BlobCache,
    ) -> None:
        """Initialize the context.

        Args:
            client: API client.
            blob_cache: Cache plugin code is materialized into.
            payload_cache: Cache import payloads are materialized into.
        """
        self.client = client
        self.blob_cache = blob_cache
        self.payload_cache = payload_cache
