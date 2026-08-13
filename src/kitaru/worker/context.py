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
"""Shared runtime dependencies for task execution."""

from dataclasses import dataclass

from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker.blob_cache import BlobCache


@dataclass
class ExecutionContext:
    """Execution context."""

    client: KitaruAPIClient
    blob_cache: BlobCache
    payload_cache: BlobCache
