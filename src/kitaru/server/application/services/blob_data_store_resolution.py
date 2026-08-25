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
"""Blob data store resolution by backend."""

from kitaru.server.application.interfaces.blob_data_store import BlobDataStore
from kitaru.server.domain.blob import BlobStorageBackend


def resolve_blob_data_store(
    data_stores: dict[BlobStorageBackend, BlobDataStore], backend: BlobStorageBackend
) -> BlobDataStore:
    """Look up the data store configured for a backend.

    Args:
        data_stores: Content stores keyed by the backend they serve.
        backend: Backend to resolve a store for.

    Raises:
        RuntimeError: No data store is configured for the backend.

    Returns:
        Store configured for the backend.
    """
    store = data_stores.get(backend)
    if store is None:
        raise RuntimeError(f"No data store configured for backend {backend}")
    return store
