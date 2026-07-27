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
"""Blob DTO conversions."""

from kitaru.api_models.v1.blobs import BlobResponse
from kitaru.server.domain.blob import Blob


def blob_to_response(blob: Blob) -> BlobResponse:
    """Convert a blob entity to its response DTO.

    Args:
        blob: Stored blob.

    Returns:
        Blob response.
    """
    assert blob.created is not None
    return BlobResponse(
        id=blob.id,
        sha256=blob.sha256,
        size=blob.size,
        media_type=blob.media_type,
        created=blob.created,
    )
