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
"""Blob API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel


class BlobResponse(ResponseModel):
    """Blob response."""

    id: uuid.UUID = Field(description="Blob id.")
    sha256: str = Field(description="Hash of the content.")
    size: int = Field(description="Content size in bytes.")
    media_type: str = Field(description="Media type of the content.")
    created: datetime = Field(description="Creation time.")
