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
"""Blob storage backend configuration."""

from typing import Self

from pydantic import BaseModel, SecretStr, model_validator

from kitaru.server.domain.blob import BlobStorageBackend


class S3BlobStorageSettings(BaseModel):
    """S3 blob storage settings."""

    bucket: str
    prefix: str = "blobs"
    region: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None


class BlobStorageSettings(BaseModel):
    """Blob storage settings."""

    backend: BlobStorageBackend = BlobStorageBackend.DATABASE
    s3: S3BlobStorageSettings | None = None

    @model_validator(mode="after")
    def validate_backend_settings(self) -> Self:
        """Validate the settings match the selected backend.

        Raises:
            ValueError: The selected backend has no matching sub-model set.

        Returns:
            The validated settings object.
        """
        if self.backend is BlobStorageBackend.S3 and self.s3 is None:
            raise ValueError(
                "Set KITARU_SERVER_BLOB_STORAGE__S3__BUCKET when "
                "KITARU_SERVER_BLOB_STORAGE__BACKEND=s3"
            )
        return self
