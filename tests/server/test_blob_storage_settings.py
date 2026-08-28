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
"""Tests for blob storage settings validation."""

import pytest
from pydantic import ValidationError

from kitaru.server.blob_storage_settings import (
    BlobStorageSettings,
    S3BlobStorageSettings,
)
from kitaru.server.domain.blob import BlobStorageBackend


def test_database_backend_needs_no_s3_settings() -> None:
    """Allow the database backend with no s3 sub-model set."""
    settings = BlobStorageSettings()
    assert settings.backend == BlobStorageBackend.DATABASE
    assert settings.s3 is None


def test_s3_backend_requires_s3_settings() -> None:
    """Reject the s3 backend with no s3 sub-model set."""
    with pytest.raises(ValidationError, match="KITARU_SERVER_BLOB_STORAGE__S3__BUCKET"):
        BlobStorageSettings(backend=BlobStorageBackend.S3)


def test_s3_backend_with_s3_settings() -> None:
    """Allow the s3 backend with a matching s3 sub-model set."""
    settings = BlobStorageSettings(
        backend=BlobStorageBackend.S3,
        s3=S3BlobStorageSettings(bucket="kitaru-blobs"),
    )
    assert settings.s3 is not None
    assert settings.s3.bucket == "kitaru-blobs"
