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
"""Import DTO conversions."""

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.server.application.models.imports import ImportCreate


def import_create_to_command(body: ImportCreateRequest) -> ImportCreate:
    """Convert an import create request to its command.

    Args:
        body: Import create request.

    Returns:
        Import create command.
    """
    return ImportCreate(
        importer=body.importer,
        agent_id=body.agent_id,
        agent_version_id=body.agent_version_id,
        version=body.version,
        payload_blob_id=body.payload_blob_id,
        params=body.params,
    )
