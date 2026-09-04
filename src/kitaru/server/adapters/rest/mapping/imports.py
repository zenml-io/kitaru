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

from kitaru.api_models.v1.imports import (
    ImportCreateRequest,
    ImportListParams,
    ImportResponse,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.adapters.rest.mapping.replay_config import (
    evaluator_config_input,
    evaluator_config_to_wire,
)
from kitaru.server.application.models.imports import ImportCreate, ImportFilter
from kitaru.server.domain.imports import Import


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
        evaluators=[evaluator_config_input(config) for config in body.evaluators],
    )


def import_to_response(import_: Import) -> ImportResponse:
    """Convert an import to its response DTO.

    Args:
        import_: Stored import.

    Returns:
        Import response.
    """
    assert import_.created is not None
    assert import_.updated is not None
    return ImportResponse(
        id=import_.id,
        owner_id=import_.owner_id,
        job_id=import_.job_id,
        agent_id=import_.agent_id,
        agent_version_id=import_.agent_version_id,
        importer_version_id=import_.importer_version_id,
        payload_blob_id=import_.payload_blob_id,
        params=import_.params,
        evaluators=[
            evaluator_config_to_wire(evaluator) for evaluator in import_.evaluators
        ],
        stats=import_.stats,
        error=import_.error,
        created=import_.created,
        updated=import_.updated,
    )


def import_list_params_to_filter(params: ImportListParams) -> ImportFilter:
    """Convert import list params to the application filter.

    Args:
        params: Import list params.

    Returns:
        Import filter.
    """
    return ImportFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )
