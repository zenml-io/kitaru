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
"""Insight DTO conversions."""

from kitaru.api_models.v1.insight import (
    InsightBatchCreateRequest,
    InsightListParams,
    InsightResponse,
    InsightUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.insight import (
    InsightCreate,
    InsightFilter,
    InsightInput,
    InsightUpdate,
)
from kitaru.server.domain.insight import Insight


def insight_batch_create_to_command(
    body: InsightBatchCreateRequest,
) -> InsightCreate:
    """Convert an insight batch create request to its application command.

    Args:
        body: Insight batch create request.

    Returns:
        Create command.
    """
    return InsightCreate(
        agent_id=body.agent_id,
        insights=[
            InsightInput(
                name=item.name,
                title=item.title,
                description=item.description,
                data=item.data,
                metadata=item.metadata,
            )
            for item in body.insights
        ],
    )


def insight_to_response(insight: Insight) -> InsightResponse:
    """Convert an insight entity to its response DTO.

    Args:
        insight: Stored insight.

    Returns:
        Insight response.
    """
    assert insight.created is not None
    assert insight.updated is not None
    return InsightResponse(
        id=insight.id,
        owner_id=insight.owner_id,
        agent_id=insight.agent_id,
        name=insight.name,
        title=insight.title,
        description=insight.description,
        data=insight.data,
        metadata=insight.metadata,
        created=insight.created,
        updated=insight.updated,
    )


def insight_list_params_to_filter(params: InsightListParams) -> InsightFilter:
    """Convert insight list params to the application filter.

    Args:
        params: Insight list params.

    Returns:
        Insight filter.
    """
    return InsightFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def insight_update_to_command(body: InsightUpdateRequest) -> InsightUpdate:
    """Convert an insight update request to its application command.

    Args:
        body: Insight update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    return InsightUpdate(**body.model_dump(exclude_unset=True))
