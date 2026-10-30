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
"""Analysis DTO conversions."""

from kitaru.api_models.v1.analysis import AnalysisCreateRequest
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.rest.mapping.analyzer_config import analyzer_config_input
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.job import AnalysisCreate
from kitaru.server.filtering import FilterCondition, FilterExpression


def analysis_create_to_command(body: AnalysisCreateRequest) -> AnalysisCreate:
    """Convert an analysis create request to its command.

    Each scope field that is set becomes one session filter condition.

    Args:
        body: Analysis create request.

    Returns:
        Analysis create command.
    """
    conditions: list[FilterExpression] = []
    if body.session_ids is not None:
        conditions.append(
            FilterCondition(field="id", op=FilterOp.IN, value=body.session_ids)
        )
    for field, op, value in (
        ("tag", FilterOp.EQ, body.tag),
        ("cohort_version_id", FilterOp.EQ, body.cohort_version_id),
        ("import_id", FilterOp.EQ, body.import_id),
        ("started_at", FilterOp.GE, body.started_after),
        ("started_at", FilterOp.LT, body.started_before),
    ):
        if value is not None:
            conditions.append(FilterCondition(field=field, op=op, value=value))
    if body.filter is not None:
        conditions.append(filter_to_expression(body.filter))
    return AnalysisCreate(
        agent_id=body.agent_id,
        scope=tuple(conditions),
        import_id=body.import_id,
        analyzers=[analyzer_config_input(config) for config in body.analyzers],
    )
