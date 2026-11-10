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
from kitaru.server.adapters.rest.mapping.analyzer_config import analyzer_config_input
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.job import AnalysisCreate


def analysis_create_to_command(body: AnalysisCreateRequest) -> AnalysisCreate:
    """Convert an analysis create request to its command.

    Args:
        body: Analysis create request.

    Returns:
        Analysis create command.
    """
    return AnalysisCreate(
        agent_id=body.agent_id,
        filter=None if body.filter is None else filter_to_expression(body.filter),
        analyzers=[analyzer_config_input(config) for config in body.analyzers],
    )
