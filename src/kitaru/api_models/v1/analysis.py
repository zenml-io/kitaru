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
"""Analysis API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.plugin import AnalyzerConfig


class AnalysisCreateRequest(RequestModel):
    """Analysis create request.

    Without a filter, every finished session of the agent is analyzed.
    """

    agent_id: uuid.UUID = Field(description="Agent whose sessions are analyzed.")
    analyzers: list[AnalyzerConfig] = Field(
        min_length=1, description="Analyzers run across the selected sessions."
    )
    filter: Filter | None = Field(
        default=None, description="Only sessions matching this session filter."
    )
