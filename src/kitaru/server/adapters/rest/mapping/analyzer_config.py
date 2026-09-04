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
"""Analyzer config DTO conversions."""

from kitaru.api_models.v1.replay_config import AnalyzerConfig as WireAnalyzerConfig
from kitaru.server.application.models.replay_config import AnalyzerConfigInput
from kitaru.server.domain.replay_config import AnalyzerConfig as DomainAnalyzerConfig


def analyzer_config_to_wire(config: DomainAnalyzerConfig) -> WireAnalyzerConfig:
    """Convert a resolved domain analyzer config to its wire value object.

    Args:
        config: Resolved domain analyzer config.

    Returns:
        Wire analyzer config, echoing the resolved name, version, and params.
    """
    return WireAnalyzerConfig(
        analyzer=config.analyzer, version=config.version, params=config.params
    )


def analyzer_config_input(config: WireAnalyzerConfig) -> AnalyzerConfigInput:
    """Convert a wire analyzer config to its unresolved application input.

    Args:
        config: Wire analyzer config.

    Returns:
        Analyzer config awaiting resolution.
    """
    return AnalyzerConfigInput(
        analyzer=config.analyzer, version=config.version, params=config.params
    )
