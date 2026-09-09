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
"""Evaluator config DTO conversions."""

from kitaru.api_models.v1.replay_config import EvaluatorConfig as WireEvaluatorConfig
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.domain.replay_config import EvaluatorConfig as DomainEvaluatorConfig


def evaluator_config_to_wire(config: DomainEvaluatorConfig) -> WireEvaluatorConfig:
    """Convert a resolved domain evaluator config to its wire value object.

    Args:
        config: Resolved domain evaluator config.

    Returns:
        Wire evaluator config, echoing the resolved name, version, and
        params.
    """
    return WireEvaluatorConfig(
        evaluator=config.evaluator, version=config.version, params=config.params
    )


def evaluator_config_input(config: WireEvaluatorConfig) -> EvaluatorConfigInput:
    """Convert a wire evaluator config to its unresolved application input.

    Args:
        config: Wire evaluator config.

    Returns:
        Evaluator config awaiting resolution.
    """
    return EvaluatorConfigInput(
        evaluator=config.evaluator, version=config.version, params=config.params
    )
