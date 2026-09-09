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
"""Replay configuration DTO conversions, shared by experiments and replays."""

from kitaru.api_models.v1.replay_config import HistoryConfig as WireHistoryConfig
from kitaru.api_models.v1.replay_config import LLMConfig as WireLLMConfig
from kitaru.api_models.v1.replay_config import (
    PassthroughConfig as WirePassthroughConfig,
)
from kitaru.api_models.v1.replay_config import ReplayOverride as WireReplayOverride
from kitaru.api_models.v1.replay_config import StaticCase as WireStaticCase
from kitaru.api_models.v1.replay_config import StaticConfig as WireStaticConfig
from kitaru.api_models.v1.replay_config import ToolConfig as WireToolConfig
from kitaru.api_models.v1.replay_config import ToolPolicy as WireToolPolicy
from kitaru.server.domain.replay_config import HistoryConfig as DomainHistoryConfig
from kitaru.server.domain.replay_config import LLMConfig as DomainLLMConfig
from kitaru.server.domain.replay_config import (
    PassthroughConfig as DomainPassthroughConfig,
)
from kitaru.server.domain.replay_config import ReplayOverride as DomainReplayOverride
from kitaru.server.domain.replay_config import StaticCase as DomainStaticCase
from kitaru.server.domain.replay_config import StaticConfig as DomainStaticConfig
from kitaru.server.domain.replay_config import ToolConfig as DomainToolConfig
from kitaru.server.domain.replay_config import ToolPolicy as DomainToolPolicy


def replay_override_to_domain(override: WireReplayOverride) -> DomainReplayOverride:
    """Convert a wire replay override to its domain value object.

    Args:
        override: Wire replay override.

    Returns:
        Domain replay override.
    """
    return DomainReplayOverride(
        model=override.model,
        system_prompt=override.system_prompt,
        prompt=override.prompt,
        model_params=override.model_params,
    )


def replay_override_to_wire(override: DomainReplayOverride) -> WireReplayOverride:
    """Convert a domain replay override to its wire value object.

    Args:
        override: Domain replay override.

    Returns:
        Wire replay override.
    """
    return WireReplayOverride(
        model=override.model,
        system_prompt=override.system_prompt,
        prompt=override.prompt,
        model_params=override.model_params,
    )


def _static_case_to_domain(case: WireStaticCase) -> DomainStaticCase:
    return DomainStaticCase(
        match=case.match, match_mode=case.match_mode, result=case.result
    )


def _static_case_to_wire(case: DomainStaticCase) -> WireStaticCase:
    return WireStaticCase(
        match=case.match, match_mode=case.match_mode, result=case.result
    )


def tool_config_to_domain(config: WireToolConfig) -> DomainToolConfig:
    """Convert a wire tool config to its domain value object.

    Args:
        config: Wire tool config.

    Returns:
        Domain tool config.
    """
    if isinstance(config, WirePassthroughConfig):
        return DomainPassthroughConfig()
    if isinstance(config, WireHistoryConfig):
        return DomainHistoryConfig(scope=config.scope, on_miss=config.on_miss)
    if isinstance(config, WireStaticConfig):
        return DomainStaticConfig(
            cases=[_static_case_to_domain(case) for case in config.cases],
            on_miss=config.on_miss,
        )
    return DomainLLMConfig(model=config.model, instructions=config.instructions)


def tool_config_to_wire(config: DomainToolConfig) -> WireToolConfig:
    """Convert a domain tool config to its wire value object.

    Args:
        config: Domain tool config.

    Returns:
        Wire tool config.
    """
    if isinstance(config, DomainPassthroughConfig):
        return WirePassthroughConfig()
    if isinstance(config, DomainHistoryConfig):
        return WireHistoryConfig(scope=config.scope, on_miss=config.on_miss)
    if isinstance(config, DomainStaticConfig):
        return WireStaticConfig(
            cases=[_static_case_to_wire(case) for case in config.cases],
            on_miss=config.on_miss,
        )
    return WireLLMConfig(model=config.model, instructions=config.instructions)


def tool_policy_to_domain(policy: WireToolPolicy) -> DomainToolPolicy:
    """Convert a wire tool policy to its domain value object.

    Args:
        policy: Wire tool policy.

    Returns:
        Domain tool policy.
    """
    return DomainToolPolicy(
        default=tool_config_to_domain(policy.default),
        tools={
            name: tool_config_to_domain(config) for name, config in policy.tools.items()
        },
    )


def tool_policy_to_wire(policy: DomainToolPolicy) -> WireToolPolicy:
    """Convert a domain tool policy to its wire value object.

    Args:
        policy: Domain tool policy.

    Returns:
        Wire tool policy.
    """
    return WireToolPolicy(
        default=tool_config_to_wire(policy.default),
        tools={
            name: tool_config_to_wire(config) for name, config in policy.tools.items()
        },
    )
