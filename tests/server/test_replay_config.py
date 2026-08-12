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
"""Tests for the replay config domain module."""

import uuid

import pytest

from kitaru.api_models.v1.replay_config import HistoryScope, ToolPolicyOnMiss
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    PassthroughConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
    default_tool_policy,
    effective_inputs,
)


def test_default_tool_policy_passes_every_tool_through() -> None:
    """Build a passthrough default with no per-tool overrides."""
    policy = default_tool_policy()
    assert policy.default == PassthroughConfig()
    assert policy.tools == {}


def test_effective_inputs_no_override() -> None:
    """Leave inputs unchanged with no override."""
    assert effective_inputs({"prompt": "hi"}, None) == {"prompt": "hi"}


def test_effective_inputs_no_prompt_fields() -> None:
    """Leave inputs unchanged when the override carries no prompt fields."""
    override = ReplayOverride(model="openai:gpt-5")
    assert effective_inputs({"prompt": "hi"}, override) == {"prompt": "hi"}


def test_effective_inputs_dict_replaces_system_prompt_key() -> None:
    """Replace a dict's system prompt key without a prompt override."""
    override = ReplayOverride(system_prompt="be terse")
    result = effective_inputs({"prompt": "hi", "system_prompt": "old"}, override)
    assert result == {"prompt": "hi", "system_prompt": "be terse"}


def test_effective_inputs_dict_replaces_prompt_key() -> None:
    """Replace a dict's prompt key, keeping the other keys."""
    override = ReplayOverride(prompt="new prompt")
    result = effective_inputs({"prompt": "hi", "temperature": 0.5}, override)
    assert result == {"prompt": "new prompt", "temperature": 0.5}


def test_effective_inputs_dict_replaces_prompt_and_system_prompt() -> None:
    """Replace both the prompt and system prompt keys of a dict."""
    override = ReplayOverride(prompt="new prompt", system_prompt="new system")
    result = effective_inputs({"prompt": "hi", "system_prompt": "old"}, override)
    assert result == {"prompt": "new prompt", "system_prompt": "new system"}


def test_effective_inputs_plain_value_replaced_by_prompt() -> None:
    """Replace a non-dict input wholesale with the override prompt."""
    override = ReplayOverride(prompt="new prompt")
    assert effective_inputs("hi", override) == "new prompt"


def test_effective_inputs_plain_value_with_prompt_and_system_prompt() -> None:
    """Wrap a non-dict input into a dict carrying both override fields."""
    override = ReplayOverride(prompt="new prompt", system_prompt="new system")
    result = effective_inputs("hi", override)
    assert result == {"prompt": "new prompt", "system_prompt": "new system"}


def test_effective_inputs_plain_value_with_system_prompt_only() -> None:
    """Wrap a non-dict input into a dict, keeping it as the prompt."""
    override = ReplayOverride(system_prompt="new system")
    result = effective_inputs("hi", override)
    assert result == {"prompt": "hi", "system_prompt": "new system"}


def _config(tool_policy: ToolPolicy) -> ReplayConfig:
    return ReplayConfig(
        owner_id=uuid.uuid4(),
        tool_policy=tool_policy,
        evaluators=[
            EvaluatorConfig(
                evaluator="accuracy",
                version=1,
                evaluator_version_id=uuid.uuid4(),
            )
        ],
    )


def test_evaluator_config_accepts_default_plugin_name() -> None:
    """Accept the reserved namespace used by server-provided evaluators."""
    config = EvaluatorConfig(
        evaluator="kitaru/cost",
        version=1,
        evaluator_version_id=uuid.uuid4(),
    )

    assert config.evaluator == "kitaru/cost"


def test_check_standalone_allows_passthrough() -> None:
    """Accept a standalone replay with a passthrough default."""
    config = _config(default_tool_policy())
    config.check_standalone()


def test_check_standalone_allows_baseline_history() -> None:
    """Accept a standalone replay with baseline-scoped history."""
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL
        )
    )
    config = _config(policy)
    config.check_standalone()


def test_check_standalone_rejects_cohort_version_history_on_default() -> None:
    """Reject a standalone replay whose default config scopes to cohort version."""
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.COHORT_VERSION, on_miss=ToolPolicyOnMiss.FAIL
        )
    )
    config = _config(policy)
    with pytest.raises(ValidationError, match="cohort-version-scoped history"):
        config.check_standalone()


def test_check_standalone_rejects_cohort_version_history_on_named_tool() -> None:
    """Reject a standalone replay whose per-tool config scopes to cohort version."""
    policy = ToolPolicy(
        default=PassthroughConfig(),
        tools={
            "search": HistoryConfig(
                scope=HistoryScope.COHORT_VERSION, on_miss=ToolPolicyOnMiss.FAIL
            )
        },
    )
    config = _config(policy)
    with pytest.raises(ValidationError, match="cohort-version-scoped history"):
        config.check_standalone()
