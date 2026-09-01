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
from kitaru.server.domain.agent_version import RuntimeCapabilities
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    PassthroughConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
    default_tool_policy,
)


def test_default_tool_policy_passes_every_tool_through() -> None:
    """Build a passthrough default with no per-tool overrides."""
    policy = default_tool_policy()
    assert policy.default == PassthroughConfig()
    assert policy.tools == {}


def _config(
    tool_policy: ToolPolicy, override: ReplayOverride | None = None
) -> ReplayConfig:
    return ReplayConfig(
        owner_id=uuid.uuid4(),
        override=override,
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


def test_check_capabilities_allows_override_and_tool_policy() -> None:
    """Accept an override and a non-passthrough policy under full capabilities."""
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL
        )
    )
    config = _config(policy, override=ReplayOverride(model="gpt-5"))
    config.check_capabilities(RuntimeCapabilities())


def test_check_capabilities_rejects_an_override() -> None:
    """Reject an override when the capabilities exclude overrides."""
    config = _config(default_tool_policy(), override=ReplayOverride(model="gpt-5"))
    with pytest.raises(ValidationError, match="does not support replay overrides"):
        config.check_capabilities(RuntimeCapabilities(overrides=False))


def test_check_capabilities_allows_a_null_override() -> None:
    """Accept a config without an override when the capabilities exclude overrides."""
    config = _config(default_tool_policy())
    config.check_capabilities(RuntimeCapabilities(overrides=False))


def test_check_capabilities_rejects_a_non_passthrough_default() -> None:
    """Reject a non-passthrough default config when policies are excluded."""
    policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL
        )
    )
    config = _config(policy)
    with pytest.raises(ValidationError, match="does not support replay tool policies"):
        config.check_capabilities(RuntimeCapabilities(tool_policies=False))


def test_check_capabilities_rejects_a_non_passthrough_named_tool() -> None:
    """Reject a non-passthrough per-tool config when policies are excluded."""
    policy = ToolPolicy(
        default=PassthroughConfig(),
        tools={
            "search": HistoryConfig(
                scope=HistoryScope.BASELINE, on_miss=ToolPolicyOnMiss.FAIL
            )
        },
    )
    config = _config(policy)
    with pytest.raises(ValidationError, match="does not support replay tool policies"):
        config.check_capabilities(RuntimeCapabilities(tool_policies=False))


def test_check_capabilities_allows_all_passthrough() -> None:
    """Accept an all-passthrough policy when the capabilities exclude policies."""
    config = _config(default_tool_policy())
    config.check_capabilities(RuntimeCapabilities(tool_policies=False))
