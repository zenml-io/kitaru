"""Unit tests for _agent.py hardening guards (no flow execution, no primed_zenml).

Tests:
* _decision_of_result raises RuntimeError on empty node_outputs.
* _decision_of_result raises RuntimeError when nodes are present but none carry
  a recognized decision key.
* _variant_proxy emits warnings when raw_config is missing model/prompt_profile/
  variant_name.
"""

from __future__ import annotations

import warnings

import pytest

from kitaru.adapters.langgraph.replay._agent import (
    _decision_of_result,
    _ReplayResult,
    _variant_proxy,
)

# --------------------------------------------------------------------------- #
# Fix 1 — loud guard on empty node_outputs
# --------------------------------------------------------------------------- #


def test_decision_of_result_raises_on_empty_node_outputs() -> None:
    """Empty node_outputs must raise RuntimeError, not silently return {}."""
    result = _ReplayResult(exec_id="fake-exec-id", node_outputs={})
    with pytest.raises(RuntimeError, match="empty"):
        _decision_of_result(result)


def test_decision_of_result_raises_when_no_decision_node_present() -> None:
    """Non-empty node_outputs that lack any decision node must also raise."""
    result = _ReplayResult(
        exec_id="fake-exec-id-2",
        node_outputs={
            "receive_request": {"tool_executions": []},
            "collect_evidence_with_tools": {"tool_executions": [{"name": "lookup"}]},
            # Neither decide_action nor final_response is present.
        },
    )
    with pytest.raises(RuntimeError, match="decision nodes"):
        _decision_of_result(result)


def test_decision_of_result_raises_when_decision_key_empty() -> None:
    """Decision nodes present but with empty/None decision must raise."""
    result = _ReplayResult(
        exec_id="fake-exec-id-3",
        node_outputs={
            "decide_action": {"decision": None},
            "final_response": {"decision": {}},
        },
    )
    with pytest.raises(RuntimeError, match="decide_action"):
        _decision_of_result(result)


def test_decision_of_result_happy_path_does_not_raise() -> None:
    """Sanity: a result with a valid decision must NOT raise."""
    result = _ReplayResult(
        exec_id="fake-exec-id-ok",
        node_outputs={
            "decide_action": {
                "decision": {
                    "risk_status": "safe",
                    "required_action": "escalate_to_human",
                }
            }
        },
    )
    decision = _decision_of_result(result)
    assert decision["risk_status"] == "safe"


# --------------------------------------------------------------------------- #
# Fix 3 — warnings on missing trace config fields
# --------------------------------------------------------------------------- #


def test_variant_proxy_warns_on_missing_model() -> None:
    raw_config = {"prompt_profile": "full_permissions", "variant_name": "baseline"}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        proxy = _variant_proxy(raw_config)
    assert any("model" in str(warning.message) for warning in w), (
        "Expected a warning mentioning 'model'"
    )
    assert proxy.model == "gpt-5-mini"  # default applied


def test_variant_proxy_warns_on_missing_prompt_profile() -> None:
    raw_config = {"model": "gpt-4o", "variant_name": "baseline"}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        proxy = _variant_proxy(raw_config)
    assert any("prompt_profile" in str(warning.message) for warning in w)
    assert proxy.prompt_profile == "full_permissions"  # default applied


def test_variant_proxy_warns_on_missing_variant_name() -> None:
    raw_config = {"model": "gpt-4o", "prompt_profile": "full_permissions"}
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        proxy = _variant_proxy(raw_config)
    assert any("variant_name" in str(warning.message) for warning in w)
    assert proxy.name == "baseline"  # default applied


def test_variant_proxy_no_warning_when_all_fields_present() -> None:
    raw_config = {
        "model": "gpt-4o",
        "prompt_profile": "full_permissions",
        "variant_name": "my_variant",
    }
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        proxy = _variant_proxy(raw_config)
    # No missing-field warnings should have been emitted.
    missing_field_warnings = [
        warning
        for warning in w
        if "missing field" in str(warning.message).lower()
        or "raw_config" in str(warning.message).lower()
    ]
    assert missing_field_warnings == []
    assert proxy.model == "gpt-4o"
    assert proxy.name == "my_variant"
