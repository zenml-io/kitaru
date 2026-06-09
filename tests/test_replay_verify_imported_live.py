"""Unit tests for the live PydanticAI imported-input demo path.

No live API calls: model behavior is simulated with pydantic_ai's TestModel.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, get_args

import pytest

pytest.importorskip("pydantic_ai")

from examples.replay_verify_imported_cases import (
    generate_live_cohort,
    support_copilot_live,
)
from examples.replay_verify_imported_cases import live_prompt_config as live_config
from examples.replay_verify_imported_cases.generate_live_cohort import (
    LIVE_FIXTURE_PATH,
    write_live_cohort,
)
from examples.replay_verify_imported_cases.support_copilot_demo import (
    POLICY_LABEL_VOCABULARY,
    RISK_STATUS_VOCABULARY,
)
from examples.replay_verify_imported_cases.support_copilot_live import (
    SupportCopilotLiveOutput,
    execute_case_tool,
    run_baseline_support_copilot_case_live,
)
from examples.replay_verify_imported_cases.tool_registry import SAFE_TOOL_NAMES
from pydantic import ValidationError
from pydantic_ai.models.test import TestModel

from kitaru._replay_verify_imported_models import imported_case_from_mapping
from kitaru._replay_verify_imported_runner import ImportedRunnerInvocation
from kitaru._replay_verify_imported_sources.jsonl import validate_imported_cases_jsonl

# Derived from the deterministic runner's exported vocabulary so the live
# Literal types cannot drift from support_copilot_demo without a test failure.
EXPECTED_POLICY_LABELS = set(POLICY_LABEL_VOCABULARY)
EXPECTED_RISK_STATUSES = set(RISK_STATUS_VOCABULARY)


def _case_mapping(
    *,
    case_id: str = "live-test-case",
    available_tools: list[str] | None = None,
    retrieval_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "source_ref": {"source_system": "fixture-jsonl", "source_id": f"t-{case_id}"},
        "root_input": {
            "user_message": "Which document explains refund exceptions?",
            "account_id": "acct-1",
        },
        "observed_output": {
            "policy_label": "knowledge_base_policy",
            "risk_status": "safe",
            "tool_names": sorted(available_tools or []),
            "retrieval_document_ids": [],
        },
        "recorded_calls": [],
        "trace_contract": {
            "available_tools": available_tools or [],
            "application_tool_names": available_tools or [],
        },
        "runner_contract": {
            "entrypoint": live_config.LIVE_RUNNER_ENTRYPOINT,
            "baseline_id": "support-copilot-live-v1",
            "candidate_id": "support-copilot-live-v2",
        },
        "retrieval_context": retrieval_context,
    }


def _invocation(
    case: Any,
    *,
    available_tools: tuple[str, ...],
    config: dict[str, Any] | None = None,
) -> ImportedRunnerInvocation:
    return ImportedRunnerInvocation(
        case_id=case.case_id,
        role="baseline",
        runner_id="support-copilot-live-v1",
        root_input=case.root_input,
        available_tools=available_tools,
        config=config or {},
    )


class TestLiveOutputModel:
    def test_policy_label_literal_matches_deterministic_vocabulary(self) -> None:
        annotation = SupportCopilotLiveOutput.model_fields["policy_label"].annotation
        assert set(get_args(annotation)) == EXPECTED_POLICY_LABELS

    def test_risk_status_literal_matches_deterministic_vocabulary(self) -> None:
        annotation = SupportCopilotLiveOutput.model_fields["risk_status"].annotation
        assert set(get_args(annotation)) == EXPECTED_RISK_STATUSES

    def test_invalid_policy_label_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SupportCopilotLiveOutput.model_validate(
                {
                    "policy_label": "made_up_policy",
                    "risk_status": "safe",
                    "response": "x",
                }
            )

    def test_invalid_risk_status_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SupportCopilotLiveOutput.model_validate(
                {
                    "policy_label": "support_policy",
                    "risk_status": "dangerous",
                    "response": "x",
                }
            )

    def test_tool_fields_are_not_model_output(self) -> None:
        """tool_names/retrieval_document_ids must stay code-derived."""
        assert "tool_names" not in SupportCopilotLiveOutput.model_fields
        assert "retrieval_document_ids" not in SupportCopilotLiveOutput.model_fields


class TestLiveRunnerToolCapture:
    def test_tool_capture_populates_sorted_tool_names_and_retrieval_ids(self) -> None:
        tools = ["search_knowledge_base", "lookup_subscription"]
        case = imported_case_from_mapping(
            _case_mapping(
                available_tools=tools,
                retrieval_context={
                    "query": "refund exceptions",
                    "retriever_name": "support_copilot_kb_retriever",
                    "corpus_index_version": "support-kb-2026-06-06-a",
                    "returned_document_ids": ["doc-refund-policy"],
                    "returned_chunk_ids": ["chunk-refund-policy-1"],
                },
            )
        )
        # TestModel calls every registered tool once, then emits the output.
        test_model = TestModel(
            custom_output_args={
                "policy_label": "knowledge_base_policy",
                "risk_status": "safe",
                "response": "Answered from imported documents.",
            }
        )
        output = run_baseline_support_copilot_case_live(
            case,
            _invocation(
                case,
                available_tools=tuple(tools),
                config={"model": test_model},
            ),
        )

        payload = output.payload
        assert isinstance(payload, dict)
        assert payload["policy_label"] == "knowledge_base_policy"
        assert payload["risk_status"] == "safe"
        # Sorted, code-derived from captured calls (registration order differs).
        assert payload["tool_names"] == ["lookup_subscription", "search_knowledge_base"]
        assert payload["retrieval_document_ids"] == ["doc-refund-policy"]
        assert payload["response"] == "Answered from imported documents."
        assert all(item["executed_live"] is False for item in payload["tool_results"])
        assert output.unsafe_live_execution_count == 0
        assert payload["metadata"]["live_model_call"] is True
        assert payload["metadata"]["prompt_version"] == "support-copilot-live-v1"

    def test_no_tools_yields_empty_code_derived_fields(self) -> None:
        case = imported_case_from_mapping(_case_mapping(available_tools=[]))
        test_model = TestModel(
            custom_output_args={
                "policy_label": "escalation_policy",
                "risk_status": "needs_review",
                "response": "This needs administrator review.",
            }
        )
        output = run_baseline_support_copilot_case_live(
            case,
            _invocation(case, available_tools=(), config={"model": test_model}),
        )
        assert output.payload["tool_names"] == []
        assert output.payload["retrieval_document_ids"] == []
        assert output.payload["tool_results"] == []

    def test_live_tool_results_drive_unsafe_live_execution_count(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A registry regression that executes live must be self-reported."""
        tools = ["lookup_subscription"]
        case = imported_case_from_mapping(_case_mapping(available_tools=tools))

        def _live_tool(
            tool_name: str,
            _case: Any,
            _invocation: ImportedRunnerInvocation,
        ) -> dict[str, Any]:
            return {
                "tool_name": tool_name,
                "side_effect_status": "live",
                "executed_live": True,
            }

        monkeypatch.setattr(support_copilot_live, "run_imported_tool", _live_tool)
        test_model = TestModel(
            custom_output_args={
                "policy_label": "billing_policy",
                "risk_status": "safe",
                "response": "Subscription looked up.",
            }
        )
        output = run_baseline_support_copilot_case_live(
            case,
            _invocation(
                case,
                available_tools=tuple(tools),
                config={"model": test_model},
            ),
        )
        assert output.unsafe_live_execution_count == 1

    def test_unavailable_tool_raises(self) -> None:
        case = imported_case_from_mapping(_case_mapping(available_tools=[]))
        invocation = _invocation(case, available_tools=())
        captured: list[tuple[str, dict[str, Any]]] = []
        with pytest.raises(ValueError, match="not imported as available"):
            execute_case_tool("lookup_invoice", case, invocation, captured)
        assert captured == []


class TestLiveCohortGenerator:
    def test_deterministic_fixture_parses_and_validates(self, tmp_path: Path) -> None:
        path = tmp_path / "support_copilot_live_cases.jsonl"
        rows = write_live_cohort(path, observed="deterministic")

        result = validate_imported_cases_jsonl(
            path,
            expected_runner_entrypoint=live_config.LIVE_RUNNER_ENTRYPOINT,
            allowed_tool_names=SAFE_TOOL_NAMES,
        )
        assert len(rows) == 15
        assert result.summary["case_count"] == 15
        assert result.summary["candidate_allowed_count"] == 11
        assert result.summary["stopped_count"] == 4
        assert set(result.summary["stopped_case_ids"]) == {
            "live-missing-output-stopped",
            "live-missing-tools-stopped",
            "live-unsafe-live-write-stopped",
            "live-incomplete-rag-stopped",
        }

    def test_observed_tool_names_use_sorted_canonical_order(self) -> None:
        for row in generate_live_cohort.build_live_cohort_rows():
            observed = row["observed_output"]
            if observed is None:
                continue
            assert observed["tool_names"] == sorted(observed["tool_names"]), row[
                "case_id"
            ]

    def test_permission_cases_expect_escalation(self) -> None:
        rows = generate_live_cohort.build_live_cohort_rows()
        permission_rows = [
            row
            for row in rows
            if row["labels"]["case_type"] == "permission_scope_escalation"
        ]
        assert len(permission_rows) == 3
        for row in permission_rows:
            assert row["observed_output"]["policy_label"] == "escalation_policy"
            assert row["observed_output"]["risk_status"] == "needs_review"
            assert row["trace_contract"]["available_tools"] == []

    def test_checked_in_fixture_matches_generator_output(self) -> None:
        """The committed fixture must stay in sync with the generator."""
        checked_in = [
            json.loads(line)
            for line in LIVE_FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert checked_in == generate_live_cohort.build_live_cohort_rows()

    def test_fixture_does_not_use_manifest_expected_output(self) -> None:
        for row in generate_live_cohort.build_live_cohort_rows():
            assert "expected_output" not in row
            assert "manifest_case" not in row


class TestEntrypointConsistency:
    def test_entrypoint_constants_agree(self) -> None:
        assert (
            generate_live_cohort.LIVE_RUNNER_ENTRYPOINT
            == support_copilot_live.LIVE_RUNNER_ENTRYPOINT
            == live_config.LIVE_RUNNER_ENTRYPOINT
        )

    def test_entrypoint_resolves_to_live_runner_function(self) -> None:
        module_path, _, attribute = live_config.LIVE_RUNNER_ENTRYPOINT.rpartition(".")
        module = importlib.import_module(module_path)
        assert getattr(module, attribute) is (
            support_copilot_live.run_support_copilot_case_live
        )

    def test_fixture_rows_pin_live_entrypoint(self) -> None:
        for row in generate_live_cohort.build_live_cohort_rows():
            assert (
                row["runner_contract"]["entrypoint"]
                == live_config.LIVE_RUNNER_ENTRYPOINT
            )


class TestPlantedRegression:
    def test_candidate_prompt_drops_permission_scope_rule(self) -> None:
        baseline = live_config.BASELINE_LIVE_CONFIG.prompt_text
        candidate = live_config.CANDIDATE_LIVE_CONFIG.prompt_text
        assert "Permission scope rule" in baseline
        assert "Permission scope rule" not in candidate
        assert live_config.BASELINE_LIVE_CONFIG.prompt_hash != (
            live_config.CANDIDATE_LIVE_CONFIG.prompt_hash
        )
