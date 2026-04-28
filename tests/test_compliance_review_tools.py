"""Pure tests for the compliance review example retrieval tools."""

from __future__ import annotations

import importlib

import pytest

tools = importlib.import_module("examples.end-to-end.compliance_review.tools")


def test_list_documents_returns_planned_company_documents() -> None:
    """The document catalog should expose the eight planned company docs."""
    documents = tools.list_documents()

    assert [document["id"] for document in documents] == [
        "employee_handbook",
        "it_security_policy",
        "vendor_contract_alpha",
        "vendor_contract_beta",
        "insurance_policy",
        "financial_statements_2024",
        "data_privacy_policy",
        "disaster_recovery_plan",
    ]
    assert documents[0]["title"] == "Employee Handbook"
    assert documents[1]["domain"] == "it_security"


def test_read_section_returns_exact_document_section() -> None:
    """A stable section ID should return readable section text."""
    section = tools.read_section("it_security_policy", "data_handling")

    assert "Data handling and retention" in section
    assert "central data retention schedule" in section
    assert "legal hold workflow" in section


def test_read_document_can_read_standards_by_id() -> None:
    """Standards are readable too so later prompts can compare docs to rules."""
    standard = tools.read_document("soc2_controls")

    assert "SOC 2 Type II Control Requirements" in standard
    assert "SOC2-RET-001" in standard
    assert "documented data retention schedule" in standard


def test_search_documents_finds_policy_and_standard_matches() -> None:
    """Search should deterministically find local JSON matches, no model needed."""
    results = tools.search_documents("data retention schedule SOC 2")
    result_keys = {
        (
            result["doc_id"],
            result["match_type"],
            result.get("section_id") or result.get("requirement_id"),
        )
        for result in results
    }

    assert ("it_security_policy", "section", "data_handling") in result_keys
    assert ("soc2_controls", "requirement", "SOC2-RET-001") in result_keys
    assert results == sorted(
        results,
        key=lambda result: (
            -int(result["score"]),
            str(result["doc_id"]),
            str(result.get("section_id") or result.get("requirement_id") or ""),
        ),
    )


def test_unknown_ids_raise_clear_errors() -> None:
    """Bad IDs should fail loudly with available lookup context."""
    with pytest.raises(ValueError, match="Unknown document ID 'missing_doc'"):
        tools.read_document("missing_doc")

    with pytest.raises(ValueError, match="Unknown section 'missing_section'"):
        tools.read_section("it_security_policy", "missing_section")
