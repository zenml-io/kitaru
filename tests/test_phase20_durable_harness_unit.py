"""Unit tests for durable harness helpers."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "examples" / "durable_harness"))

from harness import _parse_evaluation, _strip_code_fences
from models import EvaluationReport, HarnessResult, ReviewDecision


class TestParseEvaluation:
    def test_valid_json(self):
        raw = json.dumps(
            {
                "passed": True,
                "feedback": "All good",
                "criteria_met": 4,
                "criteria_total": 4,
            }
        )
        report = _parse_evaluation(raw)
        assert report.passed is True
        assert report.criteria_met == 4

    def test_json_in_markdown_fences(self):
        raw = (
            "```json\n"
            '{"passed": false, "feedback": "bad",'
            ' "criteria_met": 1, "criteria_total": 3}\n'
            "```"
        )
        report = _parse_evaluation(raw)
        assert report.passed is False
        assert report.criteria_met == 1

    def test_fallback_pass(self):
        raw = "The code looks great! PASS. Everything works."
        report = _parse_evaluation(raw)
        assert report.passed is True

    def test_fallback_fail(self):
        raw = "FAIL: Missing weather widget."
        report = _parse_evaluation(raw)
        assert report.passed is False

    def test_fallback_ambiguous_defaults_to_fail(self):
        raw = "Some things PASS but others FAIL."
        report = _parse_evaluation(raw)
        assert report.passed is False  # "FAIL" present → not passed


class TestStripCodeFences:
    def test_no_fences(self):
        assert _strip_code_fences("<html></html>") == "<html></html>"

    def test_html_fences(self):
        raw = "```html\n<html></html>\n```"
        assert _strip_code_fences(raw) == "<html></html>"

    def test_bare_fences(self):
        raw = "```\n<html></html>\n```"
        assert _strip_code_fences(raw) == "<html></html>"

    def test_fences_with_trailing_whitespace(self):
        raw = "```html\n<html></html>\n```  \n"
        assert _strip_code_fences(raw) == "<html></html>"


class TestModels:
    def test_evaluation_report(self):
        report = EvaluationReport(
            passed=False,
            feedback="Missing features",
            criteria_met=2,
            criteria_total=4,
        )
        assert not report.passed

    def test_review_decision_defaults(self):
        d = ReviewDecision(action="approve")
        assert d.feedback == ""

    def test_review_decision_with_feedback(self):
        d = ReviewDecision(action="revise", feedback="Make it darker")
        assert d.action == "revise"

    def test_harness_result_passed(self):
        r = HarnessResult(
            code="<html></html>",
            spec="spec",
            rounds_completed=2,
            outcome="passed",
        )
        assert r.passed is True
        assert r.rounds_completed == 2

    def test_harness_result_not_passed(self):
        r = HarnessResult(
            code="",
            spec="spec",
            rounds_completed=1,
            outcome="aborted_by_user",
        )
        assert r.passed is False
