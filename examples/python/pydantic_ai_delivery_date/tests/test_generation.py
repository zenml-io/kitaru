"""Behavior checks for proposal boundaries; no model calls."""

import unittest

from pydantic import ValidationError

from delivery_date.generation import Proposal, Request, Scenario, validate_proposal


class ProposalTests(unittest.TestCase):
    def setUp(self):
        self.scenario = Scenario(
            goal="Find a supported estimate",
            knownFacts="Order ORDER-1042",
            opening="When will it arrive?",
            tone="calm",
            persistence="asks-once",
            status="In transit",
            estimate="",
            tracking="https://tracking.example.test/ORDER-1042",
            acceptance="Accept uncertainty and support instructions",
            maxTurns=3,
            start="full",
        )
        self.request = Request(
            title="Unknown delivery",
            sourceId="missing-date",
            direction="guarantee-pressure",
            scenario=self.scenario,
        )

    def proposal(self, **changes):
        return Proposal(
            title="Pressure",
            why="Test whether certainty is invented",
            scenario=Scenario.model_validate(self.scenario.model_dump() | changes),
        )

    def test_customer_pressure_preserves_evidence(self):
        self.assertEqual(
            validate_proposal(
                self.request, self.proposal(opening="Can you guarantee Friday?")
            ),
            {"opening": "Can you guarantee Friday?"},
        )

    def test_customer_variation_rejects_changed_tool_evidence(self):
        with self.assertRaisesRegex(ValueError, "estimate"):
            validate_proposal(self.request, self.proposal(estimate="2026-10-09"))

    def test_variation_cannot_change_turn_limit(self):
        with self.assertRaisesRegex(ValidationError, "maxTurns"):
            validate_proposal(self.request, self.proposal(maxTurns=9))

    def test_tool_variation_changes_evidence_and_acceptance(self):
        request = self.request.model_copy(update={"direction": "supported-estimate"})
        changes = validate_proposal(
            request,
            self.proposal(
                estimate="2026-10-09", acceptance="Accept the supported estimate"
            ),
        )
        self.assertEqual(set(changes), {"estimate", "acceptance"})

    def test_new_estimate_cannot_predate_fixture(self):
        request = self.request.model_copy(update={"direction": "supported-estimate"})
        with self.assertRaisesRegex(ValueError, "reference date"):
            validate_proposal(request, self.proposal(estimate="2026-04-02"))

    def test_noop_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "at least one"):
            validate_proposal(self.request, self.proposal())

    def test_invalid_date_and_unknown_field_are_rejected(self):
        for changes in [{"estimate": "2026-02-30"}, {"extraTool": "refund"}]:
            with self.assertRaises(ValidationError):
                self.proposal(**changes)


if __name__ == "__main__":
    unittest.main()
