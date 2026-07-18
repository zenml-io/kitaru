"""Public scoring export smoke tests."""

from __future__ import annotations

import kitaru

PUBLIC_SCORING_EXPORTS = {
    "EvidenceManifest",
    "EvidenceManifestEntry",
    "ExecutionEvidence",
    "ExecutionScoreHistory",
    "GroundedCapability",
    "GroundedPolicySnapshot",
    "GroundedWorld",
    "Score",
    "ScoreAggregateReference",
    "ScoreAttemptAggregate",
    "ScoreFilter",
    "ScoreObservation",
    "ScoreObservationOutcome",
    "ScoreObservationStatus",
    "ScorerSnapshot",
    "load_score_aggregate",
    "scorer",
}


def test_public_scoring_exports_are_available() -> None:
    assert set(kitaru.__all__) >= PUBLIC_SCORING_EXPORTS
    assert all(getattr(kitaru, name) is not None for name in PUBLIC_SCORING_EXPORTS)


def test_public_scoring_exports_support_import_star() -> None:
    namespace: dict[str, object] = {}
    exec("from kitaru import *", namespace)

    assert namespace.keys() >= PUBLIC_SCORING_EXPORTS
