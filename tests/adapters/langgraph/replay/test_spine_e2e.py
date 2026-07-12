"""End-to-end spine: reconstruct -> reproduction replay -> fork -> diff.

The single gate: a trimmed-permission fork of a recorded "safe" permission case
must surface a semantic decision regression (risk_status / required_action),
while a no-edit reproduction replay reproduces the recorded decision exactly.
"""

from pathlib import Path

import pytest

from kitaru.adapters.langgraph.replay import KitaruReplayAgent

pytestmark = pytest.mark.skipif(
    not Path("examples/end_to_end/replay_fork_demo/reference_agent").exists(),
    reason="reference agent fixtures required",
)


def test_fork_drift_surfaces_permission_regression(
    reference_graph, permission_trace_rows, primed_zenml
) -> None:
    agent = KitaruReplayAgent(
        reference_graph, fanout_node="collect_evidence_with_tools"
    )
    case = agent.import_trace(permission_trace_rows)

    seed = agent.reconstruct(case)

    # reproduction: live tail, no edits — semantic decision reproduced.
    repro = agent.replay(seed, at="collect_evidence_with_tools")

    # fork: trim permissions (planted regression) — risk_status must drift.
    fork = agent.fork(
        seed,
        at="collect_evidence_with_tools",
        variant={"prompt_profile": "trimmed_permissions", "model": "gpt-5-nano"},
    )

    report = agent.diff(case, repro, fork)

    assert report.has_reproduction_drift is False
    assert report.has_fork_drift is True
    drifted = {c.field for c in report.fork if not c.matches}
    assert "risk_status" in drifted or "required_action" in drifted
