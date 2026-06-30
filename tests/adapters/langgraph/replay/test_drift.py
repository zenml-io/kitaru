from kitaru.adapters.langgraph.replay._drift import (
    DriftReport,
    compare_decisions,
)


def test_compare_flags_changed_field_only():
    base = {
        "policy_label": "billing_policy",
        "risk_status": "safe",
        "tool_names": ["a"],
        "summary": "long text x",
    }
    cand = {
        "policy_label": "billing_policy",
        "risk_status": "unsafe",
        "tool_names": ["a"],
        "summary": "totally different text y",
    }
    comps = compare_decisions(base, cand)
    by_field = {c.field: c for c in comps}
    assert by_field["risk_status"].matches is False
    assert by_field["policy_label"].matches is True
    # free-text summary is never compared
    assert "summary" not in by_field


def test_drift_report_flags():
    base = {"risk_status": "safe", "tool_names": ["a"]}
    same = {"risk_status": "safe", "tool_names": ["a"]}
    drifted = {"risk_status": "unsafe", "tool_names": ["a"]}
    report = DriftReport(
        reproduction=compare_decisions(base, same),
        fork=compare_decisions(base, drifted),
    )
    assert report.has_reproduction_drift is False
    assert report.has_fork_drift is True
