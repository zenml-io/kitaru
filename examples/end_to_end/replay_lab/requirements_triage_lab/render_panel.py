"""Render a crisp, audience-ready verdict panel from a Replay Lab report.

This is the demo's payoff screen: one glance shows how many cases are safe to
switch to the cheaper model and which are held, with the dropped requirement
named in plain English. Every number comes from the JSON report, not hardcoded.

Run:
    uv run python examples/end_to_end/replay_lab/requirements_triage_lab/render_panel.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_REPORT = (
    Path(__file__).parent / "reports" / "requirements-triage-replay-lab-demo.json"
)


def _scorecard(lane: dict[str, Any]) -> dict[str, Any]:
    return (lane or {}).get("metrics", {}).get("scorecard", {}) or {}


def _pct(base: float, cand: float) -> str:
    if not base:
        return "n/a"
    return f"{(cand - base) / base * 100:+.0f}%"


def render(report_path: Path) -> str:
    """Build the verdict panel from a Replay Lab JSON report."""
    data = json.loads(report_path.read_text(encoding="utf-8"))
    cases = data.get("cases", [])
    candidates = data.get("summary", {}).get("candidates", {})
    cand_id = next(iter(candidates), "candidate")
    aggregate = candidates.get(cand_id, {}).get("aggregate_verdict", "unknown")

    rows = []
    for case in cases:
        results = case.get("candidate_results") or []
        if not results:
            continue
        cand_sc = _scorecard(results[0].get("lane", {}))
        base_sc = _scorecard(case.get("lanes", {}).get("baseline_replay", {}))
        rows.append(
            {
                "case": case.get("case_id", "unknown"),
                "verdict": results[0].get("verdict", "unknown"),
                "base_q": float(base_sc.get("quality_score", 0)),
                "cand_q": float(cand_sc.get("quality_score", 0)),
                "base_cost": float(base_sc.get("cost_usd", 0)),
                "cand_cost": float(cand_sc.get("cost_usd", 0)),
                "missing": cand_sc.get("missing_required_terms", []) or [],
                "risky": cand_sc.get("risky_terms", []) or [],
            }
        )

    held = [r for r in rows if r["verdict"] == "hold"]
    caution = [r for r in rows if r["verdict"] == "caution"]
    ship = [r for r in rows if r["verdict"] == "ship"]

    bar = "=" * 66
    out: list[str] = [
        "",
        bar,
        "  REPLAY LAB  —  is the cheaper model safe to switch to?",
        bar,
        "",
        f"  Replayed {len(rows)} requirements-triage cases against the cheaper model.",
        "",
        f"  SHIP     {len(ship):>2}   safe to switch",
        f"  CAUTION  {len(caution):>2}   cheaper, behavior changed — review",
        f"  HOLD     {len(held):>2}   do NOT switch — a requirement was dropped",
        "",
        f"  Overall recommendation for `{cand_id}`:  {aggregate.upper()}",
    ]

    if held:
        out += ["", "  Held cases — what the cheaper model dropped:", ""]
        for r in held:
            dropped = ", ".join(r["missing"]) or "a required check"
            risky = ", ".join(r["risky"])
            out.append(f"  HOLD  {r['case']}")
            out.append(
                f"        Dropped requirement: {dropped}."
                + (f"  Unsafe phrasing: \"{risky}\"." if risky else "")
            )
            out.append(
                f"        Quality {r['base_q']:.2f} -> {r['cand_q']:.2f} "
                f"({_pct(r['base_q'], r['cand_q'])}), "
                f"cost {_pct(r['base_cost'], r['cand_cost'])}."
            )
            out.append("")

    if caution:
        out += ["  Cost wins, no quality loss (review before switching):", ""]
        for r in caution:
            out.append(
                f"  CAUTION  {r['case']}  —  cost {_pct(r['base_cost'], r['cand_cost'])}, "
                f"quality {_pct(r['base_q'], r['cand_q'])}"
            )
        out.append("")

    # Business summary — numbers from the actual run.
    saved = [r for r in rows if r["cand_cost"] < r["base_cost"]]
    avg_save = (
        sum(_safe_ratio(r) for r in saved) / len(saved) * 100 if saved else 0
    )
    headline = (
        f"  IN PLAIN ENGLISH:  Tested {len(rows)} requirements cases against a "
        f"cheaper model that costs ~{avg_save:.0f}% less. "
    )
    if held:
        first = held[0]
        dropped = ", ".join(first["missing"]) or "a required check"
        headline += (
            f"{len(held)} held back — including the load case where it dropped "
            f"the '{dropped}' requirement. Nothing unsafe was shipped."
        )
    else:
        headline += "No requirements were dropped."
    out += [bar, headline, bar, ""]
    return "\n".join(out)


def _safe_ratio(r: dict[str, Any]) -> float:
    return (r["base_cost"] - r["cand_cost"]) / r["base_cost"] if r["base_cost"] else 0


def main() -> int:
    """Render the panel for the default (or given) report path."""
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_REPORT
    if not path.exists():
        print(f"No report at {path}. Run run_replay_lab.py first.")
        return 1
    print(render(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
