"""Shared HTML renderer for Replay Lab verdict reports."""

# The renderer deliberately contains long HTML/CSS string fragments.
# ruff: noqa: E501

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

REPLAY_DRIFT_QUALITY_THRESHOLD = 0.10
EFFICIENCY_WIN_THRESHOLD = 0.10
MAX_CHANGED_OUTPUT_CHARS = 2500


@dataclass(frozen=True)
class CandidateVerdict:
    """Aggregate verdict for one candidate across all cases."""

    candidate_id: str
    label: str
    verdict: str
    completed_count: int
    changed_output_count: int
    average_cost: float | None
    average_latency_seconds: float | None
    average_quality_score: float | None
    efficiency_win_count: int
    quality_loss_count: int
    cases_to_inspect: list[str]


@dataclass(frozen=True)
class ReportVerdict:
    """Renderer-level verdict summary."""

    overall: str
    trust_label: str
    trust_detail: str
    candidates: list[CandidateVerdict]


def render_html_report(
    json_path: Path,
    output_path: Path,
    *,
    replay_drift_quality_threshold: float = REPLAY_DRIFT_QUALITY_THRESHOLD,
    efficiency_win_threshold: float = EFFICIENCY_WIN_THRESHOLD,
) -> Path:
    """Render a Replay Lab JSON report to a standalone HTML file."""
    report = load_report(json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_html_report(
            report,
            replay_drift_quality_threshold=replay_drift_quality_threshold,
            efficiency_win_threshold=efficiency_win_threshold,
        ),
        encoding="utf-8",
    )
    return output_path


def load_report(json_path: Path) -> dict[str, Any]:
    """Load a Replay Lab JSON report object from disk."""
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Replay Lab JSON report must contain an object.")
    return payload


def build_html_report(
    report: Mapping[str, Any],
    *,
    replay_drift_quality_threshold: float = REPLAY_DRIFT_QUALITY_THRESHOLD,
    efficiency_win_threshold: float = EFFICIENCY_WIN_THRESHOLD,
) -> str:
    """Build a standalone HTML report from a plural-schema Replay Lab payload."""
    verdict = build_report_verdict(
        report,
        replay_drift_quality_threshold=replay_drift_quality_threshold,
        efficiency_win_threshold=efficiency_win_threshold,
    )
    title = f"Replay Lab Report: {report.get('name', 'Untitled comparison')}"
    candidate_cards = "".join(_candidate_card(item) for item in verdict.candidates)
    evidence_rows = "".join(
        _evidence_row(index, item)
        for index, item in enumerate(verdict.candidates, start=1)
    )
    case_rows = "".join(_case_summary_row(case) for case in _cases(report))
    case_sections = "".join(_case_detail_section(case) for case in _cases(report))
    summary = _mapping(report.get("summary"))

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{ color-scheme: light; font-family: Inter, ui-sans-serif, system-ui; }}
    body {{ margin: 0; background: #f7f4ed; color: #171717; }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 40px 24px 64px; }}
    .hero {{ background: #111827; color: white; border-radius: 28px; padding: 32px; }}
    .hero p {{ color: #d1d5db; max-width: 860px; }}
    .pill {{ display: inline-block; border-radius: 999px; padding: 6px 12px; font-weight: 800; }}
    .pill.ship {{ background: #bbf7d0; color: #064e3b; }}
    .pill.caution {{ background: #fde68a; color: #78350f; }}
    .pill.hold {{ background: #fecaca; color: #7f1d1d; }}
    .pill.trust {{ background: #dbeafe; color: #1e3a8a; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 24px 0; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; margin: 20px 0 30px; }}
    .metric, .card, .case-card {{ background: white; border: 1px solid #e5e7eb; border-radius: 20px; padding: 20px; box-shadow: 0 10px 24px #11182714; }}
    .metric strong {{ display: block; font-size: 28px; margin-top: 6px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 16px; overflow: hidden; margin: 18px 0 28px; }}
    th, td {{ text-align: left; vertical-align: top; padding: 12px 14px; border-bottom: 1px solid #eee; }}
    th {{ background: #ede9fe; font-size: 13px; text-transform: uppercase; }}
    code {{ background: #f3f4f6; padding: 2px 5px; border-radius: 6px; }}
    pre {{ white-space: pre-wrap; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 14px; padding: 14px; max-height: 260px; overflow: auto; }}
    .recommendation {{ font-size: 22px; line-height: 1.35; }}
    .muted {{ color: #64748b; }}
    .section-score {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .section-score span {{ border-radius: 999px; padding: 4px 8px; background: #f1f5f9; }}
    @media (max-width: 800px) {{ .grid {{ grid-template-columns: 1fr 1fr; }} }}
  </style>
</head>
<body>
<main>
  <section class="hero">
    <span class="pill trust">{escape(verdict.trust_label)}</span>
    <h1>{escape(title)}</h1>
    <p class="recommendation">{escape(verdict.overall)}</p>
    <p>{escape(verdict.trust_detail)}</p>
  </section>

  <section class="grid" aria-label="summary metrics">
    <div class="metric">Cases<strong>{escape(str(summary.get("case_count", len(_cases(report)))))}</strong></div>
    <div class="metric">Candidates<strong>{escape(str(summary.get("candidate_count", len(verdict.candidates))))}</strong></div>
    <div class="metric">Replay drift warnings<strong>{escape(str(summary.get("replay_drift_warning_count", 0)))}</strong></div>
    <div class="metric">Failed/timed-out lanes<strong>{escape(str(summary.get("failed_or_timed_out_lane_count", 0)))}</strong></div>
  </section>

  <h2>Per-candidate verdicts</h2>
  <section class="cards">{candidate_cards}</section>

  <h2>Candidate decision evidence</h2>
  <p class="section-note">This order is evidence for this replay cohort, not a universal model leaderboard.</p>
  <table>
    <thead><tr><th>Evidence order</th><th>Candidate</th><th>Verdict</th><th>Efficiency wins</th><th>Quality losses</th><th>Inspect</th></tr></thead>
    <tbody>{evidence_rows}</tbody>
  </table>

  <h2>Case summary</h2>
  <table>
    <thead><tr><th>Case</th><th>Replay trust</th><th>Candidate</th><th>Cost Δ</th><th>Latency Δ</th><th>Quality Δ</th><th>Changed?</th><th>Verdict</th></tr></thead>
    <tbody>{case_rows}</tbody>
  </table>

  <h2>Case-level details</h2>
  {case_sections}
</main>
</body>
</html>
"""


def build_report_verdict(
    report: Mapping[str, Any],
    *,
    replay_drift_quality_threshold: float = REPLAY_DRIFT_QUALITY_THRESHOLD,
    efficiency_win_threshold: float = EFFICIENCY_WIN_THRESHOLD,
) -> ReportVerdict:
    """Read canonical aggregate verdicts from the report.

    New reports already contain the decision. The renderer should display that
    decision, not quietly invent a different one. The only recomputation below is
    an explicit legacy adapter for old demo JSON that predates canonical summary
    verdicts/trust fields.
    """
    summary = _mapping(report.get("summary"))
    candidates = _canonical_candidate_verdicts(summary)
    trust = _mapping(summary.get("replay_trust"))
    trust_label = str(trust.get("label", ""))
    trust_detail = str(trust.get("detail", ""))
    overall = str(summary.get("overall_recommendation", ""))
    if candidates and trust_label and trust_detail and overall:
        return ReportVerdict(
            overall=overall,
            trust_label=trust_label,
            trust_detail=trust_detail,
            candidates=candidates,
        )

    return _legacy_report_verdict(
        report,
        replay_drift_quality_threshold=replay_drift_quality_threshold,
        efficiency_win_threshold=efficiency_win_threshold,
    )


def case_has_high_replay_drift(
    case: Mapping[str, Any],
    *,
    replay_drift_quality_threshold: float = REPLAY_DRIFT_QUALITY_THRESHOLD,
) -> bool:
    """Return whether observed vs baseline replay drift is high for one case."""
    canonical_warning = case.get("replay_drift_warning")
    if isinstance(canonical_warning, bool):
        return canonical_warning
    quality_delta = _delta_absolute(
        _mapping(case.get("replay_drift")).get("quality_score")
    )
    if (
        quality_delta is not None
        and abs(quality_delta) >= replay_drift_quality_threshold
    ):
        return True

    observed_signature = _drift_signature(
        _mapping(
            _mapping(_mapping(case.get("lanes")).get("observed")).get("metrics")
        ).get("evaluation")
    )
    baseline_signature = _drift_signature(
        _mapping(
            _mapping(_mapping(case.get("lanes")).get("baseline_replay")).get("metrics")
        ).get("evaluation")
    )
    if observed_signature is None or baseline_signature is None:
        return False
    return observed_signature != baseline_signature


def _canonical_candidate_verdicts(summary: Mapping[str, Any]) -> list[CandidateVerdict]:
    evidence = summary.get(
        "candidate_decision_evidence", summary.get("candidate_ranking", [])
    )
    if not isinstance(evidence, list):
        return []
    verdicts: list[CandidateVerdict] = []
    for item in evidence:
        item_map = _mapping(item)
        candidate_id = item_map.get("candidate_id")
        if not candidate_id:
            return []
        verdicts.append(
            CandidateVerdict(
                candidate_id=str(candidate_id),
                label=str(item_map.get("label", candidate_id)),
                verdict=str(item_map.get("aggregate_verdict", "hold")),
                completed_count=int(item_map.get("completed_count", 0) or 0),
                changed_output_count=int(item_map.get("changed_output_count", 0) or 0),
                average_cost=_number(item_map.get("average_cost")),
                average_latency_seconds=_number(
                    item_map.get("average_latency_seconds")
                ),
                average_quality_score=_number(item_map.get("average_quality_score")),
                efficiency_win_count=int(item_map.get("efficiency_win_count", 0) or 0),
                quality_loss_count=int(item_map.get("quality_loss_count", 0) or 0),
                cases_to_inspect=[
                    str(case_id)
                    for case_id in item_map.get("cases_to_inspect", []) or []
                ],
            )
        )
    return verdicts


def _legacy_report_verdict(
    report: Mapping[str, Any],
    *,
    replay_drift_quality_threshold: float,
    efficiency_win_threshold: float,
) -> ReportVerdict:
    """Adapt old demo JSON that lacks canonical summary verdict fields."""
    candidates = [
        _legacy_candidate_verdict(report, candidate, efficiency_win_threshold)
        for candidate in _candidate_descriptors(report)
    ]
    candidates = sorted(candidates, key=_candidate_rank_key)
    drift_cases = [
        str(case.get("case_id", "unknown"))
        for case in _cases(report)
        if case_has_high_replay_drift(
            case,
            replay_drift_quality_threshold=replay_drift_quality_threshold,
        )
    ]
    failed_count = int(
        _mapping(report.get("summary")).get("failed_or_timed_out_lane_count", 0) or 0
    )
    if drift_cases:
        trust_label = "Replay trust: inspect first"
        trust_detail = (
            "High replay drift was detected for "
            f"{', '.join(drift_cases)}. Treat candidate ordering as directional "
            "until those baseline replays are understood."
        )
    elif failed_count:
        trust_label = "Replay trust: partial"
        trust_detail = (
            "One or more lanes failed or timed out, so the report can still teach "
            "you where to look but should not be treated as complete evidence."
        )
    else:
        trust_label = "Replay trust: steady"
        trust_detail = (
            "Observed and baseline replay lanes stayed within the configured drift "
            "threshold for this cohort."
        )

    if drift_cases or failed_count:
        overall = "Hold: inspect replay reliability before using this comparison as shipping evidence."
    else:
        shippable = [
            candidate for candidate in candidates if candidate.verdict == "ship"
        ]
        if shippable:
            overall = (
                f"Ship candidate `{shippable[0].candidate_id}` for a guarded trial: "
                "safe enough from this replay cohort, not a blind deployment."
            )
        elif any(candidate.verdict == "caution" for candidate in candidates):
            overall = "Caution: at least one candidate is promising, but inspect the named cases before changing production traffic."
        else:
            overall = "Hold: no candidate produced enough efficiency gain without quality risk in this cohort."

    return ReportVerdict(
        overall=overall,
        trust_label=trust_label,
        trust_detail=trust_detail,
        candidates=list(candidates),
    )


def _legacy_candidate_verdict(
    report: Mapping[str, Any],
    candidate: Mapping[str, Any],
    efficiency_win_threshold: float,
) -> CandidateVerdict:
    candidate_id = str(candidate.get("id", "unknown"))
    summary = _mapping(_mapping(report.get("summary")).get("candidates")).get(
        candidate_id
    )
    results = [
        result
        for case in _cases(report)
        for result in _candidate_results(case)
        if str(result.get("candidate_id")) == candidate_id
    ]
    efficiency_wins = sum(
        1 for result in results if _has_efficiency_win(result, efficiency_win_threshold)
    )
    quality_losses = sum(1 for result in results if _has_quality_loss(result))
    changed_cases = [
        str(case.get("case_id", "unknown"))
        for case in _cases(report)
        for result in _candidate_results(case)
        if str(result.get("candidate_id")) == candidate_id
        and result.get("output_changed_vs_baseline") is True
    ]
    caution_cases = [
        str(case.get("case_id", "unknown"))
        for case in _cases(report)
        for result in _candidate_results(case)
        if str(result.get("candidate_id")) == candidate_id
        and str(result.get("verdict", "caution")) != "ship"
    ]
    cases_to_inspect = _dedupe([*changed_cases, *caution_cases])
    failed_count = int(summary.get("failed_or_timed_out_lane_count", 0) or 0)
    completed_count = int(summary.get("completed_count", 0) or 0)

    if failed_count or not completed_count or quality_losses:
        verdict = "hold"
    elif cases_to_inspect:
        verdict = "caution"
    elif efficiency_wins:
        verdict = "ship"
    else:
        verdict = "caution"

    return CandidateVerdict(
        candidate_id=candidate_id,
        label=str(candidate.get("label", candidate_id)),
        verdict=verdict,
        completed_count=completed_count,
        changed_output_count=int(summary.get("changed_output_count", 0) or 0),
        average_cost=_number(summary.get("average_cost")),
        average_latency_seconds=_number(summary.get("average_latency_seconds")),
        average_quality_score=_number(summary.get("average_quality_score")),
        efficiency_win_count=efficiency_wins,
        quality_loss_count=quality_losses,
        cases_to_inspect=cases_to_inspect,
    )


def _candidate_card(item: CandidateVerdict) -> str:
    inspect = ", ".join(item.cases_to_inspect) if item.cases_to_inspect else "none"
    return (
        "<article class='card'>"
        f"<span class='pill {escape(item.verdict)}'>{escape(item.verdict)}</span>"
        f"<h3>{escape(item.label)}</h3>"
        f"<p><code>{escape(item.candidate_id)}</code></p>"
        f"<p>Completed cases: <strong>{item.completed_count}</strong></p>"
        f"<p>Changed outputs: <strong>{item.changed_output_count}</strong></p>"
        f"<p>Average quality: <strong>{escape(_fmt(item.average_quality_score))}</strong></p>"
        f"<p>Inspect: {escape(inspect)}</p>"
        "</article>"
    )


def _evidence_row(index: int, item: CandidateVerdict) -> str:
    inspect = ", ".join(item.cases_to_inspect) if item.cases_to_inspect else "none"
    return (
        "<tr>"
        f"<td>{index}</td>"
        f"<td><strong>{escape(item.label)}</strong><br><code>{escape(item.candidate_id)}</code></td>"
        f"<td><span class='pill {escape(item.verdict)}'>{escape(item.verdict)}</span></td>"
        f"<td>{item.efficiency_win_count}</td>"
        f"<td>{item.quality_loss_count}</td>"
        f"<td>{escape(inspect)}</td>"
        "</tr>"
    )


def _case_summary_row(case: Mapping[str, Any]) -> str:
    case_id = str(case.get("case_id", "unknown"))
    trust = _case_trust_status(case)
    rows = []
    for result in _candidate_results(case):
        effect = _mapping(result.get("effect_vs_baseline"))
        rows.append(
            "<tr>"
            f"<td>{escape(case_id)}</td>"
            f"<td>{escape(trust)}</td>"
            f"<td>{escape(str(result.get('candidate_label', result.get('candidate_id', 'unknown'))))}</td>"
            f"<td>{escape(_fmt_delta(effect.get('cost')))}</td>"
            f"<td>{escape(_fmt_delta(effect.get('latency_seconds')))}</td>"
            f"<td>{escape(_fmt_delta(effect.get('quality_score')))}</td>"
            f"<td>{escape(_fmt_bool(result.get('output_changed_vs_baseline')))}</td>"
            f"<td>{escape(str(result.get('verdict', 'caution')))}</td>"
            "</tr>"
        )
    return "".join(rows)


def _case_detail_section(case: Mapping[str, Any]) -> str:
    lanes = _mapping(case.get("lanes"))
    lane_rows = "".join(
        _lane_row(name, _mapping(lanes.get(name)))
        for name in ("observed", "baseline_replay")
    )
    candidate_lane_rows = "".join(
        _lane_row(
            f"candidate:{result.get('candidate_id', 'unknown')}",
            _mapping(result.get("lane")),
        )
        for result in _candidate_results(case)
    )
    changed_blocks = "".join(
        _changed_output_block(case, result) for result in _candidate_results(case)
    )
    evaluator_blocks = "".join(
        _evaluator_block(f"{name} evaluator", _mapping(lane).get("metrics"))
        for name, lane in lanes.items()
    ) + "".join(
        _evaluator_block(
            f"candidate:{result.get('candidate_id', 'unknown')} evaluator",
            _mapping(_mapping(result.get("lane")).get("metrics")),
        )
        for result in _candidate_results(case)
    )
    limitations = [str(item) for item in case.get("limitations", []) or []]
    limitation_block = (
        "<h4>Warnings and limitations</h4><ul>"
        + "".join(f"<li>{escape(item)}</li>" for item in limitations)
        + "</ul>"
        if limitations
        else ""
    )
    return (
        "<section class='case-card'>"
        f"<h3>{escape(str(case.get('case_id', 'unknown')))}</h3>"
        f"<p><strong>Why this case is in the cohort:</strong> {escape(str(case.get('reason', 'n/a')))}</p>"
        "<table><thead><tr><th>Lane</th><th>Execution</th><th>Status</th><th>Cost</th><th>Latency</th><th>Quality</th><th>LLM calls</th></tr></thead>"
        f"<tbody>{lane_rows}{candidate_lane_rows}</tbody></table>"
        f"{changed_blocks}{evaluator_blocks}{limitation_block}"
        "</section>"
    )


def _lane_row(name: str, lane: Mapping[str, Any]) -> str:
    metrics = _mapping(lane.get("metrics"))
    return (
        "<tr>"
        f"<td>{escape(name)}</td>"
        f"<td><code>{escape(str(lane.get('exec_id') or 'n/a'))}</code></td>"
        f"<td>{escape(str(lane.get('status', 'unknown')))}</td>"
        f"<td>{escape(_fmt(metrics.get('cost')))}</td>"
        f"<td>{escape(_fmt(metrics.get('latency_seconds'), suffix='s'))}</td>"
        f"<td>{escape(_fmt(metrics.get('quality_score')))}</td>"
        f"<td>{escape(_fmt(metrics.get('llm_call_count')))}</td>"
        "</tr>"
    )


def _changed_output_block(case: Mapping[str, Any], result: Mapping[str, Any]) -> str:
    if result.get("output_changed_vs_baseline") is not True:
        return ""
    baseline_output = _mapping(
        _mapping(_mapping(case.get("lanes")).get("baseline_replay")).get("metrics")
    ).get("output_text")
    candidate_output = _mapping(_mapping(result.get("lane")).get("metrics")).get(
        "output_text"
    )
    return (
        f"<h4>Changed output: {escape(str(result.get('candidate_id', 'unknown')))}</h4>"
        "<div class='cards'>"
        f"<article><h5>Baseline replay</h5><pre>{escape(_truncated_output(baseline_output))}</pre></article>"
        f"<article><h5>Candidate replay</h5><pre>{escape(_truncated_output(candidate_output))}</pre></article>"
        "</div>"
    )


def _truncated_output(value: Any) -> str:
    text = str(value or "n/a")
    if len(text) <= MAX_CHANGED_OUTPUT_CHARS:
        return text
    omitted = len(text) - MAX_CHANGED_OUTPUT_CHARS
    return f"{text[:MAX_CHANGED_OUTPUT_CHARS]}\n\n[… truncated {omitted} characters in HTML; JSON report is complete …]"


def _evaluator_block(title: str, metrics_value: Any) -> str:
    metrics = _mapping(metrics_value)
    evaluation = _mapping(metrics.get("evaluation"))
    if not evaluation:
        return ""
    scorecard = _mapping(evaluation.get("scorecard"))
    chips = "".join(
        f"<span>{escape(key)}: {escape(_fmt_bool(scorecard.get(key)))}</span>"
        for key in sorted(scorecard)
    )
    limitations = [str(item) for item in evaluation.get("limitations", []) or []]
    limitation_text = "; ".join(limitations) if limitations else "none"
    return (
        f"<h4>{escape(title)}</h4>"
        f"<p>Evaluator: <code>{escape(str(evaluation.get('evaluator_id', 'n/a')))}</code>; "
        f"score: <strong>{escape(_fmt(evaluation.get('quality_score')))}</strong>; "
        f"limitations: {escape(limitation_text)}</p>"
        f"<div class='section-score'>{chips}</div>"
    )


def _case_trust_status(case: Mapping[str, Any]) -> str:
    trust = _mapping(case.get("replay_trust"))
    status = trust.get("status")
    if isinstance(status, str) and status:
        return status
    return "inspect" if case_has_high_replay_drift(case) else "steady"


def _candidate_descriptors(report: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    candidates = report.get("candidates", [])
    if isinstance(candidates, list):
        return [_mapping(candidate) for candidate in candidates]
    return []


def _cases(report: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    cases = report.get("cases", [])
    if isinstance(cases, list):
        return [_mapping(case) for case in cases]
    return []


def _candidate_results(case: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    results = case.get("candidate_results", [])
    if isinstance(results, list):
        return [_mapping(result) for result in results]
    return []


def _has_efficiency_win(result: Mapping[str, Any], threshold: float) -> bool:
    effect = _mapping(result.get("effect_vs_baseline"))
    for key in ("cost", "latency_seconds"):
        percent = _delta_percent(effect.get(key))
        if percent is not None and percent <= -(threshold * 100):
            return True
    return False


def _has_quality_loss(result: Mapping[str, Any]) -> bool:
    quality_delta = _delta_absolute(
        _mapping(result.get("effect_vs_baseline")).get("quality_score")
    )
    return quality_delta is not None and quality_delta < -REPLAY_DRIFT_QUALITY_THRESHOLD


def _drift_signature(evaluation: Any) -> Any | None:
    evaluation_map = _mapping(evaluation)
    if "drift_signature" not in evaluation_map:
        return None
    return evaluation_map["drift_signature"]


def _candidate_rank_key(item: CandidateVerdict) -> tuple[int, int, int, str]:
    verdict_rank = {"ship": 0, "caution": 1, "hold": 2}.get(item.verdict, 3)
    return (
        verdict_rank,
        item.quality_loss_count,
        -item.efficiency_win_count,
        item.candidate_id,
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _delta_absolute(value: Any) -> float | None:
    return _number(_mapping(value).get("absolute"))


def _delta_percent(value: Any) -> float | None:
    return _number(_mapping(value).get("percent"))


def _fmt(value: Any, *, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3g}{suffix}"
    return f"{value}{suffix}"


def _fmt_delta(delta: Any) -> str:
    delta_map = _mapping(delta)
    absolute = _number(delta_map.get("absolute"))
    percent = _number(delta_map.get("percent"))
    if absolute is None:
        return "n/a"
    if percent is None:
        return f"{absolute:+.3g}"
    return f"{absolute:+.3g} ({percent:+.1f}%)"


def _fmt_bool(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered
