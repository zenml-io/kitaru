"""Render a Replay Lab JSON report as a small static HTML file."""

from __future__ import annotations

import argparse
import json
from html import escape
from pathlib import Path
from typing import Any

DEFAULT_REPORT_JSON = Path(__file__).parent / "reports" / "support-replay-lab-demo.json"
DEFAULT_HTML = Path(__file__).parent / "reports" / "support-replay-lab-demo.html"


def _fmt(value: Any, *, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3g}{suffix}"
    return f"{value}{suffix}"


def _fmt_delta(delta: dict[str, Any]) -> str:
    absolute = delta.get("absolute")
    percent = delta.get("percent")
    if absolute is None:
        return "n/a"
    if percent is None:
        return f"{absolute:+.3g}"
    return f"{absolute:+.3g} ({percent:+.1f}%)"


def _case_recommendation(report: dict[str, Any]) -> str:
    changed = int(report.get("summary", {}).get("changed_output_count", 0))
    drift = int(report.get("summary", {}).get("replay_drift_warning_count", 0))
    cases = report.get("cases", [])
    cost_wins = 0
    quality_losses = 0
    for case in cases:
        effect = case.get("candidate_effect", {})
        cost_delta = effect.get("cost", {}).get("absolute")
        quality_delta = effect.get("quality_score", {}).get("absolute")
        if isinstance(cost_delta, int | float) and cost_delta < 0:
            cost_wins += 1
        if isinstance(quality_delta, int | float) and quality_delta < 0:
            quality_losses += 1

    if drift:
        return "Investigate replay drift before trusting the candidate decision."
    if quality_losses or changed:
        return (
            "Candidate is cheaper, but review changed or lower-quality outputs "
            "before shipping."
        )
    if cost_wins:
        return (
            "Candidate looks promising: cheaper on replay without obvious quality loss."
        )
    return "No clear candidate win yet; inspect the case details."


def _lane_cell(case: dict[str, Any], lane_name: str, metric: str) -> str:
    lane = case["lanes"][lane_name]
    return escape(_fmt(lane["metrics"].get(metric)))


def build_html_report(report: dict[str, Any]) -> str:
    """Build a standalone HTML report from a Replay Lab JSON payload."""
    title = f"Replay Lab Report: {report['name']}"
    candidate = report.get("candidate", {})
    recommendation = _case_recommendation(report)
    summary = report.get("summary", {})

    case_rows = []
    detail_sections = []
    for case in report.get("cases", []):
        changed = case.get("output_changed_vs_baseline")
        changed_text = "unknown" if changed is None else "yes" if changed else "no"
        effect = case.get("candidate_effect", {})
        case_rows.append(
            "<tr>"
            f"<td>{escape(case['case_id'])}</td>"
            f"<td>{_lane_cell(case, 'observed', 'cost')}</td>"
            f"<td>{_lane_cell(case, 'baseline_replay', 'cost')}</td>"
            f"<td>{_lane_cell(case, 'candidate_replay', 'cost')}</td>"
            f"<td>{escape(_fmt_delta(effect.get('cost', {})))}</td>"
            f"<td>{escape(_fmt_delta(effect.get('quality_score', {})))}</td>"
            f"<td>{escape(changed_text)}</td>"
            "</tr>"
        )

        lane_rows = []
        for lane_name in ("observed", "baseline_replay", "candidate_replay"):
            lane = case["lanes"][lane_name]
            metrics = lane["metrics"]
            lane_rows.append(
                "<tr>"
                f"<td>{escape(lane_name)}</td>"
                f"<td><code>{escape(str(lane.get('exec_id') or 'n/a'))}</code></td>"
                f"<td>{escape(str(lane.get('status')))}</td>"
                f"<td>{escape(_fmt(metrics.get('cost')))}</td>"
                f"<td>{escape(_fmt(metrics.get('latency_seconds'), suffix='s'))}</td>"
                f"<td>{escape(_fmt(metrics.get('quality_score')))}</td>"
                f"<td>{escape(str(metrics.get('tool_call_count') or 'n/a'))}</td>"
                "</tr>"
            )

        limitations = "".join(
            f"<li>{escape(item)}</li>" for item in case.get("limitations", [])
        )
        limitation_block = (
            f"<h4>Warnings and limitations</h4><ul>{limitations}</ul>"
            if limitations
            else ""
        )
        detail_sections.append(
            "<section class='case-card'>"
            f"<h3>{escape(case['case_id'])}</h3>"
            f"<p><strong>Why this case is in the cohort:</strong> "
            f"{escape(case.get('reason', 'n/a'))}</p>"
            "<table>"
            "<thead><tr><th>Lane</th><th>Execution</th><th>Status</th>"
            "<th>Cost</th><th>Latency</th><th>Quality</th><th>Tools</th>"
            "</tr></thead>"
            f"<tbody>{''.join(lane_rows)}</tbody>"
            "</table>"
            f"{limitation_block}"
            "</section>"
        )

    case_count = escape(str(summary.get("case_count", 0)))
    candidate_completed = escape(str(summary.get("candidate_completed_count", 0)))
    changed_outputs = escape(str(summary.get("changed_output_count", 0)))
    drift_warnings = escape(str(summary.get("replay_drift_warning_count", 0)))

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{ color-scheme: light; font-family: Inter, ui-sans-serif, system-ui; }}
    body {{ margin: 0; background: #f7f4ed; color: #171717; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 40px 24px 64px; }}
    .hero {{ background: #111827; color: white; border-radius: 28px; padding: 32px; }}
    .hero p {{ color: #d1d5db; max-width: 780px; }}
    .pill {{ display: inline-block; background: #f59e0b; color: #111827;
      border-radius: 999px; padding: 6px 12px; font-weight: 700; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
      margin: 24px 0; }}
    .metric, .case-card {{ background: white; border: 1px solid #e5e7eb;
      border-radius: 20px; padding: 20px; box-shadow: 0 10px 24px #11182714; }}
    .metric strong {{ display: block; font-size: 28px; margin-top: 6px; }}
    table {{ width: 100%; border-collapse: collapse; background: white;
      border-radius: 16px; overflow: hidden; margin: 18px 0 28px; }}
    th, td {{ text-align: left; padding: 12px 14px; border-bottom: 1px solid #eee; }}
    th {{ background: #ede9fe; font-size: 13px; text-transform: uppercase; }}
    code {{ background: #f3f4f6; padding: 2px 5px; border-radius: 6px; }}
    .recommendation {{ font-size: 22px; line-height: 1.35; }}
    @media (max-width: 800px) {{ .grid {{ grid-template-columns: 1fr 1fr; }} }}
  </style>
</head>
<body>
<main>
  <section class="hero">
    <span class="pill">Replay Lab demo</span>
    <h1>{escape(title)}</h1>
    <p class="recommendation">{escape(recommendation)}</p>
    <p>Candidate: <strong>{escape(candidate.get("label", "n/a"))}</strong></p>
    <p>{escape(candidate.get("notes") or "")}</p>
  </section>

  <section class="grid" aria-label="summary metrics">
    <div class="metric">Cases<strong>{case_count}</strong></div>
    <div class="metric">Candidate completed<strong>{candidate_completed}</strong></div>
    <div class="metric">Changed outputs<strong>{changed_outputs}</strong></div>
    <div class="metric">Replay drift warnings<strong>{drift_warnings}</strong></div>
  </section>

  <h2>Observed vs baseline replay vs candidate replay</h2>
  <table>
    <thead><tr><th>Case</th><th>Observed cost</th><th>Baseline cost</th>
    <th>Candidate cost</th><th>Candidate cost Δ</th><th>Quality Δ</th>
    <th>Output changed?</th></tr></thead>
    <tbody>{"".join(case_rows)}</tbody>
  </table>

  <h2>Case-level details</h2>
  {"".join(detail_sections)}
</main>
</body>
</html>
"""


def render_html_report(json_path: Path, output_path: Path) -> Path:
    """Render ``json_path`` to ``output_path`` and return the output path."""
    report = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("Replay Lab JSON report must contain an object.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(build_html_report(report), encoding="utf-8")
    return output_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json-path",
        type=Path,
        default=DEFAULT_REPORT_JSON,
        help="Replay Lab JSON report path.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_HTML,
        help="Static HTML output path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Render the report and print the generated path."""
    args = parse_args(argv)
    output_path = render_html_report(args.json_path, args.output_path)
    print(f"Wrote HTML report: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
