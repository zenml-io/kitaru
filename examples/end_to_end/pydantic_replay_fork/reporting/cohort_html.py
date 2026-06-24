"""Render a cohort experiment as a self-contained HTML report.

Two sections: a numeric **summary** (cases, drift counts, per-metric means,
improvement verdict) and a **per-case** table (one row per production run, with
its reproduction/decision drift and metric values).

Single public function: ``write(path, report) -> path``. It reads the public
accessors on ``reporting.cohort_report.Report`` (``metric_aggregates`` /
``per_case`` / ``skipped_cases`` / ``improvement`` / ``decision_change_count`` /
``reproduction_drift_count``).
"""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from reporting.cohort_report import Report

_CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; background: #f6f7f9; color: #1c2024;
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
.wrap { max-width: 960px; margin: 0 auto; padding: 32px 24px 64px; }
h1 { font-size: 22px; margin: 0 0 4px; }
.sub { color: #6b7280; margin: 0 0 24px; font-size: 13px; }
.verdict { border-radius: 12px; padding: 16px 20px; margin-bottom: 24px; font-weight: 600; }
.verdict.good { background: #e9f7ec; color: #1e6b34; border: 1px solid #bfe6c8; }
.verdict.bad { background: #fdecec; color: #8a1c1c; border: 1px solid #f3c0c0; }
.cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
.stat { background: #fff; border: 1px solid #e6e8eb; border-radius: 12px; padding: 14px 16px; }
.stat .n { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }
.stat .l { color: #8a9099; font-size: 12px; text-transform: uppercase; letter-spacing: .05em; }
.card { background: #fff; border: 1px solid #e6e8eb; border-radius: 12px;
  padding: 18px 20px; margin-bottom: 20px; }
.card h2 { font-size: 12px; letter-spacing: .08em; text-transform: uppercase;
  color: #8a9099; margin: 0 0 14px; }
table { width: 100%; border-collapse: collapse; }
td, th { padding: 8px 10px; text-align: left; vertical-align: top; font-size: 13px; }
th { color: #6b7280; font-weight: 600; border-bottom: 1px solid #e6e8eb; }
tr + tr td { border-top: 1px solid #f0f1f3; }
.num { font-variant-numeric: tabular-nums; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; color: #4b5563; }
.arrow { color: #b0b6bd; padding: 0 4px; }
.tag { display: inline-block; font-size: 11px; padding: 1px 8px; border-radius: 999px; font-weight: 600; }
.tag.ok { background: #e9f7ec; color: #1e6b34; }
.tag.bad { background: #fdecec; color: #8a1c1c; }
.tag.warn { background: #fef3c7; color: #92590a; }
.tag.muted { background: #f1f2f4; color: #6b7280; }
"""


def _fmt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.3f}"
    return html.escape(str(value))


def _delta(baseline: Any, variant: Any) -> str:
    if not isinstance(baseline, (int, float)) or not isinstance(variant, (int, float)):
        return "—"
    diff = variant - baseline
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:,.3f}"


def _yes_no_tag(value: bool, *, good_when: bool) -> str:
    cls = "ok" if value is good_when else "bad"
    return f'<span class="tag {cls}">{"yes" if value else "no"}</span>'


def render(report: Report) -> str:
    """Render the full HTML string from a cohort ``Report``."""
    cases = report.per_case()
    skipped = report.skipped_cases()
    aggregates = report.metric_aggregates()
    metric_names = [agg.name for agg in aggregates]

    verdict_cls = "good" if report.improvement else "bad"
    verdict_text = (
        "Improvement — no metric regressed across the cohort."
        if report.improvement
        else "Not an improvement — at least one metric regressed."
    )

    stats = [
        (len(cases), "cases run"),
        (report.skipped, "skipped"),
        (report.reproduction_drift_count, "reproduction drift"),
        (report.decision_change_count, "edit flipped decision"),
    ]
    stat_cards = "".join(
        f'<div class="stat"><div class="n">{n}</div><div class="l">{html.escape(label)}</div></div>'
        for n, label in stats
    )

    # Aggregate metric table.
    agg_rows = ""
    for agg in aggregates:
        direction = "↓ lower better" if agg.lower_is_better else "↑ higher better"
        worse_tag = (
            '<span class="tag bad">worse</span>'
            if agg.is_worse
            else '<span class="tag ok">no worse</span>'
        )
        agg_rows += (
            f"<tr><td>{html.escape(agg.name)}</td>"
            f'<td class="num">{_fmt(agg.baseline_value)}</td>'
            f'<td class="arrow">→</td>'
            f'<td class="num">{_fmt(agg.variant_value)}</td>'
            f'<td class="num">{_delta(agg.baseline_value, agg.variant_value)}</td>'
            f"<td>{html.escape(direction)}</td><td>{worse_tag}</td></tr>"
        )

    # Per-case table.
    metric_headers = "".join(
        f"<th>{html.escape(n)} (base→var)</th>" for n in metric_names
    )
    case_rows = ""
    for case in cases:
        metric_cells = ""
        for name in metric_names:
            m = case["metrics"].get(name)
            if m is None:
                metric_cells += '<td class="num">—</td>'
                continue
            worse = m["worse"]
            cls = "bad" if worse else "ok"
            metric_cells += (
                f'<td class="num"><span class="tag {cls}">'
                f"{_fmt(m['baseline'])} → {_fmt(m['variant'])}</span></td>"
            )
        case_rows += (
            f'<tr><td class="mono">{html.escape(case["exec_id"][:12])}…</td>'
            f"<td>{_yes_no_tag(case['reproduction_faithful'], good_when=True)}</td>"
            f"<td>{_yes_no_tag(case['decision_changed'], good_when=False)}</td>"
            f"{metric_cells}</tr>"
        )
    if not case_rows:
        case_rows = f'<tr><td colspan="{3 + len(metric_names)}">no cases ran</td></tr>'

    skipped_block = ""
    if skipped:
        skipped_rows = "".join(
            f'<tr><td class="mono">{html.escape(s["exec_id"][:12])}…</td>'
            f"<td>{html.escape(s['reason'])}</td></tr>"
            for s in skipped
        )
        skipped_block = (
            '<div class="card"><h2>Skipped cases</h2>'
            f"<table><tr><th>exec id</th><th>reason</th></tr>{skipped_rows}</table></div>"
        )

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cohort experiment</title>
<style>{_CSS}</style></head>
<body><div class="wrap">
<h1>Cohort experiment</h1>
<p class="sub">The same edit applied across {len(cases)} recent production runs — \
unchanged replay (baseline) vs edited replay (variant).</p>

<div class="verdict {verdict_cls}">{verdict_text}</div>

<div class="cards">{stat_cards}</div>

<div class="card"><h2>Metrics — cohort means</h2>
<table>
<tr><th>metric</th><th>baseline</th><th></th><th>variant</th><th>delta</th>\
<th>direction</th><th>verdict</th></tr>
{agg_rows}
</table></div>

<div class="card"><h2>Per case</h2>
<table>
<tr><th>exec id</th><th>reproduced faithfully</th><th>edit flipped decision</th>\
{metric_headers}</tr>
{case_rows}
</table></div>

{skipped_block}
</div></body></html>"""


def write(path: str, report: Report) -> str:
    """Write the cohort HTML to *path* and return the resolved absolute path."""
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(render(report), encoding="utf-8")
    return str(Path(path).resolve())
