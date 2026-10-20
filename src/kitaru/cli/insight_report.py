#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Render an import's insights as one self-contained HTML file.

Every insight is drawn from its stored data, so no model is called. A
model-swap summary insight, identified by `MODEL_SWAP_REPORT_KEY` in its
metadata, gets a dedicated section built from the report it carries.
"""

import html
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.insight import (
    BinnedInsightData,
    CategoricalInsightData,
    InsightListParams,
    InsightResponse,
    TextInsightData,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.client.dashboard_urls import get_dashboard_base_url
from kitaru.client.exceptions import APIError

MODEL_SWAP_REPORT_KEY = "kitaru.model_swap/v1"

_AGREEMENT_LABELS = {
    "same_tool_same_args": "Same tool and arguments",
    "same_tool_different_args": "Same tool, different arguments",
    "different_tool": "Different tool",
    "text_instead_of_tool": "Text instead of a tool call",
    "same_text": "Same answer",
    "different_text": "Different answer",
    "tool_instead_of_text": "Tool call instead of an answer",
}


async def write_insight_report(
    client: Any, *, import_id: uuid.UUID, out: Path
) -> CommandResult:
    """Render the insights of one import to an HTML file."""
    insights = [
        insight
        async for insight in client.insights.iter(
            InsightListParams(
                filter=FilterCondition(
                    field="import_id", op=FilterOp.EQ, value=str(import_id)
                )
            )
        )
    ]
    if not insights:
        raise CLIError(
            "not_found",
            f"Import {import_id} has no insights. Run an analyzer with "
            f"`kitaru import analyze {import_id} --analyzer ...` first.",
        )
    warnings: list[str] = []
    dashboard: str | None = None
    try:
        info = await client.info.get()
    # A malformed or failing info response only costs the session links.
    except (APIError, httpx.HTTPError, ValueError):
        warnings.append("Could not resolve the dashboard URL; links are omitted.")
    else:
        dashboard = get_dashboard_base_url(info, client.base_url)
    document = render_insight_report(
        insights, import_id=import_id, dashboard_url=dashboard
    )
    try:
        out.write_text(document, encoding="utf-8")
    except OSError as error:
        raise CLIError("invalid_arguments", f"Cannot write {out}: {error}") from error
    swap = _latest_model_swap(insights)
    return CommandResult(
        item={
            "path": str(out),
            "import_id": str(import_id),
            "insights": len(insights),
            "model_swap": swap is not None,
        },
        warnings=warnings,
        next_actions=[f"open {out}"],
    )


def render_insight_report(
    insights: list[InsightResponse],
    *,
    import_id: uuid.UUID,
    dashboard_url: str | None,
    generated_at: datetime | None = None,
) -> str:
    """Build the HTML document for an import's insights."""
    generated = (generated_at or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M UTC")
    swap = _latest_model_swap(insights)
    sections = []
    if swap is not None:
        report = swap.metadata[MODEL_SWAP_REPORT_KEY]
        assert isinstance(report, dict)
        sections.append(_model_swap_section(report, dashboard_url))
    # The model-swap section already shows what its own cards chart.
    others = (
        insights
        if swap is None
        else [i for i in insights if not i.name.startswith("model-swap-")]
    )
    if others:
        sections.append(
            "<section><h2>Other insights</h2>"
            + "".join(_insight_card(insight) for insight in others)
            + "</section>"
        )
    title = "Model swap report" if swap is not None else "Insight report"
    return _PAGE.format(
        style=_STYLE,
        title=_e(title),
        subtitle=_e(f"Import {import_id} · generated {generated}"),
        body="".join(sections),
    )


def _latest_model_swap(insights: list[InsightResponse]) -> InsightResponse | None:
    """Return the most recent insight carrying a model-swap report."""
    reports = [
        insight
        for insight in insights
        if isinstance(insight.metadata.get(MODEL_SWAP_REPORT_KEY), dict)
    ]
    return max(reports, key=lambda insight: insight.created) if reports else None


def _model_swap_section(report: dict[str, Any], dashboard: str | None) -> str:
    """Render the model-swap report: verdicts, coverage, tools, disagreements."""
    parts = ["<section>"]
    if report.get("dry_run"):
        parts.append(
            '<p class="badge">Dry run: no calls were sent. Numbers are estimates.</p>'
        )
    if report.get("stopped_early"):
        parts.append(
            f'<p class="badge warn">Stopped early ({_e(report["stopped_early"])}). '
            "Calls that were not sent are counted separately.</p>"
        )
    candidates = report.get("candidates") or []
    parts.append("<h2>Candidates</h2>")
    if report.get("dry_run"):
        parts.append(
            _table(
                ["Candidate", "Model", "Calls", "Estimated spend", "Per month"],
                [
                    [
                        c.get("name"),
                        c.get("model"),
                        (c.get("estimate") or {}).get("calls"),
                        _usd((c.get("estimate") or {}).get("spend_usd")),
                        _usd(
                            ((c.get("estimate") or {}).get("cost") or {}).get(
                                "per_month_usd"
                            )
                        ),
                    ]
                    for c in candidates
                ],
            )
        )
    else:
        parts.append(
            _table(
                [
                    "Candidate",
                    "Model",
                    "Agreement",
                    "Tool choice",
                    "Judged equivalent",
                    "Failures",
                    "Cost per call",
                    "Per session",
                    "Per month",
                    "Latency p50",
                ],
                [
                    [
                        c.get("name") + (" (reference)" if c.get("reference") else ""),
                        c.get("model"),
                        _pct(c.get("agreement_rate")),
                        _pct(c.get("tool_choice_rate")),
                        c.get("judged_equivalent") if report.get("judge") else "-",
                        _pct(c.get("failure_rate")),
                        _usd((c.get("cost") or {}).get("per_call_usd")),
                        _usd((c.get("cost") or {}).get("per_session_usd")),
                        _usd((c.get("cost") or {}).get("per_month_usd")),
                        _seconds((c.get("latency_seconds") or {}).get("p50")),
                    ]
                    for c in candidates
                ],
            )
        )
        recorded_report = report.get("recorded") or {}
        recorded = recorded_report.get("cost") or {}
        models = ", ".join(recorded_report.get("models") or {})
        parts.append(
            f"<p>Recorded model ({_e(models)}): "
            f"{_usd(recorded.get('per_call_usd'))} per call, "
            f"{_usd(recorded.get('per_month_usd'))} per month.</p>"
        )
        parts.append("<h2>Recommendation</h2><ul>")
        parts.extend(
            f"<li><strong>{_e(c.get('name'))}</strong>: "
            f"{_e(c.get('recommendation'))}</li>"
            for c in candidates
        )
        parts.append("</ul>")
        parts.append(_cost_breakdown(candidates))
        parts.append(_failures(candidates))
        parts.append(_by_tool(candidates))
    parts.append(_coverage(report))
    if report.get("notes"):
        parts.append(
            "<h2>Notes</h2><ul>"
            + "".join(f"<li>{_e(note)}</li>" for note in report["notes"])
            + "</ul>"
        )
    if not report.get("dry_run"):
        parts.append(_disagreements(report, dashboard))
    parts.append("</section>")
    return "".join(parts)


def _cost_breakdown(candidates: list[dict[str, Any]]) -> str:
    """Show why a model is cheap or expensive: prices, tokens and retries."""
    rows = []
    for c in candidates:
        price = c.get("price") or {}
        tokens = c.get("tokens_per_call") or {}
        rows.append(
            [
                c.get("name"),
                _usd(price.get("input_per_mtok")) if price else "unknown",
                _usd(price.get("output_per_mtok")) if price else "unknown",
                tokens.get("input_tokens"),
                tokens.get("output_tokens"),
                tokens.get("reasoning_tokens"),
                c.get("retries"),
                price.get("source") if price else "-",
            ]
        )
    return "<h2>Why the cost differs</h2>" + _table(
        [
            "Candidate",
            "Input / MTok",
            "Output / MTok",
            "Input tokens / call",
            "Output tokens / call",
            "of which reasoning",
            "Retries",
            "Price source",
        ],
        rows,
    )


def _failures(candidates: list[dict[str, Any]]) -> str:
    """List provider failures, which are not counted as disagreement."""
    rows = [
        [c.get("name"), category, count]
        for c in candidates
        for category, count in (c.get("failures") or {}).items()
    ]
    if not rows:
        return "<h2>Provider failures</h2><p>None.</p>"
    return (
        "<h2>Provider failures</h2><p>Failed requests say more about the "
        "provider setup than the model, so they are not counted as "
        "disagreement.</p>" + _table(["Candidate", "Category", "Calls"], rows)
    )


def _by_tool(candidates: list[dict[str, Any]]) -> str:
    """Agreement per recorded tool and per argument."""
    rows = []
    for c in candidates:
        for group, values in (c.get("by_tool") or {}).items():
            fields = ", ".join(
                f"{name.split('.', 1)[-1]} {_pct(rate)}"
                for name, rate in (values.get("fields") or {}).items()
            )
            rows.append(
                [
                    c.get("name"),
                    group,
                    values.get("calls"),
                    _pct(values.get("agreement_rate")),
                    fields or "-",
                ]
            )
    if not rows:
        return ""
    return "<h2>By recorded tool</h2>" + _table(
        ["Candidate", "Recorded", "Calls", "Agreement", "Argument agreement"], rows
    )


def _coverage(report: dict[str, Any]) -> str:
    """Show which recorded calls were replayed and why others were not."""
    coverage = report.get("coverage") or {}
    rows = [
        ["Sessions", coverage.get("sessions")],
        ["Model calls seen", coverage.get("llm_calls_seen")],
        ["Replayable with real tool schemas", coverage.get("replayable")],
        ["Replayable only with inferred schemas", coverage.get("replayable_inferred")],
        ["Sampled", coverage.get("sampled")],
        *(
            [f"Skipped: {reason}", count]
            for reason, count in (coverage.get("skipped") or {}).items()
        ),
        *(
            [f"Schemas from: {source}", count]
            for source, count in (coverage.get("schema_sources") or {}).items()
        ),
        [
            "Calls whose history had reasoning",
            coverage.get("calls_with_dropped_reasoning"),
        ],
    ]
    return "<h2>Coverage</h2>" + _table(["", "Calls"], rows)


def _disagreements(report: dict[str, Any], dashboard: str | None) -> str:
    """List disagreeing calls grouped by what the recorded model did."""
    items = report.get("disagreements") or []
    total = report.get("disagreements_total", len(items))
    if not items:
        return "<h2>Disagreements</h2><p>None.</p>"
    parts = [f"<h2>Disagreements ({total})</h2>"]
    if total > len(items):
        parts.append(f"<p>Showing the first {len(items)}.</p>")
    current = None
    for item in items:
        if item.get("group") != current:
            if current is not None:
                parts.append("</div>")
            current = item.get("group")
            parts.append(f'<div class="group"><h3>Recorded: {_e(current)}</h3>')
        session_id = _e(item.get("session_id"))
        link = (
            f'<a href="{_e(dashboard)}/sessions/{session_id}">session</a>'
            if dashboard
            else f"session {_e(item.get('session_id'))}"
        )
        agreement = str(item.get("agreement"))
        judge = item.get("judge_equivalent")
        verdict = (
            ""
            if judge is None
            else f" · judge: {'equivalent' if judge else 'not equivalent'}"
        )
        parts.append(
            '<div class="diff">'
            f"<p><strong>{_e(item.get('candidate'))}</strong>: "
            f"{_e(_AGREEMENT_LABELS.get(agreement, agreement))}"
            f"{_e(verdict)} · {_e(item.get('node_name'))} · {link} · "
            f"node {_e(item.get('node_id'))}</p>"
            f'<div class="cols"><pre><b>Recorded</b>\n{_e(item.get("recorded"))}</pre>'
            f"<pre><b>Candidate</b>\n{_e(item.get('candidate_answer'))}</pre></div>"
            "</div>"
        )
    parts.append("</div>")
    return "".join(parts)


def _insight_card(insight: InsightResponse) -> str:
    """Render any insight from its stored data."""
    data = insight.data
    if isinstance(data, TextInsightData):
        body = f'<pre class="md">{_e(data.content)}</pre>'
    elif isinstance(data, CategoricalInsightData):
        peak = max((value.value for value in data.values), default=0) or 1
        body = "".join(
            f'<div class="bar"><span>{_e(value.label)}</span>'
            f'<i style="width:{value.value / peak * 100:.1f}%"></i>'
            f"<b>{value.value:g}{' ' + _e(data.unit) if data.unit else ''}</b></div>"
            for value in data.values
        )
    elif isinstance(data, BinnedInsightData):
        body = _table(
            ["From", "To", "Count"],
            [[b.lower_bound, b.upper_bound, b.count] for b in data.bins],
        )
    else:
        body = ""
    description = (
        f'<p class="muted">{_e(insight.description)}</p>' if insight.description else ""
    )
    return f'<div class="card"><h3>{_e(insight.title)}</h3>{description}{body}</div>'


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_e(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f'<div class="scroll"><table><tr>{head}</tr>{body}</table></div>'


def _e(value: Any) -> str:
    return html.escape("-" if value is None else str(value))


def _pct(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.0%}"


def _usd(value: Any) -> str:
    if value is None:
        return "unknown"
    amount = float(value)
    return f"${amount:,.2f}" if amount >= 1 else f"${amount:.6f}".rstrip("0")


def _seconds(value: Any) -> str:
    return "-" if value is None else f"{float(value):.2f}s"


_STYLE = """
:root { --bg: #fff; --fg: #1d1d1f; --muted: #666; --line: #e3e3e3;
  --accent: #e8590c; --soft: #fff4ec; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #16161a; --fg: #ececf1; --muted: #9a9aa5; --line: #2e2e36;
    --accent: #ff8a3d; --soft: #2a1d14; }
}
body { background: var(--bg); color: var(--fg); margin: 0 auto;
  font: 15px/1.5 system-ui, sans-serif; max-width: 1100px; padding: 24px 16px; }
h1 { margin: 0; }
h2 { margin-top: 32px; border-bottom: 1px solid var(--line); padding-bottom: 4px; }
.muted, .sub { color: var(--muted); }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%;
  font-variant-numeric: tabular-nums; }
th, td { border-bottom: 1px solid var(--line); padding: 6px 8px;
  text-align: left; vertical-align: top; }
th { font-weight: 600; font-size: 13px; color: var(--muted); }
.badge { background: var(--soft); border-left: 3px solid var(--accent);
  padding: 8px 12px; }
.card, .diff { border: 1px solid var(--line); border-radius: 8px;
  padding: 12px 16px; margin: 12px 0; }
.bar { display: grid; grid-template-columns: minmax(120px, 40%) 1fr auto;
  gap: 8px; align-items: center; margin: 4px 0; }
.bar i { display: block; height: 10px; background: var(--accent);
  border-radius: 5px; min-width: 2px; }
.cols { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 700px) { .cols { grid-template-columns: 1fr; } }
pre { white-space: pre-wrap; word-break: break-word; background: var(--soft);
  padding: 8px; border-radius: 6px; margin: 0; font-size: 13px; }
a { color: var(--accent); }
"""

_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{style}</style></head>
<body><h1>{title}</h1><p class="sub">{subtitle}</p>{body}</body></html>
"""
