#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Render a model-swap report as an HTML section.

`kitaru insight report` embeds the section in a page that supplies the base
styles (tables, `.badge`, `.cols`, `pre`), so the markup only uses those.
"""

import html
from typing import Any

from kitaru_model_swap.compare import AGREEMENT_LABELS


def render_section(report: dict[str, Any], dashboard_url: str | None) -> str:
    """Render the verdicts, costs, coverage and disagreements of a report."""
    candidates = report["candidates"]
    parts = ["<section><h2>Which model is right for this agent</h2>"]
    if report["dry_run"]:
        parts.append(
            '<p class="badge">Dry run: no calls were sent. Numbers are estimates.</p>'
        )
        parts.append(_estimates(candidates))
    else:
        if report["stopped_early"]:
            parts.append(
                f'<p class="badge">Stopped early ({_e(report["stopped_early"])}). '
                "Calls that were not sent are counted separately.</p>"
            )
        parts += [
            _best_for(report["best_for"]),
            _compare(candidates, judged=report["judge"] is not None),
            _recorded(report["recorded"]),
            "<ul>",
            *(
                f"<li><strong>{_e(c['name'])}</strong>: {_e(c['summary'])}</li>"
                for c in candidates
            ),
            "</ul>",
            _cost_breakdown(candidates),
            _failures(candidates),
            _by_tool(candidates),
        ]
    parts.append(_coverage(report["coverage"]))
    if report["notes"]:
        parts.append(
            "<h2>Notes</h2><ul>"
            + "".join(f"<li>{_e(note)}</li>" for note in report["notes"])
            + "</ul>"
        )
    if not report["dry_run"]:
        parts.append(_disagreements(report, dashboard_url))
    parts.append("</section>")
    return "".join(parts)


def _estimates(candidates: list[dict[str, Any]]) -> str:
    return _table(
        ["Candidate", "Model", "Calls", "Estimated spend", "Per month"],
        [
            [
                c["name"],
                c["model"],
                c["estimate"]["calls"],
                _usd(c["estimate"]["spend_usd"]),
                _usd(c["estimate"]["cost"]["per_month_usd"]),
            ]
            for c in candidates
        ],
    )


def _best_for(choices: list[dict[str, str]]) -> str:
    if not choices:
        return ""
    return (
        "<h2>Best for</h2>"
        + _table(
            ["Goal", "Candidate", "Why"],
            [[c["goal"], c["candidate"], c["why"]] for c in choices],
        )
        + '<p class="muted">Quality here means matching your current model. '
        "Your verdicts on the disagreements below decide which answers are "
        "right.</p>"
    )


def _compare(candidates: list[dict[str, Any]], *, judged: bool) -> str:
    return "<h2>Compare</h2>" + _table(
        [
            "Candidate",
            "Model",
            "Tool choice",
            "Exact match",
            "Judged equivalent",
            "Failures",
            "Cost per call",
            "Per month",
            "Latency p50",
            "Latency p95",
            "Open weights",
        ],
        [
            [
                c["name"] + (" (today)" if c["reference"] else ""),
                c["model"],
                _pct(c["tool_choice_rate"]),
                _pct(c["agreement_rate"]),
                c["judged_equivalent"] if judged else None,
                _pct(c["failure_rate"]),
                _usd(c["cost"]["per_call_usd"]),
                _usd(c["cost"]["per_month_usd"]),
                _seconds(c["latency_seconds"]["p50"]),
                _seconds(c["latency_seconds"]["p95"]),
                None
                if c["open_weights"] is None
                else ("yes" if c["open_weights"] else "no"),
            ]
            for c in candidates
        ],
    )


def _recorded(recorded: dict[str, Any]) -> str:
    cost = recorded["cost"]
    return (
        f"<p>Recorded model ({_e(', '.join(recorded['models']))}): "
        f"{_usd(cost['per_call_usd'])} per call, "
        f"{_usd(cost['per_month_usd'])} per month.</p>"
    )


def _cost_breakdown(candidates: list[dict[str, Any]]) -> str:
    """Show why a model is cheap or expensive: prices, tokens and retries."""
    rows = []
    for c in candidates:
        price = c["price"] or {}
        tokens = c["tokens_per_call"] or {}
        rows.append(
            [
                c["name"],
                _usd(price.get("input_per_mtok")),
                _usd(price.get("output_per_mtok")),
                tokens.get("input_tokens"),
                tokens.get("output_tokens"),
                tokens.get("reasoning_tokens"),
                c["retries"],
                price.get("source"),
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
        [c["name"], category, count, c["failure_examples"].get(category)]
        for c in candidates
        for category, count in c["failures"].items()
    ]
    if not rows:
        return "<h2>Provider failures</h2><p>None.</p>"
    return (
        "<h2>Provider failures</h2><p>Failed requests say more about the "
        "provider setup than the model, so they are not counted as "
        "disagreement.</p>"
        + _table(["Candidate", "Category", "Calls", "Example"], rows)
    )


def _by_tool(candidates: list[dict[str, Any]]) -> str:
    """Agreement per recorded tool and per argument."""
    rows = [
        [
            c["name"],
            group,
            values["calls"],
            _pct(values["agreement_rate"]),
            ", ".join(
                f"{name.split('.', 1)[-1]} {_pct(rate)}"
                for name, rate in values["fields"].items()
            )
            or None,
        ]
        for c in candidates
        for group, values in c["by_tool"].items()
    ]
    if not rows:
        return ""
    return "<h2>By recorded tool</h2>" + _table(
        ["Candidate", "Recorded", "Calls", "Exact agreement", "Argument agreement"],
        rows,
    )


def _coverage(coverage: dict[str, Any]) -> str:
    """Show which recorded calls were replayed and why others were not."""
    rows = [
        ["Sessions", coverage["sessions"]],
        ["Model calls seen", coverage["llm_calls_seen"]],
        ["Replayable with real tool schemas", coverage["replayable"]],
        ["Replayable only with inferred schemas", coverage["replayable_inferred"]],
        ["Sampled", coverage["sampled"]],
        *([f"Skipped: {k}", v] for k, v in coverage["skipped"].items()),
        *([f"Schemas from: {k}", v] for k, v in coverage["schema_sources"].items()),
        ["Calls whose history had reasoning", coverage["calls_with_dropped_reasoning"]],
    ]
    return "<h2>Coverage</h2>" + _table(["", "Calls"], rows)


def _disagreements(report: dict[str, Any], dashboard_url: str | None) -> str:
    """List disagreeing calls grouped by what the recorded model did."""
    items = report["disagreements"]
    total = report["disagreements_total"]
    if not total:
        return "<h2>Disagreements</h2><p>None.</p>"
    if not items:
        return f"<h2>Disagreements ({total})</h2><p>Not listed in this report.</p>"
    parts = [f"<h2>Disagreements ({total})</h2>"]
    if total > len(items):
        parts.append(f"<p>Showing the first {len(items)}.</p>")
    current = None
    for item in items:
        if item["group"] != current:
            current = item["group"]
            parts.append(f"<h3>Recorded: {_e(current)}</h3>")
        session = _e(item["session_id"])
        link = (
            f'<a href="{_e(dashboard_url)}/sessions/{session}">session</a>'
            if dashboard_url
            else f"session {session}"
        )
        judge = item["judge_equivalent"]
        verdict = (
            "" if judge is None else f" · judge: {'' if judge else 'not '}equivalent"
        )
        parts.append(
            '<div class="card">'
            f"<p><strong>{_e(item['candidate'])}</strong>: "
            f"{_e(AGREEMENT_LABELS[item['agreement']])}{_e(verdict)} · "
            f"{_e(item['node_name'])} · {link} · node {_e(item['node_id'])}</p>"
            f'<div class="cols"><pre><b>Recorded</b>\n{_e(item["recorded"])}</pre>'
            f"<pre><b>Candidate</b>\n{_e(item['candidate_answer'])}</pre></div>"
            "</div>"
        )
    return "".join(parts)


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_e(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f'<div class="scroll"><table><tr>{head}</tr>{body}</table></div>'


def _e(value: Any) -> str:
    return html.escape("-" if value is None else str(value))


def _pct(value: float | None) -> str | None:
    return None if value is None else f"{value:.0%}"


def _usd(value: str | None) -> str:
    if value is None:
        return "unknown"
    amount = float(value)
    if amount == 0:
        return "$0"
    if amount >= 1:
        return f"${amount:,.2f}"
    if amount < 0.000001:
        return f"${amount:.1e}"
    return f"${amount:.6f}".rstrip("0")


def _seconds(value: float | None) -> str | None:
    return None if value is None else f"{value:.2f}s"
