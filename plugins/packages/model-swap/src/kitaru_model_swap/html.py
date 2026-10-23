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
            _best_mix(report.get("best_mix")),
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
    parts.append(_decisions(report))
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
        + '<p class="muted">Stage 1 of 2: this screen compares single recorded '
        "calls, so it rules candidates out but does not pick a winner. Replay the "
        "candidates worth a full replay end to end, graded on outcomes, to "
        "decide.</p>"
        + _table(
            ["Goal", "Candidate", "Why"],
            [[c["goal"], c["candidate"], c["why"]] for c in choices],
        )
        + '<p class="muted">Quality here means matching your current model. '
        "Your verdicts on the disagreements below decide which answers are "
        "right, and a full replay of the finalists decides which model keeps "
        "your outcomes.</p>"
    )


def _compare(candidates: list[dict[str, Any]], *, judged: bool) -> str:
    repeated = any(c.get("consistency") for c in candidates)
    headers = [
        "Candidate",
        "Model",
        "Tool choice",
        "Exact match",
        *(["Any / all attempts match"] if repeated else []),
        "Judge: equivalent / different / unsure",
        "Failures",
        "Tokens per call (in / out, of which reasoning)",
        "Cost per call",
        "Per month",
        "Latency p50",
        "Latency p95",
        "Open weights",
        "Origin",
    ]
    rows = []
    for c in candidates:
        consistency = c.get("consistency")
        tokens = c["tokens_per_call"]
        rows.append(
            [
                c["name"]
                + (" (today)" if c["reference"] else "")
                + (" (excluded)" if c.get("excluded") else ""),
                c["model"],
                _rate_with_interval(
                    c["tool_choice_rate"], c.get("tool_choice_interval")
                ),
                _rate_with_interval(c["agreement_rate"], c.get("agreement_interval")),
                *(
                    [
                        f"{_pct(consistency['any_match_rate'])} / "
                        f"{_pct(consistency['all_match_rate'])}"
                        if consistency
                        else None
                    ]
                    if repeated
                    else []
                ),
                (
                    f"{c['judged_equivalent']} / {c['judged_different']} / "
                    f"{c['judged_unsure']}"
                    + (
                        f" ({sum(c['judge_failures'].values())} failed)"
                        if c["judge_failures"]
                        else ""
                    )
                )
                if judged
                else None,
                _pct(c["failure_rate"]),
                None
                if tokens is None
                else f"{tokens['input_tokens']:.0f} / {tokens['output_tokens']:.0f}, "
                f"{tokens['reasoning_tokens']:.0f}",
                _usd(c["cost"]["per_call_usd"]) + _partial(c),
                _usd(c["cost"]["per_month_usd"]) + _partial(c),
                _seconds(c["latency_seconds"]["p50"]),
                _seconds(c["latency_seconds"]["p95"]),
                None
                if c["open_weights"] is None
                else ("yes" if c["open_weights"] else "no"),
                _origin(c.get("origin")),
            ]
        )
    return "<h2>Compare</h2>" + _table(headers, rows)


def _origin(origin: dict[str, Any] | None) -> str | None:
    """Render a candidate's declared lab and hosting."""
    if not origin:
        return None
    lab = " ".join(
        v for v in (origin.get("lab"), _paren(origin.get("lab_country"))) if v
    )
    host = " ".join(
        v for v in (origin.get("host"), _paren(origin.get("host_region"))) if v
    )
    return " · ".join(v for v in (lab, host and f"served by {host}") if v) or None


def _paren(value: str | None) -> str | None:
    return f"({value})" if value else None


def _best_mix(mix: dict[str, Any] | None) -> str:
    """Show which step each candidate would take in the best mix."""
    if not mix:
        return ""
    rows = [
        [
            item["group"],
            _pct(item["share"]),
            (
                item["candidate"]
                + (
                    " (too few held-out calls to confirm)"
                    if item.get("held_out") == "too_few"
                    else " (seen in one half of the sessions only, not checked)"
                    if item.get("held_out") == "no_split"
                    else ""
                )
            )
            if item["candidate"]
            else (
                f"today's model (fewer than {mix['min_calls']} calls)"
                if item["too_few_calls"]
                else "today's model (it does not match itself here)"
                if item.get("noisy_reference")
                else "today's model (the pick did not hold on held-out sessions)"
                if item.get("held_out") == "failed"
                else "today's model (candidates failed too often here: "
                + ", ".join(item["unreliable"])
                + ")"
                if item.get("unreliable")
                else "today's model"
            ),
            _pct(item["rate"]),
            _pct(item.get("held_out_rate")),
            _usd(item["cost_per_call_usd"]),
        ]
        for item in mix["groups"]
    ]
    total = ""
    if mix["cost"] is not None and mix["recorded_cost"] is not None:
        total = (
            f"<p>Mixed: {_usd(mix['cost']['per_call_usd'])} per call and "
            f"{_usd(mix['cost']['per_month_usd'])} per month, against "
            f"{_usd(mix['recorded_cost']['per_call_usd'])} and "
            f"{_usd(mix['recorded_cost']['per_month_usd'])} today "
            f"(from the {_e(mix['today_source'])}).</p>"
        )
    return (
        "<h2>Best mix</h2><p>Steps are named by what comes just before a call "
        "(the first turn, or the tool whose result the model is reading), which "
        "is what a router can see. For each step, the cheapest allowed candidate "
        "that answered at least "
        f"{mix['min_calls']} of its calls and takes the same kind of action (the "
        "same tools, or a text answer) within 10 points as often as today's "
        "model there, chosen on half of the sessions and dropped where it does "
        "not hold on the other half; the rest stays on today's model. Confirm "
        "a mix with a full replay before routing traffic.</p>"
        + _table(
            ["Step", "Share of calls", "Use", "Matches", "Held out", "Cost per call"],
            rows,
        )
        + total
    )


def _recorded(recorded: dict[str, Any]) -> str:
    cost = recorded["cost"]
    return (
        f"<p>Recorded model ({_e(', '.join(recorded['models']))}): "
        f"{_usd(cost['per_call_usd'])} per call, "
        f"{_usd(cost['per_month_usd'])} per month.</p>"
    )


def _decisions(report: dict[str, Any]) -> str:
    """Show which calls a decision model could make, and how candidates did."""
    shapes = report.get("decision_shapes") or {}
    if not shapes.get("calls"):
        return ""
    share = shapes["recorded_cost_share"]
    parts = [
        "<h2>Could a decision model make these calls?</h2>",
        "<p>Decision models such as Jev, Clef, or OpenAI's Decisions API pick "
        "from options instead of writing text. A call is a <b>decision</b> when "
        "the model only chose a tool and enum or boolean values, <b>partial</b> "
        "when the tool choice is a decision but free-form values or text need "
        "an LLM, and <b>generative</b> otherwise.</p>",
        _table(
            ["Shape", "Sampled calls", "Share of recorded cost"],
            [
                [shape, shapes[shape], _pct(share[shape])]
                for shape in ("decision", "partial", "generative")
            ],
        ),
    ]
    if shapes["reasons"]:
        parts.append(
            _table(
                ["Why not fully a decision", "Calls"],
                [[reason, count] for reason, count in shapes["reasons"].items()],
            )
        )
    candidates = report.get("decision_candidates") or []
    if report["dry_run"] and candidates:
        parts.append(
            _table(
                ["Decision candidate", "Calls to ask", "Estimated spend"],
                [
                    [
                        c["name"],
                        c["estimate"]["calls"],
                        _usd(c["estimate"]["spend_usd"]),
                    ]
                    for c in candidates
                ],
            )
        )
    elif candidates:
        parts.append(
            _table(
                [
                    "Decision candidate",
                    "Tool choice",
                    "All decisions match",
                    "Argument agreement",
                    "Failures",
                    "Cost per call (decision parts)",
                    "Cost per decision call",
                    "Recorded cost per decision call",
                    "Not run",
                    "Latency p50",
                ],
                [
                    [
                        f"{c['name']} ({c['kind']} {c['model']})",
                        _pct(c["tool_choice_rate"]),
                        _pct(c["decisions_match_rate"]),
                        ", ".join(
                            f"{name} {_pct(rate)}"
                            for name, rate in c["argument_rates"].items()
                        )
                        or None,
                        ", ".join(f"{k} {v}" for k, v in c["failures"].items()) or None,
                        _usd(c["cost_per_call_usd"]),
                        _usd(c["decision_call_cost_usd"]),
                        _usd(c["recorded_decision_call_cost_usd"]),
                        ", ".join(f"{k} {v}" for k, v in c["calls_not_run"].items())
                        or None,
                        _seconds(c["latency_p50_seconds"]),
                    ]
                    for c in candidates
                ],
            )
        )
        parts.append(
            '<p class="muted">Agreement is with the recorded model\'s choices, '
            "not proof they are right. Moving a partial call means changing the "
            "agent to decide with the decision model and generate the rest with "
            "an LLM.</p>"
        )
    return "".join(parts)


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
        ["Sessions sampled", coverage["sessions_sampled"]],
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
        verdict = _judge_text(
            item["judge_equivalent"],
            item.get("judge_probability"),
            item.get("judge_failure"),
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


def _judge_text(
    equivalent: bool | None, probability: float | None, failure: str | None
) -> str:
    """Describe the judge's ruling, with its probability when it gave one."""
    if failure is not None:
        return f" · judge failed ({failure})"
    if equivalent is None and probability is None:
        return ""
    label = {True: "equivalent", False: "different", None: "unsure"}[equivalent]
    # Three decimals, as stored, so the number never contradicts the label.
    shown = "" if probability is None else f" (p={probability:.3f})"
    return f" · judge: {label}{shown}"


def _partial(candidate: dict[str, Any]) -> str:
    """Mark a cost that misses a kind of call."""
    known = candidate["cost"]["per_call_usd"] is not None
    return " (partial)" if known and not candidate["cost_complete"] else ""


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    head = "".join(f"<th>{_e(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{_e(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f'<div class="scroll"><table><tr>{head}</tr>{body}</table></div>'


def _e(value: Any) -> str:
    return html.escape("-" if value is None else str(value))


def _rate_with_interval(
    value: float | None, interval: list[float] | None
) -> str | None:
    """Show a rate with its 95% interval, so small samples read as uncertain."""
    if value is None or not interval:
        return _pct(value)
    return f"{_pct(value)} ({_pct(interval[0])} to {_pct(interval[1])})"


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
