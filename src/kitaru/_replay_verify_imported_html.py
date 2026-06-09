"""Self-contained HTML verdict page for imported Replay Verify reports.

The page is rendered entirely from the report object (no JS frameworks, inline
CSS only) so the written ``verification_report.html`` opens cleanly from
``file://`` next to the JSON and Markdown reports.
"""

from __future__ import annotations

import html
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from kitaru._replay_verify_imported_models import (
    ImportedVerificationReport,
    execution_mode_detail,
)

_PAPER = "#f6f1e8"
_INK = "#211d18"
_SOURCE_ORANGE = "#b35309"
_PASS_GREEN = "#2d6a4f"
_DRIFT_RED = "#9b2c2c"

# One source of truth per verdict: (color, badge word). Badge wording maps the
# runner verdict, not the raw field mismatches: a case where the candidate
# matched the fresh baseline but production differed (verdict "caution") must
# not read as candidate DRIFT.
_VERDICT_STYLE: dict[str, tuple[str, str]] = {
    "ship": (_PASS_GREEN, "match"),
    "caution": (_SOURCE_ORANGE, "caution"),
    "hold": (_DRIFT_RED, "drift"),
}
_VERDICT_COLORS: dict[str, str] = {
    verdict: color for verdict, (color, _) in _VERDICT_STYLE.items()
}
_VERDICT_OUTCOMES: dict[str, str] = {
    verdict: badge for verdict, (_, badge) in _VERDICT_STYLE.items()
}
_OUTCOME_COLORS: dict[str, str] = {
    badge: color for color, badge in _VERDICT_STYLE.values()
} | {"held": _DRIFT_RED}

_COUNT_STRIP_METRICS: tuple[tuple[str, str], ...] = (
    ("imported_count", "imported"),
    ("eligible_count", "eligible"),
    ("stopped_count", "stopped"),
    ("candidate_vs_baseline_drift_count", "candidate vs baseline drift"),
    ("candidate_executions_for_stopped_cases", "candidate runs for stopped cases"),
    ("unsafe_live_execution_count", "unsafe live executions"),
)

HONESTY_LINE = "imported-input fresh execution, not deterministic checkpoint replay"

_CSS = f"""
body {{
  background: {_PAPER};
  color: {_INK};
  font-family: Georgia, 'Iowan Old Style', 'Times New Roman', serif;
  margin: 0;
  padding: 2.5rem clamp(1rem, 6vw, 4rem);
  line-height: 1.5;
}}
.mono {{
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.8rem;
}}
h1 {{ font-size: 1.4rem; margin: 0 0 1.25rem; }}
.verdict-banner {{
  border: 1px solid rgba(33, 29, 24, 0.25);
  border-left-width: 10px;
  padding: 1rem 1.25rem;
  margin-bottom: 1.5rem;
  background: rgba(255, 255, 255, 0.45);
}}
.verdict-word {{
  font-size: 2.2rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  margin: 0 0 0.5rem;
}}
.mode-line {{ margin: 0.15rem 0; color: rgba(33, 29, 24, 0.85); }}
.mode-line strong {{ color: {_INK}; }}
.count-strip {{
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-bottom: 1.75rem;
}}
.count {{
  border: 1px solid rgba(33, 29, 24, 0.25);
  background: rgba(255, 255, 255, 0.45);
  padding: 0.6rem 0.9rem;
  min-width: 7.5rem;
}}
.count-value {{ display: block; font-size: 1.5rem; font-weight: 700; }}
.count-label {{
  display: block;
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: rgba(33, 29, 24, 0.7);
}}
h2 {{
  font-size: 1.05rem;
  border-bottom: 1px solid rgba(33, 29, 24, 0.3);
  padding-bottom: 0.3rem;
}}
.case-row {{
  border: 1px solid rgba(33, 29, 24, 0.2);
  background: rgba(255, 255, 255, 0.45);
  margin-bottom: 0.5rem;
  padding: 0.55rem 0.8rem;
}}
.case-row.held {{
  background: rgba(155, 44, 44, 0.08);
  border-color: {_DRIFT_RED};
}}
details.case-row > summary {{ cursor: pointer; }}
.case-id {{
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.85rem;
  font-weight: 700;
}}
.label-chip {{
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.7rem;
  border: 1px solid rgba(33, 29, 24, 0.3);
  border-radius: 0.6rem;
  padding: 0.05rem 0.45rem;
  margin-left: 0.35rem;
  color: rgba(33, 29, 24, 0.8);
}}
.badge {{
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  color: {_PAPER};
  padding: 0.15rem 0.5rem;
  margin-left: 0.5rem;
  border-radius: 0.2rem;
}}
.stop-reasons {{
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.75rem;
  color: {_DRIFT_RED};
  margin-top: 0.35rem;
}}
table.fields {{
  border-collapse: collapse;
  margin: 0.7rem 0 0.3rem;
  width: 100%;
}}
table.fields th, table.fields td {{
  border: 1px solid rgba(33, 29, 24, 0.25);
  padding: 0.3rem 0.55rem;
  text-align: left;
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.75rem;
  vertical-align: top;
}}
table.fields th.observed-head {{ color: {_SOURCE_ORANGE}; }}
td.cell-result.match {{ color: {_PASS_GREEN}; font-weight: 700; }}
td.cell-result.drift {{ color: {_DRIFT_RED}; font-weight: 700; }}
td.cell-observed.observed-drift {{
  background: rgba(179, 83, 9, 0.12);
  color: {_SOURCE_ORANGE};
  font-weight: 700;
}}
td.cell-candidate.candidate-drift {{
  background: rgba(155, 44, 44, 0.12);
  color: {_DRIFT_RED};
  font-weight: 700;
}}
footer {{
  margin-top: 2rem;
  border-top: 1px solid rgba(33, 29, 24, 0.3);
  padding-top: 0.8rem;
  font-family: ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.75rem;
  color: rgba(33, 29, 24, 0.8);
}}
footer p {{ margin: 0.25rem 0; }}
.honesty {{ color: {_SOURCE_ORANGE}; font-weight: 700; }}
"""


@dataclass(frozen=True)
class _FieldRow:
    """One comparison field joined across the two comparison lists."""

    field: str
    observed: Any
    baseline: Any
    candidate: Any
    observed_matches: bool
    candidate_matches: bool

    @property
    def matches(self) -> bool:
        return self.observed_matches and self.candidate_matches


def render_html_report(report: ImportedVerificationReport) -> str:
    """Render an imported verification report as one self-contained HTML page."""
    summary = report.summary
    verdict = str(summary.get("overall_verdict", "hold"))
    parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>{html.escape(report.name)}</title>",
        f"<style>{_CSS}</style>",
        "</head>",
        "<body>",
        f"<h1>Imported Replay Verify Report: {html.escape(report.name)}</h1>",
        _render_verdict_banner(report, verdict),
        _render_count_strip(summary),
        _render_case_grid(summary),
        _render_footer(report),
        "</body>",
        "</html>",
    ]
    return "\n".join(parts)


def _render_verdict_banner(
    report: ImportedVerificationReport,
    verdict: str,
) -> str:
    color = _VERDICT_COLORS.get(verdict, _DRIFT_RED)
    summary = report.summary
    mode_detail = str(
        summary.get("mode_detail") or execution_mode_detail(report.execution_mode)
    )
    control_status = str(summary.get("recorded_response_control_status", "unknown"))
    mode_lines = [
        (
            "execution mode",
            f"{report.execution_mode} ({mode_detail})",
        ),
        ("recorded-response control", control_status),
        ("source system", report.source_system),
    ]
    line_html = "".join(
        '<p class="mode-line mono"><strong>'
        f"{html.escape(label)}:</strong> {html.escape(value)}</p>"
        for label, value in mode_lines
    )
    return (
        f'<section class="verdict-banner" style="border-left-color: {color};">'
        f'<p class="verdict-word" style="color: {color};">'
        f"{html.escape(verdict.upper())}</p>"
        f"{line_html}"
        "</section>"
    )


def _render_count_strip(summary: Mapping[str, Any]) -> str:
    cards = []
    for metric, label in _COUNT_STRIP_METRICS:
        value = summary.get(metric, 0)
        count = value if isinstance(value, int) else 0
        cards.append(
            f'<div class="count" data-metric="{metric}">'
            f'<span class="count-value">{count}</span>'
            f'<span class="count-label">{html.escape(label)}</span>'
            "</div>"
        )
    return f'<section class="count-strip">{"".join(cards)}</section>'


def _render_case_grid(summary: Mapping[str, Any]) -> str:
    rows = [_render_case_row(case) for case in _case_results(summary)]
    if not rows:
        rows = ['<p class="mono">No case results.</p>']
    return f"<section><h2>Cases</h2>{''.join(rows)}</section>"


def _render_case_row(case: Mapping[str, Any]) -> str:
    outcome = _case_outcome(case)
    header = _render_case_header(case, outcome)
    if outcome == "held":
        reasons = [str(reason) for reason in _string_items(case.get("stop_reasons"))]
        error = case.get("error")
        if isinstance(error, str) and error:
            reasons.append(f"error: {error}")
        reason_text = ", ".join(reasons) or "unknown"
        return (
            '<div class="case-row held">'
            f"{header}"
            f'<div class="stop-reasons">stopped: {html.escape(reason_text)}</div>'
            "</div>"
        )
    return (
        '<details class="case-row">'
        f"<summary>{header}</summary>"
        f"{_render_field_table(case)}"
        "</details>"
    )


def _render_case_header(case: Mapping[str, Any], outcome: str) -> str:
    case_id = str(case.get("case_id", "unknown"))
    labels = case.get("labels")
    chips = ""
    if isinstance(labels, Mapping):
        chips = "".join(
            f'<span class="label-chip">{html.escape(str(key))}='
            f"{html.escape(str(value))}</span>"
            for key, value in sorted(labels.items())
        )
    badge_color = _OUTCOME_COLORS.get(outcome, _DRIFT_RED)
    badge = (
        f'<span class="badge" style="background: {badge_color};">'
        f"{html.escape(outcome.upper())}</span>"
    )
    return f'<span class="case-id">{html.escape(case_id)}</span>{chips}{badge}'


def _render_field_table(case: Mapping[str, Any]) -> str:
    rows = []
    for item in _joined_field_rows(case):
        result = "match" if item.matches else "drift"
        # Tint by which comparison mismatched: observed-vs-baseline drift is the
        # production environment differing (orange, like the observed column
        # header), candidate-vs-baseline drift is real candidate divergence (red).
        observed_class = "cell-observed" + (
            "" if item.observed_matches else " observed-drift"
        )
        candidate_class = "cell-candidate" + (
            "" if item.candidate_matches else " candidate-drift"
        )
        rows.append(
            "<tr>"
            f'<td class="cell-field">{html.escape(item.field)}</td>'
            f'<td class="{observed_class}">{_format_cell(item.observed)}</td>'
            f'<td class="cell-baseline">{_format_cell(item.baseline)}</td>'
            f'<td class="{candidate_class}">{_format_cell(item.candidate)}</td>'
            f'<td class="cell-result {result}">{result}</td>'
            "</tr>"
        )
    if not rows:
        return '<p class="mono">No field comparisons recorded.</p>'
    return (
        '<table class="fields">'
        "<thead><tr>"
        "<th>field</th>"
        '<th class="observed-head">observed</th>'
        "<th>baseline</th>"
        "<th>candidate</th>"
        "<th>result</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _joined_field_rows(case: Mapping[str, Any]) -> list[_FieldRow]:
    observed_entries = _comparison_entries(case.get("observed_vs_baseline"))
    candidate_entries = _comparison_entries(case.get("candidate_vs_baseline"))
    field_names = list(
        dict.fromkeys([*observed_entries.keys(), *candidate_entries.keys()])
    )
    rows: list[_FieldRow] = []
    for name in field_names:
        observed_entry = observed_entries.get(name)
        candidate_entry = candidate_entries.get(name)
        # Relabeling hazard: the runner builds observed_vs_baseline by passing
        # the observed production output as the first ("baseline") argument of
        # compare_structured_fields, so there "baseline_value" holds the
        # OBSERVED production value and "comparison_value" the fresh
        # baseline-run value. In candidate_vs_baseline, "baseline_value" is the
        # baseline-run value and "comparison_value" the candidate value.
        observed_value = (
            observed_entry.get("baseline_value") if observed_entry else None
        )
        if observed_entry is not None:
            baseline_value = observed_entry.get("comparison_value")
        elif candidate_entry is not None:
            baseline_value = candidate_entry.get("baseline_value")
        else:
            baseline_value = None
        candidate_value = (
            candidate_entry.get("comparison_value") if candidate_entry else None
        )
        rows.append(
            _FieldRow(
                field=name,
                observed=observed_value,
                baseline=baseline_value,
                candidate=candidate_value,
                observed_matches=_entry_matches(observed_entry),
                candidate_matches=_entry_matches(candidate_entry),
            )
        )
    return rows


def _render_footer(report: ImportedVerificationReport) -> str:
    summary = report.summary
    cohorts = ", ".join(_string_items(summary.get("cohorts"))) or "n/a"
    versions = ", ".join(_string_items(summary.get("trace_contract_versions"))) or "n/a"
    fields = ", ".join(_string_items(summary.get("comparison_fields"))) or "n/a"
    return (
        "<footer>"
        f"<p>cohorts: {html.escape(cohorts)}</p>"
        f"<p>trace contract versions: {html.escape(versions)}</p>"
        f"<p>comparison fields: {html.escape(fields)}</p>"
        f"<p>report: {html.escape(report.name)}, created "
        f"{html.escape(report.created_at)}</p>"
        f'<p class="honesty">{html.escape(HONESTY_LINE)}</p>'
        "</footer>"
    )


def _case_outcome(case: Mapping[str, Any]) -> str:
    if case.get("status") != "completed":
        return "held"
    verdict = str(case.get("verdict", "hold"))
    return _VERDICT_OUTCOMES.get(verdict, "drift")


def _case_results(summary: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    results = summary.get("case_results", [])
    if not isinstance(results, list):
        return []
    return [item for item in results if isinstance(item, Mapping)]


def _comparison_entries(value: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return {}
    entries: dict[str, Mapping[str, Any]] = {}
    for item in value:
        if isinstance(item, Mapping) and "field" in item:
            entries[str(item["field"])] = item
    return entries


def _entry_matches(entry: Mapping[str, Any] | None) -> bool:
    if entry is None:
        return True
    return entry.get("matches") is True


def _string_items(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _format_cell(value: Any) -> str:
    if value is None:
        return "n/a"
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    return html.escape(text)
