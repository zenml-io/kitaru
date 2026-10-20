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

Every insight is drawn from its stored data, so no model is called. An
analyzer can supply its own rendered section by storing
`{"html": ..., "covers": [...]}` under `REPORT_SECTION_KEY` in an insight's
metadata. The section is embedded only when an analyzer task created the
insight, and the insights it names in `covers` from the same analyzer version
are not drawn again as cards.
"""

import html
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    InsightListParams,
    InsightResponse,
    TextInsightData,
)
from kitaru.cli.output import CLIError, CommandResult

REPORT_SECTION_KEY = "kitaru.report_section/v1"


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
    try:
        out.write_text(
            render_insight_report(insights, import_id=import_id), encoding="utf-8"
        )
    except OSError as error:
        raise CLIError("invalid_arguments", f"Cannot write {out}: {error}") from error
    return CommandResult(
        item={
            "path": str(out),
            "import_id": str(import_id),
            "insights": len(insights),
            "sections": [
                insight.name
                for insight, _ in _get_sections(_get_latest_per_name(insights))
            ],
        },
        next_actions=[f"Open {out} in a browser."],
    )


def render_insight_report(
    insights: list[InsightResponse],
    *,
    import_id: uuid.UUID,
    generated_at: datetime | None = None,
) -> str:
    """Build the HTML document for an import's insights."""
    generated = (generated_at or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M UTC")
    latest = _get_latest_per_name(insights)
    sections = _get_sections(latest)
    hidden = {
        (owner.analyzer_version_id, name)
        for owner, section in sections
        for name in section["covers"]
    }
    owners = {owner.id for owner, _ in sections}
    body = [_strip_unsafe_tags(section["html"]) for _, section in sections]
    cards = [
        _insight_card(i)
        for i in latest
        if i.id not in owners and (i.analyzer_version_id, i.name) not in hidden
    ]
    if cards:
        heading = "<h2>Other insights</h2>" if body else ""
        body.append(f"<section>{heading}{''.join(cards)}</section>")
    return _PAGE.format(
        style=_STYLE,
        title="Insight report",
        subtitle=html.escape(f"Import {import_id} · generated {generated}"),
        body="".join(body),
    )


def _get_latest_per_name(insights: list[InsightResponse]) -> list[InsightResponse]:
    """Return the newest insight per analyzer and name, oldest first.

    Rerunning an analyzer stores new insights under the same names, so only
    the latest run of each is rendered. Different analyzers, and insights
    created by hand, may reuse a name and are all kept.
    """
    latest: dict[tuple[uuid.UUID, str], InsightResponse] = {}
    for insight in sorted(insights, key=lambda item: item.created):
        key = (insight.analyzer_version_id or insight.id, insight.name)
        latest.pop(key, None)
        latest[key] = insight
    return list(latest.values())


def _get_sections(
    insights: list[InsightResponse],
) -> list[tuple[InsightResponse, dict[str, Any]]]:
    """Return the well-formed sections of insights an analyzer task created.

    Anyone with insight access can store metadata, so a section from an
    insight without an analyzer version is ignored and the insight is drawn
    as a card instead.
    """
    sections: list[tuple[InsightResponse, dict[str, Any]]] = []
    for insight in insights:
        section = insight.metadata.get(REPORT_SECTION_KEY)
        if insight.analyzer_version_id is None:
            continue
        if not isinstance(section, dict) or not isinstance(section.get("html"), str):
            continue
        covers = section.get("covers", [])
        sections.append(
            (
                insight,
                {
                    "html": section["html"],
                    "covers": [n for n in covers if isinstance(n, str)]
                    if isinstance(covers, list)
                    else [],
                },
            )
        )
    return sections


_UNSAFE_TAG = re.compile(r"<\s*/?\s*(?:meta|base|link)\b[^>]*(?:>|$)", re.IGNORECASE)


def _strip_unsafe_tags(markup: str) -> str:
    """Remove `meta`, `base` and `link` tags from section markup.

    The page CSP cannot stop a `<meta http-equiv="refresh">` navigation, so
    such tags never reach the page. Stripping repeats until nothing matches so
    that removing one tag cannot splice a new one together.
    """
    while True:
        stripped = _UNSAFE_TAG.sub("", markup)
        if stripped == markup:
            return stripped
        markup = stripped


def _insight_card(insight: InsightResponse) -> str:
    """Render any insight from its stored data."""
    data = insight.data
    if isinstance(data, TextInsightData):
        body = f'<pre class="md">{_e(data.content)}</pre>'
    elif isinstance(data, CategoricalInsightData):
        unit = f" {_e(data.unit)}" if data.unit else ""
        peak = max(value.value for value in data.values) or 1
        body = "".join(
            f'<div class="bar"><span>{_e(value.label)}</span>'
            f'<i style="width:{value.value / peak * 100:.1f}%"></i>'
            f"<b>{value.value:g}{unit}</b></div>"
            for value in data.values
        )
    else:
        rows = "".join(
            f"<tr><td>{_e(b.lower_bound)}</td><td>{_e(b.upper_bound)}</td>"
            f"<td>{b.count}</td></tr>"
            for b in data.bins
        )
        body = f"<table><tr><th>From</th><th>To</th><th>Count</th></tr>{rows}</table>"
    description = (
        f'<p class="muted">{_e(insight.description)}</p>' if insight.description else ""
    )
    return f'<div class="card"><h3>{_e(insight.title)}</h3>{description}{body}</div>'


def _e(value: Any) -> str:
    return html.escape("-" if value is None else str(value))


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
.card { border: 1px solid var(--line); border-radius: 8px;
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

# Sections come from analyzer plugins, so the page forbids scripts, remote
# loads and form submission even when a section carries such markup.
_CSP = (
    "default-src 'none'; style-src 'unsafe-inline'; img-src data:; "
    "form-action 'none'; base-uri 'none'"
)

_PAGE = (
    '<!doctype html>\n<html lang="en"><head><meta charset="utf-8">\n'
    '<meta http-equiv="Content-Security-Policy" content="' + _CSP + '">\n'
    '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
    "<title>{title}</title>\n<style>{style}</style></head>\n"
    '<body><h1>{title}</h1><p class="sub">{subtitle}</p>{body}</body></html>\n'
)
