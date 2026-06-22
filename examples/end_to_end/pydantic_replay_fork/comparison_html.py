"""Render a rerun-vs-replay comparison as a self-contained HTML report.

The shared trunk (cached gather_context head) splits at the ``decide``
checkpoint into two branches:
  - rerun:  same agent config re-runs decide+finalize unchanged.
  - replay: a reconfigured agent (different model/prompt_profile) re-runs
    decide+finalize.

Three sections — Settings (what replay changed), Execution (cached head vs
live tail), and Outcomes (the decision fields, rerun vs replay).

Single public function: ``write(path, ...) -> path``.
"""

from __future__ import annotations

import html
import json
from collections.abc import Sequence
from typing import Any

_CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; background: #f6f7f9; color: #1c2024;
  font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
.wrap { max-width: 920px; margin: 0 auto; padding: 32px 24px 64px; }
.banner { border-radius: 12px; padding: 16px 20px; margin-bottom: 24px; font-weight: 600; }
.banner.drift { background: #fdecec; color: #8a1c1c; border: 1px solid #f3c0c0; }
.banner.clean { background: #e9f7ec; color: #1e6b34; border: 1px solid #bfe6c8; }
h1 { font-size: 22px; margin: 0 0 4px; }
.sub { color: #6b7280; margin: 0 0 28px; font-size: 13px; }
.card { background: #fff; border: 1px solid #e6e8eb; border-radius: 12px;
  padding: 18px 20px; margin-bottom: 20px; }
.card h2 { font-size: 12px; letter-spacing: .08em; text-transform: uppercase;
  color: #8a9099; margin: 0 0 14px; }
table { width: 100%; border-collapse: collapse; }
td, th { padding: 8px 10px; text-align: left; vertical-align: top; }
tr + tr td { border-top: 1px solid #f0f1f3; }
.field { color: #6b7280; width: 200px; font-variant-numeric: tabular-nums; }
.val { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
.arrow { color: #b0b6bd; padding: 0 4px; }
tr.changed td.val.to { color: #b42318; font-weight: 600; }
tr.changed { background: #fdf3f3; }
.tag { display: inline-block; font-size: 11px; padding: 1px 7px; border-radius: 999px;
  margin-left: 8px; vertical-align: middle; }
.tag.ok { background: #e9f7ec; color: #1e6b34; }
.tag.chg { background: #fdecec; color: #8a1c1c; }
.flow { display: flex; gap: 10px; align-items: stretch; flex-wrap: wrap; }
.node { font-family: ui-monospace, monospace; font-size: 12px; padding: 7px 11px;
  border-radius: 8px; border: 1px solid #e6e8eb; background: #fff; white-space: nowrap; }
.node.cached { background: #f1f2f4; color: #6b7280; border-style: dashed; }
.node.live { background: #eef4ff; color: #1d4ed8; border-color: #cfe0ff; }
.branch { border: 1px solid #e6e8eb; border-radius: 10px; padding: 12px; margin-top: 10px; }
.branch h3 { margin: 0 0 8px; font-size: 13px; }
.branch.rerun h3 { color: #1e6b34; }
.branch.replay h3 { color: #b42318; }
.legend { color: #8a9099; font-size: 12px; margin-top: 10px; }
.answers { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.answers .col h3 { margin: 0 0 6px; font-size: 13px; }
.answers .col.rerun h3 { color: #1e6b34; }
.answers .col.replay h3 { color: #b42318; }
.answers pre { white-space: pre-wrap; font: 12px/1.5 ui-monospace, monospace;
  background: #f6f7f9; border: 1px solid #eceef0; border-radius: 8px; padding: 10px; margin: 0; }
"""


def _fmt(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return html.escape(json.dumps(value, ensure_ascii=False))
    return html.escape("—" if value is None else str(value))


def _nodes_row(nodes: Sequence[str], cls: str) -> str:
    return "".join(f'<span class="node {cls}">{html.escape(n)}</span>' for n in nodes)


def render(
    *,
    exec_id: str,
    scenario: str,
    cut: str,
    nodes: Sequence[str],
    settings_changes: Sequence[tuple[str, Any, Any]],
    outcomes: Sequence[tuple[str, Any, Any, bool]],
    has_drift: bool,
    rerun_summary: str,
    replay_summary: str,
) -> str:
    """Render the full HTML string.

    Args:
        exec_id:          Baseline execution ID (used as the report title).
        scenario:         Human-readable scenario label.
        cut:              Checkpoint name where branches split (``"decide"``).
        nodes:            All checkpoint names in order.
        settings_changes: ``(key, rerun_value, replay_value)`` tuples describing
                          what replay changed.
        outcomes:         ``(field, rerun_value, replay_value, matches)`` tuples
                          for the decision-field drift table.
        has_drift:        True if the replay decision differs from the rerun.
        rerun_summary:    Short human-readable summary of the rerun decision.
        replay_summary:   Short human-readable summary of the replay decision.

    Returns:
        A self-contained HTML string.
    """
    cut_idx = list(nodes).index(cut) if cut in nodes else 0
    cached_nodes, live_nodes = nodes[:cut_idx], nodes[cut_idx:]

    banner = (
        '<div class="banner drift">Replay drift detected — '
        "the reconfigured agent changed the decision.</div>"
        if has_drift
        else '<div class="banner clean">No replay drift — '
        "the replay reproduced the same decision.</div>"
    )

    settings_rows = "".join(
        f'<tr class="changed"><td class="field">{html.escape(k)}</td>'
        f'<td class="val from">{_fmt(a)}</td><td class="arrow">→</td>'
        f'<td class="val to">{_fmt(b)}</td></tr>'
        for k, a, b in settings_changes
    ) or (
        '<tr><td class="field">—</td>'
        '<td class="val">no settings changed</td><td></td><td></td></tr>'
    )

    outcome_rows = ""
    for field, rerun_val, replay_val, matches in outcomes:
        tag = (
            '<span class="tag ok">unchanged</span>'
            if matches
            else '<span class="tag chg">changed</span>'
        )
        cls = "" if matches else "changed"
        to_cls = "val" if matches else "val to"
        outcome_rows += (
            f'<tr class="{cls}"><td class="field">{html.escape(field)}{tag}</td>'
            f'<td class="val from">{_fmt(rerun_val)}</td>'
            f'<td class="arrow">{"=" if matches else "→"}</td>'
            f'<td class="{to_cls}">{_fmt(replay_val)}</td></tr>'
        )

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rerun vs Replay — {html.escape(exec_id)}</title>
<style>{_CSS}</style></head>
<body><div class="wrap">
{banner}
<h1>Rerun vs Replay</h1>
<p class="sub">{html.escape(exec_id)} &middot; scenario <code>{html.escape(scenario)}</code> \
&middot; cut <code>{html.escape(cut)}</code></p>

<div class="card"><h2>Settings — what replay changed</h2>
<table>{settings_rows}</table></div>

<div class="card"><h2>Execution — cached head, live tail</h2>
<div class="flow">\
{_nodes_row(cached_nodes, "cached") or '<span class="legend">(no cached head)</span>'}\
</div>
<div class="branch rerun"><h3>rerun (same config)</h3>\
<div class="flow">{_nodes_row(live_nodes, "live")}</div></div>
<div class="branch replay"><h3>replay (reconfigured)</h3>\
<div class="flow">{_nodes_row(live_nodes, "live")}</div></div>
<p class="legend">Dashed = served from checkpoint cache (no model calls). \
Blue = re-executed live from the cut point.</p>
</div>

<div class="card"><h2>Outcomes — rerun vs replay</h2>
<table>{outcome_rows}</table>
<div class="answers" style="margin-top:16px">
  <div class="col rerun"><h3>rerun answer</h3>\
<pre>{html.escape(rerun_summary or "—")}</pre></div>
  <div class="col replay"><h3>replay answer</h3>\
<pre>{html.escape(replay_summary or "—")}</pre></div>
</div></div>

</div></body></html>"""


def write(
    path: str,
    *,
    exec_id: str,
    scenario: str,
    cut: str,
    nodes: Sequence[str],
    settings_changes: Sequence[tuple[str, Any, Any]],
    outcomes: Sequence[tuple[str, Any, Any, bool]],
    has_drift: bool,
    rerun_summary: str,
    replay_summary: str,
) -> str:
    """Write the comparison HTML to *path* and return the resolved absolute path."""
    from pathlib import Path

    content = render(
        exec_id=exec_id,
        scenario=scenario,
        cut=cut,
        nodes=nodes,
        settings_changes=settings_changes,
        outcomes=outcomes,
        has_drift=has_drift,
        rerun_summary=rerun_summary,
        replay_summary=replay_summary,
    )
    Path(path).write_text(content, encoding="utf-8")
    return str(Path(path).resolve())
