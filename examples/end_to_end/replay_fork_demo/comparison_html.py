"""Render a three-way replay/fork comparison as a self-contained HTML report.

The report shows what happened in order:
  - original recorded trace: the Langfuse trace imported into Kitaru.
  - unchanged replay: Kitaru reuses recorded outputs before the cut and re-runs
    the live tail from the cut with the recorded config.
  - edited fork: Kitaru starts from the same imported trace, reuses the same
    earlier recorded outputs, then re-runs the live tail with edited config.
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
.wrap { max-width: 1080px; margin: 0 auto; padding: 32px 24px 64px; }
.banner { border-radius: 12px; padding: 16px 20px; margin-bottom: 24px; font-weight: 600; }
.banner.drift { background: #fdecec; color: #8a1c1c; border: 1px solid #f3c0c0; }
.banner.warn { background: #fff8e6; color: #8a5a00; border: 1px solid #f2d184; }
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
.field { color: #6b7280; width: 180px; font-variant-numeric: tabular-nums; }
.val { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
.arrow { color: #b0b6bd; font-family: ui-monospace, monospace; white-space: nowrap; }
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
.node.recorded { background: #fff; color: #374151; }
.branch { border: 1px solid #e6e8eb; border-radius: 10px; padding: 12px; margin-top: 10px; }
.branch h3 { margin: 0 0 8px; font-size: 13px; }
.branch.original h3 { color: #374151; }
.branch.replay h3 { color: #1e6b34; }
.branch.fork h3 { color: #b42318; }
.legend { color: #8a9099; font-size: 12px; margin-top: 10px; }
.answers { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
.answers .col h3 { margin: 0 0 6px; font-size: 13px; }
.answers .col.original h3 { color: #374151; }
.answers .col.replay h3 { color: #1e6b34; }
.answers .col.fork h3 { color: #b42318; }
.answers pre { white-space: pre-wrap; font: 12px/1.5 ui-monospace, monospace;
  background: #f6f7f9; border: 1px solid #eceef0; border-radius: 8px; padding: 10px; margin: 0; }
"""


def _fmt(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return html.escape(json.dumps(value, ensure_ascii=False))
    return html.escape("—" if value is None else str(value))


def _nodes_row(nodes: Sequence[str], cls: str) -> str:
    return "".join(f'<span class="node {cls}">{html.escape(n)}</span>' for n in nodes)


def _match_tag(matches: bool) -> str:
    return (
        '<span class="tag ok">match</span>'
        if matches
        else '<span class="tag chg">changed</span>'
    )


def render(
    *,
    case_id: str,
    scenario: str,
    cut: str,
    nodes: Sequence[str],
    settings_changes: Sequence[tuple[str, Any, Any]],
    outcomes: Sequence[tuple[str, Any, Any, Any, bool, bool]],
    has_reproduction_drift: bool,
    has_fork_drift: bool,
    original_summary: str,
    replay_summary: str,
    fork_summary: str,
) -> str:
    cut_idx = list(nodes).index(cut) if cut in nodes else 0
    cached, live = nodes[:cut_idx], nodes[cut_idx:]

    if has_reproduction_drift:
        banner = (
            '<div class="banner drift">Reproduction drift detected — the '
            "unchanged replay did not match the original recorded trace.</div>"
        )
    elif has_fork_drift:
        banner = (
            '<div class="banner warn">Fork drift detected — the edited fork '
            "changed the decision after reproduction succeeded.</div>"
        )
    else:
        banner = (
            '<div class="banner clean">No decision drift — unchanged replay '
            "matched the original trace, and edited fork kept the same decision.</div>"
        )

    settings_rows = (
        "".join(
            f'<tr class="changed"><td class="field">{html.escape(k)}</td>'
            f'<td class="val from">{_fmt(a)}</td><td class="arrow">→</td>'
            f'<td class="val to">{_fmt(b)}</td></tr>'
            for k, a, b in settings_changes
        )
        or '<tr><td class="field">—</td><td class="val">no settings changed</td><td></td><td></td></tr>'
    )

    outcome_rows = ""
    for field, original, replay, fork, reproduction_ok, fork_ok in outcomes:
        cls = "" if reproduction_ok and fork_ok else "changed"
        fork_cls = "val" if fork_ok else "val to"
        outcome_rows += (
            f'<tr class="{cls}"><td class="field">{html.escape(field)}</td>'
            f'<td class="val">{_fmt(original)}</td>'
            f'<td class="val">{_fmt(replay)}</td>'
            f'<td class="{fork_cls}">{_fmt(fork)}</td>'
            f'<td class="arrow">{_match_tag(reproduction_ok)}</td>'
            f'<td class="arrow">{_match_tag(fork_ok)}</td></tr>'
        )

    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Replay three-way comparison — {html.escape(case_id)}</title><style>{_CSS}</style></head>
<body><div class="wrap">
{banner}
<h1>Replay comparison — original trace vs unchanged replay vs edited fork</h1>
<p class="sub">{html.escape(case_id)} &middot; scenario <code>{html.escape(scenario)}</code> &middot; fork point <code>{html.escape(cut)}</code></p>

<div class="card"><h2>Settings — what the fork changed</h2>
<table>{settings_rows}</table></div>

<div class="card"><h2>Execution — original trace, cached outputs, live replay tail</h2>
<div class="branch original"><h3>original recorded trace</h3><div class="flow">{_nodes_row(nodes, "recorded")}</div></div>
<div class="branch replay"><h3>unchanged replay</h3><div class="flow">{_nodes_row(cached, "cached") or '<span class="legend">(no cached head)</span>'}{_nodes_row(live, "live")}</div></div>
<div class="branch fork"><h3>edited fork</h3><div class="flow">{_nodes_row(cached, "cached") or '<span class="legend">(no cached head)</span>'}{_nodes_row(live, "live")}</div></div>
<p class="legend">The original trace is the recorded source. The replay and fork both reuse recorded outputs before the cut and re-execute the live tail from the cut.</p>
</div>

<div class="card"><h2>Outcomes — original trace → unchanged replay → edited fork</h2>
<table>
<thead><tr><th>Field</th><th>Original recorded trace</th><th>Unchanged replay</th><th>Edited fork</th><th>Trace→Replay</th><th>Replay→Fork</th></tr></thead>
<tbody>{outcome_rows}</tbody></table>
<div class="answers" style="margin-top:16px">
  <div class="col original"><h3>original trace answer</h3><pre>{html.escape(original_summary or "—")}</pre></div>
  <div class="col replay"><h3>unchanged replay answer</h3><pre>{html.escape(replay_summary or "—")}</pre></div>
  <div class="col fork"><h3>edited fork answer</h3><pre>{html.escape(fork_summary or "—")}</pre></div>
</div></div>

</div></body></html>"""


def write(path: str, **kwargs: Any) -> str:
    from pathlib import Path

    Path(path).write_text(render(**kwargs), encoding="utf-8")
    return str(Path(path).resolve())
