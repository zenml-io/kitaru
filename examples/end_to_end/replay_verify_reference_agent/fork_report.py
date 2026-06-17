# ruff: noqa: E501
"""Report data and HTML rendering for the LangGraph fork demo."""

import html
from typing import Any

from pydantic import BaseModel, Field


class VariantSummary(BaseModel):
    """Report-safe subset of an agent variant."""

    name: str
    model: str
    prompt_profile: str
    tool_policy_name: str


class ForkDemoReport(BaseModel):
    """Data needed to explain one baseline run and one native LangGraph fork."""

    thread_id: str
    scenario_id: str
    selected_checkpoint_id: str | None
    selected_checkpoint_ns: str | None = None
    baseline_latest_checkpoint_id: str | None
    fork_checkpoint_id: str | None
    fork_checkpoint_ns: str | None = None
    terminal_fork_checkpoint_id: str | None
    baseline_variant: VariantSummary
    candidate_variant: VariantSummary
    baseline_decision: dict[str, Any]
    forked_decision: dict[str, Any]
    baseline_evidence_summary: str
    forked_evidence_summary: str
    baseline_audit_relevant_tool_names: list[str] = Field(default_factory=list)
    forked_audit_relevant_tool_names: list[str] = Field(default_factory=list)
    baseline_tool_execution_names: list[str] = Field(default_factory=list)
    forked_tool_execution_names: list[str] = Field(default_factory=list)
    changed_tool_execution_names: list[str] = Field(default_factory=list)
    updated_state_keys: list[str] = Field(default_factory=list)
    matched_snapshot_count: int = 0
    selected_match_index: int = 0
    checkpoint_strategy: str = "graph_call"
    baseline_model_call_count: int = 0
    baseline_tool_call_count: int = 0
    forked_model_call_count: int = 0
    forked_tool_call_count: int = 0
    warnings: list[str] = Field(default_factory=list)
    execution_mode: str = "Kitaru-orchestrated LangGraph native fork"
    kitaru_role: str = "select checkpoint, label request, resume through adapter, record result, report"
    langgraph_role: str = (
        "checkpoint history lookup, update_state, fork checkpoint creation, resume"
    )
    checkpointer: str = "InMemorySaver; same-process demo storage only"

    @property
    def tool_collection_rerun(self) -> bool:
        """Return whether the fork changed the collected tool records."""
        return self.baseline_tool_execution_names != self.forked_tool_execution_names


def render_fork_demo_html(report: ForkDemoReport) -> str:
    """Render a compact visual report for the fork demo."""
    candidate_diff_rows = _rows(
        [
            ("Model", report.baseline_variant.model, report.candidate_variant.model),
            (
                "Prompt profile",
                report.baseline_variant.prompt_profile,
                report.candidate_variant.prompt_profile,
            ),
            (
                "Tool policy",
                report.baseline_variant.tool_policy_name,
                report.candidate_variant.tool_policy_name,
            ),
        ]
    )
    behavior_diff_rows = _rows(
        [
            (
                "Required action",
                str(report.baseline_decision.get("required_action", "unknown")),
                str(report.forked_decision.get("required_action", "unknown")),
            ),
            (
                "Risk status",
                str(report.baseline_decision.get("risk_status", "unknown")),
                str(report.forked_decision.get("risk_status", "unknown")),
            ),
            (
                "Tool collection rerun",
                "no",
                "yes" if report.tool_collection_rerun else "no",
            ),
        ]
    )
    graph_nodes = [
        ("receive_request", "reused"),
        ("collect_evidence_with_tools", "reused"),
        ("summarize_evidence", "rerun"),
        ("decide_action", "rerun"),
        ("final_response", "rerun"),
    ]
    warnings = "".join(f"<li>{_e(warning)}</li>" for warning in report.warnings)
    if not warnings:
        warnings = "<li>No additional warnings.</li>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Kitaru LangGraph fork demo</title>
  <style>
    body {{ font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f6f7fb; color: #172033; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 32px; }}
    .hero {{ background: #111827; color: white; border-radius: 20px; padding: 28px; margin-bottom: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }}
    .card {{ background: white; border: 1px solid #e5e7eb; border-radius: 16px; padding: 20px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06); }}
    h1, h2, h3 {{ margin-top: 0; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 10px 8px; text-align: left; vertical-align: top; }}
    th {{ color: #475569; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    .nodes {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .node {{ border-radius: 999px; padding: 8px 12px; font-size: 0.9rem; border: 1px solid #cbd5e1; }}
    .reused {{ background: #ecfdf5; color: #065f46; }}
    .rerun {{ background: #eff6ff; color: #1d4ed8; }}
    .muted {{ color: #64748b; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }}
    .summary {{ white-space: pre-wrap; }}
  </style>
</head>
<body>
<main>
  <section class="hero">
    <p class="muted">Execution mode</p>
    <h1>{_e(report.execution_mode)}</h1>
    <p>The public demo call is <span class="mono">kitaru.fork(fork_runner, ...)</span>. It delegates to <span class="mono">KitaruGraphRunner.fork(...)</span>. Kitaru selects and labels the fork run; LangGraph performs checkpoint history lookup, state update, fork checkpoint creation, and downstream resume.</p>
  </section>

  <section class="grid">
    <article class="card">
      <h2>Starting run</h2>
      <p><strong>Scenario:</strong> <span class="mono">{_e(report.scenario_id)}</span></p>
      <p><strong>Thread:</strong> <span class="mono">{_e(report.thread_id)}</span></p>
      <p><strong>Baseline variant:</strong> {_e(report.baseline_variant.name)}</p>
      <p><strong>Baseline decision:</strong> {_e(str(report.baseline_decision.get("required_action", "unknown")))}</p>
      <p><strong>Checkpoint strategy:</strong> <span class="mono">{_e(report.checkpoint_strategy)}</span></p>
    </article>

    <article class="card">
      <h2>Candidate diff</h2>
      <table>
        <thead><tr><th>Field</th><th>Baseline</th><th>Candidate</th></tr></thead>
        <tbody>{candidate_diff_rows}</tbody>
      </table>
    </article>
  </section>

  <section class="grid" style="margin-top: 18px;">
    <article class="card">
      <h2>Fork point</h2>
      <p>The selected checkpoint is after tool collection and before summarization:</p>
      <p class="mono">next == ("summarize_evidence",)</p>
      <p><strong>Selected checkpoint:</strong> <span class="mono">{_e(report.selected_checkpoint_id or "unknown")}</span></p>
      <p><strong>Selected checkpoint namespace:</strong> <span class="mono">{_e(report.selected_checkpoint_ns or "absent")}</span></p>
      <p><strong>Fork checkpoint:</strong> <span class="mono">{_e(report.fork_checkpoint_id or "unknown")}</span></p>
      <p><strong>Fork checkpoint namespace:</strong> <span class="mono">{_e(report.fork_checkpoint_ns or "absent")}</span></p>
      <p><strong>Updated state keys:</strong> <span class="mono">{_e(", ".join(report.updated_state_keys) or "none")}</span></p>
      <p><strong>Selector match:</strong> match {_e(str(report.selected_match_index))} of {_e(str(report.matched_snapshot_count))}</p>
      <p><strong>Terminal fork checkpoint:</strong> <span class="mono">{_e(report.terminal_fork_checkpoint_id or "unknown")}</span></p>
      <div class="nodes">{"".join(_node(name, status) for name, status in graph_nodes)}</div>
    </article>

    <article class="card">
      <h2>Behavior diff</h2>
      <table>
        <thead><tr><th>Field</th><th>Baseline</th><th>Candidate fork</th></tr></thead>
        <tbody>{behavior_diff_rows}</tbody>
      </table>
      <h3>Evidence summary changed downstream</h3>
      <p class="summary"><strong>Baseline:</strong> {_e(report.baseline_evidence_summary)}</p>
      <p class="summary"><strong>Candidate fork:</strong> {_e(report.forked_evidence_summary)}</p>
    </article>
  </section>

  <section class="grid" style="margin-top: 18px;">
    <article class="card">
      <h2>Tool records</h2>
      <p><strong>Baseline tool sequence:</strong> {_e(", ".join(report.baseline_tool_execution_names) or "none")}</p>
      <p><strong>Forked tool sequence:</strong> {_e(", ".join(report.forked_tool_execution_names) or "none")}</p>
      <p><strong>Changed tool executions:</strong> {_e(", ".join(report.changed_tool_execution_names) or "none")}</p>
      <p><strong>Tool collection did not rerun:</strong> {_e("yes" if not report.tool_collection_rerun else "no")}</p>
      <p><strong>Audit-relevant baseline tools:</strong> {_e(", ".join(report.baseline_audit_relevant_tool_names) or "none")}</p>
      <p><strong>Audit-relevant forked tools:</strong> {_e(", ".join(report.forked_audit_relevant_tool_names) or "none")}</p>
    </article>

    <article class="card">
      <h2>Honest roles</h2>
      <p><strong>Kitaru role:</strong> {_e(report.kitaru_role)}</p>
      <p><strong>LangGraph role:</strong> {_e(report.langgraph_role)}</p>
      <p><strong>Checkpointer:</strong> {_e(report.checkpointer)}</p>
      <h3>Calls-mode evidence</h3>
      <p>Baseline model/tool events: {_e(str(report.baseline_model_call_count))} / {_e(str(report.baseline_tool_call_count))}</p>
      <p>Forked model/tool events: {_e(str(report.forked_model_call_count))} / {_e(str(report.forked_tool_call_count))}</p>
      <p class="muted">Individual model/tool checkpoints appear only when calls pass through Kitaru calls-mode instrumentation.</p>
      <h3>Warnings</h3>
      <ul>{warnings}</ul>
    </article>
  </section>
</main>
</body>
</html>
"""


def _rows(rows: list[tuple[str, str, str]]) -> str:
    return "".join(
        f"<tr><td>{_e(label)}</td><td>{_e(before)}</td><td>{_e(after)}</td></tr>"
        for label, before, after in rows
    )


def _node(name: str, status: str) -> str:
    return f'<span class="node {_e(status)}">{_e(name)} · {_e(status)}</span>'


def _e(value: str) -> str:
    return html.escape(value, quote=True)
