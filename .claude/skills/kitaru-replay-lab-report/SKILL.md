---
name: kitaru-replay-lab-report
description: >-
  Interpret and refine local Replay Lab JSON, Markdown, and HTML reports under
  examples/end_to_end/replay_lab/reports/. Use when the user wants to understand
  replay drift, candidate effect, reliability, changed outputs, or the demo report.
---

# Kitaru Replay Lab: Report

Use this skill after Replay Lab has generated report files.

The job is to help the user understand what the report says, how reliable it is, and what decision it supports.

## Prototype report files

Reports are written under:

- `examples/end_to_end/replay_lab/reports/`

Common report forms:

- JSON report — structured data, best source of truth for exact metrics.
- Markdown report — readable summary.
- HTML report — static demo artifact, usually rendered by `examples/end_to_end/replay_lab/render_report.py`.

Related inputs:

- Manifest: `examples/end_to_end/replay_lab/manifests/*.json`
- Candidate descriptor: `examples/end_to_end/replay_lab/candidates/cheaper_support_agent.json`

Backend comparison source:

- MCP tool: `kitaru_replay_lab_compare`

## Reading order

1. Read the JSON report first when exact numbers matter.
2. Read the Markdown report for the narrative summary.
3. Inspect the HTML only for presentation or demo polish.
4. If a report points to execution IDs, use Kitaru inspection tools to check the underlying execution details when needed.

## Explanation pattern

Explain reports in this order:

1. **Headline recommendation** — should the candidate be accepted, rejected, or investigated further?
2. **Replay drift** — did baseline replay differ from observed production?
3. **Candidate effect** — did candidate replay improve or worsen compared with baseline replay?
4. **Changed outputs** — did the answer text or decision change in a suspicious way?
5. **Case-level story** — which cases drove the conclusion?
6. **Limitations** — what facts were missing, failed, timed out, or inconclusive?

## Concrete language to use

Prefer concrete phrasing like:

> The candidate looks cheaper on three cases, but one refund case changed the final answer. Because baseline replay was already cheaper than observed production, we should not credit the candidate for the full cost drop.

Avoid compressed claims like:

> Candidate improves cost subject to replay drift.

## Reliability checks

Call out these situations clearly:

- Large observed-vs-baseline drift.
- Missing `scorecard` or `final_response` artifacts.
- Failed, cancelled, or timed-out lanes.
- Candidate output differs from baseline output.
- Quality score improves while final answer becomes less safe or less useful.

## Report refinement

If the user asks to polish the static report, keep changes local to report rendering or generated report artifacts unless explicitly told otherwise.

Do not change backend comparison logic, demo flow code, smoke tests, examples, `../kitaru-skills`, or plugin metadata from this reporting skill unless the user explicitly asks.
