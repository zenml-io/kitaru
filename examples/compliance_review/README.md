# Compliance review with the Claude Agent SDK

A Claude agent audits a fictional company's documents against a set of standards. The interesting part isn't the audit — it's what happens around it.

A real compliance audit calls Claude many times, uses tools to read policy documents, runs for minutes, and costs real money per turn. Without durability, a crash halfway through means you restart Anthropic billing from zero, re-read every document, and re-derive every finding you already had. With Kitaru, each meaningful agent turn becomes a checkpoint: the transcript is persisted, the result is cached, and a replay from a later failure reuses everything Claude has already figured out.

This example walks through that idea in four runnable stages, each adding one durable-execution capability on top of the last.

## What you'll learn

- **Stage 1** — wrap one Claude turn in one Kitaru checkpoint so a crash doesn't burn the turn. This is the minimum viable durable agent.
- **Stage 2** — fan out to four domain checkpoints plus a synthesis checkpoint, and replay from a single failed step instead of re-running the whole audit.
- **Stage 3** — give the flow a memory of its own prior findings so a weekly re-audit can tell you whether the gaps you flagged last week are still there.
- **Stage 4** — turn the audit into a durable conversation. `kitaru.wait()` pauses the flow between Claude turns; `resume=session_id` keeps the model's full context across the gap, even if the process died in between.
- **Stage 5** — a placeholder for the deploy story; intentionally thin today.

Each stage builds on the previous one. The Claude boundary is the same from Stage 1 onwards: every Claude-running checkpoint returns a `ClaudeAgentResult` Pydantic model, and every later stage reuses that shape rather than inventing a new one.

## Quick start

```bash
cd examples/compliance_review
uv sync
kitaru init
export ANTHROPIC_API_KEY=sk-ant-...
```

If you are developing inside the Kitaru repo and want the local checkout instead of the published wheel:

```bash
uv pip install -e '../..[local]'
```

Pick a stage and run it:

| Stage | Script | One-liner |
|---|---|---|
| 1 | `stage_1_single_turn.py` | One Claude turn as one checkpoint. |
| 2 | `stage_2_multi_domain.py` | Four domain checkpoints + saved report artifact. |
| 3 | `stage_3_memory.py` | HR + IT audit with flow-scoped memory across runs. |
| 4 | `stage_4_conversational.py` | Wait/resume conversational loop over a single Claude session. |
| 5 | `stage_5_deploy.py` | Placeholder. |

```bash
uv run stage_1_single_turn.py
```

Every stage also exposes a `run_workflow()` function, so you can drive it from Python or a test:

```python
from examples.compliance_review.stage_2_multi_domain import run_workflow

result = run_workflow()
print(result.result)
```

## Stage 1 — one turn, one checkpoint

The checkpoint asks Claude one narrow question: *does Acme Corp's IT security policy meet SOC 2 data retention requirements?* Claude runs its internal tool-use loop (search, read, read section) through the Claude Agent SDK's MCP transport, and the final result falls out as structured text.

Kitaru's job is small but load-bearing: it persists the `ClaudeAgentResult` — session id, transcript path, final text, token usage, cost — as the checkpoint's output. If the process crashes the next time you run this flow with `--replay`, the completed checkpoint returns its cached result instead of calling Claude again.

This is the pattern every later stage composes.

## Stage 2 — multi-domain audit and partial replay

Four domain checkpoints (HR, IT security, vendor contracts, insurance) fan out via `.submit()`, then a synthesis checkpoint reads all four results and writes a Markdown report as a Kitaru artifact with `kitaru.save()`.

The payoff is replay. Each domain is its own checkpoint, so a failure in `check_insurance` or `synthesize_report` doesn't roll back the three domain audits that already completed. Pick up exactly where you left off:

```bash
kitaru executions replay <exec-id> --from check_insurance
```

Everything before that checkpoint returns its cached `ClaudeAgentResult`; everything at or after re-runs. For a 5-turn audit that costs a few cents per turn, this is the difference between a cheap retry and a full restart.

## Stage 3 — memory across runs

The audit should get smarter each time it runs. The second run shouldn't just re-check the same gaps in isolation; it should know what the first run found and ask "is that still true?"

Stage 3 uses **flow-scoped memory**: the `audit_with_memory` flow has its own memory keyed by flow id, not a shared namespace. The flow body reads prior findings before dispatching checkpoints, passes them in as arguments, and writes the fresh findings back after the checkpoints complete. A final change-report checkpoint compares current to prior.

Inspect and seed memory from the CLI:

```bash
# Find this flow's scope id from its latest execution.
kitaru executions list --flow audit_with_memory --limit 1 --output json

# List, seed, or compact entries for that scope.
kitaru memory list --scope <flow-scope-id> --scope-type flow
kitaru memory set findings/it_security \
  '{"status":"known_gap","summary":"Data retention schedule missing"}' \
  --scope <flow-scope-id> --scope-type flow
kitaru memory compact --scope <flow-scope-id> --scope-type flow \
  --key findings/it_security --source-mode history
```

The checkpoints themselves never touch `kitaru.memory` — memory is flow-body business, which keeps the checkpoints pure and replayable.

## Stage 4 — durable conversation

Some audits aren't one-shot; a human wants to steer the review turn by turn. Stage 4 models this as a loop:

1. The `run_claude_agent` checkpoint runs one Claude turn.
2. The flow body pauses with `kitaru.wait()` and hands Claude's answer to the operator.
3. The operator replies (or says `/done`).
4. The next checkpoint resumes the same Claude session via `resume=<session_id>`, so the model keeps its full transcript of the conversation — even though the Python process may have been down in between.

Locally, Kitaru can prompt in the terminal. For a non-interactive run, drive the wait from a second terminal:

```bash
kitaru executions input <exec-id> --value '"Please explain the highest-priority remediation."'
kitaru executions resume <exec-id>

# Finish and return the latest ClaudeAgentResult.
kitaru executions input <exec-id> --value '"/done"'
kitaru executions resume <exec-id>
```

Each turn is its own checkpoint, so a crashed conversation resumes cleanly without reshowing Claude the prior turns — the SDK's own session replay handles that on the model side.

## Data

The synthetic corpus lives in `data/`:

```text
data/
  company.json
  documents/     # employee handbook, IT security policy, vendor contracts, etc.
  standards/     # SOC 2 controls, labor law requirements, contract clauses, etc.
```

Each document is a JSON object with stable metadata, section ids, section text, and a `known_planted_findings` array recording the intended pass/gap outcome. No PDF parsing, no vector database, no external services — everything the agent sees is a local file you can read.

## Retrieval tools

`tools.py` gives the agent a deterministic surface over the JSON corpus:

- `search_documents(query)` — case-insensitive token search across sections and requirements.
- `read_document(doc_id)` / `read_document(doc_id, section=...)` / `read_section(doc_id, section)`.
- `list_documents()` — catalog view of company documents.
- `get_company_info()` — the Acme Corp profile.

These are plain Python functions, registered as Claude Agent SDK custom tools in `claude_agent.py`. No Kitaru primitives, no model calls.

## Testing

The stage tests stub Claude so they don't hit Anthropic, run the real decorated flows, and verify the durable state Kitaru captured (artifacts, memory, wait points, session resume). They follow the same pattern across stages:

```bash
uv run pytest tests/test_compliance_review_tools.py          # retrieval unit tests
uv run pytest tests/test_phase1_compliance_review_stage1.py  # Stage 1 integration
uv run pytest tests/test_phase2_compliance_review_stage2.py  # Stage 2 integration
uv run pytest tests/test_phase3_compliance_review_stage3.py  # Stage 3 integration
uv run pytest tests/test_phase4_compliance_review_stage4.py  # Stage 4 integration
```

None of these tests require `ANTHROPIC_API_KEY`.

## Credentials

The Claude-backed stages (1–4) need Anthropic credentials in the environment expected by the Claude Agent SDK — typically `ANTHROPIC_API_KEY`. The retrieval unit tests and the `tools.py` surface run without any credentials at all.
