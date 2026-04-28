# Compliance review with the Claude Agent SDK

A Claude agent audits a fictional company's documents against a set of standards. The interesting part isn't the audit — it's what happens around it.

A real compliance audit calls Claude many times, uses tools to read policy documents, runs for minutes, and costs real money per turn. Without durability, a crash halfway through means you restart Anthropic billing from zero, re-read every document, and re-derive every finding you already had. With Kitaru, each meaningful agent turn becomes a checkpoint: the transcript is persisted, the result is cached, and a replay from a later failure reuses everything Claude has already figured out.

This example walks through that idea in four stages, each adding one durable-execution capability on top of the last.

## What you'll learn

- **Stage 1** — wrap one Claude turn in one Kitaru checkpoint so a crash doesn't burn the turn. This is the minimum viable durable agent.
- **Stage 2** — run four sequential domain checkpoints plus a synthesis checkpoint, and replay from a single failed step instead of re-running the whole audit.
- **Stage 3** — give the flow a memory of its own prior findings so a weekly re-audit can tell you whether the gaps you flagged last week are still there.
- **Stage 4** — turn the audit into a durable conversation. `kitaru.wait()` pauses the flow between Claude turns; `resume=session_id` keeps the model's full context across the gap, even if the process died in between.

Each stage builds on the previous one. The Claude boundary is the same from Stage 1 onwards: every Claude-running checkpoint returns a `ClaudeAgentResult` Pydantic model, and every later stage reuses that shape rather than inventing a new one.

## Quick start

From the repository root:

```bash
uv sync --extra local --extra claude-agent-sdk
kitaru init
export ANTHROPIC_API_KEY=sk-ant-...
```

Pick a stage and run it:

| Stage | Script | One-liner |
|---|---|---|
| 1 | `stage_1_single_turn.py` | One Claude turn as one checkpoint. |
| 2 | `stage_2_multi_domain.py` | Four sequential domain checkpoints + saved report artifact, with partial replay. |
| 3 | `stage_3_memory.py` | HR + IT audit with flow-scoped memory across runs. |
| 4 | `stage_4_conversational.py` | Wait/resume conversational loop over a single Claude session. |

```bash
uv run examples/end_to_end/compliance_review/stage_1_single_turn.py
```

### Re-running the examples costs real money

Each stage's `run_workflow()` defaults to `cache=False`. That means every invocation — including back-to-back reruns with identical prompts — makes fresh Claude API calls. We default to fresh runs because the pedagogy is "watch the agent work": an implicit cache-hit would silently skip Claude on the second run and hide the behavior you came here to see. Replay (`.replay()` on a prior execution) is independent of this default and still reuses durable checkpoint outputs. If you want to experiment with re-run cache-hits explicitly, call `run_workflow(cache=True)` from Python.

### Running against a remote stack

The local default stack works out of the box. If you want to run against a remote stack (S3 artifact store, Kubernetes orchestrator, Vertex, etc.), install that stack's ZenML integration into the same venv after syncing.

A remote runner does not automatically inherit your laptop's shell `ANTHROPIC_API_KEY`. The safest path is to expose that credential at step runtime from a centralized `anthropic` secret:

```bash
kitaru stack use <your_remote_stack>
zenml integration install s3          # or kubernetes, vertex, gcp, azure, …

kitaru secrets set anthropic --ANTHROPIC_API_KEY=sk-ant-...
export KITARU_IMAGE='{"secret_environment_from":["anthropic"]}'

uv run examples/end_to_end/compliance_review/stage_2_multi_domain.py
```

This example also keeps a `kitaru.get_secret("anthropic")` fallback for known remote stacks when `ANTHROPIC_API_KEY` is still missing at runtime, so the existing local quickstart and guarded tests keep working.

If you are calling a stage from Python instead of using `KITARU_IMAGE`, pass `use_secret_environment=True` to its `run_workflow(...)` helper to send the same `secret_environment_from=["anthropic"]` override for that run.

## Stage 1 — one turn, one checkpoint

The checkpoint asks Claude one narrow question: *does Acme Corp's IT security policy meet SOC 2 data retention requirements?* Claude runs its internal tool-use loop (search, read, read section) through the Claude Agent SDK's MCP transport, and the final result falls out as structured text.

Kitaru's job is small but load-bearing: it persists the `ClaudeAgentResult` — session id, transcript path, final text, token usage, cost — as the checkpoint's output. If the process crashes the next time you run this flow with `--replay`, the completed checkpoint returns its cached result instead of calling Claude again.

This is the pattern every later stage composes.

## Stage 2 — multi-domain audit and partial replay

Four domain checkpoints (HR, IT security, vendor contracts, insurance) run sequentially, and a synthesis checkpoint then reads all four results and writes a Markdown report as a Kitaru artifact with `kitaru.save()`. The example stays sequential on purpose — the teaching point is partial replay across durable checkpoint boundaries, not parallelism.

Each domain is its own checkpoint, so a failure in `check_insurance` or `synthesize_report` doesn't roll back the three domain audits that already completed. Pick up exactly where you left off:

```bash
kitaru executions replay <exec-id> --from check_insurance
```

Everything before that checkpoint returns its cached `ClaudeAgentResult`; everything at or after re-runs. For a 5-turn audit that costs a few cents per turn, this is the difference between a cheap retry and a full restart.

## Stage 3 — memory across runs

The audit should get smarter each time it runs. The second run shouldn't just re-check the same gaps in isolation; it should know what the first run found and ask "is that still true?"

Stage 3 uses **flow-scoped memory**: the `audit_with_memory` flow has its own memory keyed by flow id, not a shared namespace. The flow body reads prior findings before dispatching checkpoints, passes them in as arguments, and writes the fresh findings back after the checkpoints complete. A final change-report checkpoint compares current to prior.

By default those reads are lenient: if an older memory entry exists but its backing artifact store is unreachable from the current runtime, Kitaru warns and Stage 3 treats it like \"no prior finding\" so the audit can continue. If you are driving the flow programmatically, `strict_memory=True` switches that to fail-fast behavior.

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

The small but important detail is the transcript file. The Claude Agent SDK stores resumable session history as JSONL under the local Claude project directory:

```text
~/.claude/projects/<encoded-working-directory>/<session-id>.jsonl
```

That local file is what makes `resume=<session_id>` work. On a laptop, it usually survives because every turn runs on the same machine. On a remote stack, turn 1 might run in pod A and turn 2 might run in pod B. Pod B receives the `ClaudeAgentResult` artifact, but it does not automatically have pod A's `~/.claude/...jsonl` file.

This example registers a `ClaudeAgentResultMaterializer` before any checkpoints execute. On save, it stores the normal Pydantic result plus the transcript JSONL inside the ZenML artifact. On load, it recreates the transcript at the path Claude expects before the next checkpoint calls `run_agent_turn(..., resume=context.session_id)`.

So the story is:

```text
turn 1 writes ~/.claude/.../abc.jsonl
      -> materializer bundles abc.jsonl into the checkpoint artifact
      -> wait pauses the flow
turn 2 starts on any machine
      -> materializer restores ~/.claude/.../abc.jsonl
      -> Claude resume=abc can see the previous conversation
```

Locally, Kitaru can prompt in the terminal. For a non-interactive run, drive the wait from a second terminal:

```bash
kitaru executions input <exec-id> --value '"Please explain the highest-priority remediation."'
kitaru executions resume <exec-id>

# Finish and return the latest ClaudeAgentResult.
kitaru executions input <exec-id> --value '"/done"'
kitaru executions resume <exec-id>
```

Each turn is its own checkpoint, so a crashed conversation resumes cleanly without reshowing Claude the prior turns — the SDK's own session replay handles that on the model side.

### Transcript security and retention

The materializer stores the raw Claude transcript JSONL. Treat it like conversation data, not harmless metadata. It may include prompts, model outputs, tool-call arguments, retrieved document snippets, and anything else the Claude Agent SDK records for that session.

For this synthetic local example, that is fine. For real compliance work, make sure your artifact store retention, access controls, encryption, and deletion policy match the sensitivity of the documents and prompts you let the agent see. If a transcript should not be retained, do not run that conversation through a durable artifact store without first changing this materializer's storage policy.

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
