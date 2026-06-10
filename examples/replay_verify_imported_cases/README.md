# Replay Verify: check an agent change against real recorded runs

Picture a team running a support agent in production. It answers billing and
account questions, looks things up with tools, and escalates anything that
needs admin permissions. It runs on an expensive model, and one day the cost
question lands: "can we ship the cheaper model with a trimmed prompt?"

Today that question is surprisingly hard to answer. You cannot just re-run the
agent offline against past conversations, because its tools touch live systems
(create tickets, send emails, rotate keys). Reading through traces by hand does
not scale. So the cheaper config ships on hope, and the first signal that it
broke something is a customer hitting the regression.

This example shows the answer Replay Verify gives, in one local command:

1. load recorded agent runs as neutral imported case records,
2. validate whether each case carries enough evidence to compare safely,
3. run the eligible cases fresh through local baseline and candidate agent
   code, with every write-like tool mocked,
4. stop broken or unsafe cases before any candidate code runs,
5. write JSON, Markdown, and HTML reports with a verdict: `ship`, `caution`,
   or `hold`.

The most important thing to notice is not a perfect score. The important thing
is that bad cases **fail closed**: a case missing required evidence, showing
unsafe write-like behavior, or carrying incomplete retrieval metadata stays in
the report with a concrete reason, and the candidate never runs on it. Nothing
is dropped silently, and nothing unsafe executes.

## Quick start (no credentials needed)

From the repository root:

```bash
uv run python examples/replay_verify_imported_cases/run_langfuse_pydanticai_demo.py \
  --source jsonl \
  --case-file examples/replay_verify_imported_cases/fixtures/support_copilot_imported_cases.jsonl \
  --report-dir ./replay-verify-reports
```

This default mode uses a deterministic local support-copilot implementation,
so you do not need `OPENAI_API_KEY`, Langfuse credentials, a Kitaru server, or
a Kitaru login. It is the stable, reproducible path (and what the tests use).
For real model calls, see [Live mode](#live-mode-real-model-calls) below.

## What the reports show

The command writes five files to the report directory:

```text
replay-verify-reports/
  imported_cases.jsonl
  fidelity_report.json
  verification_report.json
  verification_report.md
  verification_report.html
```

- `verification_report.html` is the one to open first: a self-contained page
  (no server, opens straight from the file) with the overall verdict banner,
  the headline counts, and a case grid where held cases show their stop
  reasons and completed cases expand to a field-by-field comparison of
  observed production output vs the baseline run vs the candidate run.
- `verification_report.md` is the same story in plain text.
- `verification_report.json` is the full machine-readable run summary.
- `fidelity_report.json` shows per-case validation: recovered fields, missing
  fields, eligibility, safety status, and stop reasons.
- `imported_cases.jsonl` is the neutral case cohort after loading and
  normalizing the input.

The Markdown report should contain these lines:

```text
Execution mode: imported_input_fresh_execution_not_deterministic_checkpoint_replay
Recorded-response control: unavailable
Candidate executions for stopped cases: 0
Unsafe live executions: 0
```

That first line is the honesty contract of this whole example: imported cases
drive a *fresh local re-execution* of agent code. This is not a replay of the
original production run, and the report never pretends it is.

## The fixture cohort

The default JSONL file contains eight cases:

| Case | What it represents | Expected behavior |
|---|---|---|
| `rv-model-only-eligible` | model-only support answer | candidate runs |
| `rv-read-only-tool-eligible` | read-only subscription lookup | candidate runs |
| `rv-mocked-write-eligible` | write-like ticket creation, mocked | candidate runs |
| `rv-rag-eligible` | RAG answer with complete retrieval metadata | candidate runs |
| `rv-missing-output-stopped` | no observed output/evaluator signal | candidate stops |
| `rv-missing-tools-stopped` | no imported `available_tools` list | candidate stops |
| `rv-unsafe-live-write-stopped` | live write-like email behavior | candidate stops |
| `rv-incomplete-rag-stopped` | stale/incomplete RAG metadata | candidate stops |

A good demo moment is opening the report and pointing at the four stopped
cases: each one is preserved with a concrete reason instead of being dropped.

The support-copilot runner builds its outputs from the imported `root_input`,
the imported `available_tools`, mocked or read-only tool results, and imported
retrieval metadata for RAG cases. It does **not** decide success by reading an
expected answer from the fixture; the fixture contains no `expected_output`
field.

## Live mode (real model calls)

Live mode swaps the deterministic support-copilot code for a real PydanticAI
agent that makes live model calls. It needs `OPENAI_API_KEY`:

```bash
uv run python examples/replay_verify_imported_cases/run_langfuse_pydanticai_demo.py \
  --runner live \
  --report-dir ./replay-verify-live-reports
```

Live mode defaults to its own cohort file
(`fixtures/support_copilot_live_cases.jsonl`: eleven eligible cases including
three permission-themed ones, plus the same four broken/stopped cases). The
baseline agent uses `openai:gpt-5-mini` and the candidate uses
`openai:gpt-5-nano`; override either with `--baseline-model` /
`--candidate-model`.

Even in live mode, the trust rules do not change. Tools are the same
deterministic, side-effect safe registry functions, every tool result records
`executed_live: false`, and the reported `tool_names` and
`retrieval_document_ids` come from the tool calls the agent actually made, not
from the model's self-report.

**The candidate regression is planted on purpose.** The baseline prompt tells
the agent to verify permission scope before answering account-administration
or security-sensitive requests (rotate an API key, change the account owner,
org-wide usage data) and to escalate them. The candidate prompt drops that
rule and tells the agent to answer admin/security requests directly with
self-serve steps, framed as a "cheaper config" change that cuts prompt tokens
and human-review load. The expectation: the baseline escalates the three
permission cases (`escalation_policy` / `needs_review`), the candidate answers
them directly (`support_policy` / `safe`), and Replay Verify flags the drift
and holds those cases. The detection is real; the regression is planted so the
demo has something real to catch.

In the calibrated run (2026-06-09, baseline `gpt-5-mini`, candidate
`gpt-5-nano`) the summary was: 15 imported, 11 eligible, 4 stopped, verdicts
8 ship / 7 hold (3 permission-case drifts plus 4 stopped cases), candidate
executions for stopped cases 0, unsafe live executions 0, overall verdict
`hold`.

**Live runs are not deterministic.** A real model can phrase things
differently, skip a tool call, or classify a borderline case differently
between runs, so exact counts can vary run to run. During calibration each
eligible case produced identical comparison fields across three consecutive
baseline runs, but that is an observation, not a guarantee. The deterministic
fixture mode remains the path for stable, reproducible output.

To regenerate the live cohort fixture, or to re-record `observed_output` from
a live baseline run, use the generator:

```bash
# Deterministic observed output, no API calls (this is the checked-in version)
uv run python examples/replay_verify_imported_cases/generate_live_cohort.py

# Observed output recorded from live baseline runs (requires OPENAI_API_KEY)
uv run python examples/replay_verify_imported_cases/generate_live_cohort.py --observed live
```

## Durable mode: run it as a Kitaru flow

The plain script above runs and exits; close the terminal and the results
live only in the report files. Durable mode runs the same verification as a
Kitaru flow, which changes three things:

1. **The run is an execution.** It shows up in `kitaru executions list`, has
   a status, and can be inspected later.
2. **The cohort and reports are artifacts.** `imported_cases`,
   `fidelity_report`, `verification_report`, and `verification_report_html`
   are saved on the execution, so the verdict has an address instead of a
   file path.
3. **Baseline runs are cached.** Each baseline/candidate lane is a
   checkpoint. Re-run with a different candidate and the baseline lanes are
   reused from cache instead of re-executing (in live mode, that means no
   repeated baseline model spend while you iterate on a candidate).

It needs an initialized Kitaru project first:

```bash
uv run kitaru init   # once per project
uv run python examples/replay_verify_imported_cases/run_durable_demo.py
```

Then inspect the run and its artifacts:

```bash
kitaru executions list
kitaru executions get <exec-id>
```

Live mode works here too (`--runner live`, needs `OPENAI_API_KEY`), with the
same planted-regression cohort as above. Scan mode intentionally stays a
plain script: its whole point is producing a first answer before you have
initialized anything.

## Scan mode: what can your own traces prove?

Verification needs well-instrumented traces. Most existing Langfuse projects
are not instrumented for it yet, and scan mode is how you find out what is
missing without changing any code. It reads raw observation rows, applies the
same validators the verifier uses, and writes a checklist instead of erroring:
how many traces are verifiable today, and exactly which fields would unlock
the rest.

Try it against the bundled uninstrumented fixture:

```bash
uv run python examples/replay_verify_imported_cases/run_scan_demo.py \
  --report-dir ./replay-verify-scan-reports
```

It writes `scan_checklist.md` (per-case recovered vs missing fields, plus the
top missing fields ranked by frequency) and `scan_report.json`. For the
bundled fixture the honest answer is "0 of 5 verifiable", which is the point:
the checklist names the gap precisely instead of flattering.

To scan your own Langfuse project, export observation rows first with the
standalone fetch script (it resolves its own `langfuse` dependency; nothing is
added to Kitaru):

```bash
LANGFUSE_PUBLIC_KEY=pk-... LANGFUSE_SECRET_KEY=sk-... \
LANGFUSE_HOST=https://cloud.langfuse.com \
uv run examples/replay_verify_imported_cases/fetch_langfuse_observations.py \
  --output observations.jsonl --limit 200

uv run python examples/replay_verify_imported_cases/run_scan_demo.py \
  --observations observations.jsonl \
  --report-dir ./replay-verify-scan-reports
```

Scan mode relaxes the tool-registry expectation (your tools are not in this
example's registry, so it reports rather than blocks), but it still flags
write-like tool names without a controlled side-effect status, because that is
exactly the fidelity signal you need before any verification could run.

## What this example proves, and what it does not

You can safely say:

- Replay Verify can load neutral imported-input cases from JSONL.
- It validates evidence before executing any candidate code.
- It runs local baseline and candidate callables for eligible cases, with
  write-like tools mocked and live side effects counted (and kept at zero).
- It holds incomplete, unsafe, or non-comparable cases before candidate
  execution, each with a concrete reason.
- It can catch a planted, realistic regression (a cheaper config that stops
  escalating permission-sensitive requests) using real model calls.
- Scan mode can tell you, for uninstrumented traces, exactly what is missing
  for verification.

Avoid claiming:

- "Replay Verify replays arbitrary Langfuse traces exactly."
- "Foreign traces become Kitaru checkpoint replays."
- "Recorded model/tool responses are replayed with zero live calls."
- "Any PydanticAI production run is automatically comparable."
- "Customer traces will show no drift."
- "There is a public Kitaru CLI or MCP command for imported-case verification."

## Known limitations

- The default deterministic mode does not call a live provider; live mode does
  (OpenAI only in this example), and its results vary run to run.
- Report comparison is field-based equality on `policy_label`, `risk_status`,
  `tool_names`, and `retrieval_document_ids`. Free-text responses are not
  scored.
- Recorded-response control is unavailable: even in live mode, model calls are
  fresh. The report states this explicitly.
- Scan mode reads observation rows from JSONL; the fetch script is a
  convenience exporter, not a streaming integration.
- This example intentionally does not add a public Kitaru CLI or MCP command.
