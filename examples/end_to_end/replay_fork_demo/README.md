# Replay a Langfuse case with a PydanticAI candidate

This example starts with a support trace from Langfuse and ends with a scored,
bounded replay attempt. Kitaru keeps the imported execution unchanged. Each
candidate run is a new execution with its own AgentVersion, lineage, scores,
cost, evidence quality, and verdict.

The candidate never calls a live tool during imported replay. Kitaru serves an
exact recorded response when the tool name and arguments match. It blocks every
miss, including write-capable calls, instead of falling through to the original
callable.

Two replay starts are available:

- `root_input` gives the candidate the recorded user input. The new path is
  counterfactual, so the strict verdict is normally `HOLD` even when its scores
  pass.
- `message_history` continues after one complete recorded model message or
  tool result. A model message includes its complete tool-call response, even
  when it has no text content. It can produce `PASS` when the prefix is
  complete, the candidate stays on the recorded path, and all scores and
  protections pass.

## Setup

From the repository root:

```bash
uv sync --extra local --extra pydantic-ai --extra llm --extra langfuse
cd examples/end_to_end/replay_fork_demo
uv run kitaru init
```

The checked-in JSONL fixture can be previewed and imported without Langfuse or
OpenAI credentials. Registration and candidate execution use the models in
`reference_agent/variants/`, so set `OPENAI_API_KEY` before those commands:

```bash
export OPENAI_API_KEY=sk-...
```

The commands below use two traces from
`trace_fixtures/imported-support-cases.jsonl`:

```bash
export ACCOUNT_TRACE_ID=support-account-setting
export STATUS_TRACE_ID=support-service-status
```

## 1. Register the recorded source version

The fixture declares `v2.2-json-text-imported` as its source version. Register
that exact label before importing it:

```bash
uv run python demo.py register --role source
```

This registers the `baseline` variant under the declared source label. The
fixture and generator freeze that pair, and source-role registration rejects a
different variant or label. The fixture advertises the same callable tools as
the source implementation and stores the final `SupportDecision` as validated
JSON text. The importer verifies the declaration against the trace before it writes
anything.

## 2. Preview and import the traces

Start with a read-only preview:

```bash
uv run python demo.py import-traces \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --trace-id "$ACCOUNT_TRACE_ID" \
  --trace-id "$STATUS_TRACE_ID"
```

The preview reports attribution, replay readiness, storage, and the action that
a write would take. It does not create executions or evidence artifacts.

Repeat the command with `--commit` after checking the destination:

```bash
uv run python demo.py import-traces \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --trace-id "$ACCOUNT_TRACE_ID" \
  --trace-id "$STATUS_TRACE_ID" \
  --commit
```

Copy the two `execution_id` values from the output:

```bash
export ACCOUNT_EXECUTION_ID=<account-setting-execution-id>
export STATUS_EXECUTION_ID=<service-status-execution-id>
```

You can also fetch one trace through the read-only Langfuse observations API:

```bash
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
# Optional for self-hosted Langfuse:
export LANGFUSE_BASE_URL=https://langfuse.example.com

uv run python demo.py import-traces \
  "langfuse://trace/<trace-id>"
```

Kitaru derives the source project ID from the returned observations. The
default remains a preview. Add `--commit` to store the fetched evidence. This
path reads the trace through Langfuse's observations API.

## 3. Inspect the imported evidence

Inspect attribution, readiness, and available message-history boundaries:

```bash
uv run python demo.py inspect-execution "$ACCOUNT_EXECUTION_ID"
```

The fixture contains a complete tool-result boundary at index `1` and complete
model-message boundaries for both tool-calling and textual assistant responses.
The inspection output lists only boundaries that the adapter has validated and
shows the exact `observation_id`, `sequence`, `occurrence`, and optional `call_id`
that Kitaru uses. Do not choose an arbitrary observation ID.

The normal execution command is also useful for the imported checkpoint graph:

```bash
uv run kitaru executions get "$ACCOUNT_EXECUTION_ID"
```

An imported execution is historical evidence, not executable source code.
Native checkpoint replay remains disabled for it.

## 4. Reproduce from the recorded boundary

Continue after the second recorded tool result with the baseline candidate:

```bash
uv run python demo.py resume "$ACCOUNT_EXECUTION_ID" \
  --boundary-kind tool-result \
  --boundary-index 1 \
  --candidate-variant baseline \
  --candidate-version reproduction-baseline \
  --name account-setting-reproduction \
  --idempotency-key account-setting-reproduction-v1
```

The command rebuilds the boundary from stored evidence and runs a registered
candidate. If the candidate requests the recorded escalation with the same
arguments, Kitaru serves that recorded write-capable result without invoking
the tool. A changed argument becomes a blocked miss.

Check `imported_replay_members` in the result. A clean reproduction reports one
recorded-response hit, no blocked calls, no path divergence, and
`recorded_path_comparable`. Provider output can still cause `HOLD` or `FAIL`;
the command does not invent a passing result.

## 5. Run a counterfactual candidate

The weakened variant has a trimmed permissions prompt and allows the
account-setting tool. Run it from the root input:

```bash
uv run python demo.py replay "$ACCOUNT_EXECUTION_ID" \
  --candidate-variant nano_trimmed_permissions \
  --candidate-version permissions-counterfactual \
  --name account-setting-counterfactual \
  --idempotency-key account-setting-counterfactual-v1
```

This is a new path, not a reproduction. Its evidence quality is
`counterfactual`, or `degraded` if a tool call misses or is blocked. The strict
verdict stays `HOLD` rather than treating a plausible score as proof of direct
comparability.

## 6. Register and run the candidate fix

The `mini_tool_budget_2` variant restores the full permissions prompt while
keeping a smaller tool budget. Register it explicitly:

```bash
uv run python demo.py register \
  --role candidate \
  --variant mini_tool_budget_2 \
  --version permissions-fix-v1
```

Now test it from the same complete boundary:

```bash
uv run python demo.py resume "$ACCOUNT_EXECUTION_ID" \
  --boundary-kind tool-result \
  --boundary-index 1 \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1 \
  --name account-setting-fix \
  --idempotency-key account-setting-fix-v1
```

Registration is idempotent. Repeating the same registration reuses the existing
AgentVersion. Reusing an experiment idempotency key with different inputs fails.

## 7. Read the scores and verdict

Every `replay`, `resume`, and `experiment` command attaches the deterministic
`support-resolution` objective. The registered Agent also pins the
`completed-execution` protection. These answer separate questions:

- the objective checks whether the candidate produced durable completed output;
- the protection prevents an incomplete candidate from passing;
- replay evidence says whether the candidate stayed comparable to the recording;
- the verdict combines those facts without turning missing evidence into zero.

Inspect the fixed attempt by its suite name:

```bash
uv run python demo.py inspect-experiment account-setting-fix
```

The output includes the frozen request, score aggregate, protection results,
recorded-response decisions, limits, evidence quality, and verdict.

## 8. Create a named multi-case suite

Freeze the imported execution IDs in the order you want:

```bash
uv run python demo.py experiment \
  "$ACCOUNT_EXECUTION_ID" \
  "$STATUS_EXECUTION_ID" \
  --name support-imported-regression \
  --repeats 1 \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1 \
  --idempotency-key support-imported-regression-v1
```

This command starts each case from its recorded root input. The attempt is
useful for comparing outcomes and costs, but its strict verdict is `HOLD`
because root-input reruns are counterfactual. The suite still records immutable
membership, candidate attribution, scores, protections, recorded-response
decisions, and lineage.

## 9. Rerun a comparable suite with limits

The single-case `account-setting-fix` attempt from step 6 is a named suite with
a complete message-history boundary. Rerun that frozen request with explicit
limits:

```bash
uv run python demo.py rerun account-setting-fix \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1 \
  --idempotency-key account-setting-fix-rerun-v1 \
  --max-trials 1 \
  --max-cost-usd 1.00 \
  --max-incurred-tokens 100000 \
  --max-duration-seconds 300
```

The command calls `assert_pass()`. It exits nonzero on `FAIL` or `HOLD`, so
missing usage, a reached limit, a blocked call, or incomplete evidence cannot
silently pass a regression gate. A retry with the same idempotency key returns
the same durable attempt and does not duplicate the spend.

Limits are checked before another trial starts. One model call can cross a cost
or token ceiling before Kitaru stops further work.

## 10. Inspect the durable result

Inspect one bounded page of an attempt's verified children:

```bash
uv run python demo.py inspect-experiment account-setting-fix \
  --page 1 --page-size 25
uv run python demo.py inspect-experiment support-imported-regression \
  --page 1 --page-size 25
```

Inspect a child execution from either result:

```bash
uv run python demo.py inspect-execution <candidate-child-execution-id>
```

The execution view includes immediate parent and root lineage, import
attribution when present, bounded scores, cost, and compact related-attempt
references. The experiment view serializes the full attempt once, then includes
one explicitly paginated set of per-child evidence, score aggregates,
protections, operational limits, and the final verdict.

## Fixture provenance

`trace_fixtures/imported-support-cases.jsonl` is the small deterministic fixture
used by the acceptance test. `support-traces.jsonl` is the larger exported
scenario set. Maintainers can generate fresh Langfuse traces with
`python -m trace_fixtures.generate`; see
[trace_fixtures/README.md](trace_fixtures/README.md).
