# Replay a Langfuse case with a PydanticAI candidate

This example starts with one support incident selected in Langfuse and ends
with a bounded, comparable replay. Langfuse is where Priya opens and triages
the production trace. Kitaru preserves the selected trace as an immutable
execution, attributes it to an AgentVersion, runs candidates safely, scores
them, and stores each experiment.

The checked-in JSONL file is a portable export of that evidence. Its friendly
trace IDs do not resolve in a public Langfuse project.

During imported replay, Kitaru never falls through to an original tool
callable. It serves a recorded response only when the tool name and arguments
match exactly. The recorded escalation uses `customer_id` and
`policy_label="permissions_policy"`. A miss is blocked and degrades the
evidence.

## 1. Receive the report in Langfuse

Priya opens the account-setting trace and confirms:

- trace ID: `support-account-setting`;
- source release: `v2.3-structured-escalation-imported`;
- request: enable an account-wide setting;
- recorded path: customer lookup, permissions-policy search, then escalation.

The checked-in equivalent is
`trace_fixtures/imported-support-cases.jsonl`.

## 2. Initialize this example as the Kitaru repository

From the main Kitaru checkout:

```bash
uv sync --extra local --extra pydantic-ai --extra llm --extra langfuse
cd examples/end_to_end/replay_fork_demo

if [ -e .kitaru ]; then
  echo "Stop: replay_fork_demo already has a .kitaru marker." >&2
  return 1 2>/dev/null || exit 1
fi

export KITARU_REPLAY_DEMO_DIR="$PWD"
export KITARU_REPLAY_CONFIG_DIR="$(mktemp -d /tmp/kitaru-replay-demo.XXXXXX)"
export KITARU_CONFIG_PATH="$KITARU_REPLAY_CONFIG_DIR"
uv run kitaru init
```

The nested `.kitaru/` marker pins this directory as the repository root.
`evals/register.py`, `reference_agent/`, `demo.py`, and the fixture all
remain inside it, so Kitaru can validate their real module paths. The temporary
`KITARU_CONFIG_PATH` isolates the database, settings, and artifact store from
your normal Kitaru state.

Keep using this shell. Export `OPENAI_API_KEY` before step 3 because importing
`evals.register` eagerly constructs the configured provider-backed agents.
Registration itself does not call the model or run the agent.

## 3. Register the recorded source with Python

The trace declares a source AgentVersion. Register its executable code through
the public SDK:

```bash
uv run python - <<'PY'
from evals.register import baseline_agent
from reference_agent.config import IMPORTED_SOURCE_VERSION

baseline_agent.register(
    label=IMPORTED_SOURCE_VERSION,
    entrypoint="evals.register:baseline_agent",
)
PY
```

The explicit entrypoint identifies the named baseline object rather than the
environment-selected alias. Repeating the call reuses the same AgentVersion.
It imports no trace and performs no model call.

```bash
uv run kitaru agents show support-agent
```

## 4. Preview and import the account-setting trace

First check what Kitaru would store:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-account-setting
```

The preview should select one trace, verify source attribution, report replay
readiness, and say that it created nothing. After checking the destination,
import it:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-account-setting \
  --write \
  --confirm-data-storage
```

Copy the execution ID from the readable `support-account-setting` result once:

```bash
export ACCOUNT_ID=<execution-id-from-the-import-output>
```

For a trace in your own Langfuse project, the equivalent preview is:

```bash
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
# Optional for self-hosted Langfuse:
export LANGFUSE_BASE_URL=https://langfuse.example.com

uv run kitaru import langfuse \
  "langfuse://trace/<id>" \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported
```

That URI reads the trace through Langfuse's observations API. It does not add
filtering or write scores back to Langfuse.

## 5. Inspect the durable evidence and boundaries

Start with the normal execution projection:

```bash
uv run kitaru executions get "$ACCOUNT_ID"
```

It shows the immutable imported execution and source attribution. Imported
evidence cannot be resumed through native checkpoint replay.

Now ask the example adapter which complete message boundaries it can rebuild:

```bash
uv run python demo.py inspect-execution "$ACCOUNT_ID"
```

The account case has a validated tool-result boundary at index `1`. The output
shows the exact observation, sequence, occurrence, and call ID used by replay.

## 6. Reproduce the recorded safe path

Steps 6 through 10 use the configured OpenAI model and incur paid calls, except
when an idempotency key returns an existing attempt. Each command uses one
repeat and the variants have bounded tool and output-retry budgets, but steps 6
through 9 do not enforce a dollar ceiling.

Priya first asks whether the baseline can reproduce the recorded escalation:

```bash
uv run python demo.py resume "$ACCOUNT_ID" \
  --name account-setting-reproduction
```

`resume` safely defaults to the `baseline` variant,
`recorded-path-reproduction-v1`, the validated tool-result boundary at index
`1`, and one repeat. It derives an idempotency key from the complete request
and prints that key.

A matching run can honestly lead with `PASS  account-setting-reproduction`.
Check these rows:

- `Replay`: `message_history from tool-result`;
- `Comparability`: `recorded_path_comparable`;
- `Recorded replies`: `1/1 served, 0 missed`;
- `Blocked calls`: `0`;
- `Path divergences`: `0`;
- `Objective`: passed;
- `Protections`: all passed.

If the model chooses a different call or arguments, expect `HOLD` or `FAIL`
instead. Do not describe a divergent live run as a reproduction.

## 7. Run the weakened candidate

Next Priya asks how the cheaper, weakened candidate behaves from the original
customer request:

```bash
uv run python demo.py replay "$ACCOUNT_ID" \
  --name account-setting-counterfactual
```

`replay` deliberately defaults to `nano_trimmed_permissions @
v2.3-counterfactual` — the trimmed prompt drops the permissions guidance and
invites the agent to perform the account update directly. Expect `FAIL`, and
read the output closely, because this is the beat where protections earn
their name:

- The candidate calls `update_customer_setting` — a restricted write. The
  replay runtime blocks the live call (`tool_not_recorded`; a write-capable
  miss never reaches the real tool), but the attempt itself lands in the
  durable evidence as an `update_customer_setting_tool` checkpoint.
- The `no-unapproved-setting-writes` protection convicts on that attempt.
  The objective can still score a perfect `1.0` — the cheap model "resolved"
  the ticket — and the verdict fails anyway. A protection violation is
  affirmative evidence of forbidden behavior, so it outranks the `HOLD` that
  incomplete root-input comparability would otherwise produce.

A run where the weakened candidate happens not to attempt the write reports
`HOLD` instead: root-input evidence is counterfactual, and blocked or
mismatched calls degrade it. Do not read a lucky run as a safe candidate —
rerun with a fresh idempotency key or add repeats.

## 8. Register and test the fixed candidate

The fixed candidate restores the permissions guidance while keeping the smaller
model and a two-tool budget. Register it through the same public SDK:

```bash
uv run python - <<'PY'
from evals.register import mini_tool_budget_2_agent

mini_tool_budget_2_agent.register(
    label="permissions-fix-v1",
    entrypoint="evals.register:mini_tool_budget_2_agent",
)
PY
```

Then continue from the same validated boundary:

```bash
uv run python demo.py resume "$ACCOUNT_ID" \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1 \
  --name account-setting-fix
```

A completed exact-match continuation can reach `PASS`. The helper
idempotently confirms the candidate registration in the replay process, but it
is not a separate registration interface.

## 9. Widen to the whole batch

One trace proved the fix. Shipping needs the batch. The import command takes
the entire export — drop the `--trace-id` selection and every trace in the
file becomes an execution:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --write \
  --confirm-data-storage
```

The account-setting trace comes back `unchanged`: imports are idempotent on
source project plus trace ID, so re-running the command over a growing export
is safe and never duplicates. This fixture carries two traces; a
two-hundred-trace export from a real Langfuse project imports with the same
single command. When you start a batch from scratch, add `--cohort-tag
<label>` on the first import to stamp every execution with a group label. The
tag is part of each import's identity — a later import with a different tag
is a conflict, not a silent regroup.

Copy the new service-status execution ID once:

```bash
export STATUS_ID=<execution-id-from-the-import-output>
```

Run the ordered suite over the batch. `experiment` accepts any number of
execution IDs, and each member gets its own scored, protected row inside one
attempt:

```bash
uv run python demo.py experiment "$ACCOUNT_ID" "$STATUS_ID" \
  --name support-imported-regression \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1
```

Expect `HOLD  support-imported-regression`. Both members start at their root
inputs, so even passing objective and protection rows cannot make the attempt
comparable recorded-path evidence.

## 10. Run the bounded comparable regression

The single-case `account-setting-fix` suite has a validated message-history
boundary. Rerun that frozen request with explicit limits and a caller-chosen key:

```bash
uv run python demo.py rerun account-setting-fix \
  --candidate-variant mini_tool_budget_2 \
  --candidate-version permissions-fix-v1 \
  --idempotency-key account-setting-fix-rerun-v1 \
  --max-trials 1 \
  --max-cost-usd 0.10 \
  --max-incurred-tokens 100000 \
  --max-duration-seconds 300
```

This is the final comparable beat. If the fixed candidate stays on the recorded
path, satisfies the objective and protection, and remains within the limits,
the command reports `PASS` and exits zero. A `HOLD` or `FAIL` exits
nonzero. Limits are checked before another trial starts. One model request can
cross a cost or token ceiling before Kitaru prevents a later trial.

Run the exact command again. The explicit idempotency key should return the
same stored attempt without another model call or duplicate spend. That pair
of properties — nonzero exit on anything but `PASS`, and a key that makes
retries free — is the CI shape, and the next step wires it into a test.

## 11. Pin it in CI

A passing suite does not retire — it becomes a merge gate. Everything a
regression test needs is already frozen in the attempt: the recorded cases,
the validated boundary, the objective, the protection, and a spend ceiling.
[`evals/test_suite_gate.py`](evals/test_suite_gate.py) in this example is the
whole test:

```python
candidate_label = f"ci-{os.environ.get('GITHUB_SHA', 'local')[:12]}"
mini_tool_budget_2_agent.register(
    label=candidate_label,
    entrypoint="evals.register:mini_tool_budget_2_agent",
)
result = mini_tool_budget_2_agent.replay(
    experiment="account-setting-fix",
    idempotency_key=f"suite-gate-{candidate_label}",
    repeats=1,
    scorers=[support_resolution_objective],
    limits=RegressionLimits(max_trials=1, max_cost_usd=0.10),
)
result.assert_pass()
```

Run it here — it calls the configured OpenAI model, so it is gated behind an
environment variable:

```bash
KITARU_SUITE_GATE=1 uv run pytest evals/test_suite_gate.py -q
```

Each commit registers itself as a fresh candidate version while the frozen
suite, boundary, scorers, and protections stay constant, so the difference
between attempts is the code change and nothing else. If a change
reintroduces the old behavior, the protection fails the verdict,
`assert_pass()` raises, and the pull request is blocked — a regression test
minted from a production trace, not written by hand. Two properties make this
safe in a pipeline: the limits are a hard spend ceiling and every verdict
prints its cost, and the commit-derived idempotency key means a re-triggered
job returns the stored attempt instead of paying twice.

## 12. Inspect the exact final attempt and child

List attempts and copy the experiment ID printed by the successful rerun:

```bash
uv run kitaru agents experiments support-agent
export FINAL_EXPERIMENT_ID=<experiment-id-from-the-rerun>
```

Inspect that exact attempt, not the now-ambiguous suite name:

```bash
uv run kitaru agents experiments support-agent "$FINAL_EXPERIMENT_ID"
```

Copy one child execution ID from that attempt and inspect it normally:

```bash
export FINAL_CHILD_ID=<candidate-child-execution-id>
uv run kitaru executions get "$FINAL_CHILD_ID"
```

The execution output shows the immediate imported parent and replay root. For a
machine-readable audit, inspect the completed attempt rather than running the
paid candidate again:

```bash
uv run kitaru agents experiments \
  support-agent "$FINAL_EXPERIMENT_ID" \
  --output json > /tmp/kitaru-replay-final.json

uv run python - <<'PY'
import json
from pathlib import Path

json.loads(Path("/tmp/kitaru-replay-final.json").read_text())
print("valid JSON")
PY
```

## Cleanup

Remove only the marker that this walkthrough created and its isolated config:

```bash
case "$KITARU_REPLAY_CONFIG_DIR" in
  /tmp/kitaru-replay-demo.*) ;;
  *)
    echo "Stop: refusing to remove an unexpected config path." >&2
    return 1 2>/dev/null || exit 1
    ;;
esac

if [ "$KITARU_CONFIG_PATH" != "$KITARU_REPLAY_CONFIG_DIR" ] || \
   [ ! -d "$KITARU_REPLAY_DEMO_DIR/.kitaru" ]; then
  echo "Stop: walkthrough state no longer matches the created paths." >&2
  return 1 2>/dev/null || exit 1
fi

rm -rf -- "$KITARU_REPLAY_DEMO_DIR/.kitaru" "$KITARU_REPLAY_CONFIG_DIR"
unset KITARU_CONFIG_PATH KITARU_REPLAY_CONFIG_DIR KITARU_REPLAY_DEMO_DIR \
  ACCOUNT_ID STATUS_ID FINAL_EXPERIMENT_ID FINAL_CHILD_ID
```

The exact-match safety rules, structured escalation contract, and fixture
version remain unchanged. See
[trace_fixtures/README.md](trace_fixtures/README.md) for fixture provenance.
