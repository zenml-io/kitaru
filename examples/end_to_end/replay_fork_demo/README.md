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
v2.3-counterfactual`. This starts from the root input, so expect `HOLD`
before running it. Objective and protection scores can still compare outcomes,
cost, and tool behavior, but root-input evidence is counterfactual. A blocked
or mismatched tool call degrades it further.

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

## 9. Widen the investigation to a second case

Only now does Priya add the ordinary service-status case. Preview it, then
repeat with `--write --confirm-data-storage`:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-service-status

uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-service-status \
  --write \
  --confirm-data-storage
```

Copy the new execution ID once:

```bash
export STATUS_ID=<execution-id-from-the-import-output>
```

Run the ordered two-case suite:

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
same stored attempt without another model call or duplicate spend.

## 11. Inspect the exact final attempt and child

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
