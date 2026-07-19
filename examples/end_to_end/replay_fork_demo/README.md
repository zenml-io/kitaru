# Replay a Langfuse case with a PydanticAI candidate

This example starts with a support trace from Langfuse and ends with a scored,
bounded replay attempt. Kitaru keeps the imported execution unchanged. Each
candidate run is a new execution with its own AgentVersion, lineage, scores,
cost, evidence quality, and verdict.

The candidate never calls a live tool during imported replay. The recorded
escalation is a stable structured call with `customer_id` and a constrained
`policy_label`. Kitaru serves a recorded response only when the tool name and
arguments match exactly. It blocks every miss, including write-capable calls,
instead of falling through to the original callable.

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

PLAYGROUND_DIR="$(mktemp -d "$PWD/.replay-playground.XXXXXX")"
export KITARU_CONFIG_PATH="$(mktemp -d /tmp/kitaru-replay-demo.XXXXXX)"
cd "$PLAYGROUND_DIR"
uv run kitaru init
```

The temporary directory and `KITARU_CONFIG_PATH` give the walkthrough their
own project marker, database, and local artifact store. They do not alter this
checkout's existing `.kitaru/` marker or your normal Kitaru state. Keep using
the same shell until you finish. Commands use `../demo.py` and
`../trace_fixtures/` because the shell is now inside that temporary directory.

The checked-in JSONL fixture can be previewed and imported without Langfuse or
OpenAI credentials. Registration and candidate execution use the models in
`reference_agent/variants/`, so set `OPENAI_API_KEY` before those commands:

```bash
export OPENAI_API_KEY=sk-...
```

The commands below use the `support-account-setting` and
`support-service-status` traces from
`trace_fixtures/imported-support-cases.jsonl`.

## 1. Register the recorded source version

The fixture declares `v2.3-structured-escalation-imported` as its source
version. Kitaru must know which exact code produced the trace before importing
it.

There is no public CLI command for registering executable Python Agent code.
The example helper loads `evals.register:baseline_agent` and calls
`agent.register(...)` with the fixture's label and entrypoint:

```bash
uv run python ../demo.py register --role source
```

It creates the immutable AgentVersion record without importing a trace, calling
the model, running the Agent, or deploying anything. This registers the
`baseline` variant under the declared source label. The
fixture and generator freeze that pair, and source-role registration rejects a
different variant or label. The fixture advertises the same callable tools as
the source implementation and stores the final `SupportDecision` as validated
JSON text. The importer verifies the declaration against the trace before it writes
anything.

Confirm that Kitaru can resolve the registered agent:

```bash
uv run kitaru agents show support-agent
```

## 2. Preview and import the traces

Start with a read-only preview:

```bash
uv run kitaru import langfuse \
  ../trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-account-setting \
  --trace-id support-service-status
```

The preview reports attribution, replay readiness, storage, and the action that
a write would take. It does not create executions or evidence artifacts.

Repeat the command with `--write --confirm-data-storage` after checking the
destination:

```bash
uv run kitaru import langfuse \
  ../trace_fixtures/imported-support-cases.jsonl \
  --source-project-id langfuse-replay-example \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --trace-id support-account-setting \
  --trace-id support-service-status \
  --write \
  --confirm-data-storage \
  --output json > /tmp/kitaru-replay-import.json
```

Print the generated execution IDs:

```bash
uv run python - <<'PY'
import json
from pathlib import Path

result = json.loads(Path("/tmp/kitaru-replay-import.json").read_text())
for outcome in result["item"]["outcomes"]:
    print(f'{outcome["trace_id"]}: {outcome["execution_id"]}')
PY
```

Paste the printed IDs into the placeholders below. If you omit `--output json`,
the normal CLI output also prints each full execution ID in its own trace block.

You can also fetch one trace through the read-only Langfuse observations API:

```bash
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
# Optional for self-hosted Langfuse:
export LANGFUSE_BASE_URL=https://langfuse.example.com

uv run kitaru import langfuse \
  "langfuse://trace/<trace-id>" \
  --agent support-agent \
  --agent-version v2.3-structured-escalation-imported
```

Kitaru derives the source project ID from the returned observations. The
default remains a preview. Add `--write --confirm-data-storage` to store the
fetched evidence. This path reads the trace through Langfuse's observations API.

## 3. Inspect the imported evidence

Start with Kitaru's durable execution view:

```bash
uv run kitaru executions get <account-setting-execution-id>
```

This shows the imported execution and its checkpoint graph. An imported
execution is historical evidence, not executable source code, so native
checkpoint replay remains disabled for it.

For the adapter-specific continuation in the next step, inspect the replay
readiness and available message-history boundaries computed by the example:

```bash
uv run python ../demo.py inspect-execution <account-setting-execution-id>
```

The fixture contains a complete tool-result boundary at index `1` and complete
model-message boundaries for both tool-calling and textual assistant responses.
This additional view lists only boundaries that the PydanticAI adapter has
validated and shows the exact `observation_id`, `sequence`, `occurrence`, and
optional `call_id` that Kitaru uses. Do not choose an arbitrary observation ID.

## 4. Reproduce from the recorded boundary

Continue after the second recorded tool result with the baseline candidate:

```bash
uv run python ../demo.py resume <account-setting-execution-id> \
  --boundary-kind tool-result \
  --boundary-index 1 \
  --candidate-variant baseline \
  --candidate-version account-setting-baseline-v1 \
  --name account-setting-reproduction \
  --idempotency-key account-setting-reproduction-v1
```

The command rebuilds the boundary from stored evidence and runs a registered
candidate. The recorded escalation uses the exact arguments `customer_id` and
`policy_label="permissions_policy"`. Kitaru serves its recorded write-capable result
only when both arguments match exactly, without invoking the tool. Any changed
argument is a blocked miss.

When the candidate reproduces that exact call and completes successfully, the
expected comparable result starts with the verdict-led heading
`PASS  account-setting-reproduction` and shows:

- `Status`: the candidate attempt is completed;
- `Replay`: `message_history from tool-result`;
- `Comparability`: `recorded_path_comparable`;
- `Recorded replies`: `1/1 served, 0 missed`;
- `Blocked calls`: `0`;
- `Path divergences`: `0`;
- `Objective`: `support-resolution` passed;
- `Protections`: `all passed`.

These rows are the comparable evidence that justifies `PASS`. If the provider
takes a divergent path, the verdict may instead be `HOLD` or `FAIL`. If any
exact argument misses, Kitaru blocks the call and the result cannot pass as
comparable.

## 5. Run a counterfactual candidate

The weakened variant has a trimmed permissions prompt and allows the
account-setting tool. Run it from the root input:

```bash
uv run python ../demo.py replay <account-setting-execution-id> \
  --candidate-variant nano_trimmed_permissions \
  --candidate-version v2.3-counterfactual \
  --name account-setting-counterfactual \
  --idempotency-key account-setting-counterfactual-v1
```

`v2.3-counterfactual` is the current default candidate version. This root-input
run is counterfactual, or degraded if a tool call misses or is blocked. Its
objective and protection scores remain useful, but the strict verdict is
`HOLD` because the run is not comparable recorded-path evidence.

## 6. Register and run the candidate fix

The `mini_tool_budget_2` variant restores the full permissions prompt while
keeping a smaller tool budget. Register it explicitly:

```bash
uv run python ../demo.py register \
  --role candidate \
  --variant mini_tool_budget_2 \
  --version permissions-fix-v1
```

Now test it from the same complete boundary:

```bash
uv run python ../demo.py resume <account-setting-execution-id> \
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

Inspect the fixed attempt through Kitaru by its suite name:

```bash
uv run kitaru agents experiments \
  support-agent \
  account-setting-fix
```

The default text output leads with the verdict and includes `Status`, `Trials`,
`Replay`, `Comparability`, `Recorded replies`, `Blocked calls`, `Path
divergences`, `Objective`, `Protections`, limits when present, and `Why`. Use
these rows to distinguish a comparable `PASS` from a counterfactual `HOLD`.

## 8. Create a named multi-case suite

Freeze the imported execution IDs in the order you want:

```bash
uv run python ../demo.py experiment \
  <account-setting-execution-id> \
  <service-status-execution-id> \
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
uv run python ../demo.py rerun account-setting-fix \
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

Inspect the durable attempts through Kitaru:

```bash
uv run kitaru agents experiments \
  support-agent \
  account-setting-fix
uv run kitaru agents experiments \
  support-agent \
  support-imported-regression
```

Inspect a candidate child execution from either result:

```bash
uv run kitaru executions get <candidate-child-execution-id>
```

The execution view includes the checkpoint graph, immediate parent, replay
root, and import attribution when present.

All `demo.py` commands use readable output by default. Add `--output json` when
you need the complete machine-readable record, preferably redirected to a file:

```bash
uv run python ../demo.py resume <account-setting-execution-id> \
  --boundary-kind tool-result \
  --boundary-index 1 \
  --candidate-variant baseline \
  --candidate-version account-setting-baseline-v1 \
  --name account-setting-reproduction \
  --idempotency-key account-setting-reproduction-json-v1 \
  --output json > /tmp/kitaru-replay-result.json
```

When you are finished, return to the example directory and remove the two
temporary directories:

```bash
cd ..
rm -rf "$PLAYGROUND_DIR" "$KITARU_CONFIG_PATH"
unset PLAYGROUND_DIR KITARU_CONFIG_PATH
```

Later Kitaru commands will use your normal local state again.

## Fixture provenance

`trace_fixtures/imported-support-cases.jsonl` is the small deterministic fixture
used by this walkthrough and the acceptance test. See
[trace_fixtures/README.md](trace_fixtures/README.md) for its detailed provenance,
derived-data marker, raw generation procedure, and the separate larger exported
scenario set.
