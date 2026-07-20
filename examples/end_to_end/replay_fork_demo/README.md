# Replay a Langfuse case set with PydanticAI

This example starts with five customer-support traces selected in Langfuse and
turns them into a repeatable regression study. Kitaru imports the traces as
immutable source executions, attributes them to one PydanticAI AgentVersion,
and runs three experiment attempts over the same ordered case set.

Each attempt creates one fresh execution per imported source. The resulting
shape is:

```text
customer-support-agent
└── v2.3-structured-escalation-imported
    ├── 5 imported source executions
    ├── experiment attempt 1: 5 child executions
    ├── experiment attempt 2: 5 child executions
    └── experiment attempt 3: 5 child executions
```

No candidate AgentVersion is registered in this walkthrough. Repeating an
experiment creates new experiment records and executions under the existing
AgentVersion. It does not create another flow.

The checked-in `trace_fixtures/imported-support-cases.jsonl` file is a portable,
replay-ready projection of the Langfuse evidence. The complete 35-observation
export is stored beside it as
`trace_fixtures/raw-imported-support-cases.jsonl`. Trace generation and export
instructions live in `trace_fixtures/README.md`; they are maintenance steps,
not part of this walkthrough.

During imported replay, Kitaru serves a recorded tool response only when the
candidate calls the same tool with the same arguments. An unrecorded or changed
call is blocked and the attempt is held for review. This makes repeated runs
useful even when the code and AgentVersion remain unchanged: model
nondeterminism can still produce a different tool path.

## 1. Review the selected cases

The fixture contains these source traces in order:

| Case | Langfuse trace ID | Expected action |
| --- | --- | --- |
| Account setting change | `390972667ef147cbbbd6db2b30e8ad1b` | Escalate to a human |
| Service status | `00cbb102c7844e00aeb0149e8deea83b` | Answer directly |
| Refund policy | `40860e9cee9d4d71b6a6f82208af1a75` | Answer directly |
| Usage spike | `50b5ad259d5c4c61bdfe2f593c8f8495` | Answer directly |
| Outage ticket | `88b77b16089946ee966d2ae55e0921d7` | Create a ticket when warranted |

All five traces declare source label
`v2.3-structured-escalation-imported` and were produced by the baseline
PydanticAI implementation.

## 2. Prepare the example

From the Kitaru checkout:

```bash
uv sync --extra local --extra pydantic-ai --extra llm --extra langfuse
cd examples/end_to_end/replay_fork_demo
uv run kitaru init
```

Connect to your Kitaru server and select a stack before continuing. If your
shell already provides server credentials, skip `kitaru login`.

```bash
uv run kitaru login https://your-kitaru-server.example.com
uv run kitaru agents create customer-support-agent
uv run kitaru stack use <stack-name>
```

If the Agent already exists, activate it instead:

```bash
uv run kitaru agents use customer-support-agent
```

Set `OPENAI_API_KEY` before importing `evals.register`, because that module
constructs the configured provider-backed PydanticAI agents. Registration does
not call the model.

The example uses the fixed Agent name `customer-support-agent`. There is no
Agent-name environment override.

## 3. Register the one AgentVersion

Register the executable baseline through the public SDK:

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

Repeating this call reuses the same immutable AgentVersion.

```bash
uv run kitaru agents show customer-support-agent
```

The output should report one AgentVersion with alias
`v2.3-structured-escalation-imported`.

## 4. Preview and import all five traces

Preview the complete checked-in set:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id cmqnzjkwa01m7ad0cjmj6fhpq \
  --agent customer-support-agent \
  --agent-version v2.3-structured-escalation-imported
```

Check that the preview selects five complete traces, verifies their source
attribution, and identifies the intended storage stack. Then write them:

```bash
uv run kitaru import langfuse \
  trace_fixtures/imported-support-cases.jsonl \
  --source-project-id cmqnzjkwa01m7ad0cjmj6fhpq \
  --agent customer-support-agent \
  --agent-version v2.3-structured-escalation-imported \
  --write \
  --confirm-data-storage
```

The import is content-addressed. Repeating it returns the existing executions
instead of duplicating them.

Copy the five execution IDs from the import output:

```bash
export ACCOUNT_ID=<account-setting-execution-id>
export STATUS_ID=<service-status-execution-id>
export REFUND_ID=<refund-policy-execution-id>
export USAGE_ID=<usage-spike-execution-id>
export OUTAGE_ID=<outage-ticket-execution-id>
```

Inspect any source execution before spending on experiments:

```bash
uv run python demo.py inspect-execution "$ACCOUNT_ID"
```

## 5. Run three attempts over the same corpus

Each command below targets the same baseline AgentVersion and creates five
experiment child executions. The suite name stays stable while the explicit
idempotency key identifies each attempt.

```bash
for attempt in 1 2 3; do
  uv run python demo.py experiment \
    "$ACCOUNT_ID" \
    "$STATUS_ID" \
    "$REFUND_ID" \
    "$USAGE_ID" \
    "$OUTAGE_ID" \
    --name customer-support-baseline-regression \
    --idempotency-key "customer-support-baseline-regression-attempt-$attempt" \
    --candidate-variant baseline \
    --candidate-version v2.3-structured-escalation-imported
done
```

These commands make paid model calls. Each attempt has one repeat and at most
five child executions. Reusing an idempotency key returns the stored attempt
without another model call.

A `PASS` means every case stayed on a comparable recorded path and satisfied
the objective and protection. A `HOLD` identifies cases where the model chose
an unrecorded tool or changed its arguments. Both outcomes belong to the same
AgentVersion.

## 6. Inspect experiment membership and lineage

List the three attempts:

```bash
uv run kitaru agents experiments customer-support-agent
```

Copy one experiment ID and inspect its five members:

```bash
export EXPERIMENT_ID=<experiment-id>
uv run python demo.py inspect-experiment "$EXPERIMENT_ID" --detailed
```

Each member records:

- the imported target execution;
- the child experiment execution;
- the immediate replay parent and root;
- recorded-response hits, misses, and blocked calls;
- objective and protection scores;
- token, cost, and duration evidence when available.

Inspect a child execution directly to follow its lineage back to the imported
source:

```bash
export CHILD_ID=<child-execution-id>
uv run kitaru executions get "$CHILD_ID"
```

The final Agent state should still contain one AgentVersion, five imported
source executions, and three experiment attempts with five children each.
