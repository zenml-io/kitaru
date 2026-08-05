# Current CLI operations

Run every command from `examples/canonical_example`. Export `.env` once per terminal:

```bash
set -a; source .env; set +a
```

Coding-agent tool calls usually start fresh shells, so the exported state does not persist between calls. For automated operations, either load `.env` inside every shell invocation or use `uv run --env-file .env kitaru ...`. Apply the same rule to every command started in parallel.

## Preflight

```bash
uv run kitaru status
uv run kitaru worker list
uv run kitaru importer get langfuse
uv run kitaru evaluator get cost
uv run kitaru evaluator get latency
uv run kitaru evaluator get tool-call-patterns
wc -l traces/langfuse-traces.jsonl
```

Run these independent reads as one bounded preflight. Record the selected server and exact agent dashboard link before creating resources.

Start a missing worker in a second terminal:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name returns-example-worker
```

Seed bundled development plugins only on a fresh local server:

```bash
uv run python ../../scripts/seed_default_plugins.py
```

## Register the baseline when absent

```bash
uv run kitaru agent register \
  returns-resolver \
  --entrypoint examples.canonical_example.agent:main \
  --description "Resolve one synthetic returns or delivery ticket, execute one mock action, and draft the customer reply." \
  --display-version baseline-v1 \
  --working-dir ../.. \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

## Import when the baseline has no sessions

Replace `BASELINE_VERSION` with the exact discovered version number:

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer langfuse@latest \
  --agent "returns-resolver@${BASELINE_VERSION}" \
  --tag returns-baseline \
  --params '{"source_instance":"canonical-returns-example"}' \
  --media-type application/x-ndjson \
  --wait
```

If the import skips all traces because the same source IDs already exist under another agent, create a temporary namespaced copy. Keep the checked-in export unchanged:

```bash
uv run python \
  ../../.agents/skills/kitaru-session-improvement/scripts/remap_langfuse_ids.py \
  traces/langfuse-traces.jsonl \
  /tmp/kitaru-returns-traces.jsonl \
  --namespace returns-guided-demo
```

Import `/tmp/kitaru-returns-traces.jsonl` with a distinct `source_instance`. Do not delete existing sessions to resolve this example collision.

## Start deterministic evaluations

Standard-mode MCP does not start session evaluations. Use the CLI with the cached exact baseline session IDs so a shared tag cannot widen the selection:

```bash
uv run kitaru session evaluate \
  --sessions-file /tmp/kitaru-canonical-baseline-session-ids.txt \
  --evaluator cost@EXACT_VERSION \
  --evaluator latency@EXACT_VERSION \
  --evaluator tool-call-patterns@EXACT_VERSION \
  --wait
```

## Start experiment runs

Standard-mode MCP manages the experiment definition but does not start runs. Use the CLI with exact immutable IDs:

```bash
uv run kitaru experiment run start \
  improve-returns-policy \
  --cohort-version COHORT_VERSION_ID \
  --agent "returns-resolver@${STRICT_VERSION}" \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Use `kitaru --output json` for read operations when an MCP response fails output validation. Continue to use exact IDs and versions from the structured result.

If replay-list or run-child reads are incompatible, do not use an unfiltered global job list as evidence. List replay sessions through the exact candidate agent-version ID, `origin=replay`, and the run's time window, then verify each session's replay and experiment-run metadata before joining evaluations.

## Create, test, and register policy correctness

Create the evaluator only after the behavior brief is approved:

```bash
uv run kitaru evaluator scaffold returns-policy --path evaluator.py
```

```bash
uv run kitaru evaluator test evaluator.py --entrypoint evaluate
```

Register when `returns-policy` is absent. Create a new immutable version when the approved behavior differs from the existing source:

```bash
uv run kitaru evaluator register \
  returns-policy \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Check whether the reported and accepted returns actions match the reviewed policy outcome." \
  --display-version 1.0
```

## Register the strict version when absent

```bash
uv run kitaru agent version register \
  returns-resolver \
  --entrypoint examples.canonical_example.agent:main \
  --description "Check approval and risk rules before issuing a refund." \
  --display-version strict-policy-v2 \
  --working-dir ../.. \
  --env RETURNS_POLICY_MODE=strict \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Use structured output when a CLI result must be passed into later reasoning:

```bash
uv run kitaru --output json COMMAND
```
