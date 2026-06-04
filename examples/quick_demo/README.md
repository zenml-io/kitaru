# Quick demo — agent + evals + wait & resume

A minimal Kitaru example that showcases the primitives you'll use most:

- `@flow` / `@checkpoint` — durable orchestration boundaries
- **Agent step** — a checkpoint that makes a recommendation (swap in
  `kitaru.llm(...)` to make it a real LLM agent)
- **Eval step** — a checkpoint that scores the agent's output on policy
  compliance, explanation quality, and confidence calibration
- `kitaru.wait()` — suspend execution until a human approves/rejects, seeded
  with the agent's recommendation and eval scores
- `kitaru.log()` — attach structured metadata (agent outputs, eval scores,
  final decision) to the execution for the dashboard

The demo is a mock **expense approval** workflow: prepare → agent review →
eval the agent → human approval → record.

## Run it

```bash
cd examples/quick_demo
uv pip install 'kitaru[local]'
kitaru init
python demo.py
```

When the flow hits `kitaru.wait(...)`, you can either answer the prompt
inline, or resume from another terminal using the CLI:

```bash
# Approve
kitaru executions input <exec_id> --value true
kitaru executions resume <exec_id>

# Or reject
kitaru executions input <exec_id> --value false
kitaru executions resume <exec_id>
```

The execution ID is printed to the terminal when the flow starts.

## What to point out in the demo

1. **Agent + eval pattern** — `policy_agent` produces a recommendation,
   and `evaluate_agent` scores it offline (policy compliance, explanation
   quality, confidence calibration). Both are `@checkpoint`s, so their
   outputs are persisted as artifacts and can be inspected per run.
2. **Durable suspension** — `kitaru.wait()` releases compute while the flow
   is paused. It can be resumed minutes, hours, or days later.
3. **Checkpoints are replayable** — early steps run once; if the flow is
   retried, their outputs are reused from the artifact store and only the
   failed/changed step re-executes.
4. **Structured metadata for dashboards** — eval scores and the final
   decision are logged via `kitaru.log()` and show up on the execution
   page, so you can track agent quality across runs.

## Try a few inputs

```python
# Under policy — agent APPROVEs, eval scores high
expense_approval_flow.run("Ada", 123.45, "Conference travel").wait()

# Over the $1000 limit — agent REJECTs
expense_approval_flow.run("Ada", 2500.00, "New laptop").wait()

# Restricted category — agent REJECTs
expense_approval_flow.run("Ada", 50.00, "Client gift").wait()
```

## Inspect the run

```bash
kitaru executions list
kitaru executions get <exec_id>
kitaru executions logs <exec_id>
```
