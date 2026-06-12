# PydanticAI support Replay Lab

A live PydanticAI customer-support agent whose runs feed Replay Lab: replay a
cohort of support cases against a cheaper model alias and compare cost, quality,
and behavior before switching production traffic.

The agent uses deterministic faked tools (`check_stock`, `lookup_order`, and a
guarded `issue_refund` that escalates instead of confirming a refund), so the
only thing that changes between a baseline run and a candidate run is the model.
That makes the comparison an honest model regression test.

- `support_cases.py` — synthetic, generic retail support cases (incl. a refund
  case the cheaper model is most likely to mishandle).
- `support_flow.py` — the durable `@flow` running the PydanticAI agent under a
  Kitaru model alias. Replay anchor: `run_support_agent`.
- `seed_observed.py` — run observed executions and write a Replay Lab manifest.
- `run_replay_lab.py` — replay each case against the candidate alias and report.
- `evaluator.py` — deterministic evaluator; flags a dropped refund safeguard.
- `candidates/model_matrix.json` — the cheaper-alias candidate descriptor.

See **`DEMO.md`** for the full two-act demo runbook (durable agent → Replay Lab)
and the exact commands to run.
