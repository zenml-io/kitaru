# PydanticAI support agent

This package contains the production-shaped agent used by the parent case-first
replay example.

The agent receives a support request, lets PydanticAI choose local tools, and
returns a typed `SupportDecision`. The tool surface includes customer data,
service status, usage, billing, knowledge search, ticket creation, escalation,
and customer-setting updates.

`agent.py` contains the PydanticAI agent, tool registrations, runtime policy,
and the final `KitaruAgent` wrapper. `evals/register.py` in the parent directory
is the stable registration and replay entrypoint.

The local services support two purposes:

- They make the agent complex enough to demonstrate recorded tool responses
  and write blocking during replay.
- They let `trace_fixtures/generate.py` mint production-shaped Langfuse traces
  before a demo.

Users following the replay journey begin with the imported traces described in
the parent README. They do not initialize this database or run the scenario
harness themselves.

## Variants

- `baseline` uses `gpt-5-mini`, a normal tool budget, and denies direct setting
  updates.
- `nano_trimmed_permissions` uses `gpt-5-nano` and permits the local setting
  update, creating a permission-sensitive candidate.
- `mini_tool_budget_2` keeps the baseline model and limits the agent to two tool
  calls.

The selected variant comes from `SUPPORT_AGENT_VARIANT` when
`evals.register:kagent` is imported. A real repository normally represents
these versions as separate registered Git revisions.
