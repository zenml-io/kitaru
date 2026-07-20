# PydanticAI support agent

This package contains the production-shaped agent used by the parent case-first
replay example.

The agent receives a support request, lets PydanticAI run a case-specific
investigation, and returns a typed `SupportDecision`. The tool surface includes
customer data, feature entitlements, seat usage, service status, usage, billing,
knowledge search, ticket creation, and escalation.

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
