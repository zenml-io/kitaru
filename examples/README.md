# Kitaru examples

## Complete returns workflow

Use [`python/pydantic_ai_ticket_resolver/`](python/pydantic_ai_ticket_resolver/) for the maintained product walkthrough. It provides a ready PydanticAI returns agent and checked-in Langfuse traces. Its README owns setup and import; the [complete tutorial](../docs/book/tutorials/returns-agent/README.md) continues through deterministic diagnostics, evidence-linked human review, an immutable cohort version, and bounded replay.

## Python adapter examples

[`python/pydantic_ai_delivery_date/`](python/pydantic_ai_delivery_date/) is a multi-turn delivery demo with an MCP App and local browser editor. Read captured Kitaru conversations, review generated variations, run them with a responsive customer loop, and save executed cases in versioned regression sets for pinned CI comparisons. App calls default to OpenAI `gpt-6-luna`; scripted terminal runs provide a provider-free fixture demonstration.

The Python adapter examples use packages from the independent plugin workspace:

- [`python/openai_agents_v2/`](python/openai_agents_v2/) uses `kitaru-openai-agents`.
- [`python/langgraph_v2/`](python/langgraph_v2/) uses `kitaru-langgraph`.

Install the workspace before running them:

```bash
uv sync --project plugins --frozen --all-packages
```

## TypeScript examples

The TypeScript examples use the `@zenml-io/kitaru` SDK and its framework adapters:

- [`typescript/mastra_adaptive_conversation/`](typescript/mastra_adaptive_conversation/) generates a bounded response-dependent dialogue with raw Mastra calls, records one transcript session, and evaluates it with a Python evaluator.
- [`typescript/mastra_support_triage/`](typescript/mastra_support_triage/) records and replays a Mastra support-triage agent, including tool-result reuse from history and Python evaluations.
- [`typescript/vercel_ai_support_triage/`](typescript/vercel_ai_support_triage/) records a Vercel AI SDK agent and drives a job-scoped worker replay with overrides.
- [`typescript/vercel_ai_ticket_resolver/`](typescript/vercel_ai_ticket_resolver/) is the full end-to-end returns walkthrough: baseline recording, evaluator, cohort, and replay comparison.

Each example's README owns its setup and run commands.
