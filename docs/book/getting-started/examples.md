---
description: Runnable examples — the canonical returns agent walks the whole loop, from imported traces to a measured improvement.
icon: flask
---

# Examples

## The canonical example — a returns agent, improved

[`examples/pydantic_ai_ticket_resolver/`](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver)
follows an autonomous returns agent from production evidence to a
measured improvement — the whole record → replay → improve loop in one
sitting:

1. Generate real PydanticAI traces through Langfuse.
2. Register the agent and start a worker.
3. Import the trace export as tagged Kitaru sessions.
4. Run the built-in `cost`, `latency`, and `tool-call-patterns`
   evaluators over the imported baseline.
5. Freeze the interesting sessions into cohort versions.
6. Register a policy evaluator and an improved agent version.
7. Replay the cohorts and compare baseline and candidate evaluations.

The orders, customers, and shipments are synthetic; the agent makes
real model calls when you regenerate the checked-in traces, while its
commerce tools only touch an in-memory store — so the example is safe
to run anywhere.

```bash
cd examples/pydantic_ai_ticket_resolver
cp .env.example .env
set -a; source .env; set +a
docker compose -f ../../docker-compose.yml up -d --build
uv sync --extra cli --extra worker --extra pydantic-ai --extra examples
```

The example's README carries the complete CLI walkthrough from there.

## MCP configuration

[`examples/v2/mcp/`](https://github.com/zenml-io/kitaru/tree/develop/examples/v2/mcp)
shows how to connect an MCP client to the
[Kitaru MCP server](../agent-native/mcp-server.md) in its default
read-only mode, and what each capability mode unlocks.
