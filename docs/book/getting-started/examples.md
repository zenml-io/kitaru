---
description: Import real agent traces, find useful cohorts, and compare an improved agent version.
icon: flask
---

# Examples

## Canonical returns example

The canonical example follows an autonomous returns agent from production evidence to an evaluated improvement:

1. Generate real PydanticAI traces through Langfuse.
2. Register the agent and start a worker.
3. Import the trace export as tagged Kitaru sessions.
4. Run the bundled cost, latency, and tool-call-pattern evaluators.
5. Freeze selected sessions in cohort versions.
6. Register a policy evaluator and an improved agent version.
7. Replay the cohorts and compare baseline and candidate evaluations.

The orders, customers, shipments, and actions are synthetic. The agent makes real model calls when you regenerate the checked-in traces, while its commerce tools only modify an in-memory store.

Start in the example directory:

```bash
cd examples/canonical_example
cp .env.example .env
set -a; source .env; set +a
docker compose -f ../../docker-compose.yml up -d --build
uv sync --extra cli --extra worker --extra pydantic-ai --extra examples
```

Continue with [`examples/canonical_example/README.md`](https://github.com/zenml-io/kitaru/tree/develop/examples/canonical_example) for the complete CLI walkthrough.

## Native MCP configuration

[`examples/v2/mcp/`](https://github.com/zenml-io/kitaru/tree/develop/examples/v2/mcp) shows how to connect an MCP client to Kitaru's local stdio server in its default read-only mode. The MCP server can inspect v2 registry and activity records. Standard mode can manage cohorts and experiments and start bounded workflows. Trace-file upload, registration, login, worker management, and wait loops remain CLI operations.
