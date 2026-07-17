---
description: Self-host the Kitaru server with Docker or Helm
icon: server
---

# Run the Server

The Kitaru server is where executions live: it stores every run's checkpoints
and artifacts, serves the dashboard your team uses to inspect and diff replays,
and answers the SDK, CLI, and MCP clients. One server, on your own
infrastructure.

Two ways to run it:

* [Docker](docker.md) — a single container (or Compose stack) for one host.
  The fastest path to a shared server.
* [Helm](helm.md) — a chart for Kubernetes, for teams that want the server on
  the same cluster their flows run on.

Either way, point clients at it with `kitaru login` and pick where flows
execute with [stacks](../stacks/README.md).
