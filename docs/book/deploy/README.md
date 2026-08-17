---
description: "Self-host Kitaru for your team: one server, your Postgres, workers in the environments where your agents live."
icon: server
---

# Deploy Kitaru

A Kitaru deployment is deliberately small:

- **The server** is one FastAPI service on Postgres. It stores agents, sessions, cohorts, evaluators, experiments, and replays, and serves the REST API the SDK, CLI, and workers speak. It executes no user code.
- **Workers** are processes you run wherever your agents' code and credentials live. All execution (replays, imports, evaluations) happens there. See [Workers in production](workers.md).
- **Postgres** is the only stateful dependency. Your database, your backups.

This shape is the data-privacy story: traces are stored on your server, parsed and replayed on your workers. Nothing needs to leave your systems.

## Setting up

1. [Docker](docker.md): Compose for a single host, or the published server image against your managed Postgres. Start here. On Kubernetes, use the [Helm chart](helm.md).
2. Create [accounts and API keys](authentication.md) for your team and your CI (Python client today; CLI verbs are on the way).
3. Start [workers](workers.md) in each environment agents run in.
4. Store provider credentials the server should manage as [secrets](secrets.md), and set client defaults via [configuration](configuration.md).

Steps 2 to 4 are covered in **Running in production**, alongside worker
sizing, authentication, secrets and configuration. Come back to them once
a server is up. For a first look, `kitaru login --local` in
[Installation](../getting-started/installation.md) is faster than any of
this.
