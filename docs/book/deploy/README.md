---
description: Self-host Kitaru for your team — one server, your Postgres, workers in the environments where your agents live.
icon: server
---

# Run the Server

A Kitaru deployment is deliberately small:

* **The server** — one FastAPI service on Postgres. It stores agents,
  sessions, cohorts, evaluators, experiments, and replays, and serves the
  REST API the SDK, CLI, and workers speak. It executes no user code.
* **Workers** — processes you run wherever your agents' code and
  credentials live. All execution — replays, imports, evaluations —
  happens there. See [Workers in production](workers.md).
* **Postgres** — the only stateful dependency. Your database, your
  backups.

This shape is the data-privacy story: traces are stored on your server,
parsed and replayed on your workers. Nothing needs to leave your systems.

## Setting up

1. [Docker](docker.md) — Compose for a single host, or the server
   container against your managed Postgres. Start here.
2. Create [accounts and API keys](authentication.md) for your team and
   your CI.
3. Start [workers](workers.md) in each environment agents run in.
4. Store provider credentials the server should manage as
   [secrets](secrets.md), and set client defaults via
   [configuration](configuration.md).

<!-- TODO(v2-launch): the Helm chart in the repository still targets the
     v1 (ZenML-based) server and must not be documented as the v2 install
     path. Add a Helm page back when the v2 chart lands. -->

Kubernetes users: run the server container and worker containers with
your usual manifests for now — both are single-process containers
configured entirely through environment variables. A first-party Helm
chart for the v2 server is on the roadmap.
