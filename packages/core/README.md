# `@zenml-io/kitaru`

Experimental framework-neutral TypeScript SDK and adapter foundation for Kitaru.

The public package name is `@zenml-io/kitaru`; `packages/core/` is only its repository directory. Framework integrations depend on this package rather than maintaining separate Kitaru clients, replay-policy engines, and recording lifecycles:

- `@zenml-io/kitaru-mastra` adapts Mastra 1.51.x.
- `@zenml-io/kitaru-vercel-ai` adapts AI SDK 7.

The TypeScript packages are versioned and released together.

```bash
pnpm add @zenml-io/kitaru
```

## Links

- [TypeScript SDK and adapter overview](https://docs.zenml.io/kitaru/adapters)
- [Install and start a Kitaru server](https://docs.zenml.io/kitaru/getting-started/installation)
- [Run the TypeScript returns agent example](https://github.com/zenml-io/kitaru/tree/develop/examples/typescript/vercel_ai_ticket_resolver)

## Public API

The package exports the resource-oriented Kitaru API client and JSON-safe recorder utilities from its root. Dedicated subpaths expose the client, environment resolution, errors, Node-only stored-login support, and the low-level adapter-building API:

```ts
import { KitaruClient } from "@zenml-io/kitaru/client";
import { resolveKitaruEnvironment } from "@zenml-io/kitaru/environment";
import { RunRecorder, decideToolCall } from "@zenml-io/kitaru/adapter";
import { createKitaruClient } from "@zenml-io/kitaru/node";
```

Use `createKitaruClient()` in a Node application on a developer machine to reuse the server and renewable credential selected by `kitaru login`. The Node entry reads the Python CLI credential store without rewriting it. The root package does not import filesystem modules or inspect that store, so browser-compatible consumers and processes with explicit credentials keep a runtime-neutral import graph.

The client exposes resource namespaces for accounts, server info, agents and versions, sessions and nodes, session runs, blobs, investigations and annotations, evaluators and evaluations, cohorts and versions, experiments and runs, jobs, tasks, and replays. Paginated resources provide list methods and async iterators. Jobs, experiment runs, and replays provide exact-ID wait helpers; job and experiment-run cancellation remains an explicit remote operation.

The adapter subpath provides lifecycle, normalized-step, replay-override, and tool-policy primitives. It is intended for framework adapter authors. It does not provide a framework-independent `generate`, tool, agent, or streaming abstraction.

## Shared recording and replay boundary

The shared code creates and completes Kitaru sessions, allocates node indexes, writes each LLM node before its local tool children, applies static/history/passthrough policy decisions, and preserves the first runtime failure while attempting best-effort failure recording. Each adapter still determines what its framework exposes and how framework events become normalized steps.

History matching is guaranteed only within the same adapter contract. Frameworks can validate, default, or serialize tool inputs differently, so a history key recorded by one adapter is not guaranteed to match a replay through another adapter.

The current history lookup response uses `null` both for a recorded tool failure and for a legitimate successful `null` result. TypeScript adapters therefore reject a found `null` history result instead of treating a previous failure as a successful replay or executing the tool. This fail-closed rule applies regardless of `on_miss` until the API can distinguish those states.

Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll that effect back. Use application-level idempotency keys for side-effecting tools, or choose static/history policies when replay must suppress execution.

## Data and ordering limits

Adapter calls can send task inputs, tool inputs and outputs, model metadata, and result summaries to Kitaru. The shared tool-payload recorder replaces values under the credential keys `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` with `[redacted]`. This key-name rule is only a safety net, not semantic classification, so keep secrets and unnecessary personal data out of recorded values. Individual adapters may impose stricter size and shape limits before values cross the recording boundary.

Node indexes preserve the order in which an adapter submits completed model steps, with each model node stored before its tool children. They do not prove provider-side start order or wall-clock ordering among concurrent operations. Consult the framework adapter README for its callback, concurrency, input, and privacy limits.
