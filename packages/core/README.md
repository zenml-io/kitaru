# `@zenml-io/kitaru`

Experimental framework-neutral TypeScript SDK and adapter foundation for Kitaru.

The public package name is `@zenml-io/kitaru`; `packages/core/` is only its repository directory. Framework integrations depend on this package rather than maintaining separate Kitaru clients, replay-policy engines, and recording lifecycles:

- `@zenml-io/kitaru-mastra` adapts Mastra 1.51.x.
- `@zenml-io/kitaru-vercel-ai` adapts AI SDK 7.

Release candidates use npm's `rc` tag and remain pre-1.0 compatibility previews.

```bash
pnpm add @zenml-io/kitaru@rc
```

## Public API

The package exports the Kitaru API client and JSON-safe recorder utilities from its root. Dedicated subpaths expose the client, environment resolution, errors, and the low-level adapter-building API:

```ts
import { KitaruClient } from "@zenml-io/kitaru/client";
import { resolveKitaruEnvironment } from "@zenml-io/kitaru/environment";
import { RunRecorder, decideToolCall } from "@zenml-io/kitaru/adapter";
```

The adapter subpath provides lifecycle, normalized-step, replay-override, and tool-policy primitives. It is intended for framework adapter authors. It does not provide a framework-independent `generate`, tool, agent, or streaming abstraction.

## Shared recording and replay boundary

The shared code creates and completes Kitaru sessions, allocates node indexes, writes each LLM node before its local tool children, applies static/history/passthrough policy decisions, and preserves the first runtime failure while attempting best-effort failure recording. Each adapter still determines what its framework exposes and how framework events become normalized steps.

History matching is guaranteed only within the same adapter contract. Frameworks can validate, default, or serialize tool inputs differently, so a history key recorded by one adapter is not guaranteed to match a replay through another adapter.

Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll that effect back. Use application-level idempotency keys for side-effecting tools, or choose static/history policies when replay must suppress execution.

## Data and ordering limits

Adapter calls can send task inputs, tool inputs and outputs, model metadata, and result summaries to Kitaru. Keep secrets and unnecessary personal data out of those values; the shared package does not redact application payloads. Individual adapters may impose stricter size and shape limits before values cross the recording boundary.

Node indexes preserve the order in which an adapter submits completed model steps, with each model node stored before its tool children. They do not prove provider-side start order or wall-clock ordering among concurrent operations. Consult the framework adapter README for its callback, concurrency, input, and privacy limits.
