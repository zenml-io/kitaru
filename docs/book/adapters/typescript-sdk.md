---
description: The framework-neutral TypeScript client that the Mastra and Vercel AI SDK adapters build on.
icon: code
---

# TypeScript SDK

`@zenml-io/kitaru` is the framework-neutral TypeScript SDK. It creates and inspects Kitaru resources, records sessions, submits evaluations and experiments, and waits for exact jobs. The [Mastra](mastra.md) and [Vercel AI SDK](vercel-ai.md) adapters build on it. The Python `kitaru` command remains the CLI for login and worker operations; there is no separate TypeScript CLI.

{% hint style="info" %}
The TypeScript packages require Node `>=22.22.0 <23` and are versioned and released together. Install with `pnpm add @zenml-io/kitaru`; see [Installation](../getting-started/installation.md#install-the-typescript-sdk-and-adapters).
{% endhint %}

## Reuse a developer login

First select a server with the CLI:

```bash
kitaru login https://kitaru.your-team.example
```

Then create a Node client without exporting its token:

```ts
import { createKitaruClient } from "@zenml-io/kitaru/node";

const client = await createKitaruClient();
const account = await client.accounts.getCurrent();
console.log(account.id);
```

The Node entry reads the Python CLI's selected server and stored credential. It binds the credential to that exact server, renews an expired renewable login in memory, and never rewrites the CLI store. Explicit `apiUrl`, `apiKey`, or `credentialProvider` options override stored selection. `KITARU_API_TOKEN` takes precedence over `KITARU_API_KEY` when no credential option is supplied.

The Node entry accepts HTTPS servers and cleartext HTTP only on loopback addresses, even if the Python CLI has stored another HTTP URL. If you run `kitaru login` again while a Node client is active, create a new client afterward. An existing client fails closed when the stored identity changes instead of silently adopting the replacement login.

Importing `@zenml-io/kitaru` or `@zenml-io/kitaru/client` never reads CLI files. Use those runtime-neutral entries in browsers, edge runtimes, and processes that receive credentials explicitly.

## Use explicit process credentials

CI, deployed applications, and long-running workers should use a dedicated API key or the task token injected by a Kitaru worker:

```ts
import { KitaruClient } from "@zenml-io/kitaru";

const client = new KitaruClient({
  apiUrl: process.env.KITARU_API_URL,
  apiKey: process.env.KITARU_API_TOKEN ?? process.env.KITARU_API_KEY,
});
```

Do not copy a developer's stored login into a container or CI secret. Create a separate process credential so it can be rotated and revoked independently.

## Resource namespaces

| Namespace | Operations |
| --- | --- |
| `accounts`, `info` | Read the current account and server information |
| `agents` | Create, read, list, update, and delete agents and agent versions |
| `sessions` | Create, read, list, update, and delete sessions; read full sessions and nodes |
| `sessionRuns` | Submit a registered agent version as a job |
| `blobs` | Upload, read, download, and delete evaluator or plugin source |
| `investigations`, `annotations` | Build and complete reviewed evidence |
| `evaluators`, `evaluations` | Register evaluator versions, submit evaluations, and inspect results |
| `cohorts`, `cohortVersions` | Define versioned session sets |
| `experiments`, `experimentRuns` | Create experiments, start runs, inspect child jobs, wait, cancel, and delete |
| `jobs` | List, inspect, wait for, cancel, and delete jobs; inspect their tasks |
| `tasks` | Inspect task status and execution specifications for recovery |
| `replays` | Create, inspect, list, wait for, and resolve recorded tool results |

List methods accept cursor pagination and JSON filters. Matching `iter()` methods, including specialized methods such as `iterVersions()` and `iterNodes()`, follow opaque cursors without mutating the caller's parameters.

## Wait and cancellation behavior

`jobs.wait(id)`, `experimentRuns.wait(id)`, and `replays.wait(id)` poll only the supplied ID. They return completed, failed, and canceled terminal responses instead of converting remote failure states into transport errors. A local timeout or `AbortSignal` stops polling only; the remote job continues.

Cancellation is a separate explicit call. `jobs.cancel(id)` and `experimentRuns.cancel(id)` send one request and do not blindly retry after response loss. A durable workflow should record the exact ID before cancellation, then read that ID to reconcile a timeout, conflict, or interrupted response. Replays have no cancel endpoint; cancel their `job_id` through `jobs`.

## Hand work to the existing CLI worker

Persist a submitted job ID before starting a worker, then scope the worker to that exact job:

```bash
kitaru worker start --job-id "$JOB_ID" --concurrency 1 --timeout 1800
```

An exact-job worker will not claim unrelated work. This is claim filtering, not a global reservation: another already-running broad worker can still claim the job first. On a shared server, stop broad workers or give them an appropriate server-side scope before submitting a workflow that requires a particular runtime or working directory.

The canonical TypeScript and Mastra examples keep a local manifest, commit remote IDs before handing them to a worker, and distinguish `awaiting_worker`, failed, and ambiguous recovery states. Those manifests are example workflow code, not automatic behavior in the client.
