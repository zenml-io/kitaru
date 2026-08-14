---
description: The machinery behind the loop — a FastAPI server on Postgres, workers claiming tasks, and where Kitaru sits beside your observability stack.
icon: sitemap
---

# Under the Hood

You can use Kitaru without reading this page. Read it when you want to know what actually happens between "start a replay" and "read the diff" — or when you're deciding where Kitaru sits in your stack.

## Two processes, one contract

Kitaru is a **server** and your **workers**.

The server is a single FastAPI service backed by Postgres. It stores every resource — agents, sessions and their nodes, cohorts, evaluators, experiments, replays, secrets, tags — and exposes them over a plain versioned REST API (`/v1/...`). It coordinates work but executes none of it: there is no code execution on the server, ever.

[Workers](workers.md) run in your environment and pull work from the server. Everything that executes — a replayed agent, an evaluator, an importer parsing a trace export — runs as a subprocess of a worker, next to your credentials, packages, and network. The server never needs access to your model providers or your tools.

Between them sits the **job/task** layer. Commands like "replay this session," "import this export," or "score these sessions" create a job holding one or more tasks — every job carries its kind (`session_run`, `import`, `evaluation`, `replay`), so `kitaru job` listings filter cleanly. Workers claim tasks scoped by _task_ kind — `agent`, `evaluator`, `importer`, a different axis than job kinds — or by label, heartbeat while running them, and report results. Crashed workers lose their claim; the server requeues or fails the task, so no replay is ever silently stranded. `kitaru job watch <id>` follows any of it live.

Writes are safe to retry: the client stamps every create request with an idempotency key and reuses it across retries, and the server stores the outcome — a replay or evaluation request that times out on the wire and gets retried never becomes two replays.

## How a replay actually works

1. `POST /v1/replays` stores the replay — baseline session, agent version, override, tool policy, evaluators — and creates its job with one agent task.
2. A worker claims the task and starts your agent from the agent version's run spec command, with the baseline's inputs (rewritten by the override, if any) and `KITARU_REPLAY_ID` in the environment.
3. Your agent runs for real. The adapter sees `KITARU_REPLAY_ID`, fetches the override and tool policy, applies model swaps at the model-call boundary, and answers tool calls per policy — a `history` policy looks up the recorded result by a hash of the tool name and arguments.
4. The re-run records a fresh session, node by node, `origin: replay`.
5. When the agent task completes, the server appends one evaluator task per configured evaluator; workers score the result session (and, with `evaluate_baselines`, the baseline).
6. The job settles, the replay settles, and — inside an experiment run — the run's progress advances. Results are stored rows: the result session, its nodes, its evaluations.

An [experiment run](experiments.md) is this pipeline fanned out once per session in a cohort version. Nothing about scale changes the mechanics.

## Storage and blobs

Session payloads (inputs, outputs, node payloads) live in Postgres. Uploaded artifacts — trace exports to import, script plugin code — are **blobs**: content-addressed by SHA-256, deduplicated, capped by a server setting. Workers cache blobs locally by hash, so a hundred evaluator runs fetch the evaluator's code once.

Auth is deliberately simple: [API keys](../deploy/authentication.md) (`KITKEY_` prefix) or a login token, one trusted team per deployment. Ownership records who created a resource; it does not gate access. Workers and their task subprocesses never hold your key for long — they operate on short-lived tokens scoped to one worker or one task.

## Where Kitaru sits in your stack

Kitaru is a debugger with a memory, sitting **beside** your observability stack, not replacing it. Langfuse, LangSmith, or Braintrust remain your system of record for traces; Kitaru holds runnable copies of the runs you care about and the machinery to re-execute and score them. The [import path](../getting-started/import-your-traces.md) is exactly that bridge.

On the other side, Kitaru deliberately does **not** run your production agent. Your agent runs wherever it runs today; the adapter records it. Durable execution of agents in production is [ZenML](https://docs.zenml.io)'s job — ZenML runs agents durably; Kitaru replays and improves them.

Everything here is open source (Apache 2.0) and [self-hosted](../deploy/README.md): your server, your Postgres, your workers, your data.
