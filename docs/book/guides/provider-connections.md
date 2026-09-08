---
description: Store provider credentials once on the server and let importer tasks pick them up automatically, instead of setting them in every worker's environment.
icon: plug
---

# Provider connections

Every import guide so far sets provider credentials in the worker's environment: `LANGFUSE_SECRET_KEY`, `LANGSMITH_API_KEY`, and so on. That works, but it ties every credential to one worker's process, and rotating a key means touching every worker that might claim an import task.

A **connection** is the alternative: a server-side resource that holds a provider's credentials and non-secret values, so an import task carries them to whichever worker claims it. Like a [secret](../deploy/secrets.md), the sensitive values are encrypted at rest. Unlike a secret, a connection is scoped to one `provider` and can be marked the provider's default, so most imports don't need to name one at all.

## Create one from an importer's schema

The built-in importers each declare a `connection_schema`, the set of environment variables their provider SDK reads. Naming an importer drives the create form:

```bash
kitaru connection create langfuse-prod --importer kitaru/langfuse@latest
```

This prompts for each property in the schema, in order, hiding input for anything the schema marks as secret (`LANGFUSE_SECRET_KEY`, in Langfuse's case). Skip the prompts with `--set KEY=VALUE` for a non-secret property or `--set-secret KEY=VALUE` for a secret one, repeated for as many keys as you already know:

```bash
kitaru connection create langfuse-prod \
  --importer kitaru/langfuse@latest \
  --set-secret LANGFUSE_PUBLIC_KEY=pk-... \
  --set-secret LANGFUSE_SECRET_KEY=sk-... \
  --set LANGFUSE_BASE_URL=https://cloud.langfuse.com
```

Non-interactively (`--non-interactive`, or scripted from CI), every required property must arrive through `--set` or `--set-secret`, since there is no terminal to prompt on.

## Create one for a provider directly

Skip the schema and address the provider by name, useful for a provider without a built-in importer or a custom importer that never declared a schema:

```bash
kitaru connection create internal-tracing \
  --provider internal-tracing \
  --set-secret API_KEY=... \
  --set BASE_URL=https://tracing.internal.example.com
```

## Defaults

Add `--default` on create, or promote an existing connection later:

```bash
kitaru connection set-default langfuse-prod
```

A provider has at most one default connection. Setting a new one clears the previous default for that provider in the same request, there's nothing to unset by hand. `kitaru connection update CONNECTION --no-default` clears a connection's default status without setting another.

`kitaru connection list`, `get CONNECTION`, and `update CONNECTION [--set ...] [--set-secret ...]` round out management. An update replaces the whole `env` and secret maps, so `--set` sends the stored `env` with the keys you name applied on top and keeps the other values, while `--set-secret` sends exactly the secret values you name and replaces every stored one. Responses never carry secret values, only a `secret_keys` list naming which keys are set. Deleting a connection also deletes the secret holding its values, so `delete CONNECTION` requires `--force`.

## Use one on an import

An import that fetches from a provider's API can name a connection explicitly:

```bash
kitaru session import \
  --importer kitaru/langfuse@latest \
  --agent support-agent@latest \
  --connection langfuse-prod \
  --since 7d --wait
```

`--connection` only applies to an API import (one with no FILE argument), since a file upload is parsed without talking to the provider at all. On the REST API and the Python client, name it as `connection_id` on the API `source` of the import create request.

## Resolution

An import resolves its connection at the moment a worker claims the task, in this order:

1. The connection the import named.
2. Otherwise, the default connection for the importer's `provider`.
3. Otherwise, nothing is injected, and the package falls back to reading the worker's own environment, exactly as it did before connections existed.

Self-hosted, single-tenant deployments can keep doing that. A connection overrides the worker's environment, it is never required.

The resolved connection is recorded on the import, so a later look at an import shows which credentials it ran with. If the connection is deleted before a worker claims the task, the import resolves again from step 2.

## Merge order

A connection's values land in the task process environment alongside everything else the worker already merges, lowest precedence first:

1. The worker's own environment.
2. The connection's `env` values.
3. The task's own `env` (set on the import request).
4. The connection's secret values.
5. `KITARU_*` contract variables (the API URL, the per-task token, and the like).

Only step 2 is new. Creating or updating a connection rejects a `KITARU_*` key outright, and rejects a key set as both an `env` value and a secret, since the merge order can't express "this key wins" for a collision that shouldn't exist in the first place.

## The worker-routing escape hatch

Some customers won't hand a Kitaru server their provider credentials at all, and a connection doesn't change that: it still means putting a secret on the server. For that case, every import task carries a `kitaru/provider=<provider>` label alongside its usual [task labels](../deploy/workers.md), so a worker can be pinned to it with a selector:

```bash
kitaru worker start --claim importer --selector kitaru/provider=langfuse
```

That worker only claims Langfuse import tasks, and reads `LANGFUSE_SECRET_KEY` and friends from its own environment the way every import did before this page existed. No connection is created, and none is needed.

## SDK and MCP

`client.connections` exposes `create`, `get`, `list`, `iter`, `update`, and `delete`, mirroring the CLI. On the MCP server, `kitaru_connection_read` reads connections in `read-only` mode, without secret values, `kitaru_connections_manage` creates, updates, and sets defaults in `standard` mode, and `kitaru_delete` deletes a connection (`kind: "connection"`) in `destructive` mode.

## Next

Set up the connection your provider's importer needs, then pick up where its own guide left off: [Langfuse](import-langfuse-traces.md), [LangSmith](import-langsmith-traces.md), [Braintrust](import-braintrust-traces.md), [Logfire](import-logfire-traces.md), or [Arize Phoenix](import-phoenix-traces.md).
