---
description: Copy agents, connections, sessions, and insights from one Kitaru server to another with one command.
icon: cloud-arrow-up
---

# Move a workspace to another server

`kitaru workspace push` copies the content of one Kitaru server to another. The usual case is a local open-source server you tried Kitaru on and a managed workspace you signed up for afterwards: one command takes the agents, sessions, and insights you already have to the new workspace.

## What a push copies

| Record | How it is matched on the destination | Notes |
|---|---|---|
| Agents | By name | An agent that already exists is reused, and its sessions land under it. |
| Agent versions | By version number | Secrets named in a run spec are dropped from the copy. |
| Provider connections | By name | Connections holding secret values need `--copy-secrets`. |
| Sessions with their nodes | By `imported_from` and `external_id` within the agent | Sessions still `in_progress` and replay result sessions are skipped. |
| Insights | By the source insight id recorded in the copy's metadata | References to sessions, nodes, and agents are rewritten. |

Cohorts, evaluators, evaluations, investigations, annotations, experiments, replays, tags, importers, and analyzers are not copied. Recreate the ones you need on the destination.

## Push a workspace

Log in to both servers once. Each login stores a credential for its server URL, and the last one becomes the selected server:

```bash
kitaru login http://localhost:8000
kitaru login https://acme.cloud.example.com
```

Preview the push, then run it. The global `--server` option names the source and `--to` names the destination:

```bash
kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com --dry-run

kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com
```

Without `--server`, the source is the selected server shown by `kitaru status`. The destination always authenticates with the credential stored by `kitaru login`. `KITARU_API_KEY` applies to the source only.

Limit the push to some agents by repeating `--agent`. Their sessions and insights follow them, and connections are copied regardless because they belong to the workspace:

```bash
kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com \
  --agent support-agent --agent billing-agent
```

## Read the result

The command reports, for each kind of record, how many were copied, were already present, were skipped, and failed. With `--output json` the same counts arrive under `item.counts`, next to the reason for each skipped record:

```bash
kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com \
  --output json --machine --non-interactive --no-browser
```

```json
{
  "schema_version": "1",
  "command": "workspace.push",
  "ok": true,
  "item": {
    "source": "http://localhost:8000",
    "destination": "https://acme.cloud.example.com",
    "dry_run": false,
    "counts": {
      "agents": {"copied": 1, "already_present": 0, "skipped": 0, "failed": 0},
      "agent_versions": {"copied": 2, "already_present": 0, "skipped": 0, "failed": 0},
      "connections": {"copied": 0, "already_present": 0, "skipped": 1, "failed": 0},
      "sessions": {"copied": 148, "already_present": 0, "skipped": 2, "failed": 0},
      "insights": {"copied": 6, "already_present": 0, "skipped": 0, "failed": 0}
    }
  }
}
```

With `--dry-run`, `copied` counts what a push would copy. When any record fails, the command exits with the `partial_failure` error and carries the same report in the error details.

## Run it again

A push is safe to repeat. Every record is matched on the destination before it is written, so a second run reports the earlier copies as `already_present` and adds only what is new on the source. Rerun the same command after a partial failure, or later to bring over sessions recorded since the first push.

Sessions get new ids on the destination. A session keeps the `imported_from` and `external_id` it had on the source, which is also what makes a later import of the same provider traces skip it. A session without that identity, such as one recorded live, gets `imported_from` set to `kitaru-workspace-push` and its source session id as `external_id`.

## Insights and their references

An insight points at the sessions and nodes behind it, in its metadata and inside its investigation prompt. A push rewrites every source session, node, and agent id it copied to the id of the destination record, and replaces the source server URL with the destination URL.

Some references have no destination record: the import that created the sessions, the analyzer version that wrote the insight, and any session the push skipped. Those ids are left as they are and listed under `unmapped_references` in the copy's `kitaru.push/v1` metadata, next to the source insight id. The command result repeats them and adds a warning, so a reference that leads nowhere on the destination is never silent.

## Connections and secrets

A connection without secret values is always copied. A connection holding secret values is skipped unless you pass `--copy-secrets`, because copying it sends those values to another server:

```bash
kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com --copy-secrets
```

A Kitaru server does not return the secret values of a connection once they are stored. With `--copy-secrets`, such a connection is copied with its name, provider, and non-secret values, and the result lists the secret keys it still needs under `incomplete_connections`. Set them on the destination:

```bash
kitaru --server https://acme.cloud.example.com connection update langfuse-prod \
  --set-secret LANGFUSE_PUBLIC_KEY=pk-... \
  --set-secret LANGFUSE_SECRET_KEY=sk-...
```

Secret values never appear in the command output. A copied connection becomes its provider's default only when the destination has no default for that provider yet. See [Provider connections](provider-connections.md) for managing them.
