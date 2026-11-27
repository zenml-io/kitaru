---
description: Copy agents, connections, sessions, and insights from one Kitaru server to another with one command.
icon: cloud-arrow-up
---

# Move a workspace to another server

`kitaru workspace push` copies the content of one Kitaru server to another. The usual case is a local open-source server you tried Kitaru on and a managed workspace you signed up for afterwards: one command takes the agents, sessions, and insights you already have to the new workspace.

## What a push copies

| Record | How it is matched on the destination | Notes |
|---|---|---|
| Agents | By name | A destination agent with the same name is reused, and the pushed sessions and insights merge under it. |
| Agent versions | By content: display version, description, capabilities, and run spec | A destination version is reused only when its content matches, and a new version is created otherwise. Secrets named in a run spec are dropped from the copy and ignored when matching. |
| Provider connections | By name and provider | Copied without their secret values. A destination connection of the same name for another provider fails that connection. |
| Sessions with their nodes | By `imported_from` and `external_id` within the agent, or by the source session id | Sessions still `in_progress` and replay result sessions are skipped. |
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

Without `--server`, the source is the selected server shown by `kitaru status`. A destination that requires authentication uses the credential stored by `kitaru login`, and one that runs without authentication, such as a second local server, needs no login. `KITARU_API_KEY` applies to the source only.

Before it writes anything, the push asks both servers for their server id and refuses to run when they match, so two URLs of one server, such as `localhost` and `127.0.0.1`, cannot push a server into itself.

Limit the push to some agents by repeating `--agent`. Their sessions and insights follow them, and connections are copied regardless because they belong to the workspace:

```bash
kitaru --server http://localhost:8000 workspace push \
  --to https://acme.cloud.example.com \
  --agent support-agent --agent billing-agent
```

## Read the result

The command reports, for each kind of record, how many were copied, were already present, were skipped, and failed. With `--output json` the report arrives under `item`, and `warnings` and `next_actions` sit next to it in the result envelope:

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
  "warnings": [
    "1 connection(s) on https://acme.cloud.example.com lack secret values, because a Kitaru server never returns the secret values of a connection. Their keys are listed under `incomplete_connections`."
  ],
  "links": {},
  "next_actions": [
    "Run `kitaru --server https://acme.cloud.example.com agent list` to inspect the destination.",
    "Set the secret values of each incomplete connection with one command naming all of its keys, because an update replaces every stored secret value:",
    "`kitaru --server https://acme.cloud.example.com connection update langfuse-prod --set-secret LANGFUSE_PUBLIC_KEY=<value> --set-secret LANGFUSE_SECRET_KEY=<value>`",
    "Recreate what a push does not copy on the destination: cohorts, evaluators, evaluations, investigations, annotations, experiments, replays, tags, importers, and analyzers."
  ],
  "item": {
    "source": "http://localhost:8000",
    "destination": "https://acme.cloud.example.com",
    "dry_run": false,
    "counts": {
      "agents": {"copied": 1, "already_present": 0, "skipped": 0, "failed": 0},
      "agent_versions": {"copied": 2, "already_present": 0, "skipped": 0, "failed": 0},
      "connections": {"copied": 1, "already_present": 0, "skipped": 0, "failed": 0},
      "sessions": {"copied": 148, "already_present": 0, "skipped": 1, "failed": 0},
      "insights": {"copied": 6, "already_present": 0, "skipped": 0, "failed": 0}
    },
    "skipped": [
      {
        "kind": "sessions",
        "record": "0198f0a2-7c1e-7d3a-9b55-2f4c8e6a1b07",
        "reason": "The session is still in progress on the source."
      }
    ],
    "failed": [],
    "incomplete_connections": [
      {
        "connection": "langfuse-prod",
        "missing_keys": ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"],
        "command": "kitaru --server https://acme.cloud.example.com connection update langfuse-prod --set-secret LANGFUSE_PUBLIC_KEY=<value> --set-secret LANGFUSE_SECRET_KEY=<value>"
      }
    ],
    "unmapped_references": [],
    "details_truncated": false,
    "not_copied": ["cohorts", "evaluators", "evaluations", "investigations", "annotations", "experiments", "replays", "tags", "importers", "analyzers"]
  }
}
```

`skipped`, `failed`, `incomplete_connections`, and `unmapped_references` each list at most 200 records, and `details_truncated` says when one was cut short. The counts are always complete.

With `--dry-run`, `copied` counts what a push would copy. When any record fails, the command exits with the `partial_failure` error, whose `details` carry the same report together with the `warnings` and `next_actions`. In text mode the error prints the counts, the failures, the warnings, and the follow-up commands.

Each entry under `failed` says whether a rerun alone can copy it in `retryable`. A dropped connection, a server error, or a conflict with a concurrent writer is retryable. A record the destination rejects as invalid, or a connection whose name the destination uses for another provider, is not, and the error's own `retryable` is false as long as any such failure remains.

## Run it again

A push is safe to repeat. Every record is matched on the destination before it is written, so a second run reports the earlier copies as `already_present` and adds only what is new on the source. Rerun the same command after a partial failure, or later to bring over sessions recorded since the first push.

An insight that references a session or another record whose copy failed in the same run is not written. It is reported as failed and copied by the rerun that copies the record, so its references are rewritten once instead of being left pointing at the source.

A destination numbers agent versions itself and never reuses the number of a deleted version, so a copy can carry another number than its source. The result warns when it does, and sessions and insights point at the matching copy either way. A source version edited after a push no longer matches its earlier copy, and the next push copies it again.

Sessions get new ids and new session numbers on the destination. A session that has both `imported_from` and `external_id` on the source keeps them, which is also what makes a later import of the same provider traces skip it. A session that lacks either one, such as one recorded live, gets `imported_from` set to `kitaru-workspace-push` and its source session id as `external_id`, because several source sessions can share the half they have. It stays a recorded session, and it also matches a session filter on `imported_from`.

## Insights and their references

An insight points at the sessions and nodes behind it, in its metadata and inside its investigation prompt. A push rewrites every source session, node, and agent id it copied to the id of the destination record, and replaces the source server URL with the destination URL.

Some references have no destination record: the import that created the sessions, the analyzer version that wrote the insight, and any session the push skipped. Those ids are left as they are and listed under `unmapped_references` in the copy's `kitaru.push/v1` metadata, next to the source insight id. The command result repeats them and adds a warning, so a reference that leads nowhere on the destination is never silent.

`kitaru.push/v1` is a reserved insight metadata key. A push uses it to recognize its own copies, so an insight you write yourself should not set it.

## Connections and secrets

A Kitaru server never returns the secret values of a connection once they are stored, so a push cannot copy them. A connection is copied with its name, provider, and non-secret values, and the result lists the secret keys it still needs under `incomplete_connections`. A dry run lists them too, and so does every later push until the keys are set.

Each entry carries the command that completes the connection. It names every secret key of the connection in one call, because `kitaru connection update --set-secret` replaces all stored secret values with the ones it is given:

```bash
kitaru --server https://acme.cloud.example.com connection update langfuse-prod \
  --set-secret LANGFUSE_PUBLIC_KEY=pk-... \
  --set-secret LANGFUSE_SECRET_KEY=sk-...
```

A default connection without its secret values would be selected for jobs on the destination and leave them without those keys, so a copied connection that still lacks secret values is never made the default. When its source was the default and the destination has no default for that provider yet, its command ends with `--default`, which makes it the default once the secrets are set. A complete connection becomes its provider's default right away under the same condition.

A destination connection of the same name is reused only when it addresses the same provider. When its non-secret values differ from the source, the result warns and leaves them unchanged. See [Provider connections](provider-connections.md) for managing them.
