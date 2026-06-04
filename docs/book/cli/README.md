---
description: "Durable execution for AI agents. Create deployments with `kitaru deploy`; inspect existing deployments with `kitaru flow`."
icon: terminal
---

# CLI Reference

## Usage

```bash
kitaru COMMAND [OPTIONS]
```

## Global flags

| Flag | Description |
| --- | --- |
| `--help`, `-h` | Display help and exit |
| `--version`, `-V` | Display the installed version and exit |

## Output formats

Most agent-facing commands support `--output json` (or `-o json`) in addition to the default text output.

- **Text output** is designed for people reading the terminal directly.
- **JSON output** is designed for agents and scripts that need a stable structure.
- Single-item commands emit `{command, item}`.
- List commands emit `{command, items, count}`.
- `kitaru executions logs --follow --output json` is the special case: it emits one JSON event per line while following the stream.

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--version` | `bool` | No | `False` | Show the Kitaru version and exit. |

## Commands

| Command | Description |
| --- | --- |
| [`analytics`](analytics/README.md) | Manage anonymous usage analytics preferences. |
| [`auth`](auth/README.md) | Manage active Kitaru server authentication helpers. |
| [`build`](build.md) | Build an immutable deployment version from a flow target. |
| [`clean`](clean/README.md) | Reset Kitaru state. |
| [`deploy`](deploy.md) | Deploy a new flow version and attach one routing tag. |
| [`executions`](executions/README.md) | Inspect and manage flow executions. |
| [`flow`](flow/README.md) | Inspect existing deployments and manage deployment routing. Create new deployments with `kitaru deploy`. |
| [`info`](info.md) | Show detailed environment information for the current setup. |
| [`init`](init.md) | Initialize a Kitaru project in the current directory. |
| [`invoke`](invoke.md) | Invoke a deployed flow snapshot. |
| [`log-store`](log-store/README.md) | Manage global runtime log-store settings. |
| [`login`](login.md) | Connect to a remote server, or start and connect to a local server. |
| [`logout`](logout.md) | Log out from the current Kitaru server and clear stored auth state. |
| [`model`](model/README.md) | Manage local model aliases for kitaru.llm(). |
| [`secrets`](secrets/README.md) | Manage centralized runtime secrets. |
| [`stack`](stack/README.md) | Inspect, create, delete, and switch stacks. |
| [`status`](status.md) | Show the current connection state and active stack context. |

