---
description: Inspect and operate Kitaru v2 agents, sessions, cohorts, experiments, evaluators, replays, and asynchronous jobs through a compact local MCP server
icon: plug
---

# MCP Server

Kitaru includes an optional local [Model Context Protocol](https://modelcontextprotocol.io/) server for coding agents. It is a thin, typed adapter over the Kitaru API: it does not shell out to the CLI, read local files, start a Kitaru server, or expose credentials.

The server uses stdio and advertises only two read tools by default. Standard and destructive capabilities must be selected explicitly when the process starts.

## Install

{% tabs %}
{% tab title="uv" %}
```bash
uv add kitaru --extra mcp
```
{% endtab %}

{% tab title="pip" %}
```bash
pip install "kitaru[mcp]"
```
{% endtab %}
{% endtabs %}

The `kitaru-mcp` entry point is present in the base wheel. `kitaru-mcp --help` and `kitaru-mcp --version` work without the extra, while starting the protocol without it exits with installation guidance.

## Connect to Kitaru

The target is selected once at startup in this order:

1. `--server URL`
2. `KITARU_MCP_SERVER`
3. `KITARU_API_URL`

Startup fails if none of these selects a server.

The credential is selected independently in this order:

1. `KITARU_API_KEY`
2. the stored credential for the selected URL
3. anonymous access

`KITARU_TASK_TOKEN` is deliberately ignored because a long-lived local MCP process must not silently inherit a task-scoped identity. The process does not persist a selected target. An existing credential stored for the selected URL may refresh and update `credentials.json`.

The target and credential provenance are fixed for the life of the process. Restart `kitaru-mcp` after changing the connection or credential. Startup diagnostics are redacted and written to stderr; stdout is reserved for MCP protocol traffic.

Run the read-only server:

```bash
kitaru-mcp
```

Pin a server explicitly:

```bash
kitaru-mcp --server https://kitaru.example.com
```

All runtime options have `KITARU_MCP_*` environment equivalents. Command-line arguments take precedence.

```text
--mode read-only|standard|destructive
--server URL
--timeout SECONDS
--handler-timeout SECONDS
--pool-size COUNT
--max-concurrency COUNT
--debug
```

## Configure an MCP client

`kitaru-mcp` must resolve to the environment where `kitaru[mcp]` is installed. An absolute virtual-environment path is the most reliable choice.

### Claude Code

Register a project-scoped stdio server:

```bash
claude mcp add --scope project kitaru -- /absolute/path/to/.venv/bin/kitaru-mcp
```

Or commit a project `.mcp.json`:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/.venv/bin/kitaru-mcp",
      "args": []
    }
  }
}
```

Use `claude mcp list` to verify discovery. See the [Claude Code MCP documentation](https://docs.anthropic.com/en/docs/claude-code/mcp) for scopes and approvals.

### Codex

Add the local server to `~/.codex/config.toml`:

```toml
[mcp_servers.kitaru]
command = "/absolute/path/to/.venv/bin/kitaru-mcp"
args = []
```

Restart Codex after changing the configuration. Add `"--mode", "standard"` or `"--mode", "destructive"` to `args` only after reviewing the capability risks below.

### Cursor

Create `.cursor/mcp.json` for a project server or `~/.cursor/mcp.json` for a global server:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/.venv/bin/kitaru-mcp",
      "args": []
    }
  }
}
```

Cursor asks for tool approval by default. See the [Cursor MCP documentation](https://docs.cursor.com/context/model-context-protocol) for configuration locations and approval controls.

Do not put API keys in committed MCP configuration. Prefer the process environment or your MCP client's private environment configuration. `kitaru login SERVER` may store a credential for an explicitly selected matching URL, but it does not persist the MCP target.

## Capability modes

Modes are cumulative and filter the registry before `tools/list`.

| Mode | Tools | Risk |
|---|---:|---|
| `read-only` | 2 | Remote reads only; this is the default. |
| `standard` | 5 | Adds metadata writes and workflows that may execute registered code or consume compute. |
| `destructive` | 7 | Adds cancellation and allowlisted deletion. |

A tool that is not allowed in the selected mode is absent from discovery rather than advertised and rejected later. MCP annotations are client hints; Kitaru authentication and API authorization remain authoritative.

Start standard mode:

```bash
kitaru-mcp --mode standard
```

Start destructive mode only when the connected client and user approval policy are appropriate:

```bash
kitaru-mcp --mode destructive
```

## Measured tool surface

The schemas below are generated through the public MCP SDK registry and checked into `tests/mcp/snapshots/`. The destructive discovery response is 92,898 bytes, below the 192 KiB budget. Every individual input-plus-output schema is below 32 KiB; the largest is `kitaru_activity_read` at 32,729 bytes.

| Tool | First mode | Operations |
|---|---|---|
| `kitaru_registry_read` | read-only | `list`, `get`, `list_versions`, `get_version` for agent, cohort, experiment, importer, and evaluator registry records. |
| `kitaru_activity_read` | read-only | `list` and `get` sessions, replays, evaluations, experiment runs, and jobs; `list_children` for session nodes, experiment-run jobs, and job tasks. |
| `kitaru_cohorts_manage` | standard | `create`, `update`, `create_version`, `update_version`. |
| `kitaru_experiments_manage` | standard | `create`, `update` with exact evaluator versions. |
| `kitaru_session_import` | standard | Import sessions from one existing payload blob with exact importer and agent versions. |
| `kitaru_workflow_cancel` | destructive | Cancel an exact job or experiment run. |
| `kitaru_delete` | destructive | Delete an exact cohort, cohort version, experiment, or experiment run. |

The MCP SDK exposes each discriminated input under a required `request` property. For example:

```json
{
  "request": {
    "operation": "get",
    "kind": "session",
    "id": "10000000-0000-0000-0000-000000000001"
  }
}
```

Registry names are exact and case-sensitive. UUID lookups make one direct request; name lookups inspect one bounded page of at most two matches. Mutations require exact IDs and, where applicable, exact positive evaluator or importer version numbers.

## Pagination and transcript content

List operations return one page only. The default size is 20 and the MCP maximum is 100. Pass the opaque `next_cursor` back unchanged to request the next page; the server never walks every page or deduplicates results across pages.

Session-node payloads are excluded by default. Set `include_payloads=true` only when stored inputs and outputs are safe to place in the MCP transcript.

## Importing sessions

`kitaru_session_import` accepts an existing payload blob ID only. It never reads a local path or uploads a blob. The operation performs exactly four bounded preflight reads: blob metadata, the exact importer version, its parent importer, and the exact agent version. It then submits one import and returns immediately. Poll the returned job with `kitaru_activity_read`; there is no wait or watch tool.

Blob-backed import retains the existing API's domain deduplication only. It does not accept a caller-supplied request ID and is not protected across arbitrary repeated MCP invocations. After a dropped response, read the relevant jobs or sessions before deciding whether to retry.

### Evaluator selection bounds

Experiment evaluator selections use exact evaluator IDs and positive version numbers. The MCP adapter resolves each selection to the existing API's evaluator name and version fields before submitting the request.

## Cancellation and deletion

Destructive operations require exact UUIDs. They do not accept names, `force`, or a confirmation flag and are not advertised as idempotent. The MCP client is responsible for obtaining any required user approval before the call.

Deleting a cohort cascades its versions. Deleting an experiment run removes its jobs, tasks, and replay rows. Other deletion behavior remains governed by the API and its referential constraints; the MCP server does not add a broader cascade or force mode.

## Results and errors

Every handled call returns a versioned envelope in both `structuredContent` and a canonical JSON text block. The two representations are equivalent.

Success:

```json
{
  "schema_version": "1",
  "ok": true,
  "data": {},
  "warnings": []
}
```

Expected failures set MCP `isError=true` and return a redacted error envelope with a stable code, `retryable`, bounded details, and optional recovery guidance. Codes include `invalid_arguments`, `invalid_configuration`, `authentication_failed`, `permission_denied`, `not_found`, `conflict`, `rate_limited`, `timeout`, `network_error`, `remote_failed`, `remote_canceled`, `partial_failure`, and `internal_error`.

Malformed tool arguments are rejected by the MCP SDK as a protocol-level invalid-params/tool-call validation error before a Kitaru handler runs. They do not produce a Kitaru error envelope and cannot trigger a remote API call.

## Deliberate exclusions

The first release does not expose agent, importer, or evaluator registration; blob upload or local file reads; login/logout or context mutation; accounts, API keys, secrets, workers, tags, or server lifecycle; wait/watch loops; resources or prompts; remote HTTP/SSE transport; or v1 compatibility aliases.

Kitaru agents can still consume third-party or provider MCP servers through their framework adapters. That is separate from this native local Kitaru server.
