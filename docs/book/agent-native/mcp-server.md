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

By default, `kitaru-mcp` reuses the active context and stored credential created by the Kitaru CLI. The target is selected once at startup in this order:

1. `--server URL`
2. `--context NAME`
3. `KITARU_API_URL`
4. the active persisted CLI context

The credential is selected independently in this order:

1. `KITARU_API_KEY`
2. the stored credential for the selected URL
3. anonymous access

`--server` and `--context` are mutually exclusive. `KITARU_TASK_TOKEN` is deliberately ignored because a long-lived local MCP process must not silently inherit a task-scoped identity. The process never writes `config.json`, although the existing credential source may refresh a stored token and atomically update `credentials.json`.

The target and credential provenance are fixed for the life of the process. Restart `kitaru-mcp` after switching context or logging in. Startup diagnostics are redacted and written to stderr; stdout is reserved for MCP protocol traffic.

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
--context NAME
--timeout SECONDS
--handler-timeout SECONDS
--retries COUNT
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

Do not put API keys in committed MCP configuration. Prefer `kitaru login`, the process environment, or your MCP client's private environment configuration.

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

The schemas below are generated through the public MCP SDK registry and checked into `tests/mcp/snapshots/`. The destructive discovery response is 110,552 bytes, below the 192 KiB budget. Every individual input-plus-output schema is below 32 KiB; the largest is `kitaru_activity_read` at 31,782 bytes.

| Tool | First mode | Operations |
|---|---|---|
| `kitaru_registry_read` | read-only | `list`, `get`, `list_versions`, `get_version` for agent, cohort, experiment, importer, and evaluator registry records. |
| `kitaru_activity_read` | read-only | `list` and `get` sessions, replays, evaluations, experiment runs, and jobs; `list_children` for session nodes, experiment-run jobs, and job tasks. |
| `kitaru_cohorts_manage` | standard | `create`, `update`, `create_version`, `update_version`. |
| `kitaru_experiments_manage` | standard | `create`, `update` with exact evaluator versions. |
| `kitaru_workflow_start` | standard | `replay`, `session_run`, `session_import`, `session_evaluation`, `experiment_run`. |
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

## Starting workflows

All workflow starts return immediately. Poll the returned job, replay, evaluation, or experiment-run identity with `kitaru_activity_read`; there is no wait or watch tool.

Replay, session run, session evaluation, and experiment run require a stable `request_id`. Before mutation, the MCP server checks that the target advertises `idempotency.v1`; older servers are rejected with `unsupported_server`. Reusing the same request ID with the same request lets the API replay its authoritative response, while changing the request under the same ID returns `idempotency_mismatch`.

{% hint style="warning" %}
Protected receipts confirm `idempotency="server-enforced"`, but they cannot distinguish whether that receipt was newly stored or replayed. The typed Kitaru resources intentionally return DTOs and discard response headers, including the server's stored-versus-replayed header.
{% endhint %}

The combined `kitaru_workflow_start` tool is annotated non-idempotent because its `session_import` operation does not use the protected-request contract. After a dropped response from an unprotected operation or management call, read the relevant resource before deciding whether to retry.

### Existing-blob import

`session_import` accepts an existing payload blob ID only. It never reads a local path or uploads a blob. The operation performs exactly four bounded preflight reads: blob metadata, the exact importer version, its parent importer, and the exact agent version. It then submits one import and returns immediately.

Blob-backed import retains the API's domain deduplication only. It does not accept a `request_id` and is not protected across arbitrary repeated MCP invocations.

### Evaluation bounds

A session evaluation accepts at most 100 unique sessions, 10 evaluator selections, and 100 session/evaluator pairs. Experiment and replay evaluator selections use exact evaluator IDs and positive version numbers.

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

Expected failures set MCP `isError=true` and return a redacted error envelope with a stable code, `retryable`, bounded details, and optional recovery guidance. Codes include `invalid_arguments`, `invalid_configuration`, `authentication_failed`, `permission_denied`, `not_found`, `unsupported_server`, `idempotency_mismatch`, `request_in_progress`, `conflict`, `rate_limited`, `timeout`, `network_error`, `remote_failed`, `remote_canceled`, `partial_failure`, and `internal_error`.

Malformed tool arguments are rejected by the MCP SDK as a protocol-level invalid-params/tool-call validation error before a Kitaru handler runs. They do not produce a Kitaru error envelope and cannot trigger a remote API call.

## Deliberate exclusions

The first release does not expose agent, importer, or evaluator registration; blob upload or local file reads; login/logout or context mutation; accounts, API keys, secrets, workers, tags, or server lifecycle; wait/watch loops; resources or prompts; remote HTTP/SSE transport; or v1 compatibility aliases.

Kitaru agents can still consume third-party or provider MCP servers through their framework adapters. That is separate from this native local Kitaru server.
