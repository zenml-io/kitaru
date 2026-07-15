---
description: Query and manage Kitaru executions, deployments, artifacts, projects, stacks, and secret creation through Model Context Protocol tools
icon: plug
---

# MCP Server

Kitaru ships an MCP server so a coding agent (Claude Code, Codex, Cursor) can
drive the run/replay/improve loop directly: run a flow, replay it from a
checkpoint with one input changed, diff the two, and hill-climb on cost,
latency, and quality. The agent calls structured tools instead of parsing CLI
text, so it can read execution state, change one variable, and measure the
result without a human in the loop.

The same tools also cover the supporting surface: querying executions,
publishing and invoking deployments, inspecting artifacts, switching projects,
and managing stacks and secrets.

## Install MCP support

{% tabs %}
{% tab title="uv (recommended)" %}
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

If you also want agents to start and stop the local Kitaru server, install the
`local` extra too:

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv add kitaru --extra mcp --extra local
```
{% endtab %}

{% tab title="pip" %}
```bash
pip install "kitaru[mcp,local]"
```
{% endtab %}
{% endtabs %}

## Start the server

```bash
kitaru-mcp
```

The server uses stdio transport by default.

## Configure in Claude Code

{% hint style="info" %}
`kitaru-mcp` has to resolve to the Python environment where you installed
`kitaru[mcp]`. Claude Code inherits the PATH of the shell that launched it,
not whatever virtualenv you activate later — so either activate your venv
*before* starting Claude, or point `command` at the absolute path to
`kitaru-mcp` inside that venv (e.g. `/path/to/project/.venv/bin/kitaru-mcp`).
The absolute-path form is the most reliable.
{% endhint %}

### Option 1: project `.mcp.json`

Add this to `.mcp.json` in your project root (committed to the repo, so the
whole team picks it up):

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "kitaru-mcp",
      "args": []
    }
  }
}
```

Or, using an absolute venv path:

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

### Option 2: `claude mcp add` CLI

Claude Code can register the server for you. Scope controls where the
registration lives:

```bash
# Just you, just this project (default scope: local)
claude mcp add kitaru -- kitaru-mcp

# Shared with the team via .mcp.json in this repo
claude mcp add -s project kitaru -- kitaru-mcp

# Available in every project on your machine
claude mcp add -s user kitaru -- kitaru-mcp
```

Verify with `claude mcp list`. If `kitaru-mcp` isn't on PATH, pass the
absolute venv path instead:

```bash
claude mcp add -s project kitaru -- /absolute/path/to/.venv/bin/kitaru-mcp
```

You can also just ask Claude: *"add the Kitaru MCP server to this project"* —
it will run `claude mcp add` for you.

## Tool set

Execution tools:

- `kitaru_executions_list`
- `kitaru_executions_get`
- `kitaru_executions_latest`
- `kitaru_executions_statistics`
- `get_execution_logs`
- `kitaru_executions_run`
- `kitaru_executions_cancel`
- `kitaru_executions_abort_wait`
- `kitaru_executions_input`
- `kitaru_executions_resume`
- `kitaru_executions_retry`
- `kitaru_executions_replay`
- `kitaru_executions_cohort`
- `kitaru_executions_diff`
- `kitaru_executions_diff_matrix`

Deployment tools:

- `kitaru_deployments_deploy`
- `kitaru_deployments_invoke`
- `kitaru_deployments_list`
- `kitaru_deployments_get`
- `kitaru_deployments_delete`
- `kitaru_deployments_tag`
- `kitaru_deployments_untag`

Artifact tools:

- `kitaru_artifacts_list`
- `kitaru_artifacts_get`

Secret tools:

- `kitaru_secrets_create`
- `kitaru_secrets_list`

`kitaru_secrets_list` accepts `page` and `size` (defaults: `1` and `20`) and
returns only metadata for each secret: `id`, `name`, `visibility`, `keys`,
`keys_known`, and `has_missing_values`. It never returns secret values. Pages
beyond the available results return `[]`.

List responses use `keys_known=false` because the backend does not include key
metadata there. In that state, `keys=[]` and `has_missing_values=false` mean the
metadata is unavailable, not that the secret is empty or fully readable.
`kitaru_secrets_create` returns `keys_known=true`, so its `keys` and
`has_missing_values` fields are authoritative.

`kitaru_secrets_create` also returns metadata only. The MCP server intentionally
does not expose `kitaru_secrets_delete`; use the CLI or Python SDK for deletion.

Project tools:

- `kitaru_projects_list`
- `kitaru_projects_current`
- `kitaru_projects_show`
- `kitaru_projects_use`

The MCP server exposes project read/switch operations only. It intentionally does
not expose project create/delete tools in this first pass; use the CLI or Python
SDK for durable project creation and deletion.

Connection tools:

- `kitaru_start_local_server`
- `kitaru_stop_local_server`
- `kitaru_status`
- `kitaru_stacks_list`
- `manage_stack`

Diagnostics and maintenance tools:

- `kitaru_info`
- `kitaru_clean_preview`

`kitaru_clean_preview` is strictly read-only: it returns what `kitaru clean
<scope> --dry-run` would delete and never performs the cleanup itself.

## Copy-paste prompts

Use prompts like these in an MCP-capable assistant after you configure the
Kitaru MCP server.

Read-only status check:

```text
Check my Kitaru status and list the five latest executions. Summarize anything waiting for input.
```

Execution health summary:

```text
Use Kitaru execution statistics to summarize execution health by status and by day. Do not fetch every execution unless the aggregate result shows a problem that needs detail.
```

Tag cohort analysis:

```text
Use Kitaru execution statistics to compare completed and failed executions for the nightly and customer-facing tag cohorts. Start with aggregate counts; only fetch individual executions if one cohort looks unhealthy.
```

Start and watch a flow:

```text
Run `examples/features/basic_flow/first_working_flow.py:research_agent` with topic="durable execution", then watch the execution until it finishes.
```

Resolve a waiting execution safely:

```text
Find executions waiting for input. If exactly one is waiting, show me the question and ask me for the value before calling the input tool.
```

Plan and run a replay:

```text
Replay the latest failed execution from the checkpoint before the failing one. Explain the replay plan before running it.
```

Replay with one change and diff against a baseline (the hill-climb loop):

```text
Take the latest completed execution. Replay it once with no overrides to get a baseline, then replay it again from the same checkpoint with flow_overrides setting model to a cheaper model. Diff the two runs and tell me whether cost dropped without quality regressing.
```

Inspect results from a completed execution:

```text
Get the latest completed execution and show me its response artifacts.
```

Manage a local stack:

```text
Create a local Kitaru stack named local-dev if it does not already exist, then show me the current Kitaru status.
```

Check or switch projects:

```text
Check the current Kitaru project. If it is not production, switch to production, then list the five latest executions.
```

Deploy and invoke a shared flow route:

```text
Deploy `flows/research.py:research_agent` with topic="durable execution" as a canary deployment, then invoke the canary route and show me the started execution ID.
```

## Querying execution statistics

Use `kitaru_executions_statistics` when an assistant needs counts or numeric
aggregates instead of full execution records. A good agent pattern is: ask the
cheap aggregate question first, then fetch individual executions only for the
group that looks interesting.

Example payloads:

```json
{
  "group_by": ["status"],
  "max_groups": 10
}
```

```json
{
  "group_by": ["time:day", "status"],
  "metrics": ["duration_avg:duration:avg"],
  "flow": "content_pipeline",
  "max_groups": 30
}
```

```json
{
  "group_by": ["flow"],
  "metrics": ["llm_display_cost", "llm_total_tokens"],
  "max_groups": 20
}
```

```json
{
  "group_by": ["metadata:customer_tier", "status"],
  "tags": ["customer-facing"],
  "max_groups": 100
}
```

The result shape is:

```json
{
  "groups": [
    {
      "keys": {"status": "completed"},
      "execution_count": 12,
      "metrics": {"duration_avg": 43.2}
    },
    {
      "keys": {"status": "failed"},
      "execution_count": 2,
      "metrics": {"duration_avg": 18.7}
    }
  ],
  "truncated": false,
  "group_count": 2
}
```

Supported groupings are `status`, `flow`, `stack`, `tag`, `time:hour`,
`time:day`, `time:week`, `time:month`, and `metadata:<key>`. `flow` and
`stack` groupings return IDs as `flow_id` and `stack_id`; filters can still use
names. Optional metrics use the same strings as the CLI and SDK. For common LLM
totals, use shortcuts such as `llm_display_cost`, `llm_estimated_cost`,
`llm_total_tokens`, and `llm_incurred_tokens`. For other metrics, use
`<name>:<source>:<avg|sum|min|max>` for built-in numeric sources such as
`duration`, or `<name>:metadata:<metadata_key>:<avg|sum|min|max>` for numeric
execution metadata. The tool does not yet filter by time range or metadata
value. When `max_groups` truncates a time-grouped result, Kitaru keeps the
newest time rows and still returns them from oldest to newest.

{% hint style="warning" %}
Grouping by `metadata:<key>` includes the matching metadata values in the MCP
response. Only use it for metadata keys whose values are safe for the MCP
client and transcript to see.
{% endhint %}

## Starting executions with `kitaru_executions_run`

The `kitaru_executions_run` tool requires a `target` string in the format:

```text
<module_or_file>:<flow_name>
```

The left side can be an importable module path or a `.py` filesystem path.
The right side is the flow attribute name in that module.

Examples:

```text
examples/features/basic_flow/first_working_flow.py:research_agent
./examples/features/basic_flow/first_working_flow.py:research_agent
```

Pass flow inputs as `args` (a JSON object) and optionally specify a `stack`:

```json
{
  "target": "my_app.flows:research_flow",
  "args": {"topic": "durable execution"},
  "stack": "prod-k8s"
}
```

When `stack` is provided, the tool passes it to `.run(stack=...)` so the
execution targets that stack.

## Deployment tools

The deployment tools let assistants publish and invoke versioned flow routes
without shelling out to `kitaru deploy` or `kitaru invoke`.

| Tool | Use it for |
|---|---|
| `kitaru_deployments_deploy` | Create a new deployment version from `<module_or_file>:<flow_name>` |
| `kitaru_deployments_invoke` | Start a new execution from a deployed flow by `default`, tag, or version |
| `kitaru_deployments_list` | List all deployment versions, optionally filtered to one flow |
| `kitaru_deployments_get` | Inspect one deployment by version or tag |
| `kitaru_deployments_delete` | Delete one version when no exclusive tag protects it |
| `kitaru_deployments_tag` | Attach or move a public tag to a version |
| `kitaru_deployments_untag` | Remove a non-reserved public tag from a version |

`kitaru_deployments_deploy` accepts deployment-time flow inputs plus optional
deployment controls:

```json
{
  "target": "flows/research.py:research_agent",
  "inputs": {"topic": "durable execution"},
  "tag": "canary",
  "exclusive": true,
  "stack": "production",
  "image": {
    "requirements": ["kitaru[openai]"],
    "secret_environment_from": ["openai-creds"]
  },
  "cache": false,
  "retries": 1
}
```

`image` accepts either a base image string or an object matching
`kitaru.ImageSettings`.

That deploy-time image config is saved into the deployment snapshot. Later
`kitaru_deployments_invoke` calls can override flow inputs, but they do not
rewrite the deployment image.

The first deployment of a flow gets the reserved `default` tag automatically.
`default` is always exclusive and cannot be removed. Non-default tags are shared
by default; pass `exclusive=true` when the tag should move to exactly one
version, such as `canary`, `stable`, or `prod`.

`kitaru_deployments_invoke` is the MCP equivalent of the primary CLI command
`kitaru invoke`. If neither `version` nor `tag` is provided, it invokes the
reserved `default` route:

```json
{
  "flow": "research_agent",
  "inputs": {"topic": "serverless routing"}
}
```

Pin a version or named route when needed:

```json
{
  "flow": "research_agent",
  "tag": "stable",
  "inputs": {"topic": "consumer request"}
}
```

```json
{
  "flow": "research_agent",
  "version": 2,
  "inputs": {"topic": "reproducible request"}
}
```

Use the list/get/tag tools for the producer side of a shared flow:

```json
{"flow": "research_agent"}
```

```json
{"flow": "research_agent", "tag": "stable", "version": 2, "exclusive": true}
```

Then consumers can invoke by flow name and tag; they do not need the producer's
source file path.

For the full deployment model, including auto-versioning, tag exclusivity,
serverless routing, and auth context, see [Deployments](../concepts/deployments.md).

## Example query flow

1. Call `kitaru_executions_statistics(group_by=["status"])` to get the execution health overview
2. Call `kitaru_executions_list(status="waiting")` if the overview shows waiting executions
3. Use the pending-wait name or ID shown by the execution, or already known by the workflow
4. Ask the user whether to provide input or explicitly abort that wait
5. Call `kitaru_executions_input(exec_id=..., wait=..., value=...)` or `kitaru_executions_abort_wait(exec_id=..., wait=...)` (MCP requires explicit `wait`; CLI auto-detects)
6. Inspect the returned execution's `status` and singular `pending_wait`
7. If `pending_wait` is absent but the execution does not continue, call `kitaru_executions_resume(exec_id=...)` (for example, because the original runner exited)

Aborting a wait resolves only that selected wait; it does not cancel the whole
execution. The input and abort-wait tools do not call resume themselves, though
an existing runner may continue after the wait is resolved.

To provision or clean up a local stack, use `manage_stack(action="create", name="local-dev")`
or `manage_stack(action="delete", name="local-dev", force=True)`.

## Authentication and context

The MCP server reuses the same config/auth context as `kitaru` CLI and SDK.
If you want MCP tools to target a local server, start one first with bare
`kitaru login` or via `kitaru_start_local_server(...)`. If you want MCP tools
to target a deployed Kitaru server or managed workspace, connect first with
`kitaru login <server-or-workspace> --api-key <workspace-api-key>` before
starting `kitaru-mcp`, or set `KITARU_SERVER_URL`, `KITARU_AUTH_TOKEN`, and
`KITARU_PROJECT` in the MCP server environment. If you can run `kitaru status`,
MCP tools use that same connection.

Deployment MCP calls do not use per-deployment tokens. `kitaru_deployments_deploy`,
`kitaru_deployments_invoke`, and the deployment management tools authorize through the active
workspace/project context, just like `kitaru deploy`, `kitaru invoke`, and
`KitaruClient().deployments.invoke(...)`.

## Replay behavior

Replay is the tool an agent uses to test a change. A replay re-executes a real
recorded run from a checkpoint. Rerun it with no overrides and you get a
faithful baseline; rerun it again with one input changed (a different model, a
different prompt profile) and the diff between the two is your change, not replay
noise. This is the loop the agent hill-climbs.

`kitaru_executions_replay` replays explicit source executions and returns the shared replay submission JSON.

The tool accepts:

- `exec_ids`: one or more execution IDs to replay;
- `at`: the recorded checkpoint invocation, tool call, model call, or unambiguous checkpoint name where replay starts rerunning work;
- `flow_overrides`: flow parameters for the replay run (this is where you change the model or prompt profile);
- `checkpoint_overrides`: overrides keyed by checkpoint name, applied to every matching invocation;
- `invocation_overrides`: overrides keyed by one invocation ID or call ID;
- `skip`: invocation IDs or call IDs that should reuse recorded outputs even though they are at or after `at`;
- `tag`, `wait`, and `on_error` for batch handling and replay labeling.

The returned object includes `submission_id`, `plan`, `results`, `failures`, `skipped`, `summary`, and compare URLs where Kitaru can build them.

For diffs, use:

- `kitaru_executions_diff` for one original execution against one or more replays;
- `kitaru_executions_diff_matrix` for many original executions against their auto-discovered replays.

Both tools calculate deltas as replay minus original, but tokens and cost use
different accounting bases. `token_delta` measures workload tokens, including
model work that the replay reused. It is null only when neither side has
canonical usage. `cost_delta_usd` measures incurred display cost, and explicitly
reused work contributes zero. It is null when either side contains unpriced work
that was not explicitly reused.

Replay does not support `wait.*` overrides. If the replayed execution reaches a
wait, resolve it through the normal input flow afterward.

After resolving the wait with `kitaru_executions_input` or
`kitaru_executions_abort_wait`, inspect the returned execution. If
`pending_wait` is absent but the execution does not continue, call
`kitaru_executions_resume` (for example, because the original runner exited).
The input and abort-wait tools do not call resume themselves.
