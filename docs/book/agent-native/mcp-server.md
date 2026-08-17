---
description: Kitaru observes your production agents; your coding assistant is how you talk to Kitaru — the loop, driven from Claude Code, Codex, or Cursor.
icon: plug
---

# Drive it from your coding agent

Everything in the Kitaru loop is scriptable: the CLI covers the whole record → replay → improve journey, and a typed async Python client sits over a plain REST API. The MCP server exposes a bounded set of typed tools for inspecting sessions, replays, tags, and workers, importing sessions from existing blobs, managing evaluators, investigations, and tags, and starting evaluations or experiment runs. Single-session replay creation and blob upload remain CLI or Python-client operations.

The division of labor: **Kitaru observes your production agents; your coding assistant is how you talk to Kitaru.**

## The MCP server

Kitaru ships an MCP server, so assistants that speak MCP — Claude Code, Cursor, and friends — get typed, bounded tools instead of shelling out:

```bash
uv add "kitaru[mcp]"
```

Then register it with your assistant (`.mcp.json` for Claude Code):

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "uv",
      "args": ["run", "kitaru-mcp", "--server", "http://localhost:8000", "--mode", "standard"]
    }
  }
}
```

Three details in that snippet matter. The command is `uv run kitaru-mcp` rather than a bare `kitaru-mcp` because installing with uv puts the `kitaru-mcp` executable inside your project's virtual environment, and your assistant starts the server as a plain subprocess without activating that environment — a bare `kitaru-mcp` is not on `PATH` there, so the process never starts. `http://localhost:8000` is the server `kitaru login --local` provisions; point it at your team's server URL instead when you have one. And `--mode standard` is shown because tools above the current mode are never registered: on the default `read-only` an assistant driving the investigation loop sees no write tools at all and quietly falls back to the CLI. Starting `read-only` is still a reasonable posture — just expect a read-only assistant to be able to look and not touch.

The server needs an explicit target — `--server URL`, `KITARU_MCP_SERVER`, or `KITARU_API_URL`, in that order; startup fails if none selects a server. Credentials come from `KITARU_API_KEY` or the stored credential for that URL (a task-scoped `KITARU_API_TOKEN` is deliberately ignored). The target and credential source are selected at startup. A stored credential may be refreshed or updated while the process runs; restart `kitaru-mcp` after changing the target or an environment-provided API key.

Tools are gated by a **capability mode** — `read-only` (the default), `standard`, or `destructive` — set with `--mode` or `KITARU_MCP_MODE`. Tools above the current mode aren't just blocked; they're never registered, so the assistant doesn't even see them:

| Tool | Mode | What it does |
| --- | --- | --- |
| `kitaru_registry_read` | read-only | Read agents, cohorts, experiments, importers, evaluators, and their versions; list and filter tags; list workers or get one by exact UUID |
| `kitaru_activity_read` | read-only | Read sessions, replays, evaluations, runs, jobs, and their children |
| `kitaru_review_read` | read-only | Read [investigations and annotations](../concepts/investigations.md) |
| `kitaru_cohorts_manage` | standard | Create or update cohorts and cohort versions |
| `kitaru_experiments_manage` | standard | Create or update experiments |
| `kitaru_session_import` | standard | Import sessions from an already-uploaded blob |
| `kitaru_review_manage` | standard | Manage investigations and annotations; create or rename tags and link them to resources |
| `kitaru_workflow_start` | standard | Start a session evaluation or experiment run, return immediately |
| `kitaru_evaluators_manage` | standard | Create or update evaluators from an existing blob or pinned package |
| `kitaru_workflow_cancel` | destructive | Cancel a job or experiment run |
| `kitaru_delete` | destructive | Delete a cohort, experiment, investigation, annotation, evaluator, version, run, or tag; unlink an exact tag-resource tuple |

Start assistants in `read-only`, move to `standard` when you want them building cohorts and starting runs, and reserve `destructive` for sessions where you're watching.

Tag operations follow the same split. In `read-only`, `kitaru_registry_read` can list tags and filter them by name. Existing filtered registry or activity reads can then find sessions, agent versions, cohort versions, cohorts, experiments, and experiment runs carrying that tag. The MCP server cannot enumerate a tag's links directly. In `standard`, `kitaru_review_manage` supports `create_tag`, `update_tag`, and `link_tag`. In `destructive`, `kitaru_delete` can unlink one exact `(tag, resource type, resource id)` tuple or delete the tag. Deleting a tag also deletes every link that points from it.

Worker inspection is deliberately read-only. Use `kitaru_registry_read` with `kind: "worker"` to list workers, or `operation: "get_worker"` with an exact worker UUID. The returned `live` and `last_seen_at` fields report recent heartbeat observations; they do not guarantee that a worker will claim a particular task. Worker registration, task assignment, credentials, and lifecycle control remain outside MCP.

`kitaru_review_manage` accepts `pending`, `in_progress`, or `completed` when updating an investigation. This does not bypass server transition rules: for example, the server can still reject moving a completed investigation back to pending. A linked session's verdict remains a separate field and does not accept `pending`.

## The other surfaces

Give the assistant the connection:

```bash
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="KITKEY_..."
```

- **CLI** — the full journey has commands: `kitaru session import`, `kitaru replay create`, `kitaru session evaluate`, `kitaru cohort create`, `kitaru experiment run start`, plus registration, workers, and jobs. Commands take `--output json`, so assistant-driven invocations parse cleanly.
- **Python client** — `KitaruAPIClient()` reaches everything, including single-session replays. Your assistant writes the same snippets these docs show.
- **REST** — the server's OpenAPI schema at `/docs` on your server, when the assistant wants the raw contract.

## Prompts that work

The loop compresses well into assistant tasks. Some starting points, ready to paste:

> The last run of support-agent failed. Fetch the most recent failed session and its nodes with the Kitaru client, and tell me which tool call went wrong.

> Replay session `<id>` unchanged with the refund-check evaluator and a baseline history tool policy. When it completes, compare evaluations and cost against the baseline and summarize.

> Here are five things our support lead says a good refund reply does: `<criteria>`. Write a Kitaru evaluator that checks them, test it offline with `kitaru evaluator test`, and register it as refund-quality.

> Take every session where refund-quality failed, freeze them into a cohort called refund-hard-cases, and start an experiment that replays them with the system prompt in `prompts/support_v2.txt`.

Each is a bounded task with a verifiable artifact at the end — a session, an evaluator version, an experiment run — which is exactly the shape coding assistants are good at.

## Guardrails worth setting

- Give the assistant a **read-mostly posture**: creating evaluators and starting evaluations is cheap and reversible; deleting cohorts or experiments is not. Over MCP that's the capability mode; review deletes yourself.
- Keep a worker running under _your_ control. The assistant creating a replay doesn't execute anything — your worker does, in the environment you configured. That separation is the safety property; preserve it.
- Watch tool policies in assistant-written replays: insist on `history` + `on_miss="fail"` defaults for anything with side effects, same as you would in review. See [Tool policies](../guides/tool-policies.md).

## Agent skills

MCP gives an assistant the _interface_; [agent skills](skills.md) give it the _judgment_ — which sessions are worth reviewing, when a behavior is real enough to freeze into a cohort, and what a replay result does and does not prove. They ship separately, as Markdown procedures in [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills):

```bash
npx skills add zenml-io/kitaru-skills
```

The CLI knows whether they are present. `kitaru` with no arguments discovers installed Kitaru skills — in project and user locations, and those installed through the Claude marketplace — and, when it finds none, offers the install command as a next action. The machine-readable output reports the same under a `skills` key, so an assistant can check its own footing before it starts.

Skills and MCP are complementary, not alternatives: the skills say how to work, the server bounds what can be touched. Start with `kitaru-investigation`, the front door. See [Agent skills](skills.md) for what each one is for.
