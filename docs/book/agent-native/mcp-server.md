---
description: Kitaru observes your production agents; your coding assistant is how you talk to Kitaru — the loop, driven from Claude Code, Codex, or Cursor.
icon: plug
---

# Drive it from your coding agent

Everything in the Kitaru loop is scriptable: the CLI covers the whole
record → replay → improve journey, a typed async Python client sits over
a plain REST API, and an MCP server exposes the same loop as typed
tools. Which means the agent you already code with can drive it —
inspect a failing session, write the evaluator, start the experiment,
and read the diff back, while you review.

The division of labor: **Kitaru observes your production agents; your
coding assistant is how you talk to Kitaru.**

## The MCP server

Kitaru ships an MCP server, so assistants that speak MCP — Claude Code,
Cursor, and friends — get typed, bounded tools instead of shelling out:

```bash
pip install "kitaru[mcp]"
```

Then register it with your assistant (`.mcp.json` for Claude Code):

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

The server needs an explicit target — `--server URL`,
`KITARU_MCP_SERVER`, or `KITARU_API_URL`, in that order; startup fails
if none selects a server. Credentials come from `KITARU_API_KEY` or the
stored credential for that URL (a task-scoped `KITARU_API_TOKEN` is
deliberately ignored). Both are fixed for the life of the process —
restart `kitaru-mcp` after changing either.

Tools are gated by a **capability mode** — `read-only` (the default),
`standard`, or `destructive` — set with `--mode` or `KITARU_MCP_MODE`.
Tools above the current mode aren't just blocked; they're never
registered, so the assistant doesn't even see them:

| Tool | Mode | What it does |
|---|---|---|
| `kitaru_registry_read` | read-only | Read agents, evaluators, importers, and their versions |
| `kitaru_activity_read` | read-only | Read sessions, runs, jobs, and their children |
| `kitaru_cohorts_manage` | standard | Create or update cohorts and cohort versions |
| `kitaru_experiments_manage` | standard | Create or update experiments |
| `kitaru_session_import` | standard | Import sessions from an already-uploaded blob |
| `kitaru_workflow_cancel` | destructive | Cancel a job or experiment run |
| `kitaru_delete` | destructive | Delete a cohort, experiment, version, or run |

Start assistants in `read-only`, move to `standard` when you want them
building cohorts, and reserve `destructive` for sessions where you're
watching. Starting replays, evaluations, and experiment runs stays with
the CLI (which the assistant can also drive — see below), where `--wait`
and exit codes make the outcome checkable.

## The other surfaces

Give the assistant the connection:

```bash
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="KITKEY_..."
```

* **CLI** — the full journey has commands: `kitaru session import`,
  `kitaru session evaluate`, `kitaru cohort create`,
  `kitaru experiment run start`, plus registration, workers, and jobs.
  Commands take `--output json`, so assistant-driven invocations parse
  cleanly.
* **Python client** — `KitaruAPIClient()` reaches everything,
  including single-session replays. Your assistant writes the same
  snippets these docs show.
* **REST** — the server's OpenAPI schema at `/docs` on your server, when
  the assistant wants the raw contract.

## Prompts that work

The loop compresses well into assistant tasks. Some starting points, ready
to paste:

> The last run of support-agent failed. Fetch the most recent failed
> session and its nodes with the Kitaru client, and tell me which tool
> call went wrong.

> Replay session `<id>` unchanged with the refund-check evaluator and a
> baseline history tool policy. When it completes, compare evaluations
> and cost against the baseline and summarize.

> Here are five things our support lead says a good refund reply does:
> `<criteria>`. Write a Kitaru evaluator that checks them, test it
> offline with `kitaru evaluator test`, and register it as
> refund-quality.

> Take every session where refund-quality failed, freeze them into a
> cohort called refund-hard-cases, and start an experiment that replays
> them with the system prompt in `prompts/support_v2.txt`.

Each is a bounded task with a verifiable artifact at the end — a
session, an evaluator version, an experiment run — which is exactly the
shape coding assistants are good at.

## Guardrails worth setting

* Give the assistant a **read-mostly posture**: creating replays and
  evaluators is cheap and reversible; deleting sessions or cohorts is
  not. Over MCP that's the capability mode; review deletes yourself.
* Keep a worker running under *your* control. The assistant creating a
  replay doesn't execute anything — your worker does, in the environment
  you configured. That separation is the safety property; preserve it.
* Watch tool policies in assistant-written replays: insist on
  `history` + `on_miss="fail"` defaults for anything with side effects,
  same as you would in review. See [Tool policies](../guides/tool-policies.md).
