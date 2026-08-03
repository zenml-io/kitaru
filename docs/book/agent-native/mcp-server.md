---
description: Kitaru observes your production agents; your coding assistant is how you talk to Kitaru — the loop, driven from Claude Code, Codex, or Cursor.
icon: plug
---

# Drive it from your coding agent

Everything in the Kitaru loop is scriptable: registration, workers, and
jobs each have a CLI command, and the whole record → replay → improve
loop is a typed async Python client over a plain REST API. Which means the
agent you already code with can drive it — inspect a failing session,
write the evaluator, start the replay, and read the diff back, while you
review.

The division of labor: **Kitaru observes your production agents; your
coding assistant is how you talk to Kitaru.**

## Point your assistant at Kitaru

Give the assistant the connection and the surfaces:

```bash
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="KITKEY_..."
```

* **CLI** — `kitaru agent list`, `kitaru evaluator register`,
  `kitaru worker start`, `kitaru job watch`. Every command takes
  `--output json` and a `--non-interactive` flag, so assistant-driven
  invocations parse cleanly. `kitaru schema` dumps the command tree for
  the assistant to learn.
* **Python client** — `KitaruAPIClient.from_env()` reaches everything the
  CLI doesn't cover yet: sessions, replays, cohorts, evaluations,
  experiments. Your assistant writes the same snippets these docs show.
* **REST** — the server's OpenAPI schema at `/docs` on your server, when
  the assistant wants the raw contract.

<!-- TODO(v2-launch): the v1 `kitaru-mcp` server and the `kitaru[mcp]`
     extra do not exist in v2 yet. Restore an MCP section here when the
     v2 MCP surface ships; until then the CLI + client are the
     assistant-facing surfaces. -->

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

* Give the assistant a **read-mostly key posture**: creating replays and
  evaluators is cheap and reversible; deleting sessions or cohorts is
  not. Review deletes yourself.
* Keep a worker running under *your* control. The assistant creating a
  replay doesn't execute anything — your worker does, in the environment
  you configured. That separation is the safety property; preserve it.
* Watch tool policies in assistant-written replays: insist on
  `history` + `on_miss="fail"` defaults for anything with side effects,
  same as you would in review. See [Tool policies](../guides/tool-policies.md).
