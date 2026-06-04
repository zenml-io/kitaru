# Slack AI coworker

> **Hand an agent a goal in Slack. It works for as long as the goal needs,
> pauses to ask you before anything leaves the company, and sleeps for free
> while it waits — even overnight — then picks up exactly where it left off.**

This example turns a Kitaru flow into a teammate that lives in Slack. You give
it a request (`/coworker prep the QBR for Acme and send it`), it drafts a
deliverable, and then it stops and asks you to **Approve / Revise / Reject**.
That approval is a durable `kitaru.wait`: the underlying compute is released, so
the run costs nothing while it waits for you. When you click a button, the run
resumes from the exact point it paused.

The interesting part isn't the draft — it's the **lifecycle**. A run can stay
alive across a coffee break or a weekend, survive restarts, and afterwards you
can inspect every step and **replay** it from the command line with a different
model to compare cost and quality.

## What it demonstrates

| Primitive | Where |
|---|---|
| `@flow` deployed and triggered remotely | `coworker.py`, `deployments.invoke` |
| `kitaru.wait(...)` — durable human-in-the-loop, compute released | the approval gate in `coworker.py` |
| `KitaruClient.executions.input(...)` — resume a waiting run | the Slack buttons in `slack_app.py` |
| `@checkpoint` boundaries + `kitaru.log(...)` | `deliver`, the agent turns, the decision trail |
| `KitaruAgent` (PydanticAI) | drafting and revising |
| Replay / observability from the CLI | the "Inspect and rewind" section below |

## How it's wired

```
Slack  ──/coworker──▶  slack_app.py  ──deployments.invoke──▶  coworker flow (remote stack)
  ▲                        │                                        │
  │   Approve / Revise     │  poll executions.get                   │  kitaru.wait  ⏸  (pod freed)
  │   buttons in-thread ◀──┘                                        │
  └────────────────────── executions.input ───────────────────────▶ resumes ▶ deliver ▶ outcome
```

`slack_app.py` runs in **Socket Mode**, so it connects out to Slack over a
WebSocket — you do **not** need a public URL or a tunnel.

## Prerequisites

- A Kitaru server and a **remote stack** you can deploy to (Kubernetes, Vertex,
  SageMaker, AzureML). Deployments don't run on local stacks.
- An OpenAI API key.
- A Slack workspace where you can install an app.

## 1. Create the Slack app

You only do this once. Go to <https://api.slack.com/apps> → **Create New App** →
**From scratch**, name it (e.g. *Coworker*), and pick your workspace. Then:

1. **Socket Mode** (left sidebar) → toggle **Enable Socket Mode** on. When
   prompted, generate an **App-Level Token** with the `connections:write` scope.
   Copy it — it starts with `xapp-`. This is your `SLACK_APP_TOKEN`.
2. **OAuth & Permissions** → under **Bot Token Scopes**, add `commands` and
   `chat:write`. Then click **Install to Workspace** at the top and copy the
   **Bot User OAuth Token** (`xoxb-…`). This is your `SLACK_BOT_TOKEN`.
3. **Slash Commands** → **Create New Command**: command `/coworker`, a short
   description, and any usage hint. In Socket Mode you don't need a Request URL.
4. **Interactivity & Shortcuts** → toggle **Interactivity** on (so the buttons
   and the revise modal work). No Request URL is needed in Socket Mode.
5. Invite the bot to a channel: in Slack, `/invite @Coworker`.

## 2. Deploy the flow

```bash
cd examples/integrations/slack_coworker
uv sync --extra pydantic-ai
uv add --dev slack-bolt

# Give checkpoint pods your OpenAI key (the flow's image injects it).
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...

# Deploy and tag it "prod" — the Slack app invokes this tag.
kitaru deploy coworker.py:coworker --tag prod --stack <remote-stack> --exclusive
```

## 3. Run the Slack app

```bash
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
uv run python slack_app.py
```

Now, in your channel:

```
/coworker Prepare a QBR brief for Acme Corp and send it to the account team
```

The app acks immediately and starts the flow. When the agent has a draft it
posts **Approve / Revise / Reject** in the thread:

- **Approve** → the flow runs `deliver` and posts the result.
- **Revise** → a modal opens for your notes; the agent rewrites and asks again.
- **Reject** → the flow stops and says why.

Close your laptop between the draft and your decision — the run is suspended and
costs nothing. Come back later and click the button; it resumes mid-task.

## Inspect and rewind

Because the run is made of durable checkpoints, you can examine and replay it
afterwards from the terminal — this is the part a hand-rolled agent loop can't
give you:

```bash
# Find the run and see its status, checkpoints, and pending waits.
kitaru executions list --flow coworker
kitaru executions get <exec_id>

# Read the decision trail (drafting → decision → revising → delivered).
kitaru executions logs <exec_id>

# Rewind: re-run from a checkpoint with a different model to compare
# cost and quality, without touching the live conversation.
# Use the draft checkpoint name shown by `executions get`.
kitaru executions replay <exec_id> --from <draft_checkpoint> --args '{"model": "openai:gpt-4o"}'
```

## How it works

The agent is a real PydanticAI `KitaruAgent` that drives its own loop. The
human-in-the-loop is a **tool the agent calls** — `request_approval` — not
control flow wrapped around the agent. That tool wraps `kitaru.wait` (via
`wait_for_input`), so on a remote stack the execution suspends inside the tool
call and the pod is released until a human responds. This is the same pattern as
the `chatbot` example's `say_and_wait` tool.

The Slack app never holds conversation state itself — it invokes the deployment,
polls `executions.get`, and feeds your decision back with `executions.input`,
which resumes the run. The agent's final answer is persisted as the named
`outcome` artifact (by the `record_outcome` checkpoint) so the Slack app can load
and post it.

In a real deployment you'd give the agent your own tools (CRM lookups, drive
search, drafting an email), and approval would gate the irreversible action.
