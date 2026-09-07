---
description: Install the MCP server and agent skills, then drive the whole Kitaru loop from Claude Code, Codex, or Cursor.
icon: robot
---

# Set up your coding agent

Kitaru observes your production agents; your coding assistant is how you talk to Kitaru. The whole loop is scriptable, and two installable pieces let the assistant drive it without improvising:

- The **MCP server** gives it typed, bounded Kitaru operations, with capability modes for actions that create, change, or delete state.
- The **agent skills** give it the workflow: which sessions are worth reviewing, when a behavior is clear enough to freeze into a cohort, and what a replay result does and does not prove.

Skills and MCP work together: the skills say how to work, and the server bounds what can be touched.

{% hint style="success" %}
Used the [one-line installer](../getting-started/installation.md)? It ran `kitaru setup`, which installed the skills and registered the MCP server with every coding agent it found: Claude Code (in your repo's `.mcp.json` when run inside a repository, user scope otherwise), Codex, Cursor, and Windsurf, pointed at `http://localhost:8000` in `standard` mode. Skip to [Capability modes and tools](#capability-modes-and-tools) unless you use another assistant or a different server URL.

Installed a new coding agent since, or changed servers? Run `kitaru setup` again (`uv run kitaru setup` inside a project). It replaces the previous `kitaru` entry rather than adding a second one, and rewrites each installed skill directory from the current release (local edits under `~/.agents/skills/kitaru-*` are overwritten); `--mode read-only` and the global `--server URL` change the mode and target, and `--no-skills` / `--no-mcp` limit it to one half.
{% endhint %}

## Install the MCP server

Assistants that speak MCP, such as Claude Code and Cursor, get typed tools instead of relying on shell commands for every operation:

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

Installing with uv puts the `kitaru-mcp` executable inside your project's virtual environment. Your assistant starts the server as a plain subprocess and does not activate that environment first, so a bare `kitaru-mcp` is often missing from `PATH`. Going through `uv run` gives the assistant the right environment.

{% hint style="warning" %}
Two settings trip people up:

- The `--server` URL must match the server you are logged into. `http://localhost:8000` is the default after `kitaru login --local`; if you selected a different local port, use the URL shown by `kitaru status`. On a managed or self-hosted workspace, use your workspace URL. The MCP server does not follow the CLI's current selection, and a mismatch usually looks like an empty workspace.
- The default mode is `read-only`, which leaves an assistant mid-investigation with nothing it can write. `--mode standard` lets it build cohorts and start runs; read-only is still a sensible place to start, as long as you expect that.
{% endhint %}

The server needs an explicit target: `--server URL`, `KITARU_MCP_SERVER`, or `KITARU_API_URL`, in that order. Startup fails if none selects a server. Credentials come from `KITARU_API_KEY` or the stored credential for that URL (a task-scoped `KITARU_API_TOKEN` is deliberately ignored). Restart `kitaru-mcp` after changing the target or an environment-provided API key.

## Capability modes and tools

Tools are gated by a **capability mode**, either `read-only` (the default), `standard`, or `destructive`, set with `--mode` or `KITARU_MCP_MODE`. Tools above the current mode are never registered, so the assistant does not see them:

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

Start assistants in `read-only`, move to `standard` when you want them building cohorts and starting runs, and reserve `destructive` for sessions where you are watching closely.

Tag operations follow the same split. In `read-only`, `kitaru_registry_read` can list tags and filter them by name. Existing filtered registry or activity reads can then find sessions, agent versions, cohort versions, cohorts, experiments, and experiment runs carrying that tag. The MCP server cannot enumerate a tag's links directly. In `standard`, `kitaru_review_manage` supports `create_tag`, `update_tag`, and `link_tag`. In `destructive`, `kitaru_delete` can unlink one exact `(tag, resource type, resource id)` tuple or delete the tag. Deleting a tag also deletes every link that points from it.

Worker inspection is read-only by design. Use `kitaru_registry_read` with `kind: "worker"` to list workers, or `operation: "get_worker"` with an exact worker UUID. The returned `live` and `last_seen_at` fields report recent heartbeat observations; they do not guarantee that a worker will claim a particular task. Worker registration, task assignment, credentials, and lifecycle control remain outside MCP.

`kitaru_review_manage` accepts `pending`, `in_progress`, or `completed` when updating an investigation. This does not bypass server transition rules: for example, the server can still reject moving a completed investigation back to pending. A linked session's verdict remains a separate field and does not accept `pending`.

## Install the agent skills

Skills ship separately from Kitaru as Markdown procedures in [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills). A skill does not start another service or process; your assistant reads the document and follows its procedure with the tools already available in the host.

{% hint style="info" %}
**Want to see a Kitaru skill in action before installing it?** Watch the 26-minute [guided tour](https://youtu.be/aYLfzXEr2Rk). It follows the `kitaru-guided-tour` skill from a prepared session review through a deterministic evaluator, frozen cohort, replay experiment, and comparison.
{% endhint %}

{% tabs %}
{% tab title="Any skill-aware host" %}
```bash
npx skills add zenml-io/kitaru-skills
```
{% endtab %}

{% tab title="Claude Code plugin" %}
```
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```
{% endtab %}
{% endtabs %}

If your host supports neither, copy the skill directory you want into wherever it reads skills from.

**Verify:** run `kitaru` with no arguments. It searches project and user locations, plus the Claude marketplace, for installed Kitaru skills and prints the installation command if it finds none. Machine-readable output reports the same under a `skills` key, so an assistant can check its own setup before it starts.

## The investigation skill

`kitaru-investigation` is the front door for your own agent, and it reflects the product design: **you do not author investigations, your assistant does**. It maps your sessions, generates a baseline [investigation](../concepts/investigations.md), and interviews you against the trace. Your job is answering. Use it when you have one surprising session, or a larger population you want to sample before defining a failure category.

It picks one of two entry paths from what you already have:

| You have | The skill does |
|---|---|
| A specific session that went wrong | Reads it fully, then builds a small worklist of related sessions and at least one counterexample |
| A population but no clear failure | Builds a diverse sample, normally 15–30 sessions, with a random subset alongside coverage-based selections |

It begins by surveying the selected sessions, then examines relevant ones in detail. If the review identifies a useful set of cases, it can help you create a [cohort](../concepts/cohorts.md) version for later replays. It can also select an installed evaluator that matches your criterion, and writes a new one only if none fit.

**You assign the human labels.** The assistant selects, summarizes, and organizes evidence, but an [annotation](../concepts/investigations.md) should record your judgment rather than the assistant's suggestion. Observed behavior stays separate from expected behavior: the procedure distinguishes the agent's actions, dependency behavior, and product requirements instead of treating every unexpected outcome as an agent failure.

Before creating remote state or using worker or model compute, the skill explains the operation and asks for confirmation where required. You must confirm cohort membership explicitly. If a required payload, permission, or worker is missing, the skill records a checkpoint so the investigation can resume later. Open observations come before proposed failure categories, which helps keep the first review batch from inheriting a bad taxonomy.

## The other skills

| Skill | Use it when |
|---|---|
| `kitaru-guided-tour` | First contact with no agent of your own: a value-first tour on the PydanticAI returns agent example, from a prepared three-session review to an evaluator and one approved replay experiment |
| `kitaru-investigation` | Reviewing sessions, recording evidence, and creating a cohort from confirmed cases |
| [`kitaru-replay-experiment`](../guides/replay-and-overrides.md) | Testing one candidate change against an accepted cohort with pinned evaluators, and reading whether the evidence improved, regressed, traded off, or stayed inconclusive |
| [`kitaru-adapter-builder`](../adapters/README.md) | Building a Python or TypeScript [adapter](../adapters/README.md) for a framework that Kitaru does not support yet, with explicit recording and replay capabilities |
| [`kitaru-importer-builder`](../guides/importing-sessions.md) | Building and locally validating an importer for an unsupported provider export; registration requires separate approval |

The replay skill stops short of the deployment decision: it reports what the evidence supports and leaves the call to you. The two builder skills default to finishing on your machine, and register or upload only when you ask for each step.

## Skills, MCP, and the CLI

Skills define the procedure and identify decisions that require human judgment. The MCP server provides bounded Kitaru operations and gates destructive ones. Skills fall back to the structured CLI for operations MCP does not cover, such as uploading a local file or waiting for a job. You can also follow every procedure manually with the CLI.

None of the three executes your agent on the Kitaru server. Replays run on a [worker](../concepts/workers.md) you control, in the environment you configured for it. Guardrails worth setting:

- Give the assistant a **read-mostly posture**: creating evaluators and starting evaluations is cheap and reversible, and deleting cohorts or experiments is not. Over MCP that's the capability mode; review deletes yourself.
- Keep a worker running under _your_ control. The assistant creating a replay doesn't execute anything; your worker does. That separation is the safety property; preserve it.
- Watch tool policies in assistant-written replays: insist on `history` + `on_miss="fail"` defaults for anything with side effects, same as you would in review. See [Tool policies](../guides/tool-policies.md).

## The other surfaces

Give the assistant the connection:

```bash
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="KITKEY_..."
```

- **CLI:** the full journey has commands: `kitaru session import`, `kitaru replay create`, `kitaru session evaluate`, `kitaru cohort create`, `kitaru experiment run start`, plus registration, workers, and jobs. Commands take `--output json`, so assistant-driven invocations parse cleanly.
- **Python client:** `KitaruAPIClient()` reaches everything, including single-session replays. Your assistant writes the same snippets these docs show.
- **REST:** the server's OpenAPI schema at `/docs` on your server, when the assistant wants the raw contract.

## Prompts that work

The loop compresses well into assistant tasks. Some starting points, ready to paste:

```
Use kitaru-investigation to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Show me the recorded evidence before asking for a judgment, and ask before creating resources, changing code, or starting paid replay.
```

New to Kitaru with no agent or traces of your own yet? Start with the tour instead:

```
Use kitaru-guided-tour to walk me through Kitaru on the returns agent example. I am new; explain each step as we go, and ask before anything paid or live.
```

```
The last run of support-agent failed. Fetch the most recent failed session and its nodes with the Kitaru client, and tell me which tool call went wrong.
```

```
Replay session <id> unchanged with the refund-check evaluator and a baseline history tool policy. When it completes, compare evaluations and cost against the baseline and summarize.
```

```
Here are five things our support lead says a good refund reply does: <criteria>. Write a Kitaru evaluator that checks them, test it offline with kitaru evaluator test, and register it as refund-quality.
```

```
Take every session where refund-quality failed, freeze them into a cohort called refund-hard-cases, and start an experiment that replays them with the system prompt in prompts/support_v2.txt.
```

Each is a bounded task with a verifiable artifact at the end: a session, an evaluator version, or an experiment run. That shape gives both you and the assistant something concrete to inspect.
