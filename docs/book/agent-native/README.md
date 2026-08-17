---
description: Install the MCP server and agent skills so Claude Code, Codex, or Cursor can drive the Kitaru loop with you.
icon: robot
---

# Set up your coding agent

Kitaru is built to be driven from a coding assistant: Kitaru observes your production agents, and your assistant is how you talk to Kitaru. Two installable pieces make that work, and both take a minute to set up:

- The **[MCP server](mcp-server.md)** gives the assistant typed, bounded Kitaru operations, gated so it cannot do anything destructive unless you allow it.
- The **[agent skills](skills.md)** are Markdown procedures that teach the assistant *how* to run an investigation, a replay experiment, or an adapter build, and where your judgment is required.

## Install the MCP server

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

{% hint style="warning" %}
Two settings trip people up. The `--server` URL must be the server you are actually logged into (`http://localhost:8000` is only right after `kitaru login --local`; `kitaru status` shows the URL you resolved). And the default mode is `read-only`; `--mode standard` lets the assistant build cohorts and start runs. See [capability modes and the full tool table](mcp-server.md#the-mcp-server).
{% endhint %}

## Install the agent skills

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

**Verify:** run `kitaru` with no arguments. It searches project and user locations for installed Kitaru skills and prints the installation command if it finds none.

## How the pieces divide the work

[Skills](skills.md) define the procedure and identify decisions that require human judgment. The [MCP server](mcp-server.md) provides bounded Kitaru operations and gates destructive ones. Skills fall back to the structured CLI for operations MCP does not cover, such as uploading a local file or waiting for a job; you can also follow every procedure manually with the CLI.

None of the three executes your agent on the Kitaru server. Replays run on a [worker](../concepts/workers.md) you control, in the environment you configured for it.

## Try it

Open your agent repository in your assistant and ask:

> Use `kitaru-investigation` to investigate this agent and help me test one meaningful improvement. Assume I am new to Kitaru. Show me the recorded evidence before asking for a judgment, and ask before creating resources, changing code, or starting paid replay.

## Go deeper

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Drive it from your coding agent</strong></td><td>The full MCP reference: capability modes, tools, and tag operations.</td><td><a href="mcp-server.md">mcp-server.md</a></td></tr><tr><td><strong>Agent skills</strong></td><td>What each skill does and where your judgment is required.</td><td><a href="skills.md">skills.md</a></td></tr><tr><td><strong>Quickstart</strong></td><td>The five-step method the skills walk you through.</td><td><a href="../getting-started/quickstart.md">../getting-started/quickstart.md</a></td></tr></tbody></table>
