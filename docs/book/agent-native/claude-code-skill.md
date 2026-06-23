---
description: Install the zenml-io/kitaru-skills package for Kitaru quickstarts, workflow authoring, and adapter migrations
icon: wand-magic-sparkles
---

# Agent Skills

The [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills)
package teaches your coding agent how to build and operate Kitaru flows. It is
eight reusable Markdown agent skills for scoping, authoring, migrating, and
running durable workflows. Pair them with the [MCP server](mcp-server.md) so a
coding agent (Claude Code, Codex, Cursor) can both write the flow and drive the
run → replay → improve loop against it.

The skills do not add a new runtime product to Kitaru. They are Markdown
procedures that Claude Code and other skill-aware agent hosts read so they know
the current Kitaru patterns: where to put checkpoints, where waits are safe,
which adapter boundary is honest, and which side effects need extra care. The
MCP server gives the agent the tools to execute and replay; the skills give it
the judgment to author correctly.

## Skill inventory

### Core workflow skills

Use these when you are learning Kitaru or designing new durable workflow code.

| Skill | Claude Code command | Use it for |
|---|---|---|
| `kitaru-quickstart` | `/kitaru:kitaru-quickstart` | Interactive onboarding: scaffold a small demo flow, then walk through crash recovery, replay, `wait()`, artifacts, dashboard inspection, and optional MCP setup. |
| `kitaru-scoping` | `/kitaru:kitaru-scoping` | A structured interview that decides whether durable execution helps your workflow and produces a concrete `flow_architecture.md` plan. |
| `kitaru-authoring` | `/kitaru:kitaru-authoring` | Implementation help for `@flow`, `@checkpoint`, waits, artifacts, `kitaru.llm()`, `KitaruClient`, CLI/MCP operations, deployments, secrets, and adapter usage. |

### Adapter migration skills

Use these when you already have framework-specific agent code and want to add
Kitaru without pretending Kitaru can replay hidden framework internals.

| Skill | Claude Code command | Use it for |
|---|---|---|
| `kitaru-pydantic-ai-migration` | `/kitaru:kitaru-pydantic-ai-migration` | Move existing PydanticAI code to `KitaruAgent`, choose `calls` vs `turn` boundaries, check human-in-the-loop safety, and produce a migration report. |
| `kitaru-openai-agents-migration` | `/kitaru:kitaru-openai-agents-migration` | Move OpenAI Agents SDK code to `KitaruRunner`, `OpenAIRunRequest`, and `OpenAIRunResult`; handle runner-call vs per-call checkpoints, approvals, resumes, and context serialization. |
| `kitaru-langgraph-migration` | `/kitaru:kitaru-langgraph-migration` | Move LangGraph, LangChain `create_agent(...)`, or Deep Agents-style code to `KitaruGraphRunner`; choose `graph_call` or middleware-backed `calls` boundaries. |
| `kitaru-claude-agent-sdk-migration` | `/kitaru:kitaru-claude-agent-sdk-migration` | Move Claude Agent SDK code to `KitaruClaudeRunner` with one invocation checkpoint and explicit caveats for Claude-owned tools, Bash, MCP, sessions, and workspace files. |
| `kitaru-gemini-interactions-migration` | `/kitaru:kitaru-gemini-interactions-migration` | Move Gemini Interactions, Google GenAI Interactions, or Antigravity managed-agent code to `KitaruGeminiInteractionsRunner`; handle stable responses, polling, `requires_action`, and Google-owned internals. |

## Recommended workflow

For new Kitaru work:

1. **Quickstart first** — run `/kitaru:kitaru-quickstart` to see the crash-and-replay
   story before touching your own code.
2. **Scope second** — run `/kitaru:kitaru-scoping` to turn your workflow idea into
   checkpoint boundaries, wait points, artifact names, and an MVP shape.
3. **Author third** — run `/kitaru:kitaru-authoring` against the scoped plan to write
   the actual Kitaru flow code.

For existing framework code:

1. **Pick the migration skill that matches the source framework.** For example,
   use `/kitaru:kitaru-langgraph-migration` for an existing LangGraph graph, not the
   generic authoring skill first.
2. **Let the skill classify the boundary.** The useful output is not just code;
   it is a report that says which behavior maps directly to Kitaru, which needs
   an approximate boundary, and which behavior stays owned by the framework.
3. **Use `/kitaru:kitaru-authoring` for follow-up Kitaru work.** After the adapter seam
   is in place, authoring helps with extra checkpoints, waits, artifacts,
   deployment commands, CLI/MCP inspection, and operational polish.

## Install from the Claude Code marketplace

Claude Code users can install the packaged skills from the Claude Code plugin
marketplace:

```bash
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```

Once installed from the marketplace, Claude Code can select plugin skills
automatically from context. You can also invoke them explicitly with the
plugin-qualified command names:

```text
/kitaru:kitaru-quickstart
/kitaru:kitaru-scoping
/kitaru:kitaru-authoring
/kitaru:kitaru-pydantic-ai-migration
/kitaru:kitaru-openai-agents-migration
/kitaru:kitaru-langgraph-migration
/kitaru:kitaru-claude-agent-sdk-migration
/kitaru:kitaru-gemini-interactions-migration
```

These are Markdown skill directories, so the same source package can also be
used by other agent hosts that support local skill folders or explicit
context-loading workflows. Check your host's current skill-loading mechanism and
copy the relevant directories from `skills/`.

{% hint style="info" %}
The `/kitaru:...` command names above are for the Claude Code marketplace
plugin. If you manually copy the skill directories into `.claude/skills`, use
the unqualified command names such as `/kitaru-authoring` and
`/kitaru-langgraph-migration` instead.
{% endhint %}

## Manual installation

If you prefer to copy the skill directories directly:

```bash
tmpdir=$(mktemp -d)
git clone --depth 1 https://github.com/zenml-io/kitaru-skills.git "$tmpdir/kitaru-skills"
mkdir -p .claude/skills
cp -R "$tmpdir/kitaru-skills/skills/kitaru-quickstart" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-scoping" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-authoring" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-pydantic-ai-migration" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-openai-agents-migration" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-langgraph-migration" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-claude-agent-sdk-migration" .claude/skills/
cp -R "$tmpdir/kitaru-skills/skills/kitaru-gemini-interactions-migration" .claude/skills/
rm -rf "$tmpdir"
```

Copy each full skill directory, not just the top-level `SKILL.md`. Several skills
include `references/` files that the agent reads during the guided workflow.

## Good prompts to pair with the skills

### Quickstart

- "Use `/kitaru:kitaru-quickstart` and show me what Kitaru does with a tiny local demo."
- "Give me a five-minute Kitaru tour that includes replay, waits, artifacts, and the dashboard."
- "Set up a Kitaru demo shaped like my research workflow."
- "Walk me through Kitaru with MCP integration, but keep the first run local."

### Scoping

- "Use `/kitaru:kitaru-scoping` to decide whether this customer-support agent needs durable execution."
- "Help me find the right checkpoint and wait boundaries for this coding-agent workflow."
- "Interview me and produce a `flow_architecture.md` for a durable research workflow."
- "I have a ten-step agent. Help me cut it to the smallest useful Kitaru MVP."

### Authoring

- "Use `/kitaru:kitaru-authoring` to refactor this script into one `@flow` with explicit checkpoints."
- "Add a flow-level `kitaru.wait()` approval gate before publishing the final report."
- "Add named `kitaru.save()` artifacts so I can inspect the plan, draft, and final output later."
- "Show me how to inspect executions, waits, logs, and artifacts from the CLI and MCP tools."

### Migration

- "Use `/kitaru:kitaru-pydantic-ai-migration` on this PydanticAI agent and tell me which tools are safe for per-call checkpoints."
- "Use `/kitaru:kitaru-openai-agents-migration` and choose between `calls` and `runner_call` for this OpenAI Agents workflow."
- "Use `/kitaru:kitaru-langgraph-migration` on this LangGraph graph and explain whether `graph_call` or middleware-backed `calls` is honest here."
- "Use `/kitaru:kitaru-claude-agent-sdk-migration` to make this Claude Agent SDK invocation durable without claiming granular tool replay."
- "Use `/kitaru:kitaru-gemini-interactions-migration` to wrap this Antigravity interaction and flag polling or `requires_action` risks."

## Related pages

- [Adapters](../adapters/README.md)
- [MCP Server](mcp-server.md)
- [Execution management](../guides/execution-management.md)
