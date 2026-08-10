<p align="center">
  <a href="https://kitaru.ai">
    <img src="assets/kitaru_logo.png" alt="Kitaru" width="240">
  </a>
</p>

<h3 align="center">Traces you can run, not just read.</h3>

<p align="center">
  Kitaru (来る, "to arrive") records every agent run as a full trace — every model call, tool call, and decision — and replays it against your real code. Reproduce the trace exactly. Fork it with one thing changed. Trust the diff. It works underneath whatever framework you already use, self-hosted on your own infrastructure, and it can deploy and run your agents too.
</p>

<p align="center">
  <a href="https://pypi.org/project/kitaru/"><img alt="PyPI" src="https://img.shields.io/pypi/v/kitaru?color=blue"></a>
  <a href="https://pypi.org/project/kitaru/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/kitaru"></a>
  <a href="https://github.com/zenml-io/kitaru/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/zenml-io/kitaru"></a>
</p>

<p align="center">
  <a href="https://docs.zenml.io/kitaru">Docs</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="https://docs.zenml.io/kitaru/getting-started/examples">Examples</a> &middot;
  <a href="GETTING_STARTED.md">Getting Started Guide</a> &middot;
  <a href="https://www.zenml.io/roadmap">Roadmap</a> &middot;
  <a href="https://www.zenml.io/slack">Community</a>
</p>

---

<p align="center">
  <img src="assets/dashboard.png" alt="Kitaru Dashboard" width="720">
</p>

## 🎯 Why Kitaru?

Most traces are transcripts — you read them. A Kitaru trace re-executes:
your actual code runs again, with the trace answering for everything the
original run saw. Kitaru is a debugger with a memory, sitting beside your
observability stack — it tells you what happened; Kitaru re-runs it. That
turns production traffic into the eval suite you never had to write: every
incident is a reproducible test case, and "would the cheaper model have
held?" is an experiment over real traces instead of a guess.

- **Every trace is a recording.** Each checkpoint output — model call, tool
  call, decision — is written to your object store as a typed, versioned
  artifact. Step through it, diff it against other runs, trace a bad output
  back to the step that produced it.
- **Replay is re-execution, not re-scoring.** An unchanged replay reproduces
  the original exactly — and that faithful baseline is what lets you fork
  from any checkpoint with one thing changed and trust that the diff is your
  change, not replay noise.
- **Decide with evidence.** Every trace includes the model traffic — prompt,
  response, tokens, latency, estimated cost — recorded automatically by the
  framework adapters, or by `kitaru.llm()` in raw Python.

<a id="quick-start"></a>

## 🔁 The loop

```bash
uv add "kitaru[pydantic-ai]"   # plain `kitaru` for the raw @flow/@checkpoint path
kitaru init
```

No decorators, no graph, no rewrite. Wrap the agent you already have and run
it — Kitaru opens a flow around the call and records every model request and
tool call as a checkpoint:

```python
# agent.py
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent)
support.run_sync("Refund order #4821 — the card reader was double-charged.")
```

Traces recorded elsewhere land the same way — import them, and they become
executions like any other:

```python
from kitaru import KitaruClient

client = KitaruClient()
client.executions.import_traces("support-traces.jsonl", format="otel")
client.imports.langfuse(
    "langfuse-observations.jsonl",
    source_project_id="prod",
    agent_name="support-agent",
)
```

Every run is now a trace you can replay:

```python
trace = client.executions.latest()

# Replay — start from the agent's first model call, and your real code
# runs again against the recorded world. Unchanged, it reproduces the
# original exactly. That's your baseline.
client.executions.replay(trace.exec_id, at="support-agent_model_request")

# Fork — same trace, one thing changed: patch the recorded tool output.
# What would the agent have done if the refund had succeeded?
client.executions.replay(
    trace.exec_id,
    at="refund_payment_tool",
    checkpoint_overrides={
        "refund_payment_tool": {"output": "refund issued: $129.00"},
    },
)

# Widen — the same call takes a list. Replay last week's traces against
# the code in your working tree, and the cohort is a regression test.
traces = client.executions.list(limit=20)
client.executions.replay(
    [t.exec_id for t in traces],
    at="support-agent_model_request",
    tag="pr-1234-check",
)
```

Overrides can also swap the model on an LLM call, edit tool arguments, or
swap a checkpoint's code — see
[Replay and overrides](https://docs.zenml.io/kitaru/guides/replay-and-overrides).
Explicit `@flow`/`@checkpoint` decorators are there when you want named
replay boundaries or multi-turn workflows, and `flow.deploy()` ships a winner
as a versioned deployment invoked by name — optional; stopping at the
regression test is a fine place to stop.

### Durable execution (the plumbing)

Recording a run means surviving one. Checkpoints double as crash recovery — a
crash or pod eviction resumes from cached outputs instead of re-burning
tokens. `kitaru.wait()` pauses a flow for hours or days until a human or
webhook responds. `flow.deploy()` freezes versioned snapshots that consumers
invoke by name, and `@checkpoint(runtime="isolated")` runs heavy steps in
their own pod on Kubernetes, AWS, GCP, or Azure. This is how a faithful
recording gets minted — not the reason you reach for Kitaru.

### Works with your agent SDK

Adapters for six agent frameworks — wrap your existing agent, no rewrite:

| Framework | Adapter |
|---|---|
| PydanticAI | `kitaru.adapters.pydantic_ai.KitaruAgent` |
| OpenAI Agents SDK | `kitaru_openai_agents.KitaruRunner` (install `kitaru-openai-agents`) |
| Claude Agent SDK | `kitaru.adapters.claude_agent_sdk.KitaruClaudeRunner` |
| LangGraph | `kitaru.adapters.langgraph.KitaruGraphRunner` |
| Gemini | `kitaru.adapters.gemini.KitaruGeminiInteractionsRunner` |
| Google ADK | `kitaru.adapters.google_adk.KitaruADKRunner` |

For raw-Python agents, `@flow` and `@checkpoint` around your calls give you
the same recording without an adapter. Your model, your tools, your
framework — Kitaru wraps them, not the other way around.

The OpenAI Agents SDK adapter currently lives only in this repository's plugin workspace. It is not exported by the installed `kitaru` package.

### Inspect it from your coding agent

Kitaru's optional v2 MCP server gives Claude Code, Codex, Cursor, and other MCP clients a compact typed interface for agents, sessions, cohorts, experiments, evaluators, replays, and asynchronous jobs. It starts in read-only mode with two tools; write and destructive tools are absent unless you explicitly select a broader mode.

```bash
uv add kitaru --extra mcp
claude mcp add --scope project kitaru -- kitaru-mcp
```

See the [MCP server guide](https://docs.zenml.io/kitaru/agent-native/mcp-server) before enabling standard or destructive mode.

Claude Code users can also install the
[kitaru-skills](https://github.com/zenml-io/kitaru-skills) plugin —
quickstart, workflow authoring, and adapter-migration skills:

```
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```

### Self-hosted, batteries included

A single server on your own infra. Flows run on whichever **stack** you
pick — local, Kubernetes, GCP, AWS, or Azure — with artifacts in your own
S3/GCS/Azure Blob bucket, and a built-in UI to step through executions, diff
replays, and approve human-in-the-loop wait steps. No mandatory SaaS control
plane.

## 📚 Learn more

| Resource | Description |
|---|---|
| [Getting Started Guide](GETTING_STARTED.md) | Full setup walkthrough with all examples |
| [Documentation](https://docs.zenml.io/kitaru) | Complete reference and guides |
| [Agents guide](https://docs.zenml.io/user-guides/agents-guide) | Run, replay, and improve production agents end to end |
| [Examples](https://docs.zenml.io/kitaru/getting-started/examples) | Runnable workflows for every feature |
| [Stacks](https://docs.zenml.io/kitaru/stacks) | Deploy to Kubernetes, AWS, GCP, or Azure |
| [MCP server](https://docs.zenml.io/kitaru/agent-native/mcp-server) | Inspect and operate the Kitaru v2 API from a compact, read-only-by-default MCP server |

## 🌱 Origins

Kitaru is built by the team behind [ZenML](https://zenml.io), drawing on five
years of production orchestration experience (JetBrains, Adeo, Brevo). The
orchestration primitives (stacks, artifacts, lineage) are purpose-rebuilt here
for autonomous agents.

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development
setup, code style, and how to submit changes. The default branch is `develop` —
all PRs should target it.

## 💬 Community and support

- [Discussions](https://www.zenml.io/slack) — ask questions, share ideas
- [Issues](https://github.com/zenml-io/kitaru/issues) — report bugs, request features
- [Roadmap](https://www.zenml.io/roadmap) — see what's coming next
- [Docs](https://docs.zenml.io/kitaru) — guides and reference

## 📄 License

[Apache 2.0](LICENSE)
