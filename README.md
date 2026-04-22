<p align="center">
  <a href="https://kitaru.ai">
    <img src="https://kitaru.ai/kitaru-logo.svg" alt="Kitaru" width="240">
  </a>
</p>

<h3 align="center">The runtime layer underneath your agent stack.</h3>

<p align="center">
  Kitaru (来る, "to arrive") is a self-hosted, framework-agnostic runtime for autonomous agents — underneath the harness your team already picked. You keep your agent SDK, your prompts, your tools, your model. Kitaru adds durable execution: checkpoints, replay, resume, <code>wait()</code>, versioned deployments, and isolated runtimes, running on your own infrastructure.
</p>

<p align="center">
  <a href="https://pypi.org/project/kitaru/"><img alt="PyPI" src="https://img.shields.io/pypi/v/kitaru?color=blue"></a>
  <a href="https://pypi.org/project/kitaru/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/kitaru"></a>
  <a href="https://github.com/zenml-io/kitaru/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/zenml-io/kitaru"></a>
</p>

<p align="center">
  <a href="https://kitaru.ai/docs">Docs</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="https://kitaru.ai/docs/getting-started/examples">Examples</a> &middot;
  <a href="GETTING_STARTED.md">Getting Started Guide</a> &middot;
  <a href="https://kitaru.ai/roadmap">Roadmap</a> &middot;
  <a href="https://kitaru.ai/community">Community</a>
</p>

---

<p align="center">
  <img src="assets/dashboard.png" alt="Kitaru Dashboard" width="720">
</p>

## Where Kitaru fits

Agent stacks break cleanly into three layers. Kitaru is exactly one of them.

| Layer | What it does | Examples |
|---|---|---|
| **Harness** | How the agent *thinks* — prompts, tools, model loop, framework choice | PydanticAI, Deep Agents, LangGraph, Claude Agent SDK, OpenAI Agents SDK, raw Python |
| **Runtime (Kitaru)** | How the agent *survives and executes over time* — checkpoints, replay, resume, `wait()`, versioned deployments, isolated runtimes | `@flow`, `@checkpoint`, `kitaru.wait()`, `kitaru.memory` |
| **Platform** | How your org *governs* — auth, entitlements, interceptors, observability, product UI, policy | Your existing stack |

Kitaru lives in the middle row. Harnesses define behavior, your stack defines
policy, and Kitaru gives you the durable execution layer in between.

If you're *buying* an agent platform, Kitaru may feel low-level. If you're
*building* one, that's the point.

Platform teams get the durable execution layer they'd otherwise build
themselves — run lifecycle, checkpoint boundaries, replay, invocation
routing, and self-hosted execution — without mandating which harness
application teams use on top.

## Why Kitaru?

### Works with your agent SDK

Wrap an existing PydanticAI agent with `KitaruAgent` — no rewrite. For agents
built on the OpenAI Agents SDK, Anthropic Agent SDK, or raw Python, use `@flow`
and `@checkpoint` around your calls. Your model, your tools, your framework —
Kitaru wraps them, not the other way around.

```python
from kitaru import flow
from kitaru.adapters.pydantic_ai import KitaruAgent
from pydantic_ai import Agent

researcher = KitaruAgent(
    Agent("openai:gpt-5.4", system_prompt="You summarize research topics.")
)

@flow
def research_flow(topic: str) -> str:
    return researcher.run_sync(topic).output
```

### Python-first, no graph DSL

Write normal Python. Use `if`, `for`, `try/except` — whatever your agent needs.
Kitaru gives you two decorators (`@flow` and `@checkpoint`) and a handful of
utility functions. That's it.

```python
from kitaru import checkpoint, flow

@checkpoint
def research(topic: str) -> str:
    return do_research(topic)

@checkpoint
def write_draft(research: str) -> str:
    return generate_draft(research)

@flow
def writing_agent(topic: str) -> str:
    data = research(topic)
    return write_draft(data)

result = writing_agent.run("quantum computing").wait()
```

### Durable execution and memory

Kitaru keeps agent state on disk and in infrastructure, not just in process
memory. Checkpoints persist intermediate outputs so you can replay from failure,
resume waiting runs, and inspect what happened. Durable memory adds scoped,
versioned state for long-running agents across Python, CLI, client, and MCP
surfaces.

### Deploy on your cloud

Kitaru runs locally with zero config and scales to production as a single
self-hosted server backed by SQL. Flows execute on whichever **stack** you
configure — local, Kubernetes, GCP, AWS, or Azure — with
artifacts in your own S3/GCS/Azure Blob bucket. There is no mandatory SaaS
control plane in the path of your agent's data.

### Built-in UI

Every execution is observable from day one. See your agent runs, inspect
checkpoint outputs, and approve human-in-the-loop wait steps, all from a visual
dashboard that ships with the Kitaru server.

To start the server locally, run `kitaru login` after installing `kitaru[local]`.
To connect to an existing remote server, run `kitaru login <server>`.

## Quick Start

### Install

```bash
pip install kitaru
```

Or with [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv pip install kitaru
```

To wrap a PydanticAI agent, install the adapter extra:

```bash
uv pip install "kitaru[pydantic-ai]"
```

### Optional: start a local Kitaru server

Flows run locally by default with the base install. If you also want the local
dashboard and REST API, install the local extra and then run bare `kitaru login`:

```bash
uv pip install "kitaru[local]"
kitaru login
kitaru status
```

### Optional: connect to an existing remote Kitaru server

If you already have a deployed Kitaru server, connect to it explicitly:

```bash
kitaru login https://my-server.example.com
# add --project <PROJECT> or other remote-login flags if your setup requires them
kitaru status
```

### Initialize your project

```bash
kitaru init
```

### Write your first flow

```python
# agent.py
from kitaru import checkpoint, flow

@checkpoint
def fetch_data(url: str) -> str:
    return "some data"

@checkpoint
def process_data(data: str) -> str:
    return data.upper()

@flow
def my_agent(url: str) -> str:
    data = fetch_data(url)
    return process_data(data)

result = my_agent.run("https://example.com").wait()
print(result)  # SOME DATA
```

### Run it

```bash
python agent.py
```

Every checkpoint's output is persisted automatically. You can inspect what
happened, replay from any checkpoint, or resume a waiting flow:

```bash
kitaru executions list
kitaru executions get <EXECUTION_ID>
kitaru executions logs <EXECUTION_ID>
kitaru executions replay <EXECUTION_ID> --from process_data
```

## Learn more

| Resource | Description |
|---|---|
| [Getting Started Guide](GETTING_STARTED.md) | Full setup walkthrough with all examples |
| [Documentation](https://kitaru.ai/docs) | Complete reference and guides |
| [PydanticAI adapter](https://kitaru.ai/docs/guides/pydantic-ai-adapter) | Wrap a PydanticAI agent with `KitaruAgent` |
| [Memory guide](https://kitaru.ai/docs/guides/memory) | Durable memory concepts, scopes, history, and compaction |
| [Examples](https://kitaru.ai/docs/getting-started/examples) | Runnable workflows for every feature |
| [Stacks](https://kitaru.ai/docs/stacks) | Deploy to Kubernetes, AWS, GCP, or Azure |

## Origins

Kitaru is built by the team behind [ZenML](https://zenml.io), drawing on five
years of production orchestration experience (JetBrains, Adeo, Brevo). The
orchestration primitives (stacks, artifacts, lineage) are purpose-rebuilt here
for autonomous agents.

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development
setup, code style, and how to submit changes. The default branch is `develop` —
all PRs should target it.

## Community and support

- [Discussions](https://kitaru.ai/community) — ask questions, share ideas
- [Issues](https://github.com/zenml-io/kitaru/issues) — report bugs, request features
- [Roadmap](https://kitaru.ai/roadmap) — see what's coming next
- [Docs](https://kitaru.ai/docs) — guides and reference

## License

[Apache 2.0](LICENSE)

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=633f1c0b-9a82-47af-8a6c-251a150bcc16" alt="" />
