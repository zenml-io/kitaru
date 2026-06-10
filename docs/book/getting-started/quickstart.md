---
description: Run your first durable agent flow with Kitaru
icon: rocket
---

# Quickstart

This guide walks you through setting up a model and running your first durable
flow. If you haven't installed Kitaru yet, start with
[Installation](installation.md).

## Initialize your project

```bash
kitaru init
```

This creates a `.kitaru/` directory that marks your project root.

## Set up a model

`kitaru.llm()` needs to know which model to call and how to authenticate.
Set your provider API key and a default model:

```bash
export OPENAI_API_KEY=sk-...
export KITARU_DEFAULT_MODEL=openai/gpt-5-nano
```

`kitaru.llm()` picks up both from the environment.

{% hint style="info" %}
For production, register a model alias so you can swap models or credentials
without changing code:

```bash
kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
kitaru model register fast --model openai/gpt-5-nano --secret openai-creds
```

See [Secrets + Model Registration](../guides/secrets-and-model-registration.md)
for the full setup.
{% endhint %}

## Optional: start a local Kitaru server

{% hint style="info" %}
Flows always run where you execute them — the server does not run your code.
It stores execution metadata, secrets, model aliases, and serves the UI.
If you already have a deployed server and want your executions tracked there,
connect first and verify with `kitaru status`. If you are just trying Kitaru
locally, skip this section.
{% endhint %}

{% tabs %}
{% tab title="uv project" %}
```bash
uv run kitaru login https://my-server.example.com
uv run kitaru status
```
{% endtab %}

{% tab title="pip environment" %}
```bash
kitaru login https://my-server.example.com
kitaru status
```
{% endtab %}
{% endtabs %}

## Run your first flow

Create a file called `agent.py`:

```python
import kitaru
from kitaru import checkpoint, flow

@checkpoint
def research(topic: str) -> str:
    return kitaru.llm(f"Summarize {topic} in two sentences.")

@checkpoint
def draft_report(summary: str) -> str:
    return kitaru.llm(
        f"Write a short report based on this summary:\n\n{summary}"
    )

@flow
def research_agent(topic: str) -> str:
    summary = research(topic)
    return draft_report(summary)

if __name__ == "__main__":
    research_agent.run(topic="durable execution for AI agents").wait()
```

Then run it:

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv run agent.py
```
{% endtab %}

{% tab title="pip environment" %}
```bash
python agent.py
```
{% endtab %}
{% endtabs %}

What happens here:

1. `@flow` marks the top-level execution boundary — everything inside is tracked
2. Each `@checkpoint` persists its return value automatically
3. `kitaru.llm()` picks up your API key from the environment, calls the model, and captures the prompt, response, token usage, and latency
4. `.run()` starts the execution and returns a `FlowHandle`; `.wait()` blocks until completion

## Why checkpoints matter

Now imagine `research` succeeds but `draft_report` fails — maybe you hit a rate
limit or the model returned an error. Without Kitaru you would re-run the entire
script, paying for the `research` call again and losing progress. With Kitaru, you replay from the
failure point:

```bash
kitaru executions list          # find the execution ID
kitaru executions replay <exec-id> --from draft_report
```

Replay creates a new execution that **reuses the recorded output of `research`**
and only re-executes `draft_report`. The more checkpoints your flow has, the
less work you repeat when something goes wrong.

This works the same whether you use `kitaru.llm()` or bring your own client.

That's the runtime layer running on your laptop. The same flow survives
the move into production unchanged: the harness inside your checkpoints keeps
owning how the agent thinks, Kitaru keeps owning the durable run, and your
platform (auth, policy, UI) keeps sitting in front. When you're ready,
`my_agent.deploy(...)` captures an immutable versioned snapshot that your
platform invokes by flow name — no per-deployment tokens, no separate
service per version.

## Deploy to a remote stack

Everything above runs locally. To run on remote infrastructure (Kubernetes,
Vertex AI, SageMaker, or AzureML), point your flow at a remote stack. When targeting a remote stack, Kitaru automatically builds a container image
with your code and dependencies. You can control the base image, Python
packages, system packages, and environment variables through the `image`
parameter:

```python
@flow(
    stack="prod-k8s",
    image={
        "base_image": "python:3.12-slim",
        "requirements": ["httpx", "pydantic-ai"],
        "apt_packages": ["git"],
    },
)
def research_agent(topic: str) -> str:
    ...
```

See the [Containerization guide](../guides/containerization.md) for the full set of
image options, custom Dockerfiles, and how Kitaru packages your source code.

## What's Next

Now that Kitaru is installed and you've run a flow, follow the next path that
fits what you want to learn:

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Agents Guide</strong></td><td>The recommended end-to-end tour: build a durable agent harness platform stage by stage on Kitaru + PydanticAI, in the ZenML Learn section</td><td><a href="https://docs.zenml.io/user-guides/agents-guide">https://docs.zenml.io/user-guides/agents-guide</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Understand flows, checkpoints, and the execution model</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr><tr><td><strong>Execution Management</strong></td><td>Inspect runs, fetch logs, replay, retry, and resume</td><td><a href="../guides/execution-management.md">../guides/execution-management.md</a></td></tr><tr><td><strong>Configuration</strong></td><td>Configure runtime defaults and precedence</td><td><a href="../guides/configuration.md">../guides/configuration.md</a></td></tr><tr><td><strong>Examples</strong></td><td>Browse runnable Kitaru workflows grouped by goal</td><td><a href="examples.md">examples.md</a></td></tr><tr><td><strong>Containerization</strong></td><td>Control base images, dependencies, and Dockerfiles for remote execution</td><td><a href="../guides/containerization.md">../guides/containerization.md</a></td></tr><tr><td><strong>Wait, Input, and Resume</strong></td><td>Pause flows for external input and continue later</td><td><a href="../guides/wait-and-resume.md">../guides/wait-and-resume.md</a></td></tr><tr><td><strong>Tracked LLM Calls</strong></td><td>Use kitaru.llm() with captured prompt/response artifacts</td><td><a href="../guides/llm-calls.md">../guides/llm-calls.md</a></td></tr><tr><td><strong>Secrets + Model Setup</strong></td><td>Store provider credentials, register an alias, and use kitaru.llm()</td><td><a href="../guides/secrets-and-model-registration.md">../guides/secrets-and-model-registration.md</a></td></tr><tr><td><strong>MCP Server</strong></td><td>Query and manage executions through assistant tool calls</td><td><a href="../agent-native/mcp-server.md">../agent-native/mcp-server.md</a></td></tr></tbody></table>
