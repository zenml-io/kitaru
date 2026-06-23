---
description: Run a flow, replay it with one change, and compare the two runs
icon: rocket
---

# Quickstart

In the next five minutes you'll run a durable agent flow, replay it faithfully,
then replay it again with one input changed and compare the two runs. That
replay loop — reproduce a real run, change one thing, diff the result — is what
Kitaru is for. Durable execution is the mechanism that makes the replay
faithful, not the point.

If you haven't installed Kitaru yet, start with [Installation](installation.md).

## Set up a project and model

```bash
kitaru init
```

This creates a `.kitaru/` directory that marks your project root.

`kitaru.llm()` reads its provider key and default model from the environment:

```bash
export OPENAI_API_KEY=sk-...
export KITARU_DEFAULT_MODEL=openai/gpt-5-nano
```

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

## Run a flow

Create `agent.py`. Note that `model` is a flow input — that's what makes it
overridable on replay later.

```python
import kitaru
from kitaru import checkpoint, flow

@checkpoint
def research(topic: str, model: str) -> str:
    return kitaru.llm(f"Summarize {topic} in two sentences.", model=model)

@checkpoint
def draft_report(summary: str, model: str) -> str:
    return kitaru.llm(
        f"Write a short report based on this summary:\n\n{summary}",
        model=model,
    )

@flow
def research_agent(topic: str, model: str = "openai/gpt-5-nano") -> str:
    summary = research(topic, model)
    return draft_report(summary, model)

if __name__ == "__main__":
    handle = research_agent.run(topic="durable execution for AI agents")
    result = handle.wait()
    print("exec_id:", handle.exec_id)
    print(result)
```

Run it:

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

What happened:

1. `@flow` marks the top-level execution boundary; everything inside is tracked.
2. Each `@checkpoint` records its inputs and return value durably.
3. `kitaru.llm()` calls the model and captures the prompt, response, token usage,
   and latency.
4. `.run()` starts the execution and returns a `FlowHandle`; `.wait()` blocks
   until completion. `handle.exec_id` is the durable record of this run — save it.

## Replay it: reproduce, then change one thing

Replay re-executes a recorded run from a checkpoint. Run two replays from the
`exec_id` you just printed.

**First, a faithful rerun with no change.** This is your control. Because every
model and tool call was recorded, replaying with nothing changed reproduces the
original run — the baseline you measure against.

```python
import kitaru

EXEC_ID = "kr-..."  # the exec_id printed above

baseline = research_agent.replay(EXEC_ID, from_="research").wait()
```

**Then replay again with one input changed** — here, a different model:

```python
candidate = research_agent.replay(
    EXEC_ID,
    from_="research",
    model="openai/gpt-5",
).wait()
```

Replay passes flow inputs as keyword arguments, so `model="openai/gpt-5"`
overrides the original `model` input. `from_="research"` re-executes from the
`research` checkpoint forward. Everything upstream of that checkpoint is reused
from the recorded run, so you don't pay for or re-run work you aren't changing.

**Now compare the two runs.** Open both executions in the dashboard, or inspect
their captured outputs and metadata from the SDK:

```python
client = kitaru.KitaruClient()
for exec_id in (EXEC_ID, "<candidate exec_id>"):
    info = client.executions.get(exec_id)
    print(exec_id, info.status)
```

Because the baseline reproduced, the difference between the two runs is your
change — the new model — not replay noise. This is the core loop: reproduce a
real run, change exactly one thing, and trust the diff.

{% hint style="info" %}
This is not re-scoring stored outputs like an offline eval. Replay
re-executes the real run from a checkpoint with one input changed, so the
model and tool calls downstream actually run again.
{% endhint %}

### Drive replay from the CLI or a coding agent

The same loop is available from the CLI, so a coding agent (Claude Code, Codex,
Cursor) can run it through Kitaru's [MCP server](../agent-native/mcp-server.md)
and hill-climb on cost, latency, and quality:

```bash
kitaru executions list                       # find the exec_id
kitaru executions replay kr-... --from research \
  --args '{"model":"openai/gpt-5"}'
```

For checkpoint-output overrides, selector rules, and divergence handling, see
[Replay and Overrides](../guides/replay-and-overrides.md).

## Replay also resumes from failure

Replay isn't only for experiments. If `research` succeeds but `draft_report`
fails — a rate limit, a transient error — replay from the failure point instead
of re-running the whole script:

```bash
kitaru executions replay kr-... --from draft_report
```

The recorded output of `research` is reused; only `draft_report` re-executes.
The more checkpoints your flow has, the less work you repeat. This works the
same whether you use `kitaru.llm()` or bring your own client.

## Deploy to a remote stack

Everything above runs where you launch it. The same flow moves into production
unchanged. To run on remote infrastructure (Kubernetes, Vertex AI, SageMaker,
or AzureML), point the flow at a remote stack; Kitaru builds a container image
with your code and dependencies. Control the base image, packages, and
environment through the `image` parameter:

```python
@flow(
    stack="prod-k8s",
    image={
        "base_image": "python:3.12-slim",
        "requirements": ["kitaru[pydantic-ai,openai]", "httpx"],
        "apt_packages": ["git"],
    },
)
def research_agent(topic: str, model: str = "openai/gpt-5-nano") -> str:
    ...
```

Agents run on the same stacks, server, and dashboard as ZenML pipelines.

This example includes `kitaru[pydantic-ai,openai]` explicitly because setting
`base_image` means you control the image contents. Kitaru can auto-add plain
`kitaru` when it builds the requirements list for you, but it does not guess
optional extras such as the PydanticAI/OpenAI adapter dependencies.

See the [Containerization guide](../guides/containerization.md) for the full set of
image options, custom Dockerfiles, and how Kitaru packages your source.

{% hint style="info" %}
Flows always run where you execute them — a Kitaru server does not run your
code. It stores execution metadata, secrets, model aliases, and serves the UI.
To track local executions on a deployed server, run
`kitaru login https://my-server.example.com` then `kitaru status` before
running your flow.
{% endhint %}

## What's next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Replay and Overrides</strong></td><td>Flow and checkpoint overrides, selector rules, and divergence handling</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Agent Harness Platform</strong></td><td>The recommended end-to-end tour: build a durable agent harness platform stage by stage on Kitaru + PydanticAI</td><td><a href="../agent-harness-platform/README.md">../agent-harness-platform/README.md</a></td></tr><tr><td><strong>Core Concepts</strong></td><td>Understand flows, checkpoints, and the execution model</td><td><a href="../concepts/README.md">../concepts/README.md</a></td></tr><tr><td><strong>Execution Management</strong></td><td>Inspect runs, fetch logs, replay, retry, and resume</td><td><a href="../guides/execution-management.md">../guides/execution-management.md</a></td></tr><tr><td><strong>Examples</strong></td><td>Browse runnable Kitaru workflows grouped by goal</td><td><a href="examples.md">examples.md</a></td></tr><tr><td><strong>Containerization</strong></td><td>Control base images, dependencies, and Dockerfiles for remote execution</td><td><a href="../guides/containerization.md">../guides/containerization.md</a></td></tr><tr><td><strong>Tracked LLM Calls</strong></td><td>Use kitaru.llm() with captured prompt/response artifacts</td><td><a href="../guides/llm-calls.md">../guides/llm-calls.md</a></td></tr><tr><td><strong>Secrets + Model Setup</strong></td><td>Store provider credentials, register an alias, and use kitaru.llm()</td><td><a href="../guides/secrets-and-model-registration.md">../guides/secrets-and-model-registration.md</a></td></tr><tr><td><strong>MCP Server</strong></td><td>Drive replay and diff from a coding agent through tool calls</td><td><a href="../agent-native/mcp-server.md">../agent-native/mcp-server.md</a></td></tr></tbody></table>
