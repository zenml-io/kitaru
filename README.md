<p align="center">
  <a href="https://kitaru.ai">
    <img src="assets/kitaru_logo.png" alt="Kitaru" width="240">
  </a>
</p>

<h3 align="center">Traces you can run, not just read.</h3>

<p align="center">
  Kitaru (来る, "to arrive") is replay-based evals for AI agents. It records every agent run as a session (every model call, tool call, and decision) and replays it against your real code: tool calls answered from the recording, nothing touching real systems. Reproduce a run exactly. Fork it with one thing changed. Trust the diff. Open source, self-hosted, framework-agnostic, with Python and TypeScript SDKs and adapters. From the team behind <a href="https://zenml.io">ZenML</a>: ZenML is for ML pipelines, Kitaru is for agents.
</p>

<p align="center">
  <a href="https://pypi.org/project/kitaru/"><img alt="PyPI" src="https://img.shields.io/pypi/v/kitaru?color=blue"></a>
  <a href="https://pypi.org/project/kitaru/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/kitaru"></a>
  <a href="https://github.com/zenml-io/kitaru/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/zenml-io/kitaru"></a>
</p>

<p align="center">
  <a href="https://docs.zenml.io/kitaru">Docs</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="https://www.zenml.io/roadmap">Roadmap</a> &middot;
  <a href="https://kitaru.ai/slack">Community</a>
</p>

---

## 🎯 Why Kitaru?

You can't unit-test an agent that writes to real systems, and your agent
left the test suite behind the day it shipped. But it has been generating
test cases ever since: every production run. The problem is that traces
are transcripts. You read them, nod, and still can't answer the only
question that matters: *would it have gone better with the new prompt,
the cheaper model, the fix in my working tree?*

Kitaru makes prod's past your test bench. A Kitaru trace re-executes:
your actual code runs again, with the recording answering for everything
the original run saw. Kitaru is a debugger with a memory, sitting beside
your observability stack. Traces tell you what happened; Kitaru re-runs
it.

- **Every run is a recording.** Wrap your agent once, or import the
  traces you already collect; Langfuse stays your system of record.
- **Replay is re-execution.** An unchanged replay reproduces the
  original; that faithful baseline is what makes the diff of a fork
  trustworthy. Recorded tool calls are answered from the recording, so
  no card gets refunded twice.
- **Improvement is measured, not vibed.** Evaluators, compiled from your
  domain expert's criteria and calibrated against human labels, score both
  sides. Cohorts freeze the population. Experiments replay a cohort
  against a change and show what improved and what regressed. The cohort
  that caught a failure becomes the regression gate that keeps it caught.

<a id="quick-start"></a>

## 🔁 The loop

```bash
pip install "kitaru[cli,worker]" kitaru-pydantic-ai
kitaru login --local          # provisions a local server (Docker); or: kitaru login <your-team-url>
kitaru agent register support-agent --command "python support.py"
```

One wrapper, no rewrite:

```python
# support.py
from pydantic_ai import Agent
from kitaru_pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent, agent_id=AGENT_ID)  # id printed by `kitaru agent register`
support.run_sync("Refund order #4821, the card reader double-charged me.")
```

Already tracing elsewhere? Import instead of wrapping; same result.
Importers for Langfuse, LangSmith, Braintrust, and native Kitaru JSONL
are built in:

```bash
kitaru session import langfuse-export.jsonl \
  --importer kitaru/langfuse@latest --agent support-agent@latest \
  --media-type application/x-ndjson \
  --tag imported-baseline --wait
```

Every run is now a session you can replay. Define what "good" means once
(`kitaru evaluator register refund-check --script refund_check.py
--entrypoint evaluate`), start a worker (`kitaru worker start`), and ask
real questions of real traffic:

```python
# Baseline: re-run it unchanged, tools answered from the recording.
# If this doesn't reproduce, stop: nothing forked from it can be trusted.
await client.replays.create(ReplayCreateRequest(
    baseline_session_id=session_id,
    evaluators=[EvaluatorConfig(evaluator="refund-check")],
    tool_policy=ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail")),
    evaluate_baselines=True,
))

# Fork: would the cheaper model have held? Same run, one thing changed.
await client.replays.create(ReplayCreateRequest(
    baseline_session_id=session_id,
    override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
    evaluators=[EvaluatorConfig(evaluator="refund-check")],
    tool_policy=ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail")),
))

# Widen: freeze a week of traffic into a cohort, make the change an
# experiment, and replay the population: pass rates and cost, both sides.
run = await client.experiments.start_run(experiment_id, ExperimentRunCreateRequest(
    cohort_version_id=cohort_version_id,
    agent_version_id=agent_version_id,
    evaluate_baselines=True,
))
```

The [Quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) explains the five-step method. Start from the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template) for a ready PydanticAI agent and checked-in Langfuse traces, then use the [complete returns-agent tutorial](https://docs.zenml.io/kitaru/guides/returns-agent) for the review, cohort, and replay workflow.

### Works with your agent SDK

Adapters wrap your existing agent: your model, your tools, your
framework. In Python: PydanticAI (`kitaru-pydantic-ai`), LangGraph
(`kitaru-langgraph`, which also covers LangChain agents and Deep Agents),
and the OpenAI Agents SDK (`kitaru-openai-agents`). In TypeScript: the
Vercel AI SDK (`@zenml-io/kitaru-vercel-ai`) and Mastra
(`@zenml-io/kitaru-mastra`).

TypeScript packages require Node 22.22 or later in the Node 22 release line. Start with the [Mastra adapter](https://docs.zenml.io/kitaru/adapters/mastra) or [Vercel AI SDK adapter](https://docs.zenml.io/kitaru/adapters/vercel-ai), then run the focused examples under [`v2_examples/`](https://github.com/zenml-io/kitaru/tree/develop/v2_examples).

**Framework not on that list? You are not blocked.** Import the traces
you already collect from Langfuse, LangSmith, or Braintrust with the
built-in importers, and convert any other format to Kitaru JSONL. Or
write a project-local adapter: the recording API is two client
calls, and an agent skill will draft it for you. Or wrap nothing at all:
register the agent as a function, and Kitaru asks *your* system to run
it, then adopts the trace you import. See
[no adapter for your framework](https://docs.zenml.io/kitaru/adapters/custom).

### Drive it from your coding agent

Kitaru observes your production agents; your coding assistant is how you
talk to Kitaru. Every step is scriptable: an MCP server
(`pip install "kitaru[mcp]"`, tools gated read-only → standard →
destructive), a CLI with `--output json` covering the whole loop
(`kitaru session import`, `kitaru session evaluate --tag`,
`kitaru experiment run start --wait`), and a typed async Python client, so
Claude Code, Codex, or Cursor can triage a failing session, write the
evaluator, run the experiment, and report the diff while you review.

Install the **agent skills** and it knows *how*, not just *what*:

```bash
npx skills add zenml-io/kitaru-skills
```

`kitaru-investigation` is the front door: hand it a bad session or a
week of traffic and it walks you to a reviewed cohort, choosing the
review batch, keeping the human labels yours, and stopping at
checkpoints you can resume from. Others cover running an experiment and
reading its result honestly, and building an adapter or importer for
anything unsupported.

### Self-hosted, by design

One FastAPI + Postgres server on your infrastructure (published Docker
image and Helm chart included), and no code executes on it. Replays,
imports, and evaluations run on **workers** in your own environment: your
virtualenv, your credentials, your network. Workers hold your API key
only long enough to trade it for short-lived scoped tokens. Traces don't
leave your systems. Apache 2.0, no mandatory SaaS control plane.

## 📚 Learn more

| Resource | Description |
|---|---|
| [Documentation](https://docs.zenml.io/kitaru) | Concepts, guides, and the quickstart |
| [Quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) | Understand the Observe, Judge, Define, Replay, Compare method |
| [Kitaru template](https://github.com/zenml-io/kitaru-template) | Try the method with a ready PydanticAI agent and checked-in Langfuse traces |
| [Complete tutorial](https://docs.zenml.io/kitaru/guides/returns-agent) | Run the full method with a synthetic returns agent |
| [Import your traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces) | Start from the history you already have |
| [No adapter for your framework](https://docs.zenml.io/kitaru/adapters/custom) | Import, build an adapter, or let Kitaru call your agent |
| [Agent skills](https://docs.zenml.io/kitaru/agent-native/skills) | Teach your coding assistant the loop |
| [Build a regression suite](https://docs.zenml.io/kitaru/guides/regression-suite) | Production traffic as your test suite, gated in CI |
| [Deploy Kitaru](https://docs.zenml.io/kitaru/deploy) | Self-host for your team |

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for
development setup, code style, and how to submit changes. The default
branch is `develop`; all PRs should target it.

## 💬 Community and support

- [Slack community](https://kitaru.ai/slack): ask questions, share ideas
- [Report a bug](https://kitaru.ai/help): goes straight to GitHub issues
- [support@kitaru.ai](mailto:support@kitaru.ai): when email is easier
- [Roadmap](https://www.zenml.io/roadmap): see what's coming next

Hit something broken? Any of the three channels above reaches a human;
an issue with a trace or session ID attached gets fixed fastest.

## 📄 License

[Apache 2.0](LICENSE)
