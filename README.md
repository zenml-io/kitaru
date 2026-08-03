<p align="center">
  <a href="https://kitaru.ai">
    <img src="assets/kitaru_logo.png" alt="Kitaru" width="240">
  </a>
</p>

<h3 align="center">Traces you can run, not just read.</h3>

<p align="center">
  Kitaru (来る, "to arrive") is replay-based evals for AI agents. It records every agent run as a session — every model call, tool call, and decision — and replays it against your real code: tool calls answered from the recording, nothing touching real systems. Reproduce a run exactly. Fork it with one thing changed. Trust the diff. Open source, self-hosted, framework-agnostic.
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
  <a href="https://www.zenml.io/slack">Community</a>
</p>

---

## 🎯 Why Kitaru?

You can't unit-test an agent that writes to real systems — and your agent
left the test suite behind the day it shipped. But it has been generating
test cases ever since: every production run. The problem is that traces
are transcripts. You read them, nod, and still can't answer the only
question that matters: *would it have gone better with the new prompt,
the cheaper model, the fix in my working tree?*

Kitaru makes prod's past your test bench. A Kitaru trace re-executes:
your actual code runs again, with the recording answering for everything
the original run saw. Kitaru is a debugger with a memory, sitting beside
your observability stack — traces tell you what happened; Kitaru re-runs
it.

- **Every run is a recording.** Wrap your agent once, or import the
  traces you already collect — Langfuse stays your system of record.
- **Replay is re-execution, not re-scoring.** An unchanged replay
  reproduces the original; that faithful baseline is what makes the diff
  of a fork trustworthy. Recorded tool calls are answered from the
  recording, so no card gets refunded twice.
- **Improvement is measured, not vibed.** Evaluators — compiled from your
  domain expert's criteria, calibrated against human labels — score both
  sides. Cohorts freeze the population. Experiments replay a cohort
  against a change and show what improved and what regressed. The cohort
  that caught a failure becomes the regression gate that keeps it caught.

<a id="quick-start"></a>

## 🔁 The loop

```bash
pip install "kitaru[cli,pydantic-ai]"
docker compose up -d          # server, from this repo — or your team's server
kitaru login --local
kitaru agent register support-agent --command "python support.py"
```

One wrapper, no rewrite:

```python
# support.py
from pydantic_ai import Agent
from kitaru.adapters.pydantic_ai import KitaruAgent

agent = Agent("openai:gpt-5.4", name="support-agent",
              system_prompt="You resolve support tickets.")

@agent.tool_plain
def refund_payment(order_id: str) -> str:
    return payments.refund(order_id)  # your real API

support = KitaruAgent(agent, agent_id=AGENT_ID)
support.run_sync("Refund order #4821 — the card reader was double-charged.")
```

Already tracing elsewhere? Import instead of wrapping — same result:

```python
blob = await client.blobs.upload(Path("langfuse-export.jsonl").read_bytes())
await client.imports.create(ImportCreateRequest(
    importer="langfuse", agent_id=AGENT_ID, payload_blob_id=blob.id,
))
```

Every run is now a session you can replay. Define what "good" means once
(`kitaru evaluator register refund-check --script refund_check.py
--entrypoint evaluate`), start a worker (`kitaru worker start`), and ask
real questions of real traffic:

```python
# Baseline: re-run it unchanged, tools answered from the recording.
# If this doesn't reproduce, stop — nothing forked from it can be trusted.
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
# experiment, and replay the population — pass rates and cost, both sides.
run = await client.experiments.start_run(experiment_id, ExperimentRunCreateRequest(
    cohort_version_id=cohort_version_id,
    agent_version_id=agent_version_id,
    evaluate_baselines=True,
))
```

The full walkthrough — same support agent, same ticket — is the
[Quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart).

### Works with your agent SDK

Adapters wrap your existing agent — your model, your tools, your
framework. PydanticAI ships today
(`kitaru.adapters.pydantic_ai.KitaruAgent`); more adapters are being
ported to the v2 recording API. Any framework that traces to Langfuse can
come in through the
[import path](https://docs.zenml.io/kitaru/getting-started/import-your-traces)
today, and the recording API itself is two client calls if you want to
wire a framework directly.

### Drive it from your coding agent

Kitaru observes your production agents; your coding assistant is how you
talk to Kitaru. Every step is scriptable — a CLI with `--output json`
(`kitaru agent register`, `kitaru evaluator test`, `kitaru worker start`,
`kitaru job watch`) and a typed async Python client — so Claude Code,
Codex, or Cursor can triage a failing session, write the evaluator, run
the experiment, and report the diff while you review.

### Self-hosted, by design

One FastAPI + Postgres server on your infrastructure — and no code
executes on it. Replays, imports, and evaluations run on **workers** in
your own environment: your virtualenv, your credentials, your network.
Traces don't leave your systems. Apache 2.0, no mandatory SaaS control
plane.

## 🌱 Where ZenML fits

Kitaru is built by the team behind [ZenML](https://zenml.io) and is a
ZenML sub-brand. The split is clean: **ZenML runs agents durably; Kitaru
replays and improves them.** Durable execution, checkpointed pipelines,
and orchestration live in ZenML. Kitaru assumes your agent already runs
somewhere — its job is what the recordings can teach you.

## 📚 Learn more

| Resource | Description |
|---|---|
| [Documentation](https://docs.zenml.io/kitaru) | Concepts, guides, and the quickstart |
| [Import your traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces) | Start from the Langfuse history you already have |
| [Build a regression suite](https://docs.zenml.io/kitaru/guides/regression-suite) | Production traffic as your test suite, gated in CI |
| [Run the Server](https://docs.zenml.io/kitaru/deploy) | Self-host for your team |

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for
development setup, code style, and how to submit changes. The default
branch is `develop` — all PRs should target it.

## 💬 Community and support

- [Community](https://www.zenml.io/slack) — ask questions, share ideas
- [Issues](https://github.com/zenml-io/kitaru/issues) — report bugs, request features
- [Roadmap](https://www.zenml.io/roadmap) — see what's coming next

## 📄 License

[Apache 2.0](LICENSE)
