<p align="center">
  <a href="https://kitaru.ai">
    <img src="assets/kitaru_logo.png" alt="Kitaru" width="240">
  </a>
</p>

<h3 align="center">Traces you can run, not just read.</h3>

<p align="center">
  Kitaru (来る, "to arrive") is replay-based evals for AI agents. It turns your agent's production traces into a regression suite: record or import every run, replay it against your next change, and see what improved and what broke before you ship. Open source, self-hosted, Python and TypeScript. From the team behind <a href="https://zenml.io">ZenML</a>.
</p>

<p align="center">
  <a href="https://pypi.org/project/kitaru/"><img alt="PyPI" src="https://img.shields.io/pypi/v/kitaru?color=blue"></a>
  <a href="https://pypi.org/project/kitaru/"><img alt="Python" src="https://img.shields.io/pypi/pyversions/kitaru"></a>
  <a href="https://github.com/zenml-io/kitaru/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/zenml-io/kitaru"></a>
</p>

<p align="center">
  <a href="https://docs.zenml.io/kitaru">Docs</a> &middot;
  <a href="#-get-started">Get Started</a> &middot;
  <a href="https://www.zenml.io/roadmap">Roadmap</a> &middot;
  <a href="https://kitaru.ai/slack">Community</a>
</p>

---

## 🎯 Why

Your agent has already been tested thousands of times, in production. Every one of those runs is sitting in a trace store as a transcript you can read but not run. So you change a prompt, swap a model, refactor a tool, and find out whether it broke from an angry user.

Kitaru makes those traces runnable:

- **Every run is a recording.** Wrap your agent once, or import the traces you already collect from Langfuse, LangSmith, or Braintrust. Your trace store stays your system of record.
- **Replay is re-execution.** Your real code runs again, tool calls answered from the recording, so no card gets refunded twice. Unchanged, the replay reproduces the original; forked, the diff you see is your change.
- **Improvement is measured, not vibed.** Your coding assistant interviews you over the sessions that matter, your judgments calibrate evaluators, and experiments replay a frozen cohort against your change: what improved, what regressed, before it ships. The cohort that caught a failure becomes the regression gate that keeps it caught.

## ⚡ Get started

**1. Install and log in.** The local server (FastAPI + Postgres) provisions itself in Docker:

```bash
pip install "kitaru[cli,worker,mcp]" kitaru-pydantic-ai
kitaru login --local        # or: kitaru login <your-team-url>
```

**2. Make your coding assistant Kitaru-capable.** This is the intended way to drive it: skills teach it the method, the MCP server gives it bounded operations.

```bash
npx skills add zenml-io/kitaru-skills
```

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

**3. Get an agent with traces.** The fastest way to feel the loop is the template: a ready PydanticAI agent with checked-in Langfuse traces to investigate.

```bash
git clone https://github.com/zenml-io/kitaru-template
cd kitaru-template    # its README has the setup and import commands
```

Already have an agent in production? [Import its traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces) from Langfuse, LangSmith, or Braintrust, or [record with an adapter](https://docs.zenml.io/kitaru/adapters/record-in-production): one wrapper, no rewrite.

**4. Drive results.** Open the repo (the template or your own agent) in Claude Code, Codex, or Cursor and ask:

> Use `kitaru-investigation` to investigate this agent and help me test one meaningful improvement. Show me the recorded evidence before asking for a judgment, and ask before creating resources or starting paid replay.

The assistant sweeps the sessions with the built-in deterministic evaluators, interviews you over the ones that matter, drafts the evaluator, runs the experiment, and reports what improved and what regressed. You judge; it does the legwork. The [tutorial](https://docs.zenml.io/kitaru/guides/returns-agent) walks the same loop on the template, step by step.

## 🔌 Languages and frameworks

Python and TypeScript SDKs, one server. Adapters: **PydanticAI** (`kitaru-pydantic-ai`), **LangGraph** (`kitaru-langgraph`, also LangChain agents and Deep Agents), **OpenAI Agents SDK** (`kitaru-openai-agents`), **Mastra** (`@zenml-io/kitaru-mastra`), **Vercel AI SDK** (`@zenml-io/kitaru-vercel-ai`).

Anything else still works: [import your traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces), [write a one-page custom importer](https://docs.zenml.io/kitaru/import-your-traces/custom-importer) (an agent skill drafts it), or [build a small adapter](https://docs.zenml.io/kitaru/adapters/custom).

## 🔒 Self-hosted, by design

One server on your infrastructure (Docker image and Helm chart included), and no user code executes on it. Replays, imports, and evaluations run on workers in your environment: your virtualenv, your credentials, your network. Traces never have to leave your systems. Apache 2.0, no mandatory SaaS control plane.

## 📚 Learn more

| Resource | Description |
|---|---|
| [Documentation](https://docs.zenml.io/kitaru) | Concepts, guides, and the quickstart |
| [Quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) | From an agent in production to your first replay-backed decision |
| [Set up your coding agent](https://docs.zenml.io/kitaru/getting-started/setup) | The MCP server and skills, in one page |
| [Kitaru template](https://github.com/zenml-io/kitaru-template) | A ready agent and traces to try the method on |
| [Import your traces](https://docs.zenml.io/kitaru/getting-started/import-your-traces) | Langfuse, LangSmith, Braintrust, or any format |
| [Build a regression suite](https://docs.zenml.io/kitaru/guides/regression-suite) | Production traffic as your test suite, gated in CI |
| [Deploy Kitaru](https://docs.zenml.io/kitaru/getting-started/deploy) | Self-host for your team |

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, and how to submit changes. The default branch is `develop`; all PRs should target it.

## 💬 Community and support

- [Slack community](https://kitaru.ai/slack): ask questions, share ideas
- [Report a bug](https://kitaru.ai/help): goes straight to GitHub issues
- [support@kitaru.ai](mailto:support@kitaru.ai): when email is easier

Hit something broken? Any of the three reaches a human; an issue with a session ID attached gets fixed fastest.

## 📄 License

[Apache 2.0](LICENSE)
