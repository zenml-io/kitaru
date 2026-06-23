# News Scout

An agent that reads the internet so you don't have to. Give it your interests,
let it loose, and it comes back with a scored briefing of what actually matters.

Built on Kitaru + PydanticAI. Every tool call is its own durable checkpoint —
you can see what the agent did, replay any step, and retry what failed. The
agent's final report lands in the dashboard as a first-class `final_report`
artifact you can read without scrolling through every tool call.

## What happens when you run it

The agent starts from either the interests you pass with `--interests` or
the built-in default list. It searches news and social sources for each
interest, picks the headlines
that look promising, pulls up the full articles to actually read them, and
scores each on novelty, consequence, and relevance. It stops once it's
covered every interest area (or hits the 50-request cap). Output is a
scored briefing — top picks worth surfacing now, a digest of secondary
items, and the rest discarded.

Every search, article fetch, and scoring decision becomes its own Kitaru
checkpoint, so the dashboard shows the full trail of what the agent did
and why. If a step fails (site down, rate limit, model hiccup), you can
replay from exactly that step without re-paying for everything before it.

## What running it looks like

```
$ python scout.py --interests "quantum computing,fusion"

Kitaru: Starting flow `news_scout`.
Kitaru: Checkpoint `news_scout_model_request` started.
Kitaru: Checkpoint `search_news_tool` started.
Kitaru: Checkpoint `search_news_tool_2` started.     ← runs in parallel
Kitaru: Checkpoint `search_news_tool` finished in 2.5s.
...
Kitaru: Checkpoint `investigate_tool_3` finished in 2.8s.
...
Kitaru: Checkpoint `publish_report` finished in 0.1s.
Kitaru: Flow completed in 3m3s.

========================================================================
News scout report
========================================================================

## 🚀 SEND NOW — Top Picks

### 1. 🧲 ITER Fusion Project Crosses Milestone with World's Most Powerful Magnet
Score: 9/10 · Reuters · May 2025

The global ITER project hit a real engineering milestone — credible source,
not vapor. After years of delays and overruns, this one counts.

### 2. ⚛️ Microsoft's Majorana 1 Chip: Topological Qubit Breakthrough
Score: 8/10 · NYT / Nature · Feb 2025
...
```

## Quick start

```bash
cd examples/end_to_end/news_scout
kitaru init
uv sync --extra local --extra pydantic-ai --extra llm
```

For a local run, drop your provider API keys in `.env` — the example loads
them with `python-dotenv` before PydanticAI touches the environment:

```
ANTHROPIC_API_KEY=<your-anthropic-api-key>
XAI_API_KEY=xai-...        # optional, unlocks the search_twitter tool
```

(See [Running on Kubernetes](#running-on-kubernetes-or-any-remote-stack)
below for the secret-based setup when you move to a remote stack.)

Then:

```bash
# Normal usage: uses the built-in default interests and runs one full pass —
# search, investigate, score, and emit the final briefing to the terminal.
python scout.py

# Choose the topics for this run. This does not save anything for later;
# pass the list again next time if you want the same custom briefing.
python scout.py --interests "robotics,biotech"
```

Default model is `anthropic:claude-sonnet-4-6` (the latest Sonnet). Override
with `KITARU_SCOUT_MODEL=anthropic:claude-opus-4-7` for smarter-but-pricier,
`openai:gpt-4o`, or any PydanticAI model string. Grok model for
`search_twitter` is overridable via `KITARU_GROK_MODEL`.

## The agent's toolbox

| Tool | Does |
|---|---|
| `search_news` | Hacker News + Google News |
| `search_twitter` | Grok with live X access |
| `investigate` | Fetches and reads a URL |
| `fetch_url` | Raw HTTP — escape hatch |

The agent decides what to search, when to dig deeper, and when it has enough.
A hard cap of 50 model requests keeps runaway agents from emptying your wallet.

## Why every tool call is its own checkpoint

The agent is wrapped with `KitaruAgent(checkpoint_strategy="calls")`. That turns
each model request, each tool call, and each MCP invocation into its own Kitaru
checkpoint — individually cached, individually replayable, visible in the
dashboard.

```
@flow news_scout
  ├── new_scout_agent().run_sync(prompt)
  │     ├── model_request_1      ← checkpoint
  │     ├── search_news_tool     ← checkpoint
  │     ├── model_request_2      ← checkpoint
  │     ├── investigate_tool     ← checkpoint
  │     └── ...
  └── publish_report(...)         ← checkpoint (produces `final_report` artifact)
```

If `investigate_tool_5` fails because a site was down, you can replay from
exactly that point. Everything before it is cached. Nothing is re-paid.

## Reading the dashboard

`kitaru login` opens the local server at http://127.0.0.1:8383. After a run:

- The flow page shows the full trace: model requests, tool calls, timings.
- The `final_report` artifact on the flow holds the agent's text output.
- Tool arguments and results are saved as artifacts (via `tool_capture="full"`
  on the adapter's `CapturePolicy`).

## Running on Kubernetes (or any remote stack)

On a remote stack the pod has no access to your shell environment, and baking
keys into the image layer would leak them through registry metadata and
logs. The scout switches credential sources from your shell to a Kitaru
secret whenever the active stack is remote:

```bash
kitaru secrets set news-scout-keys \
  --ANTHROPIC_API_KEY=<your-anthropic-api-key> \
  --XAI_API_KEY=xai-...        # optional, unlocks the search_twitter tool

kitaru stack use my-k8s-stack
python scout.py
```

Under the hood, `scout.py` attaches `ImageSettings.secret_environment_from=["news-scout-keys"]`
to the run when the active stack is remote. Kitaru resolves the secret at
step dispatch time and exposes each key (`ANTHROPIC_API_KEY`, `XAI_API_KEY`)
in the pod's environment — values never enter image layers, logs, or the
frozen execution spec.

## Replay a failed run

```bash
kitaru executions list
kitaru executions replay <exec_id> --from search_news_tool_3
kitaru executions retry <exec_id>
```

## Layout

```
scout.py        — flow + agent + CLI
models.py       — Article + Source enum
tools/          — the four tools
prompts.py      — system prompt + user prompt
utils/          — dotenv loader, HTTP helpers
```

## What it doesn't do yet

- Track which articles have been surfaced before (no dedup across runs). To
  add this cleanly, have the agent emit structured JSON with fingerprints at
  the end of each run, then save those fingerprints in your own store.
- Send alerts anywhere — just prints to your terminal.
- Schedule itself — pair with a K8s CronJob or `just schedule`.
- Learn from thumbs up / down — no feedback loop yet.

Pull requests welcome.
