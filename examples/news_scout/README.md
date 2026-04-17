# News Scout

An agent that reads the internet so you don't have to. Give it your
interests, let it loose, and it comes back with a scored briefing of what
actually matters today.

Built on Kitaru + PydanticAI. Every tool call becomes its own durable
checkpoint — you can see what the agent did, replay any step, and retry
what failed.

## What it looks like running

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
Kitaru: Flow completed in 3m3s.

========================================================================
News scout report
========================================================================

## 🚀 SEND NOW — Top Picks

### 1. 🧲 ITER Fusion Project Crosses Milestone with World's Most Powerful Magnet
Score: 9/10 · Reuters · May 2025

The global ITER project hit a real engineering milestone — credible
source, not vapor. After years of delays and overruns, this one counts.

### 2. ⚛️ Microsoft's Majorana 1 Chip: Topological Qubit Breakthrough
Score: 8/10 · NYT / Nature · Feb 2025

Microsoft claims a new state of matter enabling inherently noise-resistant
qubits. A Nature paper backs the results, though some academic debate
remains. The most technically consequential quantum news of the year.

...
```

## Quick start

```bash
cd examples/news_scout
kitaru init
uv sync --extra local --extra pydantic-ai --extra llm
```

Drop your keys in `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...
XAI_API_KEY=xai-...        # optional, unlocks the search_twitter tool
```

Then:

```bash
python scout.py --seed-profile   # write default interests to memory (once)
python scout.py                   # sweep
python scout.py --interests "robotics,biotech"
```

Default model is `anthropic:claude-sonnet-4-6`. Override with
`KITARU_SCOUT_MODEL=openai:gpt-4o` or any PydanticAI model string.

## The agent's toolbox

| Tool | Does |
|---|---|
| `search_news` | Hacker News + Google News |
| `search_twitter` | Grok with live X access |
| `investigate` | Fetches and reads a URL |
| `fetch_url` | Raw HTTP — escape hatch |

The agent decides what to search, when to dig deeper, and when it has
enough. A hard cap of 50 model requests keeps runaway agents from
emptying your wallet.

## Why every tool call is its own checkpoint

We wrap the agent with `KitaruAgent(granular_checkpoints=True)`. That
turns each model request, each tool call, and each MCP invocation into
its own Kitaru checkpoint — individually cached, individually
replayable, individually visible in the dashboard.

```
@flow news_scout
  └── scout_agent.run_sync(prompt)
        ├── model_request_1      ← checkpoint
        ├── search_news_tool     ← checkpoint
        ├── model_request_2      ← checkpoint
        ├── investigate_tool     ← checkpoint
        └── ...
```

If `investigate_tool_5` fails because a site was down, you can replay
from exactly that point. Everything before it is cached. Nothing is
re-paid.

## Running on Kubernetes (or any remote stack)

The flow declares its own image:

```python
SCOUT_IMAGE = ImageSettings(
    requirements=["pydantic-ai>=1.80", "openai>=1.0"],
    environment=_collect_env(),   # picks up API keys from local env
)

@flow(image=SCOUT_IMAGE)
def news_scout(...): ...
```

`_collect_env()` reads the keys that matter (`ANTHROPIC_API_KEY`,
`OPENAI_API_KEY`, `XAI_API_KEY`, `KITARU_SCOUT_MODEL`) from your local
environment at module-load time and bakes them into the container image
when Kitaru builds it for the remote stack. Nothing secret leaves this
file that wasn't already in your shell.

To ship it:

```bash
kitaru stack use my-k8s-stack
python scout.py
```

> **Heads up:** baking keys into images is fine for a personal scout,
> not for shared infrastructure. For production, use `kitaru secrets`
> and model aliases with `--secret`.

## Memory

Two keys in the `news_scout` namespace:

- `interests` — what you care about
- `seen_fingerprints` — articles the scout has surfaced before

Poke at them directly:

```bash
kitaru memory get --scope-type=namespace --scope=news_scout interests
kitaru memory list --scope-type=namespace --scope=news_scout
```

Memory is read outside the flow (in `main()`) and passed in as
arguments. This is because granular-mode agents run at flow scope, and
flow-scope needs concrete values rather than DAG refs.

## Replay a failed run

```bash
kitaru executions list
kitaru executions replay <exec_id> --from search_news_tool_3
kitaru executions retry <exec_id>
```

## Layout

```
scout.py        — flow + agent + CLI
models.py       — Article, JudgedItem
tools/          — the four tools
prompts.py      — system prompt + user prompt
utils/          — dotenv loader, HTTP helpers
```

## What it doesn't do yet

- Parse fingerprints out of the agent's report to auto-update the
  seen-set (the agent returns prose, not JSON)
- Send alerts anywhere — just prints to your terminal
- Schedule itself — pair with a K8s CronJob or `just schedule`
- Learn from thumbs up / down — no feedback loop yet

Pull requests welcome.
