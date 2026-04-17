# News Scout

An agentic news monitor built on Kitaru's durable execution layer and PydanticAI.
A single agent with 4 tools autonomously searches news sources, investigates
articles, and decides what's worth surfacing. Every tool call is its own
durable Kitaru checkpoint.

## Quick start

```bash
cd examples/news_scout
kitaru init
```

Create a `.env` file with your API keys:

```
ANTHROPIC_API_KEY=sk-ant-...
XAI_API_KEY=xai-...            # optional — enables the search_twitter tool
```

Install dependencies and run:

```bash
uv sync --extra local --extra pydantic-ai --extra llm
python scout.py --seed-profile   # one-time: seed interests into memory
python scout.py                   # run one agentic sweep
```

## How it works

The agent has 4 tools:

| Tool | What it does |
|---|---|
| `search_news(query)` | Searches Hacker News + Google News |
| `search_twitter(query)` | Asks Grok what X/Twitter is saying |
| `investigate(url)` | Fetches and summarizes an article |
| `fetch_url(url)` | Raw HTTP GET for anything else |

The agent is wrapped with `KitaruAgent(..., granular_checkpoints=True)` which
means **each tool call, each model request, and each MCP call becomes its own
Kitaru checkpoint** — individually cached, individually replayable, and shown
as separate steps in the Kitaru dashboard.

```
@flow news_scout
  └── scout_agent.run_sync(prompt)        # runs at flow scope
        ├── @checkpoint: model_request_1
        ├── @checkpoint: tool_call search_news
        ├── @checkpoint: model_request_2
        ├── @checkpoint: tool_call investigate
        ├── @checkpoint: model_request_3
        └── ...
```

## Why the agent is at flow scope (not inside a checkpoint)

Granular mode cannot coexist with an enclosing `@checkpoint` — the adapter
runs inline inside a parent checkpoint instead. So the agent call lives
directly in the flow body, which means its inputs must be concrete Python
values, not ZenML artifact refs.

That's why this example reads memory **detached** (outside the flow) in
`main()` and passes `interests` + `seen_fingerprints` into the flow as
arguments. The agent sees real lists, not DAG placeholders.

## Memory layout

Both keys live in the `news_scout` namespace scope:

- `interests` — topics the user cares about (read on every run)
- `seen_fingerprints` — articles already surfaced (passed to the agent as
  prompt context so it can skip them)

```bash
kitaru memory scopes
kitaru memory get --scope-type=namespace --scope=news_scout interests
kitaru memory get --scope-type=namespace --scope=news_scout seen_fingerprints
```

The seen-set is currently read-only from the flow's perspective (the agent
returns free text, so we don't parse new fingerprints out). To update it,
use the CLI or write a follow-up flow that parses the agent's output.

## Switching models

Default model is `anthropic:claude-sonnet-4-6`. Override via env var:

```bash
KITARU_SCOUT_MODEL=openai:gpt-4o python scout.py
KITARU_SCOUT_MODEL=gemini:gemini-2.5-flash python scout.py
KITARU_SCOUT_MODEL=ollama:llama3.3 python scout.py
```

PydanticAI's model strings — anything it supports works here.

## Replay and retry

Because each tool call is a checkpoint, you can:

```bash
kitaru executions list
kitaru executions replay <exec_id> --from <checkpoint_name>
kitaru executions retry <exec_id>
```

The tool-level `retries` config on the agent (2 for model, 1 for tools) also
auto-retries transient failures inline.

## CLI flags

```
--seed-profile          Write default interests into namespace memory and exit
--interests TOPICS      Comma-separated interests to override for this run
```

## File layout

```
scout.py        — @flow + agent construction + CLI
models.py       — Article, JudgedItem (used by tools)
tools/          — search_news, search_twitter, investigate, fetch_url
prompts.py      — system prompt + user prompt builder
utils/          — dotenv loader, HTTP helpers
```

## Next steps (not implemented)

- Parse fingerprints from the agent's output to update seen-set automatically
- Add Discord/email delivery (`send_alert` as a fifth tool)
- Schedule via Kubernetes cron (run every N minutes)
- Add `kitaru.wait()` for human approval before surfacing certain items
- Add a preference-learning loop (thumbs up/down feeds back into the prompt)
