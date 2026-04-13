# News Scout

An agentic news monitor built with PydanticAI + Kitaru. The agent autonomously
searches news sources, investigates articles, and judges what is worth surfacing.
Kitaru handles durable memory so consecutive runs feel "always-on."

## Quick start

```bash
cd examples/news_scout
kitaru init
```

Create a `.env` file with your API keys:

```
ANTHROPIC_API_KEY=sk-ant-...
XAI_API_KEY=xai-...            # optional — enables search_twitter tool
```

Install dependencies and run:

```bash
uv sync --extra local --extra pydantic-ai --extra llm
python scout.py --seed-profile   # one-time: seed interests into memory
python scout.py                   # run one agentic sweep
python scout.py                   # second sweep — dedup kicks in via memory
```

## How it works

The agent has 4 tools:

| Tool | What it does |
|---|---|
| `search_news(query)` | Searches Hacker News + Google News |
| `search_twitter(query)` | Asks Grok what X/Twitter is saying |
| `investigate(url)` | Fetches and summarizes an article |
| `fetch_url(url)` | Raw HTTP GET for anything else |

The flow body handles memory:
1. Reads user interests from namespace memory
2. Reads seen-fingerprints from flow-scoped memory
3. Passes context to the agent (one `@checkpoint(type="llm_call")`)
4. Agent runs autonomously — searches, investigates, judges
5. Flow writes new fingerprints back to memory

## Switching models

The agent defaults to `anthropic:claude-sonnet-4-6`. Override via env var:

```bash
KITARU_SCOUT_MODEL=openai:gpt-4o python scout.py
KITARU_SCOUT_MODEL=gemini:gemini-2.5-flash python scout.py
KITARU_SCOUT_MODEL=ollama:llama3.3 python scout.py
```

## Inspecting memory

```bash
kitaru memory scopes
kitaru memory list --scope-type=namespace --scope=news_scout
kitaru memory list --scope-type=flow --scope=news_scout
```

## CLI flags

```
--seed-profile          Write default interests into namespace memory and exit
--interests TOPICS      Comma-separated interests to override for this run
```

## File layout

```
scout.py        — flow + agent + CLI
models.py       — Article, JudgedItem, ScoutContext, ScoutReport
tools/          — search_news, search_twitter, investigate, fetch_url
prompts.py      — system prompt + user prompt builder
utils/          — dotenv loader, HTTP helpers
```

## Next steps (not implemented)

- Add Discord/email delivery
- Schedule via Kubernetes cron
- Add `kitaru.wait()` for human-in-the-loop alert approval
- Add a feedback loop to learn from user reactions
