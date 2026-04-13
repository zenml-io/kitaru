# News Scout v2 — Agentic Redesign Spec

**Date:** 2026-04-14
**Target branch:** `feature/memory-base`
**Location:** `examples/news_scout/`
**Replaces:** v1 workflow-based spec (2026-04-11)

## Summary

Redesign the news scout from a fixed linear workflow into a PydanticAI agent
with tools. The agent autonomously decides what to search, when to dig deeper,
and when to stop. The flow body handles memory (interests, seen-fingerprints)
around the agent — the agent never writes its own state.

The example demonstrates: Kitaru `memory` + PydanticAI adapter + provider-agnostic
tool calling + durable replay. It should feel like an agent, not a cron job.

## Architecture

```
@flow news_scout
  ├── memory.get("interests")              # flow body — read namespace
  ├── memory.get("seen_fingerprints")      # flow body — read flow scope
  ├── resolve_context(interests, seen)     # @checkpoint — normalize refs
  │
  ├── run_scout(context)                   # @checkpoint(type="llm_call")
  │     └── PydanticAI Agent loop          #   agent decides what to do
  │           ├── search_news(query)       #   tool
  │           ├── search_twitter(query)    #   tool
  │           ├── investigate(url)         #   tool
  │           └── fetch_url(url)           #   tool
  │
  ├── update_seen(seen, report)            # @checkpoint — extend fingerprints
  └── memory.set("seen_fingerprints", ...) # flow body — write flow scope
```

**Key principle:** The agent is the brain (search, reason, judge). The flow is
the infrastructure (memory, dedup, state). The agent cannot write memory.

## Model

```python
MODEL = os.environ.get("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")
```

PydanticAI model string format — supports `anthropic:`, `openai:`, `gemini:`,
`ollama:`, etc. natively. Provider-agnostic, no lock-in.

## Agent setup

```python
from pydantic_ai import Agent
from kitaru.adapters import pydantic_ai as kp

scout_agent = kp.wrap(
    Agent(
        MODEL,
        tools=[search_news, search_twitter, investigate, fetch_url],
        system_prompt=SYSTEM_PROMPT,
    ),
    tool_capture_config={"mode": "full"},
)
```

The agent is wrapped once at module scope. Called inside `@checkpoint(type="llm_call")`
per the kitaru-authoring skill's safe default pattern.

## Tools (4 total)

### `search_news(query: str) -> list[Article]`

Searches HN Algolia + Google News RSS for the query. Returns a merged, deduped
list of `Article` models with fingerprints computed. This is the primary
discovery tool.

Implementation: reuses the existing HN Algolia + Google News RSS logic from v1,
combined into one tool the agent can call with any query string.

### `search_twitter(query: str) -> list[Article]`

Asks Grok (via xAI's OpenAI-compatible endpoint) what X/Twitter is saying about
the query. Returns `Article` models. Gracefully returns `[]` with a message if
`XAI_API_KEY` is missing.

Implementation: reuses the existing Grok logic from v1.

### `investigate(url: str) -> str`

Fetches a URL and returns a text summary of the content. For when the agent
sees a promising headline and wants to read the full article before judging.
Uses stdlib `urllib` + basic HTML-to-text extraction (strip tags). Returns
first ~2000 chars of text content.

### `fetch_url(url: str) -> str`

Raw HTTP GET. Returns the response body as text (capped at 5000 chars). For
when the agent wants to check something specific that isn't a news article.

## Tool return shapes

All tools return either `list[Article]` or `str`. The `Article` model:

```python
class Article(BaseModel):
    title: str
    url: str
    summary: str = ""
    source: str
    fingerprint: str = ""
```

## Agent prompt

The system prompt tells the agent:
- You are a news scout. Your job is to find genuinely interesting news.
- You'll be given a user interest profile and a list of already-seen fingerprints.
- Use your tools to search across sources. Try different queries.
- Investigate articles that look promising before judging them.
- Reward novelty, consequence, and direct relevance. Penalize clickbait.
- When you've found enough (or exhausted sources), return a structured report.
- You MUST stop after finding your top items — do not loop forever.

The user message injects the runtime context:

```
Your interests: {interests}
Already seen (fingerprints): {seen_fingerprints_summary}
Run your sweep now.
```

## Agent output

The agent returns a structured string (or structured output via PydanticAI's
`result_type`) with the report. For v2, a plain text report is fine — the
flow body parses fingerprints from the articles the agent found.

If PydanticAI's `result_type` works cleanly with the Kitaru adapter, use:

```python
class ScoutReport(BaseModel):
    items: list[JudgedItem]
    summary: str
```

Verify at implementation — fall back to string output if `result_type` causes
serialization issues with the adapter.

## Exit condition

**Hybrid:** The agent decides when it's done (satisfaction-based), with a hard
cap at 30 tool calls. PydanticAI supports `usage_limits` for this:

```python
from pydantic_ai.usage import UsageLimits

result = scout_agent.run_sync(
    user_prompt,
    usage_limits=UsageLimits(request_limit=30),
)
```

If the agent hits the cap, it returns whatever it has. The system prompt tells
it to wrap up proactively rather than hitting the wall.

## Flow body

```python
@flow
def news_scout() -> None:
    # --- Memory reads ---
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    interests_raw = memory.get("interests")
    memory.configure(scope_type="flow")
    seen_raw = memory.get("seen_fingerprints")

    # --- Normalize memory refs via checkpoint ---
    context = resolve_context(interests_raw, seen_raw)

    # --- Agent runs ---
    report = run_scout(context)

    # --- Update seen fingerprints ---
    updated = update_seen(context, report)
    memory.set("seen_fingerprints", updated)
```

Three checkpoints total: `resolve_context`, `run_scout`, `update_seen`.
The agent checkpoint (`run_scout`) is the replay boundary — replaying
re-runs the entire agent session.

## Checkpoints

### `resolve_context(interests_raw, seen_raw) -> ScoutContext`

Normalizes the memory artifact refs into concrete values. Returns a
`ScoutContext` Pydantic model with `interests: list[str]` and
`seen_fingerprints: list[str]`. Also logs the starting state.

### `run_scout(context: ScoutContext) -> ScoutReport`

The main agent checkpoint. Builds the user prompt from context, runs the
PydanticAI agent with tool calling, returns the report. Tagged
`type="llm_call"` for the Kitaru dashboard.

### `update_seen(context: ScoutContext, report: ScoutReport) -> list[str]`

Extracts fingerprints from the report's articles, appends to the existing
seen set, caps at `SEEN_FINGERPRINT_WINDOW` (500). Returns the updated list.

## File layout

```
examples/news_scout/
  scout.py              # @flow + CLI entrypoint (~80 lines)
  models.py             # Article, JudgedItem, ScoutContext, ScoutReport
  tools/
    __init__.py          # re-exports all tools
    sources.py           # search_news, search_twitter
    web.py               # investigate, fetch_url
  prompts.py             # SYSTEM_PROMPT constant
  utils/
    __init__.py
    http.py              # _http_get_json, _http_get_text
    dotenv.py            # _load_dotenv
  README.md
  SPEC.md                # this file
  .env                   # user's API keys (gitignored)
```

## Dependencies

- `pydantic-ai` — installed via `kitaru[pydantic-ai]`
- `openai` — installed via `kitaru[llm]` (for Grok xAI calls)
- `pydantic` — already a kitaru dep
- No new deps beyond what kitaru extras provide.

Install: `uv sync --extra local --extra pydantic-ai --extra llm`

## CLI surface

```bash
python scout.py --seed-profile          # seed interests into namespace memory
python scout.py                          # run one agentic sweep
python scout.py --interests "ai,robots"  # override interests for this run
```

Same as v1. No changes to CLI flags.

## What changes from v1

| Aspect | v1 (workflow) | v2 (agent) |
|---|---|---|
| Control flow | Fixed: fetch → filter → judge → report | Agent decides what to search and when to stop |
| Tool calling | None — linear checkpoints | PydanticAI native tool calling |
| Provider lock-in | `kitaru.llm()` (openai/anthropic/ollama/openrouter) | PydanticAI (anthropic/openai/gemini/ollama/etc.) |
| Checkpoints | 10 | 3 (resolve_context, run_scout, update_seen) |
| Memory management | Flow body | Flow body (unchanged) |
| Agent autonomy | None | Searches, investigates, reasons, judges |
| File count | 1 (scout.py) | 8 files across 3 dirs |

## What stays the same

- Memory scopes: namespace `news_scout` for interests, flow scope for seen fingerprints
- `.env` loading for API keys
- `--seed-profile` and `--interests` CLI flags
- Console output of the report
- Graceful Grok skip when `XAI_API_KEY` missing

## Risks

1. **PydanticAI `result_type` + Kitaru adapter serialization.** If structured
   output causes issues with the adapter's checkpoint tracking, fall back to
   string output and parse the report manually.
2. **Agent loops too long.** The 30-call cap mitigates this. System prompt
   should also tell the agent to be efficient.
3. **`from __future__ import annotations` conflict.** v1 hit this — ZenML
   can't resolve string annotations for step outputs. Do not use it.
4. **Memory artifact refs.** Same pattern as v1 — `memory.get()` returns
   DAG refs inside a flow, so `resolve_context` checkpoint materializes them.
