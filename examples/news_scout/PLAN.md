# News Scout v2 — Agentic Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite the news scout from a linear workflow into a PydanticAI agent with 4 tools, durable memory, and provider-agnostic model selection.

**Architecture:** A PydanticAI `Agent` wrapped via `kp.wrap()` runs inside a single `@checkpoint(type="llm_call")`. The flow body reads/writes memory around the agent. The agent has 4 tools (`search_news`, `search_twitter`, `investigate`, `fetch_url`) and autonomously decides what to search, when to dig deeper, and when to stop.

**Tech Stack:** Kitaru (flow, checkpoint, memory), PydanticAI 1.80+, Anthropic Claude Sonnet 4.6 (swappable via PydanticAI model strings), stdlib urllib for HTTP.

---

## File Structure

```
examples/news_scout/
  scout.py              # @flow + agent setup + CLI entrypoint
  models.py             # Article, JudgedItem, ScoutContext, ScoutReport
  tools/
    __init__.py          # re-exports: search_news, search_twitter, investigate, fetch_url
    sources.py           # search_news(), search_twitter()
    web.py               # investigate(), fetch_url()
  prompts.py             # SYSTEM_PROMPT, build_user_prompt()
  utils/
    __init__.py          # re-exports: load_dotenv, http_get_json, http_get_text
    http.py              # http_get_json(), http_get_text()
    dotenv.py            # load_dotenv()
  README.md              # updated for v2
  SPEC.md                # design spec
```

**What gets deleted:** The entire current `scout.py` (591 lines). Replaced by the above structure.

---

### Task 1: Create models.py

**Files:**
- Create: `examples/news_scout/models.py`

- [ ] **Step 1: Write models.py with all data models**

```python
"""Data models shared across the news scout agent."""

import hashlib

from pydantic import BaseModel, Field


class Article(BaseModel):
    """One candidate news item from any source."""

    title: str
    url: str
    summary: str = ""
    source: str
    fingerprint: str = ""

    def compute_fingerprint(self) -> str:
        raw = f"{self.url.strip().lower()}|{self.title.strip().lower()}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


class JudgedItem(BaseModel):
    """An article the agent has scored."""

    article: Article
    score: float = Field(ge=0.0, le=10.0)
    verdict: str
    reason: str


class ScoutContext(BaseModel):
    """Runtime context passed into the agent checkpoint."""

    interests: list[str]
    seen_fingerprints: list[str]


class ScoutReport(BaseModel):
    """Structured output from the agent."""

    items: list[JudgedItem]
    summary: str
```

- [ ] **Step 2: Verify it imports**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from models import Article, ScoutContext, ScoutReport; print('ok')"`

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add examples/news_scout/models.py
git commit -m "Add data models for news scout v2"
```

---

### Task 2: Create utils/

**Files:**
- Create: `examples/news_scout/utils/__init__.py`
- Create: `examples/news_scout/utils/http.py`
- Create: `examples/news_scout/utils/dotenv.py`

- [ ] **Step 1: Create utils/dotenv.py**

```python
"""Minimal .env loader — no python-dotenv dependency."""

import os
from pathlib import Path


def load_dotenv() -> None:
    """Load KEY=VALUE pairs from ``.env`` alongside the caller's file."""
    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
```

- [ ] **Step 2: Create utils/http.py**

```python
"""HTTP helpers using stdlib urllib."""

import json
import urllib.request


def http_get_json(url: str, timeout: float = 15.0) -> dict:
    """GET a URL and parse the response as JSON."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "kitaru-news-scout/0.2"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get_text(url: str, timeout: float = 15.0) -> str:
    """GET a URL and return the response body as text."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "kitaru-news-scout/0.2"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")
```

- [ ] **Step 3: Create utils/__init__.py**

```python
"""News scout utility re-exports."""

from utils.dotenv import load_dotenv
from utils.http import http_get_json, http_get_text

__all__ = ["load_dotenv", "http_get_json", "http_get_text"]
```

- [ ] **Step 4: Verify imports**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from utils import load_dotenv, http_get_json; print('ok')"`

Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add examples/news_scout/utils/
git commit -m "Add utils package (dotenv, http helpers)"
```

---

### Task 3: Create tools/sources.py

**Files:**
- Create: `examples/news_scout/tools/sources.py`

- [ ] **Step 1: Write tools/sources.py with search_news and search_twitter**

```python
"""Source-searching tools for the news scout agent."""

import json
import os
import urllib.parse
import xml.etree.ElementTree as ET

from models import Article
from utils.http import http_get_json, http_get_text

HN_ENDPOINT = "http://hn.algolia.com/api/v1/search"
GOOGLE_NEWS_ENDPOINT = "https://news.google.com/rss/search"
GROK_MODEL = "grok-4-latest"
GROK_BASE_URL = "https://api.x.ai/v1"


def search_news(query: str) -> list[Article]:
    """Search Hacker News and Google News for a query.

    Returns a merged, deduped list of articles with fingerprints.
    """
    articles: list[Article] = []
    seen_urls: set[str] = set()

    # --- Hacker News ---
    try:
        hn_url = f"{HN_ENDPOINT}?query={urllib.parse.quote(query)}&hitsPerPage=15"
        payload = http_get_json(hn_url)
        for hit in payload.get("hits", []):
            title = hit.get("title") or hit.get("story_title") or ""
            url = hit.get("url") or (
                f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
                if hit.get("objectID")
                else ""
            )
            if not title or not url or url in seen_urls:
                continue
            seen_urls.add(url)
            article = Article(title=title, url=url, source="hn")
            article.fingerprint = article.compute_fingerprint()
            articles.append(article)
    except Exception:
        pass  # HN down — continue with Google News

    # --- Google News RSS ---
    try:
        gn_query = urllib.parse.urlencode(
            {"q": f"{query} when:1d", "hl": "en-US", "gl": "US", "ceid": "US:en"}
        )
        gn_url = f"{GOOGLE_NEWS_ENDPOINT}?{gn_query}"
        raw = http_get_text(gn_url)
        root = ET.fromstring(raw)
        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            if title_el is None or link_el is None:
                continue
            title = (title_el.text or "").strip()
            link = (link_el.text or "").strip()
            if not title or not link or link in seen_urls:
                continue
            seen_urls.add(link)
            article = Article(
                title=title, url=link, summary=f"via Google News for '{query}'",
                source="gnews",
            )
            article.fingerprint = article.compute_fingerprint()
            articles.append(article)
    except Exception:
        pass  # Google News down — return what we have

    return articles


def search_twitter(query: str) -> list[Article]:
    """Ask Grok what X/Twitter is saying about a query.

    Returns articles. Returns [] with no error if XAI_API_KEY is missing.
    """
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        return []

    try:
        from openai import OpenAI
    except ImportError:
        return []

    client = OpenAI(api_key=api_key, base_url=GROK_BASE_URL)
    user_prompt = (
        f"What are the most notable X/Twitter discussions about '{query}' "
        f"in the last 60 minutes? Return strict JSON: "
        '{"items": [{"title": str, "summary": str, "url": str}]}'
    )

    try:
        response = client.chat.completions.create(
            model=GROK_MODEL,
            messages=[
                {"role": "system", "content": "Return factual JSON only."},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    except Exception:
        return []

    content = response.choices[0].message.content or "{}"
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        return []

    articles: list[Article] = []
    for raw_item in payload.get("items", [])[:8]:
        title = (raw_item.get("title") or "").strip()
        url = (raw_item.get("url") or "").strip()
        summary = (raw_item.get("summary") or "").strip()
        if not title or not url:
            continue
        article = Article(
            title=title, url=url, summary=summary, source="grok:x"
        )
        article.fingerprint = article.compute_fingerprint()
        articles.append(article)
    return articles
```

- [ ] **Step 2: Verify it imports**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from tools.sources import search_news, search_twitter; print('ok')"`

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add examples/news_scout/tools/sources.py
git commit -m "Add search_news and search_twitter tools"
```

---

### Task 4: Create tools/web.py

**Files:**
- Create: `examples/news_scout/tools/web.py`

- [ ] **Step 1: Write tools/web.py with investigate and fetch_url**

```python
"""Web-fetching tools for the news scout agent."""

import re

from utils.http import http_get_text


def investigate(url: str) -> str:
    """Fetch a URL and return a plain-text summary of the content.

    Strips HTML tags and returns the first ~2000 characters. Use this
    when a headline looks promising and you want to read the article.
    """
    try:
        raw = http_get_text(url, timeout=10.0)
    except Exception as exc:
        return f"Failed to fetch {url}: {exc}"

    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", raw)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:2000]


def fetch_url(url: str) -> str:
    """Raw HTTP GET. Returns the response body as text (capped at 5000 chars).

    Use this when you want to check something specific that isn't a news
    article — an API response, a social media page, etc.
    """
    try:
        raw = http_get_text(url, timeout=10.0)
    except Exception as exc:
        return f"Failed to fetch {url}: {exc}"
    return raw[:5000]
```

- [ ] **Step 2: Verify it imports**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from tools.web import investigate, fetch_url; print('ok')"`

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add examples/news_scout/tools/web.py
git commit -m "Add investigate and fetch_url tools"
```

---

### Task 5: Create tools/__init__.py

**Files:**
- Create: `examples/news_scout/tools/__init__.py`

- [ ] **Step 1: Write tools/__init__.py re-exporting all tools**

```python
"""News scout agent tools — re-exports for clean imports."""

from tools.sources import search_news, search_twitter
from tools.web import fetch_url, investigate

__all__ = ["search_news", "search_twitter", "investigate", "fetch_url"]
```

- [ ] **Step 2: Verify the re-exports work**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from tools import search_news, search_twitter, investigate, fetch_url; print('ok')"`

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add examples/news_scout/tools/__init__.py
git commit -m "Add tools package init with re-exports"
```

---

### Task 6: Create prompts.py

**Files:**
- Create: `examples/news_scout/prompts.py`

- [ ] **Step 1: Write prompts.py with system prompt and user prompt builder**

```python
"""Prompts for the news scout agent."""


SYSTEM_PROMPT = """\
You are a news scout. Your job is to find genuinely interesting, high-signal \
news for the user based on their interest profile.

## How to work

1. You'll receive the user's interests and a list of already-seen article \
fingerprints.
2. Use your tools to search across sources. Try different queries — specific \
and broad. Search for each interest area.
3. When you find a promising headline, use `investigate` to read the full \
article before judging it.
4. Skip articles whose fingerprints appear in the already-seen list.

## Judgment criteria

- **Novelty**: Is this genuinely new, or a rehash of old news?
- **Consequence**: Does this matter? Will it affect the user's world?
- **Relevance**: How closely does it match the user's interests?
- **Source quality**: Is this a credible source or clickbait?

## Scoring

Score each article 0-10:
- 7-10 = "send_now" — worth interrupting the user
- 4-6 = "digest" — include in a summary
- 0-3 = "ignore" — not worth mentioning

## When to stop

Stop when you've:
- Searched across all the user's interest areas
- Investigated the most promising leads
- Found your top items (or confirmed nothing interesting is happening)

Do NOT loop endlessly. Be efficient — a typical sweep is 8-15 tool calls.\
"""


def build_user_prompt(interests: list[str], seen_fingerprints: list[str]) -> str:
    """Build the user message that kicks off the agent's sweep."""
    interests_str = ", ".join(interests)
    seen_count = len(seen_fingerprints)
    seen_sample = ", ".join(seen_fingerprints[:10])
    seen_note = (
        f"You have seen {seen_count} articles before. "
        f"Sample fingerprints: [{seen_sample}{'...' if seen_count > 10 else ''}]. "
        f"Skip any article with a fingerprint in this set."
        if seen_count > 0
        else "You have not seen any articles yet — everything is new."
    )

    return (
        f"Your interests: {interests_str}\n\n"
        f"{seen_note}\n\n"
        f"Run your sweep now."
    )
```

- [ ] **Step 2: Verify it imports**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -c "from prompts import SYSTEM_PROMPT, build_user_prompt; print(len(SYSTEM_PROMPT), 'chars'); print(build_user_prompt(['ai'], ['abc123'])[:100])"`

Expected: prints char count and prompt preview.

- [ ] **Step 3: Commit**

```bash
git add examples/news_scout/prompts.py
git commit -m "Add system prompt and user prompt builder"
```

---

### Task 7: Rewrite scout.py (flow + agent + CLI)

**Files:**
- Rewrite: `examples/news_scout/scout.py`

- [ ] **Step 1: Replace scout.py with the v2 agent-based flow**

```python
"""News scout v2 — an agentic news monitor powered by PydanticAI + Kitaru.

A PydanticAI agent with 4 tools autonomously searches news sources, investigates
articles, and judges what is worth surfacing. Kitaru handles durable memory
(interests, seen fingerprints) and replay.

Usage::

    python scout.py --seed-profile       # one-time: seed the user profile
    python scout.py                       # run one agentic sweep
    python scout.py --interests ai,llms   # override interests for this run
"""

import argparse
import os
import sys
from typing import Annotated

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

import kitaru
from kitaru import checkpoint, flow, memory
from kitaru.adapters import pydantic_ai as kp

from models import ScoutContext, ScoutReport
from prompts import SYSTEM_PROMPT, build_user_prompt
from tools import fetch_url, investigate, search_news, search_twitter
from utils import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAMESPACE = "news_scout"
MODEL = os.environ.get("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")
MAX_TOOL_CALLS = 30
SEEN_FINGERPRINT_WINDOW = 500

DEFAULT_INTERESTS: list[str] = [
    "artificial intelligence",
    "startups",
    "open source",
    "developer tools",
]

# ---------------------------------------------------------------------------
# Agent — wrapped once at module scope
# ---------------------------------------------------------------------------

scout_agent = kp.wrap(
    Agent(
        MODEL,
        tools=[search_news, search_twitter, investigate, fetch_url],
        system_prompt=SYSTEM_PROMPT,
        output_type=ScoutReport,
    ),
    tool_capture_config={"mode": "full"},
)

# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint
def resolve_context(
    interests_raw: list[str] | None,
    seen_raw: list[str] | None,
    override: list[str] | None,
) -> Annotated[ScoutContext, "scout_context"]:
    """Normalize memory artifact refs into a concrete ScoutContext."""
    interests = override or interests_raw or DEFAULT_INTERESTS
    seen = list(seen_raw) if seen_raw else []
    kitaru.log(
        event="resolve_context",
        interests_count=len(interests),
        seen_count=len(seen),
    )
    return ScoutContext(interests=list(interests), seen_fingerprints=seen)


@checkpoint(type="llm_call")
def run_scout(context: ScoutContext) -> Annotated[ScoutReport, "scout_report"]:
    """Run the PydanticAI agent. This is the main replay boundary."""
    user_prompt = build_user_prompt(context.interests, context.seen_fingerprints)
    result = scout_agent.run_sync(
        user_prompt,
        usage_limits=UsageLimits(request_limit=MAX_TOOL_CALLS),
    )
    report = result.output

    # Print report to console
    print()
    print("=" * 72)
    print(f"News scout — {len(report.items)} items found")
    print("=" * 72)
    if not report.items:
        print("(nothing interesting surfaced this run)")
    else:
        for idx, item in enumerate(report.items, start=1):
            print(f"\n{idx}. [{item.verdict} {item.score:.1f}] {item.article.title}")
            print(f"   source: {item.article.source}")
            print(f"   why:    {item.reason}")
            print(f"   link:   {item.article.url}")
    print()
    print(f"Summary: {report.summary}")
    print()

    return report


@checkpoint
def update_seen(
    context: ScoutContext,
    report: ScoutReport,
) -> Annotated[list[str], "seen_fingerprints_out"]:
    """Extend the seen-fingerprint set with articles from the report."""
    new_fps = [item.article.fingerprint for item in report.items]
    updated = (context.seen_fingerprints + new_fps)[-SEEN_FINGERPRINT_WINDOW:]
    kitaru.log(event="update_seen", added=len(new_fps), total=len(updated))
    return updated


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow
def news_scout(interests_override: list[str] | None = None) -> None:
    """Agentic news scout with durable memory."""
    # --- Memory reads ---
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    interests_raw = memory.get("interests")
    memory.configure(scope_type="flow")
    seen_raw = memory.get("seen_fingerprints")

    # --- Resolve context ---
    context = resolve_context(
        interests_raw=interests_raw,
        seen_raw=seen_raw,
        override=interests_override,
    )

    # --- Agent runs ---
    report = run_scout(context=context)

    # --- Memory write ---
    updated = update_seen(context=context, report=report)
    memory.set("seen_fingerprints", updated)


# ---------------------------------------------------------------------------
# Profile seeding (outside flow)
# ---------------------------------------------------------------------------


def seed_profile(interests: list[str]) -> None:
    """Write interests into namespace memory."""
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    memory.set("interests", interests)
    print(f"Seeded {len(interests)} interests into namespace '{NAMESPACE}':")
    for interest in interests:
        print(f"  - {interest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Kitaru agentic news scout.")
    parser.add_argument(
        "--seed-profile",
        action="store_true",
        help="Write default interests into namespace memory and exit.",
    )
    parser.add_argument(
        "--interests",
        type=str,
        default=None,
        help="Comma-separated interests to override for this run.",
    )
    args = parser.parse_args(argv)

    override = (
        [p.strip() for p in args.interests.split(",") if p.strip()]
        if args.interests
        else None
    )

    if args.seed_profile:
        seed_profile(override or DEFAULT_INTERESTS)
        return 0

    news_scout.run(interests_override=override)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify syntax**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python -m py_compile scout.py && echo "ok"`

Expected: `ok`

- [ ] **Step 3: Verify --help works**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python scout.py --help`

Expected: Shows usage with `--seed-profile` and `--interests`.

- [ ] **Step 4: Commit**

```bash
git add examples/news_scout/scout.py
git commit -m "Rewrite scout.py as PydanticAI agent with tools"
```

---

### Task 8: Update README.md

**Files:**
- Modify: `examples/news_scout/README.md`

- [ ] **Step 1: Replace README.md with v2 content**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add examples/news_scout/README.md
git commit -m "Update README for v2 agentic redesign"
```

---

### Task 9: End-to-end test run

**Files:** None — verification only.

- [ ] **Step 1: Seed profile**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python scout.py --seed-profile`

Expected: prints 4 seeded interests.

- [ ] **Step 2: Run first sweep**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python scout.py`

Expected: Agent makes multiple tool calls, prints a report with scored items. Flow completes.

- [ ] **Step 3: Run second sweep (dedup test)**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/python scout.py`

Expected: Agent finds fewer new items (seen-fingerprints filter kicks in). Flow completes.

- [ ] **Step 4: Verify memory state**

Run: `/Users/htahir1/Workspace/kitaru/.venv/bin/kitaru memory list --scope-type=flow --scope=news_scout`

Expected: Shows `seen_fingerprints` key with version > 1.

- [ ] **Step 5: Test model override**

Run: `KITARU_SCOUT_MODEL=anthropic:claude-sonnet-4-6 /Users/htahir1/Workspace/kitaru/.venv/bin/python scout.py --interests "quantum computing"`

Expected: Runs with specified model and interests.

- [ ] **Step 6: Final commit with all files**

```bash
git add examples/news_scout/
git commit -m "Complete news scout v2 agentic redesign

Rewrites the news scout from a linear workflow into a PydanticAI agent
with 4 tools. The agent autonomously decides what to search, when to
investigate, and when to stop. Kitaru memory handles cross-run state.

Provider-agnostic via PydanticAI model strings."
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Every section of SPEC.md has a corresponding task. Architecture, tools, prompts, flow body, memory, CLI, file layout, dependencies — all covered.
- [x] **Placeholder scan:** No TBDs, no "implement later", no "similar to Task N". All code blocks are complete.
- [x] **Type consistency:** `ScoutContext`, `ScoutReport`, `Article`, `JudgedItem` used consistently across all tasks. `search_news` returns `list[Article]`, `investigate` returns `str` — matches spec.
