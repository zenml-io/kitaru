"""News scout — a durable news-monitoring flow that demonstrates kitaru.memory.

Runs a sweep across a handful of free sources (Hacker News, Google News per
interest, and optionally Grok for the X/Twitter firehose), dedupes against a
rolling memory set, asks an LLM to judge what is worth surfacing, and prints
the shortlist.

The point of the example is memory: the scout *feels* always-on because each
run reads what past runs learned (user profile, seen fingerprints) and writes
the new fingerprints back. Nothing in this file is long-running — every
"always-on" property comes from the ``flow``-scoped memory keys persisted
across executions.

Usage::

    python scout.py --seed-profile       # one-time: seed the user profile
    python scout.py                       # run one sweep
    python scout.py --interests ai,llms   # override interests for this run

A ``.env`` file in this directory is loaded automatically so you can drop
``XAI_API_KEY=...`` in there to enable the Grok source. The Grok checkpoint is
skipped gracefully if the key is missing.
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field

import kitaru
from kitaru import checkpoint, flow, memory

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAMESPACE = "news_scout"
"""Namespace scope name. Seeded once with the user profile."""

DEFAULT_INTERESTS: list[str] = [
    "artificial intelligence",
    "startups",
    "open source",
    "developer tools",
]

HN_ENDPOINT = "http://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=30"
GOOGLE_NEWS_ENDPOINT = "https://news.google.com/rss/search"
JUDGE_MODEL = os.environ.get(
    "KITARU_JUDGE_MODEL", "anthropic/claude-sonnet-4-20250514"
)
GROK_MODEL = "grok-4-latest"
GROK_BASE_URL = "https://api.x.ai/v1"
GROK_MAX_ITEMS = 8

SEEN_FINGERPRINT_WINDOW = 500
"""Cap on the flow-scoped seen-fingerprint set so it stops growing."""

MAX_JUDGE_ITEMS = 25
"""Upper bound on items fed to the LLM judge in one call, to keep cost sane."""

TOP_N_REPORT = 8
"""How many items to show in the console report."""


# ---------------------------------------------------------------------------
# Data models — serializable Pydantic models cross checkpoint boundaries
# ---------------------------------------------------------------------------


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
    """An article that the judge has scored."""

    article: Article
    score: float = Field(ge=0.0, le=10.0)
    verdict: str
    reason: str


# ---------------------------------------------------------------------------
# .env loading — no python-dotenv dependency, just a tiny stdlib parser
# ---------------------------------------------------------------------------


def _load_dotenv() -> None:
    """Load KEY=VALUE pairs from ``.env`` alongside this file into os.environ."""
    dotenv_path = Path(__file__).resolve().parent / ".env"
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


# ---------------------------------------------------------------------------
# Collectors — each one is a pure checkpoint that returns a list[Article]
# ---------------------------------------------------------------------------


def _http_get_json(url: str, timeout: float = 15.0) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "kitaru-news-scout/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_text(url: str, timeout: float = 15.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "kitaru-news-scout/0.1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


@checkpoint
def fetch_hn_frontpage() -> Annotated[list[Article], "hn_items"]:
    """Pull the Hacker News front page via the Algolia search endpoint."""
    payload = _http_get_json(HN_ENDPOINT)
    articles: list[Article] = []
    for hit in payload.get("hits", []):
        title = hit.get("title") or hit.get("story_title") or ""
        url = hit.get("url") or (
            f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
            if hit.get("objectID")
            else ""
        )
        if not title or not url:
            continue
        article = Article(title=title, url=url, summary="", source="hn")
        article.fingerprint = article.compute_fingerprint()
        articles.append(article)
    kitaru.log(event="hn_fetch", count=len(articles))
    return articles


@checkpoint
def fetch_google_news(
    interests: list[str],
) -> Annotated[list[Article], "gnews_items"]:
    """Hit Google News RSS once per interest and parse with stdlib ET."""
    seen_urls: set[str] = set()
    articles: list[Article] = []
    for interest in interests:
        query = urllib.parse.urlencode(
            {"q": f"{interest} when:1d", "hl": "en-US", "gl": "US", "ceid": "US:en"}
        )
        url = f"{GOOGLE_NEWS_ENDPOINT}?{query}"
        try:
            raw = _http_get_text(url)
            root = ET.fromstring(raw)
        except (urllib.error.URLError, ET.ParseError) as exc:
            kitaru.log(event="gnews_error", interest=interest, error=str(exc))
            continue
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
                title=title,
                url=link,
                summary=f"From Google News for '{interest}'",
                source=f"gnews:{interest}",
            )
            article.fingerprint = article.compute_fingerprint()
            articles.append(article)
    kitaru.log(
        event="gnews_fetch",
        count=len(articles),
        interest_count=len(interests),
    )
    return articles


@checkpoint
def fetch_grok_twitter_pulse(
    interests: list[str],
) -> Annotated[list[Article], "grok_items"]:
    """Ask Grok what X is saying right now, via xAI's OpenAI-compatible endpoint.

    Gracefully returns an empty list when ``XAI_API_KEY`` is missing so the
    example runs out of the box without any paid keys.
    """
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        kitaru.log(event="grok_skip", reason="no_key")
        return []

    try:
        from openai import OpenAI
    except ImportError:
        kitaru.log(event="grok_skip", reason="no_openai_sdk")
        return []

    client = OpenAI(api_key=api_key, base_url=GROK_BASE_URL)

    interests_str = ", ".join(interests)
    system_prompt = (
        "You have live access to X (formerly Twitter). The user is running a "
        "news scout and wants factual, non-editorial summaries of what is being "
        "discussed right now. Return strict JSON only, no prose."
    )
    user_prompt = (
        f"In the last 60 minutes, what are up to {GROK_MAX_ITEMS} notable X "
        f"discussions relevant to these interests: {interests_str}?\n\n"
        'Return JSON with shape: {"items": [{"title": str, "summary": str, '
        '"url": str}, ...]}\n'
        '"url" should be a representative X post URL for the discussion. '
        "If you have fewer than that, return what you have."
    )

    try:
        response = client.chat.completions.create(
            model=GROK_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    except Exception as exc:
        kitaru.log(event="grok_skip", reason="api_error", error=str(exc)[:200])
        return []

    content = response.choices[0].message.content or "{}"
    usage = response.usage
    kitaru.log(
        event="grok_call",
        model=GROK_MODEL,
        input_tokens=getattr(usage, "prompt_tokens", None),
        output_tokens=getattr(usage, "completion_tokens", None),
    )

    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        kitaru.log(event="grok_skip", reason="bad_json", sample=content[:200])
        return []

    articles: list[Article] = []
    for raw_item in payload.get("items", [])[:GROK_MAX_ITEMS]:
        title = (raw_item.get("title") or "").strip()
        url = (raw_item.get("url") or "").strip()
        summary = (raw_item.get("summary") or "").strip()
        if not title or not url:
            continue
        article = Article(title=title, url=url, summary=summary, source="grok:x")
        article.fingerprint = article.compute_fingerprint()
        articles.append(article)
    return articles


# ---------------------------------------------------------------------------
# Pipeline stages — merge, filter, judge, report
# ---------------------------------------------------------------------------


@checkpoint
def resolve_interests(
    profile_interests: list[str] | None,
    override: list[str] | None,
) -> Annotated[list[str], "interests"]:
    """Pick the interest list: override > profile > default.

    Runs as a checkpoint so the flow body can pass it raw ``memory.get`` refs
    and let ZenML materialize the concrete list at runtime.
    """
    chosen = override or profile_interests or DEFAULT_INTERESTS
    source = (
        "override" if override else ("memory" if profile_interests else "default")
    )
    kitaru.log(event="resolve_interests", source=source, count=len(chosen))
    return list(chosen)


@checkpoint
def normalize_seen(
    raw: list[str] | None,
) -> Annotated[list[str], "seen_fingerprints_in"]:
    """Coerce a missing or ``None`` seen-set into an empty list."""
    seen = list(raw) if raw else []
    kitaru.log(event="normalize_seen", count=len(seen))
    return seen


@checkpoint
def extend_seen(
    seen: list[str],
    new_items: list[Article],
) -> Annotated[list[str], "seen_fingerprints_out"]:
    """Append the new-item fingerprints to the seen set and cap the window."""
    new_fps = [item.fingerprint for item in new_items]
    updated = (seen + new_fps)[-SEEN_FINGERPRINT_WINDOW:]
    kitaru.log(
        event="extend_seen",
        added=len(new_fps),
        total=len(updated),
        capped=(len(seen) + len(new_fps)) > SEEN_FINGERPRINT_WINDOW,
    )
    return updated


@checkpoint
def merge_and_tag(
    hn: list[Article],
    gnews: list[Article],
    grok: list[Article],
) -> Annotated[list[Article], "merged_items"]:
    """Concatenate collector outputs, deduping by fingerprint within this run."""
    seen: set[str] = set()
    merged: list[Article] = []
    for batch in (hn, gnews, grok):
        for article in batch:
            if article.fingerprint in seen:
                continue
            seen.add(article.fingerprint)
            merged.append(article)
    kitaru.log(
        event="merge",
        hn=len(hn),
        gnews=len(gnews),
        grok=len(grok),
        merged=len(merged),
    )
    return merged


@checkpoint
def filter_new(
    items: list[Article],
    seen_fingerprints: list[str],
) -> Annotated[list[Article], "new_items"]:
    """Drop any item whose fingerprint is already in the seen set."""
    seen_set = set(seen_fingerprints)
    new = [item for item in items if item.fingerprint not in seen_set]
    kitaru.log(
        event="filter_new",
        incoming=len(items),
        new=len(new),
        already_seen=len(items) - len(new),
    )
    return new


def _keyword_score(article: Article, interests: list[str]) -> float:
    """Fallback scorer used when the LLM judge is unavailable."""
    text = f"{article.title} {article.summary}".lower()
    hits = sum(1 for interest in interests if interest.lower() in text)
    return min(10.0, float(hits) * 3.0)


@checkpoint
def judge(
    items: list[Article],
    interests: list[str],
) -> Annotated[list[JudgedItem], "judged_items"]:
    """Score items by "interestingness" using kitaru.llm(), with a keyword fallback."""
    if not items:
        return []

    capped = items[:MAX_JUDGE_ITEMS]
    interests_str = ", ".join(interests)

    rendered_items = "\n".join(
        f"{idx}. [{it.source}] {it.title} — {it.url}"
        for idx, it in enumerate(capped)
    )
    prompt = (
        "You are a news scout. Given a user profile and a list of candidate "
        "articles, decide which are genuinely worth interrupting the user for. "
        "Reward novelty, consequence, and direct relevance. Penalize clickbait, "
        "duplication, and shallow posts.\n\n"
        f"User interests: {interests_str}\n\n"
        f"Candidates:\n{rendered_items}\n\n"
        "Return strict JSON only with this shape: "
        '{"items": [{"index": int, "score": float between 0 and 10, '
        '"verdict": "send_now" | "digest" | "ignore", "reason": "one short sentence"}]}'
    )

    try:
        content = kitaru.llm(
            prompt=prompt,
            model=JUDGE_MODEL,
            name="judge_llm_call",
            temperature=0.1,
        )
    except Exception as exc:
        kitaru.log(event="judge_fallback", reason=str(exc)[:200])
        return _keyword_judge(capped, interests)

    try:
        # The model may wrap JSON in fences; strip them if present.
        trimmed = content.strip()
        if trimmed.startswith("```"):
            trimmed = trimmed.split("```", 2)[1]
            if trimmed.startswith("json"):
                trimmed = trimmed[4:]
            trimmed = trimmed.strip().rstrip("`").strip()
        payload = json.loads(trimmed)
    except (json.JSONDecodeError, IndexError):
        kitaru.log(event="judge_fallback", reason="bad_json")
        return _keyword_judge(capped, interests)

    judged: list[JudgedItem] = []
    for entry in payload.get("items", []):
        idx = entry.get("index")
        if not isinstance(idx, int) or idx < 0 or idx >= len(capped):
            continue
        judged.append(
            JudgedItem(
                article=capped[idx],
                score=float(entry.get("score", 0.0)),
                verdict=str(entry.get("verdict", "ignore")),
                reason=str(entry.get("reason", "")),
            )
        )
    judged.sort(key=lambda j: j.score, reverse=True)
    kitaru.log(event="judge", judged=len(judged), mode="llm")
    return judged


def _keyword_judge(items: list[Article], interests: list[str]) -> list[JudgedItem]:
    """Zero-LLM fallback used when kitaru.llm() is unavailable."""
    judged = [
        JudgedItem(
            article=item,
            score=_keyword_score(item, interests),
            verdict="digest" if _keyword_score(item, interests) > 0 else "ignore",
            reason="keyword-match fallback (no LLM)",
        )
        for item in items
    ]
    judged.sort(key=lambda j: j.score, reverse=True)
    return judged


@checkpoint
def report(judged: list[JudgedItem]) -> Annotated[int, "reported_count"]:
    """Print the top-N shortlist to the console. Returns how many were shown."""
    shortlist = [j for j in judged if j.verdict != "ignore"][:TOP_N_REPORT]
    print()
    print("=" * 72)
    print(f"News scout — {len(shortlist)} items worth looking at")
    print("=" * 72)
    if not shortlist:
        print("(nothing new surfaced this run)")
        return 0
    for idx, item in enumerate(shortlist, start=1):
        header = f"\n{idx}. [{item.verdict:9s} {item.score:4.1f}] {item.article.title}"
        print(header)
        print(f"   source: {item.article.source}")
        print(f"   why:    {item.reason}")
        print(f"   link:   {item.article.url}")
    print()
    kitaru.log(event="report", reported=len(shortlist))
    return len(shortlist)


# ---------------------------------------------------------------------------
# The flow — memory reads at top, checkpoints in the middle, memory writes at end
# ---------------------------------------------------------------------------


@flow
def news_scout(interests_override: list[str] | None = None) -> None:
    """Durable news scout. Reads user profile + seen set, runs the pipeline,
    writes the new seen-set back. This is the only place memory ops live.

    Memory ops inside a flow return DAG artifact refs, not Python values, so
    any logic that derives from them must go through a ``@checkpoint`` — that
    is what ``resolve_interests``, ``normalize_seen``, and ``extend_seen`` do.
    """

    # --- Memory reads: return artifact refs, not concrete values ---
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    profile_interests = memory.get("interests")

    memory.configure(scope_type="flow")
    seen_raw = memory.get("seen_fingerprints")

    # --- Normalize memory refs into concrete lists (runtime) ---
    interests = resolve_interests(
        profile_interests=profile_interests,
        override=interests_override,
    )
    seen_fingerprints = normalize_seen(raw=seen_raw)

    # --- Collectors + pipeline ---
    hn_items = fetch_hn_frontpage()
    gnews_items = fetch_google_news(interests=interests)
    grok_items = fetch_grok_twitter_pulse(interests=interests)
    merged = merge_and_tag(hn=hn_items, gnews=gnews_items, grok=grok_items)
    new_items = filter_new(items=merged, seen_fingerprints=seen_fingerprints)
    judged = judge(items=new_items, interests=interests)
    report(judged=judged)

    # --- Memory write: use a checkpoint to build the new list ---
    updated = extend_seen(seen=seen_fingerprints, new_items=new_items)
    memory.set("seen_fingerprints", updated)


# ---------------------------------------------------------------------------
# Profile seeding — runs outside the flow, so it writes namespace memory directly
# ---------------------------------------------------------------------------


def seed_profile(interests: list[str]) -> None:
    """Write the default interest list into namespace memory.

    Runs outside the flow body, so ``memory.configure`` + ``memory.set``
    go straight to the local artifact store without spawning a synthetic step.
    """
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    memory.set("interests", interests)
    print(f"Seeded {len(interests)} interests into namespace '{NAMESPACE}':")
    for interest in interests:
        print(f"  - {interest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_interests(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="Kitaru durable news scout.")
    parser.add_argument(
        "--seed-profile",
        action="store_true",
        help="Write the default interest list into namespace memory and exit.",
    )
    parser.add_argument(
        "--interests",
        type=str,
        default=None,
        help="Comma-separated interests to override the profile for this run.",
    )
    args = parser.parse_args(argv)

    override = _parse_interests(args.interests)

    if args.seed_profile:
        seed_profile(override or DEFAULT_INTERESTS)
        return 0

    news_scout.run(interests_override=override)
    return 0


if __name__ == "__main__":
    sys.exit(main())
