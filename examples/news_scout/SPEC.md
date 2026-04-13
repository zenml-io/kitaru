# News Scout — Design Spec

**Date:** 2026-04-11
**Target branch:** `feature/memory-base`
**Location:** `examples/news_scout/`
**Status:** Draft — pending approval

## Summary

A new Kitaru example that demonstrates the `kitaru.memory` feature (shipping on `feature/memory-base`) through a durable "news scout" flow. On each invocation, the scout fetches stories from a small set of public sources, dedupes them against per-flow memory, asks an LLM to judge which are worth surfacing given a user-defined interest profile (also in memory), and prints the shortlist.

The example's job is to make the "always-on agent" story concrete without actually shipping a product: runs are short, outputs stay on the console, and memory is what makes consecutive runs feel continuous.

## What this example teaches

1. **`kitaru.memory` with two scopes.** `namespace="news_scout"` for the user profile (read once per run), and `flow` scope for the rolling set of already-seen story fingerprints (read + written every run). This is the "dedup across runs" story that justifies why memory is a first-class primitive.
2. **Memory is a flow-body concern.** Memory ops are forbidden inside `@checkpoint`, so reads happen at the top of the flow body and writes happen at the bottom. Checkpoints are pure data pipelines. This constraint is part of the pedagogy.
3. **Multi-source collection as a replay-safe pipeline.** Each source is its own `@checkpoint`, so replay can re-run one source without re-running the others. That's a concrete win for users debugging flaky collectors.
4. **`kitaru.llm()` with a user-default alias.** The judge routes through `kitaru.llm(model="default", ...)` so it picks up whatever alias the user already has registered (for this project, `default` → `coding-agent` → `anthropic/claude-sonnet-4-20250514`). No new setup required on a machine that already has a working Kitaru.
5. **Calling a non-`kitaru.llm()` provider from inside a checkpoint.** The Grok source calls the `openai` SDK directly with `base_url="https://api.x.ai/v1"` because `kitaru.llm()` does not currently route to `xai` as a provider. The checkpoint uses `kitaru.log()` to capture prompt / response / usage manually. This is a useful pattern in its own right and worth demoing.
6. **Graceful degradation.** Missing `XAI_API_KEY` → skip Grok. No model alias → fall back to keyword-match scoring. The example runs end-to-end with zero keys, and gets progressively more interesting as keys are added.

Non-goals: email/Discord delivery, HITL `wait()`, learning loop, clustering, scheduling config, multi-user support, a dashboard screenshot. Each of these is an obvious extension and gets one sentence in the README "next steps" section.

## Sources (final list)

| Source | Shape | Requires | Notes |
|---|---|---|---|
| Hacker News | Algolia HTTP JSON (`http://hn.algolia.com/api/v1/search?tags=front_page`) | nothing | Always-on baseline, reliable |
| Google News per topic | RSS per query (`https://news.google.com/rss/search?q=<interest>+when:1d`) | nothing | One query per interest from the profile — gives the scout a free "topic-aware" web feed |
| Grok live search | `openai` SDK with `base_url="https://api.x.ai/v1"`, model `grok-4-latest` (or whatever xAI currently exposes) | `XAI_API_KEY` | The X/Twitter signal channel, legitimately. Skipped gracefully if the key is missing. |

No Exa, no Reddit, no Discord, no email, no Firecrawl, no investigate_deeply, no fallback web-search LLM.

## Flow shape

```python
@flow
def news_scout() -> None:
    # --- Flow body: memory reads ---
    interests: list[str] = memory.get("interests") or DEFAULT_INTERESTS
    seen_fingerprints: list[str] = memory.get("seen_fingerprints") or []

    # --- Checkpoints: pure data pipeline ---
    hn_items = fetch_hn_frontpage()
    gnews_items = fetch_google_news(interests)
    grok_items = fetch_grok_twitter_pulse(interests)   # skipped if no XAI_API_KEY
    all_items = merge_and_tag(hn_items, gnews_items, grok_items)

    new_items = filter_new(all_items, seen_fingerprints)
    judged = judge(new_items, interests)                # kitaru.llm() or fallback
    report(judged)

    # --- Flow body: memory writes ---
    updated = (seen_fingerprints + [item.fingerprint for item in new_items])[-500:]
    memory.set("seen_fingerprints", updated)
```

Seven checkpoints total. `fetch_grok_twitter_pulse` always runs but returns `[]` when `XAI_API_KEY` is absent (with a single `print` explaining why). Replay can re-run any one checkpoint while the others use cached outputs.

## Checkpoint contract details

- **`fetch_hn_frontpage() -> list[Article]`** — GET the Algolia front-page endpoint, parse hits into `Article` Pydantic models, return up to 30.
- **`fetch_google_news(interests: list[str]) -> list[Article]`** — For each interest, build a `when:1d` Google News RSS URL, parse with `feedparser` (already a zenml transitive dep — verify at implementation), return deduped-by-URL merged list.
- **`fetch_grok_twitter_pulse(interests: list[str]) -> list[Article]`** — If `XAI_API_KEY` missing, return `[]` early. Else: one structured-output call via `openai` SDK with `base_url="https://api.x.ai/v1"`, asking Grok for "up to 8 notable X discussions in the last hour relevant to these interests, as JSON with {title, summary, url, evidence_urls}". Uses `kitaru.log()` for prompt/response/usage tracking.
- **`merge_and_tag(...) -> list[Article]`** — Tag each item with its source, fold into one list.
- **`filter_new(items: list[Article], seen: list[str]) -> list[Article]`** — Pure filter by fingerprint set membership. Fingerprint = SHA1 of normalized URL + title.
- **`judge(items: list[Article], interests: list[str]) -> list[JudgedItem]`** — Build one prompt that asks for `{fingerprint, verdict, score, reason}` per item. Route via `kitaru.llm(model="default", ...)`. On any error (no alias, no provider key, HTTP failure), fall back to `keyword_match_score(item, interests)` with a single `print` explaining the fallback. Return items sorted by score desc.
- **`report(judged: list[JudgedItem]) -> None`** — Print a plain-text block with top N items (headline, source, score, one-line reason, link). No Rich, no panels, no markdown — just `print`.

## Memory contract

**Namespace scope = `"news_scout"`**, typed namespace scope:
- `interests` → `list[str]` — topics the user cares about. Seeded once via `python scout.py --seed-profile` (or `kitaru memory set --scope-type=namespace --scope=news_scout interests '[...]'`).

**Flow scope** (auto-inferred from the `@flow` function identity, which on this branch means the durable flow UUID per PR #125):
- `seen_fingerprints` → `list[str]` — bounded to last 500 entries so the rolling window doesn't grow unbounded. Purging older history is out of scope for the example — the CLI's `kitaru memory compact` / `purge` can demo that separately.

Module-level `kitaru.memory.*` takes no per-call scope arg, so the flow body activates `namespace="news_scout"` via `memory.configure(scope_type="namespace", scope="news_scout")` once at the top, then switches to `memory.configure(scope_type="flow")` before the seen-fingerprints call. (If the memory API supports a scoped context manager, prefer that — verify at implementation.)

**Memory ops are never inside a `@checkpoint` body.** Violations will raise `KitaruContextError`; the tests should catch any regression.

## File layout

```
examples/news_scout/
  README.md          # what / why / run story / next steps (<= 80 lines)
  __init__.py
  scout.py           # the entire flow + CLI entrypoint
```

Three files total. Everything — models, collectors, flow, CLI — lives in `scout.py`. If the file grows past ~350 lines during implementation, split along collector boundaries (`sources.py`) but not preemptively.

## CLI surface

```bash
python scout.py --seed-profile          # one-time: write default interests into namespace memory
python scout.py                         # run one sweep
python scout.py --interests ai,robots   # override interests for this run (does not touch memory)
```

Three flags total: `--seed-profile`, `--interests`, `--help`. No Rich, no subcommands, no config file.

## Run story (README)

```bash
# One-time setup
git checkout feature/memory-base
uv venv --python 3.13 && source .venv/bin/activate
uv sync --extra local --extra llm
cd examples/news_scout
kitaru init

# First run: seed the profile, then sweep
python scout.py --seed-profile
python scout.py                         # everything is new — judge runs on all items
python scout.py                         # dedup kicks in via flow memory — only new items go through the judge

# Optional: enable Grok for the X/Twitter source
export XAI_API_KEY=sk-...
python scout.py

# Inspect memory after a run
kitaru memory list --scope-type=flow
kitaru memory get --scope-type=namespace --scope=news_scout interests
```

## Extension points (README "next steps")

One-line each, not implemented:
- Wire up Discord via a `DISCORD_WEBHOOK_URL` env var
- Schedule via Kubernetes cron (`kitaru` on a K8s stack + ZenML schedule)
- Add HITL: `kitaru.wait()` before sending an alert to let a human approve
- Add a feedback loop: second flow that ingests thumbs-up/down and updates `namespace` memory
- Add `investigate_deeply` checkpoint with Firecrawl or computer-use for paywalled articles

## Testing

Not included in MVP. Adding a test that hits live HTTP + LLM APIs is flaky and not worth the example's scope. If we want a test later, the pattern is: mock the three fetch_* functions, run the flow end-to-end in-memory, assert the report output.

## Dependencies

**Zero new dependencies.** Everything used:
- `urllib.request` + `xml.etree.ElementTree` (stdlib) for HN and Google News. `feedparser` is not installed in this venv and the example should stay install-free.
- `openai` for the Grok call — already installed via `kitaru[llm]`.
- `pydantic` for `Article` / `JudgedItem` — already a kitaru dep.

The example must not add anything to `pyproject.toml`.

## Risks and rough edges (resolved / flagged)

1. **RSS parsing without `feedparser`** — resolved. Use stdlib `xml.etree.ElementTree`. ~15 lines, good enough for Google News' well-formed output.
2. **Grok model name drift.** `grok-4-latest` may not exist under that exact string. Will verify against the xAI API at implementation; if the call 404s, fall back to `grok-beta` or whatever the current public model is. The README tells users to override via an env var or code constant.
3. **`memory.configure` scope switching semantics.** Must verify whether mid-flow scope switching is supported by reading `examples/memory/flow_with_memory.py` on this branch. If it is, use it directly. If not, coalesce reads into one namespace call and one flow call with explicit `memory.configure` between them, or use a context-manager form if available.
4. **Durable flow ID fix (PR #125) not yet on this branch** — verified via `git log`. Flow-scoped memory will key on the `@flow` function name (`news_scout`). This name is unique in the example set, so collision risk is zero. No action needed.

## Open questions for the user

None — the scope has been iterated down from the original brainstorm and all remaining decisions are implementation details the author can resolve against the live API.

## Links

- Memory branch: `feature/memory-base`
- Related PRs: #82 (main memory), #119 (memory in coding agent — draft), #125 (durable flow IDs for flow-scoped memory)
- Existing memory example for API shape reference: `examples/memory/flow_with_memory.py`
