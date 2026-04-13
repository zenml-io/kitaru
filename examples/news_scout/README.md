# News Scout

A durable news-monitoring flow that demonstrates `kitaru.memory`. On each run
the scout sweeps three sources, dedupes against a rolling memory set, asks an
LLM to judge what is worth surfacing, and prints the shortlist. Consecutive
runs feel "always-on" because memory persists across executions.

## Quick start

```bash
cd examples/news_scout
kitaru init
```

Create a `.env` file with your API keys:

```
ANTHROPIC_API_KEY=sk-ant-...
XAI_API_KEY=xai-...            # optional — enables Grok/X source
```

Run:

```bash
python scout.py --seed-profile   # one-time: seed interests into namespace memory
python scout.py                   # first sweep — everything is new
python scout.py                   # second sweep — dedup kicks in via memory
```

## What it demonstrates

- **`kitaru.memory` with two scopes** — namespace (user profile) and flow
  (seen-fingerprint rolling set). Memory reads/writes happen in the flow body
  because memory is forbidden inside `@checkpoint`.
- **Multi-source collection** — each source is its own checkpoint, so replay
  can re-run one without re-running the others.
- **`kitaru.llm()`** for the judge, with a keyword-match fallback when no
  model credentials are available.
- **Grok via xAI** — calls the OpenAI-compatible `api.x.ai` endpoint inside a
  checkpoint, tracked via `kitaru.log()`. Skipped gracefully if `XAI_API_KEY`
  is missing.

## Sources

| Source | Requires | Notes |
|---|---|---|
| Hacker News (Algolia) | nothing | Always-on baseline |
| Google News RSS | nothing | One query per interest |
| Grok (xAI) | `XAI_API_KEY` | X/Twitter signal, skipped if key missing |

## Inspecting memory after a run

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

## Next steps (not implemented)

- Wire up Discord delivery via a `DISCORD_WEBHOOK_URL` env var
- Schedule via Kubernetes cron (`kitaru` on a K8s stack + ZenML schedule)
- Add `kitaru.wait()` before sending to let a human approve alerts
- Add a feedback loop that ingests thumbs-up/down and updates the profile
- Add `investigate_deeply` checkpoint with Firecrawl for paywalled articles
