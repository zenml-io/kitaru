# Mastra support triage

This demo records and replays a real Mastra `Agent` using OpenAI `gpt-5-nano`. The agent investigates a delayed order and suspected duplicate charge with three local tools:

- `lookupAccount` and `lookupOrder` read versioned fixtures.
- `queueRefundReview` appends one line on every real call.

The Python driver registers the agent command and evaluator with the current Kitaru API, creates a session-run job, and executes the compiled Node command through a job-scoped `Worker`. It then creates and runs a replay job. During replay, Kitaru returns the original `queueRefundReview` result from history, so the append-only outbox stays at one line. The replay also replaces the input, system instructions, and `maxOutputTokens`, then runs three Python evaluations against the result session.

Every LLM node records `openai/gpt-5-nano` as the requested model and whatever model id the provider served, which is not the same string. Kitaru never prices a call itself, so `agent.ts` passes a `costCalculator` that turns recorded token usage into dollars; without it the session would total `$0`.

## Run

Use Node 22 and a running Kitaru API backed by PostgreSQL. Export `KITARU_API_URL` and, when the server requires it, `KITARU_API_KEY`. Then install, build, and run:

```bash
pnpm install --frozen-lockfile
pnpm --filter @zenml-io/kitaru-mastra build
pnpm --filter @zenml-io/kitaru-example-mastra-support-triage build
OPENAI_API_KEY='your-openai-key' uv run python -m v2_examples.mastra_support_triage.demo
```

The command prints session and replay IDs, node counts, both outbox counts, the mocked history action, and individual evaluation scores. It never prints credentials.

Mutable output is written under `.state/`, which is gitignored. The JSON fixtures are never changed.

## Focused validation

The cross-language test uses a deterministic public Mastra model test utility, but otherwise runs the same compiled agent, tools, HTTP API, job-scoped worker, history replay, and Python evaluation path. It needs the repository's PostgreSQL test service on port 5433:

```bash
docker compose up -d db
PATH=/opt/homebrew/opt/node@22/bin:$PATH pnpm build
PATH=/opt/homebrew/opt/node@22/bin:$PATH uv run --extra server pytest tests/typescript/test_mastra_adapter.py
```
