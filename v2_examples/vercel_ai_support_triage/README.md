# Vercel AI SDK support triage

This demo records and replays an AI SDK 7 `generateText` call using OpenAI `gpt-5-nano`. The agent investigates a delayed order and suspected duplicate charge with three local tools:

- `lookupAccount` and `lookupOrder` read copied, versioned fixtures.
- `queueRefundReview` appends one line on every real call.

The Python driver registers the compiled command and evaluator with the current Kitaru API, creates a session-run job, and executes the Node command through a job-scoped `Worker`. It then creates and runs a replay job. During replay, Kitaru returns the original `queueRefundReview` result from history, so the append-only outbox stays at one line. The replay also replaces the bounded task prompt, instructions, model, and `maxOutputTokens`, then runs three Python evaluations against the result session.

Every LLM node records `openai/gpt-5-nano` as the requested model and whatever model id the provider served, which is not the same string. Kitaru never prices a call itself, so `agent.ts` passes a `costCalculator` that turns recorded token usage into dollars; without it the session would total `$0`.

## Run

Use Node 22 and a running Kitaru API backed by PostgreSQL. Export `KITARU_API_URL` and, when the server requires it, `KITARU_API_KEY`. Then install, build, and run:

```bash
pnpm install --frozen-lockfile
pnpm --filter @zenml-io/kitaru-vercel-ai build
pnpm --filter @zenml-io/kitaru-example-vercel-ai-support-triage build
OPENAI_API_KEY='your-openai-key' uv run python -m v2_examples.vercel_ai_support_triage.demo
```

The command prints session and replay IDs, node counts, both outbox counts, the mocked history action, and individual evaluation scores. It never prints credentials.

Mutable output is written under `.state/`, which is gitignored. The JSON fixtures are copies owned by this example and are never changed.

## Focused validation

The deterministic path uses the public AI SDK 7 `MockLanguageModelV4` test model, but otherwise runs the same compiled adapter and tools, and it reports a provider-shaped model id (`gpt-5-nano-fixture`) so it cannot hide a live model-id mismatch. A local smoke run does not need a Kitaru server:

```bash
KITARU_VERCEL_AI_SMOKE=1 KITARU_VERCEL_AI_TEST_MODEL=1 KITARU_AGENT_ID=018f0000-0000-7000-8000-000000000100 pnpm --filter @zenml-io/kitaru-example-vercel-ai-support-triage build
KITARU_VERCEL_AI_SMOKE=1 KITARU_VERCEL_AI_TEST_MODEL=1 KITARU_AGENT_ID=018f0000-0000-7000-8000-000000000100 node v2_examples/vercel_ai_support_triage/dist/main.js
```

The full Worker-backed path needs the repository's PostgreSQL test service on port 5433 and is exercised by the TypeScript integration test suite.
