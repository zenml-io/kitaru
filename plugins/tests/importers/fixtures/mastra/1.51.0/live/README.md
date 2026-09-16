# Bounded live validation

This synthetic fixture was generated and replayed on 2026-09-09 using real OpenAI `gpt-4.1-mini` calls, resolved to `gpt-4.1-mini-2025-04-14`. It contains no customer data. `ORBIT-47` is an invented conversation fact, not a credential. The parent directory's package lock pins Mastra core 1.51.0 and observability 1.16.0.

`generate.mjs` uses native Mastra history recall over `MastraMemory` and `InMemoryStore`. The prior user message contains the code; the new root invocation does not. The initial model-generation input includes the recalled message. This fixture deliberately excludes semantic, working, and observational memory, custom processors, and `prepareStep`.

The generation made two provider calls and one live `double` execution. It exported 14 native spans into `trace.json`. `evidence.json` contains configuration, outputs, and provider request bodies without headers or credentials.

The export was then imported with `replay_context: "history-only"` through a fresh candidate-wheel worker into an isolated candidate server and PostgreSQL database. First import created one session; the second skipped one duplicate. A registered agent version ran `replay.mjs` through the worker, using the context-capable #1050 adapter compiled in a separate checkout. The replay job, including a completion evaluator, completed successfully. The local stack and database were removed after validation.

The replay made two real model calls and one real Kitaru tool-history lookup. It returned “Your secret code is ORBIT-47, and double 3 is 6.” with zero live tool executions. The probe deliberately supplied wrong live instructions, caller input, and memory selectors; none reached the model. `worker-evidence.json` records import counts and replay completion; `replay-evidence.json` records provider requests and API paths; `validation.json` identifies the tested adapter artifact. These receipts describe one run, not a general fidelity guarantee.

## Reproduction

Use Node 22 and a scratch copy of the parent package manifest/lock plus `generate.mjs`. Run `npm ci`, provide `OPENAI_API_KEY` through the environment, then run `node generate.mjs`. This makes billable provider calls, bounded to three requests with retries disabled and 150 output tokens per call. Generation normally takes two calls.

Import the resulting `trace.json` into an explicitly selected local server through the Mastra importer, passing `{"replay_context":"history-only"}`. Associate the import with a runnable agent version whose command runs `node replay.mjs`. Copy the probe next to its installed Mastra dependencies. Supply `MASTRA_LIVE_AGENT_ID`, `OPENAI_API_KEY`, and `KITARU_MASTRA_ADAPTER_ENTRYPOINT` (the absolute compiled #1050 adapter `dist/index.js`) through the local worker environment or run specification. Keep credentials out of checked-in run specifications. The worker supplies the task inputs and replay identifiers.

Create a replay using baseline tool history with `on_miss: "fail"` and a completion evaluator. The probe asserts full context restoration, removal of live memory selectors, one history lookup, zero live tool execution, and the expected recalled fact and computed result. Replay is independently bounded to three provider requests with retries disabled and 150 output tokens per call. It writes a fresh `replay-evidence.json` beside the script. Tear down the local server and database afterward.

The #1050 adapter must be integrated before this mode is released. Default tests parse the checked-in live export without any model calls. The deterministic replay probe in the parent directory provides repeatable adapter coverage without provider access.
