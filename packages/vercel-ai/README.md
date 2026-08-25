# `@zenml-io/kitaru-vercel-ai`

`@zenml-io/kitaru-vercel-ai` adds Kitaru recording and replay to AI SDK 7 `ToolLoopAgent.generate()` and the non-streaming `generateText` function.

This adapter depends on the framework-neutral `@zenml-io/kitaru` package, whose repository directory is `packages/core/`. The packages are versioned and released together.

```bash
pnpm add @zenml-io/kitaru-vercel-ai ai@7.0.65
```

## Links

- [Vercel AI SDK adapter documentation](https://docs.zenml.io/kitaru/adapters/vercel-ai)
- [Install and start a Kitaru server](https://docs.zenml.io/kitaru/getting-started/installation)
- [Run the TypeScript returns agent example](https://github.com/zenml-io/kitaru/tree/main/examples/typescript/vercel_ai_ticket_resolver)

```ts
import { openai } from "@ai-sdk/openai";
import { createKitaruToolLoopAgent } from "@zenml-io/kitaru-vercel-ai";

const agent = createKitaruToolLoopAgent(
  {
    id: "support-agent",
    model: openai("gpt-5"),
  },
  { agentId: "your-kitaru-agent-id" },
);

const result = await agent.generate({
  prompt: "Triage this support request",
});
```

The returned object implements AI SDK's public `Agent` interface. Its `generate()` method returns the native AI SDK result object, and its tools, call options, `prepareCall`, callbacks, runtime context, output type, retries, timeouts, and abort signal remain native. The adapter calls AI SDK's public `ToolLoopAgent`, callback, and local tool `execute` APIs; it does not reproduce the SDK's generation loop.

`createKitaruGenerateText(...)` remains available when an application uses the function API directly.

Replay supports local executable tools with passthrough, static, and history policies. Static and history hits return the configured value without calling the original `execute`; passthrough calls the original function. Replay registers tool calls in model-output order before local execution begins, so a failing earlier policy prevents later queued local side effects. Baseline execution retains AI SDK concurrency. Recorded node indexes represent completed adapter callbacks and parent-before-child storage, not provider-side start order or wall-clock order among concurrent work.

Successful static and history values are validated against a tool's `outputSchema`. A schema supplied with `jsonSchema()` must include its optional runtime `validate` callback so replay can enforce it; replay fails closed when a declared output schema has no runtime validator. An `error_result` miss is a deliberate error sentinel, not a successful tool value, so it bypasses that schema, records a failed tool node, and lets the generation continue.

History matching is guaranteed only for traces recorded and replayed through this Vercel AI SDK adapter. Another framework may validate, default, or serialize the same logical tool input differently, so cross-framework history replay is not a compatibility promise.

Kitaru looks recorded tool results up by tool name and arguments. Baseline-scoped history consumes repeated matching calls in their recorded order. Agent- and cohort-version-scoped history use the newest completed matching call instead, so the adapter writes a `console.warn` when those scopes repeat a call and may diverge from the baseline trajectory.

## Model identity

Each LLM node records `requested_model` (the Kitaru model id the run asked for, before any replay override), `model` (the model id the provider says it served, such as `gpt-5-nano-2026-08-07`), and `model_provider` (the bare provider family, such as `openai`). The AI SDK reports transport-qualified provider strings such as `openai.responses`; the adapter keeps that original string as the `provider_id` attribute so evaluator model policies can match one exact provider family.

## Cost

The adapter records the model, provider, and token usage of every step, and Kitaru stores whatever cost the adapter sends. Nothing computes a price server-side, so cost stays `null` and a session totals `$0` unless you pass `costCalculator`:

```ts
const generateText = createKitaruGenerateText({
  agentId: "your-agent-id",
  costCalculator: ({ model, tokens }) =>
    priceFor(model, tokens?.input_tokens ?? 0, tokens?.output_tokens ?? 0),
});
```

Each LLM node carries a `cost` attribute recording where the number came from: `disabled` with no calculator, `estimated` for a calculated value, and `unavailable` when the calculator throws or returns nothing. A throwing calculator never fails the run.

## Data boundary

`KITARU_TASK_INPUTS` must contain a JSON prompt string or message array, and prompt strings Kitaru injects are limited to 4,096 characters. Replay prompt and instruction overrides must also be bounded strings. Values the caller supplies and values the run produces are not held to that bound: recorded inputs, model text, tool arguments, and tool results are recorded in full up to 1 MiB per payload. A payload beyond that ceiling is replaced with a `{"kitaru_recording": "degraded"}` marker, and a single value that cannot be converted, such as a circular reference or a function, is replaced in place with `[circular]`, `[truncated]`, or `[unsupported]`; recording never aborts a generation the caller's own code handled. The adapter never records provider request data and sets LLM-node inputs to `null`. Tool arguments and results replace values under the credential keys `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` with `[redacted]`; redacted arguments are not eligible for history lookup because they no longer identify the original call. This key-name rule is only a safety net, so keep secrets and unnecessary personal data out of recorded values.

## Current scope

This experimental release supports AI SDK `>=7.0.60 <8` for non-streaming `ToolLoopAgent.generate()` and `generateText` calls with local executable tools. The Agent's required `stream()` method remains a native, recording-free passthrough during ordinary execution. Kitaru does not record streaming calls, and `stream()` rejects before provider or tool execution when `KITARU_REPLAY_ID` is set.

Durable manual approval continuation is not supported. If `generate()` returns an unresolved manual tool approval request, the native result is returned but the Kitaru session is marked failed with an unsupported-continuation diagnostic. Automatic approval decisions complete normally. Agent replay rejects approval configuration or approval messages before model or tool execution. Replay also rejects provider-executed or dynamic tools, sandboxed tools, per-step overrides through `prepareStep`, async-iterable tools, and the LLM tool policy.

Replay runs tools one at a time in model-output order. `ticketTimeoutMs` (30 seconds by default) bounds how long a queued tool waits for its predecessor to *start*, not how long that predecessor runs, so a slow passthrough tool does not fail the calls queued behind it.

The direct `generateText` wrapper disables generation retries so one recorded node represents one provider attempt. The Agent API preserves native retry settings. Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure. The adapter reports that failure, but it cannot roll back the completed effect. Use application-level idempotency keys for side-effecting tools, or prefer static/history replay when execution must be suppressed.
