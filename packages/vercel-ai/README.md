# `@zenml-io/kitaru-vercel-ai`

`@zenml-io/kitaru-vercel-ai` adds Kitaru recording and replay to the non-streaming AI SDK 7 `generateText` function.

This adapter depends on the framework-neutral `@zenml-io/kitaru` package, whose repository directory is `packages/core/`. Release candidates use npm's `rc` tag and remain pre-1.0 compatibility previews.

```bash
pnpm add @zenml-io/kitaru-vercel-ai@rc ai@7.0.55
```

```ts
import { openai } from "@ai-sdk/openai";
import { createKitaruGenerateText } from "@zenml-io/kitaru-vercel-ai";

const generateText = createKitaruGenerateText({
  agentId: "your-agent-id",
});

const result = await generateText({
  model: openai("gpt-5"),
  prompt: "Triage this support request",
});
```

The returned function has the native AI SDK `generateText` signature and returns its native result object. The adapter calls AI SDK's public `generateText`, callback, and local tool `execute` APIs; it does not reproduce the SDK's generation loop.

Replay supports local executable tools with passthrough, static, and history policies. Static and history hits return the configured value without calling the original `execute`; passthrough calls the original function. Replay registers tool calls in model-output order before local execution begins, so a failing earlier policy prevents later queued local side effects. Baseline execution retains AI SDK concurrency. Recorded node indexes represent completed adapter callbacks and parent-before-child storage, not provider-side start order or wall-clock order among concurrent work.

History matching is guaranteed only for traces recorded and replayed through this Vercel AI SDK adapter. Another framework may validate, default, or serialize the same logical tool input differently, so cross-framework history replay is not a compatibility promise.

## Data boundary

`KITARU_TASK_INPUTS` must contain a JSON string of at most 16,384 characters. Replay prompt and instruction overrides must also be bounded strings. The adapter records bounded tool values, model settings, usage, response metadata, and result summaries. It never records provider request data and sets LLM-node inputs to `null`. Even so, tool values and result summaries can contain application data. Do not put secrets or unnecessary personal data in recorded values; neither this adapter nor the shared package performs semantic redaction.

## Current scope

This experimental release supports AI SDK 7 non-streaming `generateText` with local executable tools. It does not support streaming generation, provider-executed or dynamic tools, tool approval, sandboxed replay, async-iterable tools during replay, or the LLM tool policy.

The adapter disables generation retries. Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure. The adapter reports that failure, but it cannot roll back the completed effect. Use application-level idempotency keys for side-effecting tools, or prefer static/history replay when execution must be suppressed.
