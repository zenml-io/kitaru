# `@zenml-io/kitaru-mastra`

Experimental non-streaming recording and replay support for Mastra 1.51.x.

This adapter depends on the framework-neutral `@zenml-io/kitaru` package, whose repository directory is `packages/core/`. Both packages can be built and packed locally, but neither has been published to npm.

```ts
import { KitaruAgent } from "@zenml-io/kitaru-mastra";

const recorded = new KitaruAgent(existingAgent, {
  agentId: process.env.KITARU_AGENT_ID!,
  agentVersionId: process.env.KITARU_AGENT_VERSION_ID,
  requestedModelId: "openai/gpt-5-mini",
  resolveModel: (modelId) => modelRegistry[modelId],
});

const result = await recorded.generate(messages, options);
```

The wrapper calls the existing agent's public `generate()` method. It does not recreate tools, inspect private agent fields, install model middleware, or change the returned Mastra result.

## Recording

Each call creates isolated run state and:

1. creates a Kitaru session and in-progress root node;
2. records each completed Mastra step through public `onStepFinish`;
3. sends one LLM node and its local tool children in parent-before-child order;
4. completes the same root node and session after the agent succeeds.

Tool-node inputs are the arguments requested by the model. Mastra 1.51 exposes those same arguments to replay hooks and step results. Schema defaults and coercion happen later, inside tool execution, so they are not added to recorded inputs.

Recording uses the public response model, provider, request body, usage, content, finish information, and provider metadata exposed by Mastra. The step callback has no public start time, so step nodes omit `started_at` and do not claim latency.

Model nodes follow completed `onStepFinish` callbacks, and each model node is written before its local tool children. This is adapter callback order, not proof of provider-side start order or wall-clock ordering among concurrent operations.

## Replay

When `KITARU_REPLAY_ID` is set, the wrapper fetches the replay and applies its model, system-instruction, model-parameter, and tool-policy overrides through public per-run `generate()` options and tool hooks.

Input precedence is `KITARU_TASK_INPUTS`, then caller messages. The Kitaru worker puts the effective baseline or replay input in `KITARU_TASK_INPUTS`, so the wrapper does not need to reconstruct it from the replay resource. Replay overrides take precedence over the legacy `KITARU_OVERRIDE` fallback; they are never merged.

`KITARU_TASK_INPUTS` must contain valid JSON. Recording can include caller messages, provider request bodies and metadata, tool inputs and outputs, and the final result. Do not put secrets or unnecessary personal data in those values; neither this adapter nor the shared package redacts application payloads.

Supported tool policies are passthrough, static, and history, including `fail`, `passthrough`, and `error_result` miss behavior. History lookup computes the same SHA-256 cache key as the Python server from the tool name and JSON inputs. A found `null` result is still a hit. The `llm` policy fails before tool execution because it is not supported in this release.

History matching is guaranteed only for traces recorded and replayed through this Mastra adapter. Another framework may validate, default, or serialize the same logical tool input differently, so cross-framework history replay is not a compatibility promise.

## Callback composition

Per-run Mastra hooks replace configured hooks. During replay, Kitaru evaluates its policy first. Passthrough calls then invoke an explicitly supplied configured hook followed by the caller's per-run hook. Kitaru-mocked calls do not invoke user tool hooks. Step recording completes before configured and caller `onStepFinish` callbacks.

Pass configured callbacks explicitly with `configuredOnStepFinish`, `configuredBeforeToolCall`, and `configuredAfterToolCall`. The wrapper never uses `getConfiguredToolHooks()`. Callbacks that are configured on the agent but not supplied to the wrapper cannot be preserved when replay must replace the same per-run hook.

Mastra merges per-run model settings with configured defaults. Kitaru can replace supplied keys but cannot remove configured keys it cannot inspect.

## JSON boundary

Recorded and lookup payloads preserve JSON values, convert dates to ISO strings, bigints to decimal strings, and errors to `{name, message}`. Unsupported functions, symbols, circular references, and non-finite numbers fail the run. Serialization never changes the Mastra result or tool output returned to the application.

## Current scope

This experimental release supports non-streaming `Agent.generate()` with local function tools. Streaming, workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions and tools, LLM tool policy, and TypeScript scorers are intentionally not implemented.

Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or prefer static/history replay when execution must be suppressed.
