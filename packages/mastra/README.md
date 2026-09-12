# `@zenml-io/kitaru-mastra`

Experimental non-streaming recording and replay support for Mastra `>=1.51.0 <1.65.0`.

This adapter depends on the framework-neutral `@zenml-io/kitaru` package, whose repository directory is `packages/core/`. The packages are versioned and released together.

```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.64.0
```

## Links

- [TypeScript and Mastra evaluator guide](https://docs.zenml.io/kitaru/guides/typescript-evaluators)
- [Mastra adapter documentation](https://docs.zenml.io/kitaru/adapters/mastra)
- [Install and start a Kitaru server](https://docs.zenml.io/kitaru/getting-started/installation)
- [Run the Mastra support-triage example](https://github.com/zenml-io/kitaru/tree/main/examples/typescript/mastra_support_triage)

```ts
import { KitaruAgent } from "@zenml-io/kitaru-mastra";

const recorded = new KitaruAgent(existingAgent, {
  agentId: process.env.KITARU_AGENT_ID!,
  agentVersionId: process.env.KITARU_AGENT_VERSION_ID,
  allowedReplayModels: ["openai/gpt-5-mini", "openai/gpt-5"],
  requestedModelId: "openai/gpt-5-mini",
  resolveModel: (modelId) => modelRegistry[modelId],
});

const result = await recorded.generate(messages, options);
```

The wrapper calls the existing agent's public `generate()` method. It does not recreate tools, inspect private agent fields, or change the returned Mastra result. For a per-run `structuredOutput.model`, it resolves the secondary model through public `getModel()` and wraps that model's `doStream()` for this invocation without mutating the original model.

## Recording

Each call creates isolated run state and:

1. creates a Kitaru session and in-progress root node;
2. records each completed Mastra step through public `onStepFinish`;
3. sends one LLM node and its local tool children in parent-before-child order;
4. completes the same root node and session after the agent succeeds.

Tool-node inputs are the arguments requested by the model. Mastra 1.51 exposes those same arguments to replay hooks and step results. Schema defaults and coercion happen later, inside tool execution, so they are not added to recorded inputs.

Recording uses the public response model, provider, usage, finish information, and provider metadata exposed by Mastra. The step callback has no public start time, so the adapter never claims provider-side latency.

Each LLM node records `requested_model` (the Kitaru model id the run asked for, before any replay override), `model` (the model id the provider says it served), and `model_provider` (the bare provider family, such as `openai`). Mastra reports transport-qualified provider strings such as `openai.responses`; the adapter keeps that original string as the `provider_id` attribute so evaluator model policies can match one exact provider family.

Recording is bounded on purpose. Parent step nodes record no model inputs, because the provider request body repeats the whole system prompt and message history on every step. Step outputs keep the finish reason, text, tool calls, tool results, tripwire details, and warnings; the session output keeps the finish reason, step count, and final text. Tool strings longer than 4096 characters, arrays longer than 100 items, objects with more than 100 keys, and nesting deeper than 8 levels are truncated, and values under the credential keys `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` are replaced with `[redacted]`. Provider metadata is not part of the replay contract, so it also hides values under keys that carry blobs or transport envelopes, such as `data`, `file`, `request`, and `url`.

Model nodes follow completed `onStepFinish` callbacks, and each model node is written before its local tool children. This is adapter callback order, not proof of provider-side start order or wall-clock ordering among concurrent operations.

## Cost

The adapter records the model, provider, and token usage of every step, and Kitaru stores whatever cost the adapter sends. Nothing computes a price server-side, so cost stays `null` and a session totals `$0` unless you pass `costCalculator`:

```ts
const recorded = new KitaruAgent(existingAgent, {
  agentId: "your-agent-id",
  costCalculator: ({ model, tokens }) =>
    priceFor(model, tokens?.input_tokens ?? 0, tokens?.output_tokens ?? 0),
  requestedModelId: "openai/gpt-5-mini",
});
```

Each LLM node carries a `cost` attribute recording where the number came from: `disabled` with no calculator, `estimated` for a calculated value, and `unavailable` when the calculator throws or returns nothing. A throwing calculator never fails the run.

## Replay

When `KITARU_REPLAY_ID` is set, the wrapper fetches the replay and applies its model, system-instruction, model-parameter, and tool-policy overrides through public per-run `generate()` options and tool hooks.

Input precedence is `KITARU_TASK_INPUTS`, then caller messages. The Kitaru worker puts the effective baseline or replay input in `KITARU_TASK_INPUTS`, so the wrapper does not need to reconstruct it from the replay resource. Replay overrides take precedence over the legacy `KITARU_OVERRIDE` fallback; they are never merged.

`KITARU_TASK_INPUTS` must contain valid JSON. Recording can include caller messages, provider metadata, tool inputs and outputs, and the final text. Key-name redaction is a safety net, not a classifier: do not put secrets or unnecessary personal data in tool inputs, tool outputs, or prompts.

Mastra's `structuredOutput.model` option starts a second model call whose events do not reach the parent agent's callbacks. Kitaru records that call through the secondary model's public `doStream()` method. Each provider attempt gets a separate `structured_output` LLM node with its own requested and served model, bounded prompt and output, token usage, settings, and failure status. The original Mastra result, including `result.object`, is preserved. Schema-only `structuredOutput` remains supported.

Supply the secondary model in the per-run `generate()` options. Agent-default secondary models remain rejected; move that configuration to the call. The secondary model must use the v2, v3, or v4 model interface. `useAgent: true` and `errorStrategy: "warn"` or `"fallback"` remain rejected before execution: conversation-aware structuring needs additional context guarantees, and suppressed validation errors need a separate stage-failure recording contract. A strict validation failure can leave a successful provider node with raw output while the run fails validation.

Replay model and model-parameter overrides apply only to the parent agent. The secondary model and its provider options remain as configured by the entrypoint. The secondary call runs again against the replayed parent's fresh output; it is not a cached structured result. Its elapsed time includes stream consumption and is not a claim about provider-only latency.

A replacement model from a replay override runs only when `allowedReplayModels` lists it, so an override cannot switch the run to an arbitrary, far more expensive model. Overridden `model_params` are validated against the settings Mastra forwards to a model (`temperature`, `topP`, `topK`, `maxOutputTokens`, `presencePenalty`, `frequencyPenalty`, `seed`, `stopSequences`) with numeric bounds, and are merged into the caller's `modelSettings` instead of replacing them, so an override that changes only temperature leaves the caller's token cap in place.

Replay refuses to start when a tool cannot be intercepted. Mastra applies tool hooks by wrapping a tool's local `execute` function, so a provider-executed or otherwise non-executable tool would run for real during a replay. The adapter enumerates the agent's tools, including function-valued tools resolved with the run's `requestContext`, plus per-run `clientTools` and `toolsets`, and fails with a `ToolPolicyError` naming the tool. Approval-gated runs (`requireToolApproval`) are rejected for the same reason. Tools that Mastra adds only at execution time, and tools a provider executes on its own side, remain outside this check.

Mastra rewrites registry keys that contain characters outside letters, numbers, `_`, and `-`, start with a number or `-`, or exceed 63 characters before exposing them to the model. Replay rejects those keys before starting a session because a policy configured for the raw key would not apply to the rewritten runtime name. Rename the tool key so Mastra leaves it unchanged.

Supplied message arrays and recalled memory are distinct. An explicit array can still receive recalled history when the invocation targets a memory thread. For memory-dependent calls, the adapter records the effective conversation immediately before the first model step in a versioned `mastra_conversation_context` envelope. It keeps `supplied_messages` separately from the snapshot, whose `source: "recalled"` tag identifies memory-dependent input. The snapshot combines system, recalled, and supplied messages; it is not a recalled-only array. Replay restores the snapshot, including system messages, without reading or writing the original thread. This reproduces one invocation's conversation context, not an adaptive dialogue.

The adapter drops per-run `memory`, `threadId`, `resourceId`, and `savePerStep`, and clones `requestContext` without Mastra's thread, resource, and internal memory keys. Default memory options remain rejected because Mastra would merge them back. Working memory, semantic recall, observational memory, and original invocations with user input processors or `prepareStep` are not replayable from these snapshots; they can add tools or change context beyond the first model step. Explicit messages without memory selectors keep their existing input format.

Missing, incomplete, or lossy snapshots fail replay before model execution with instructions to record the invocation again or supply its complete recorded messages without live memory selectors. Existing recordings cannot recover history that was never recorded. Legacy raw inputs carry no memory provenance, so removing memory selectors from an entrypoint cannot prove that a legacy recording contains the full conversation. Record legacy memory-dependent invocations again before replaying them. Prompt and system-instruction overrides on conversation snapshots are rejected because replacing them can discard part of the recorded context.

A tool-policy failure aborts the replay. Mastra turns a rejected tool hook into a tool-error result and keeps the agent loop running, so the adapter stops the run itself: later tools refuse to execute, the run rejects with the original policy error, and the session is recorded as failed rather than completed. Replay also runs tool calls one at a time (`toolCallConcurrency: 1`) so that a policy failure stops the step before a sibling tool fires its side effect.

Supported tool policies are passthrough, static, and history, including `fail`, `passthrough`, and `error_result` miss behavior. History lookup computes the same SHA-256 cache key as the Python server from the tool name and JSON inputs. A completed match replays its result, including `null`, without executing the live tool. A failed match throws its stored error and does not execute the live tool. Only a genuine miss follows `on_miss`. The `llm` policy fails before tool execution because it is not supported in this release.

History matching is guaranteed only for traces recorded and replayed through this Mastra adapter. Another framework may validate, default, or serialize the same logical tool input differently, so cross-framework history replay is not a compatibility promise.

## Callback composition

Per-run Mastra hooks replace configured hooks. During replay, Kitaru evaluates its policy first. Passthrough calls then invoke an explicitly supplied configured hook followed by the caller's per-run hook. Kitaru-mocked calls do not invoke user tool hooks. Step recording completes before configured and caller `onStepFinish` callbacks.

Pass configured callbacks explicitly with `configuredOnStepFinish`, `configuredBeforeToolCall`, and `configuredAfterToolCall`. The wrapper never uses `getConfiguredToolHooks()`. Callbacks that are configured on the agent but not supplied to the wrapper cannot be preserved when replay must replace the same per-run hook.

Mastra merges per-run model settings with configured defaults. Kitaru can replace supplied keys but cannot remove configured keys it cannot inspect.

## JSON boundary

Recorded payloads preserve JSON values, convert dates to ISO strings, bigints to decimal strings, and errors to `{name, message}`. Functions, symbols, circular references, and non-finite numbers are replaced with a marker instead of failing the run, because a recording problem must not break the agent. Replay tool inputs go through the same bounded converter that records them, so a history cache key computed during replay matches the key the server computed from the recorded call. Serialization never changes the Mastra result or tool output returned to the application.

## Current scope

This experimental release supports non-streaming `Agent.generate()` with local function tools, including function-valued tools resolved from the run's `requestContext`. Replay rejects `prepareStep` and input processors because they can replace the model, prompt, or tools after preflight. Streaming, workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions, and LLM tool policy are intentionally not implemented.

Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or prefer static/history replay when execution must be suppressed.
