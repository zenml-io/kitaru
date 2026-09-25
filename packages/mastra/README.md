# `@zenml-io/kitaru-mastra`

Experimental recording and replay support for Mastra. `generate()` supports `@mastra/core >=1.51.0 <1.68.0`; recorded and replayed `stream()` calls require a stable `@mastra/core 1.67.x` release.

For native working and observational memory, use the opt-in [isolated memory replay factory](#isolated-memory-replay) on exact Mastra core 1.67.0 and memory 1.30.0, with a Kitaru server newer than 0.27.1.

This adapter depends on the framework-neutral `@zenml-io/kitaru` package, whose repository directory is `packages/core/`. The packages are versioned and released together.

```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.67.0
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

The wrapper calls the existing agent's public method. It does not recreate tools, inspect private agent fields, or change the returned Mastra result. For a per-run `generate()` with `structuredOutput.model`, it resolves the secondary model through public `getModel()` and wraps that model's `doStream()` for this invocation without mutating the original model.

## Streaming

On Mastra 1.67.x, `KitaruAgent.stream()` returns the native Mastra result and records model and local-tool steps as Mastra completes them, including during replay:

```ts
const output = await recorded.stream(messages, {
  structuredOutput: { schema: supportDecisionSchema },
});

for await (const chunk of output.textStream) {
  process.stdout.write(chunk);
}
```

Kitaru does not store token-by-token events. Mastra's final callbacks supply the completed steps and resolved output that Kitaru records. Schema-only structured output is supported and stays available on `output.object`; a secondary `structuredOutput.model` is not supported for streaming.

Ordinary setup failures, before native `stream()` starts, reject the initial call. Memory-backed streams initialize from a public Mastra input processor after recall so Kitaru can record the effective context. Mastra may return the native stream before that processor runs. In that case, initialization failure rejects the native aggregate such as `getFullOutput()` during consumption and prevents model and tool execution; it need not reject the initial `stream()` promise.

Recording failures after native execution starts stay separate from the application stream and do not disable later application tools. Supply `onRecordingError` to observe one bounded report without exposing the raw payload:

```ts
const recorded = new KitaruAgent(agent, {
  agentId,
  requestedModelId,
  onRecordingError: ({ stage, sessionId }) => {
    console.error(`Kitaru recording failed at ${stage}`, { sessionId });
  },
});
```

`stage` is `"setup"`, `"step"`, or `"complete"`, and `sessionId` is optional. `reason`, when present, is a short code for the failure; for a memory replay turn it is the session's `mastra_replay_reason`. The callback runs once and its return value is not awaited, so it cannot delay native completion. A thrown, rejected, or never-settling reporter does not change the Mastra result.

Failed sessions store a bounded failure category rather than the raw callback message, which can contain request bodies or credentials. Provider errors from the AI SDK, and errors that carry an HTTP status, also keep the status and up to 500 characters of the provider's message, with URL credentials, `Bearer` and `Basic` values, and `sk-`-style keys replaced by `REDACTED`, so a rate limit, an outage, and a bad key read differently. The native Mastra error and caller callbacks remain unchanged.

Mastra 1.67 continues model execution in the background when the application stops reading or cancels its reader. Kitaru records the eventual finish callback and completed result in that case. Kitaru does not drain the returned reader itself. After queued steps settle, the finish callback chooses the terminal status once. An error or abort observed before that decision records failure; a later abort cannot reverse completion because the API does not reopen terminal sessions.

## Recording with `KitaruAgent`

Each call creates isolated run state and:

1. creates a Kitaru session and in-progress root node;
2. records each completed Mastra step through public `onStepFinish`;
3. sends one LLM node and its local tool children in parent-before-child order;
4. completes the same root node and session after the agent succeeds.

Tool-node inputs are the arguments requested by the model. Mastra 1.51 exposes those same arguments to replay hooks and step results. Schema defaults and coercion happen later, inside tool execution, so they are not added to recorded inputs.

Recording uses the public response model, provider, usage, finish information, and provider metadata exposed by Mastra. The step callback has no public start time, so the adapter never claims provider-side latency.

Each LLM node records `requested_model` (the Kitaru model id the run asked for, before any replay override), `model` (the model id the provider says it served), and `model_provider` (the bare provider family, such as `openai`). Mastra reports transport-qualified provider strings such as `openai.responses`; the adapter keeps that original string as the `provider_id` attribute so evaluator model policies can match one exact provider family.

Recording is bounded on purpose. Parent step nodes record no model inputs, because the provider request body repeats the whole system prompt and message history on every step. Step outputs keep the finish reason, text, tool calls, tool results, tripwire details, and warnings; the session output keeps the finish reason, step count, and final text. Tool strings longer than 4096 characters, arrays longer than 100 items, objects with more than 100 keys, and nesting deeper than 8 levels are truncated by default. Set `recordingLimits` when your tools return larger values:

```ts
const recorded = new KitaruAgent(agent, {
  agentId,
  requestedModelId,
  recordingLimits: { maxStringChars: 6_000, maxItems: 120, maxDepth: 10 },
});
```

Each limit is a positive integer, at most 1,048,565 characters, 9,000 items per array or object, or 64 levels of nesting, respectively. The whole recorded value must also fit within 1 MiB and 9,000 items; an over-budget value becomes a bounded degraded marker. These limits apply to dedicated tool-call nodes; duplicate tool details in model-step summaries keep the default bounds. They do not change final stream text, model output, or provider metadata. Credential keys `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` remain redacted at every setting. The recorder marks truncated, degraded, or redacted tool values as incomplete. Provider metadata is not part of the replay contract, so it also hides values under keys that carry blobs or transport envelopes, such as `data`, `file`, `request`, and `url`.

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

## Replay with `KitaruAgent`

When `KITARU_REPLAY_ID` is set, `generate()` or `stream()` fetches the replay and applies its model, system-instruction, model-parameter, and tool-policy overrides through public per-run options and tool hooks. The stream entrypoint still returns Mastra's native result; callers must consume it through completion so Kitaru can finalize the replay session.

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

A tool-policy failure stops later live tools and model steps, and Kitaru records a failed replay session. Mastra turns a rejected tool hook into a tool-error result, so the adapter aborts the run and executes replay tool calls one at a time (`toolCallConcurrency: 1`). On `generate()`, the call rejects with the policy error. On `stream()`, Mastra may settle its native text or aggregate result without rejecting; inspect the Kitaru replay session for the final failure status. Throwing again from `onStepFinish` can leave Mastra's native stream unresolved, so Kitaru preserves its completion behavior.

Supported tool policies are passthrough, static, and history, including `fail`, `passthrough`, and `error_result` miss behavior. History lookup computes the same SHA-256 cache key as the Python server from the tool name and JSON inputs. A completed match replays its result, including `null`, without executing the live tool. A failed match throws its stored error and does not execute the live tool. A recorded tool call marked with incomplete arguments or results is a history miss and follows `on_miss`; `passthrough` therefore executes the live tool and may cause a side effect. Use `on_miss: "fail"` when that must not happen. Older recordings without a result-fidelity flag remain readable, but Kitaru cannot determine whether their results were truncated; record them again for trustworthy history replay. Imports retain their source payloads and do not use `recordingLimits`, although the executing adapter needs limits high enough to match imported tool arguments. The `llm` policy fails before tool execution because it is not supported in this release.

History matching is guaranteed only for traces recorded and replayed through this Mastra adapter. Another framework may validate, default, or serialize the same logical tool input differently, so cross-framework history replay is not a compatibility promise.

## Isolated memory replay

Import `createMemoryReplayAgent()` from `@zenml-io/kitaru-mastra/memory` when a consumed stream needs thread-scoped schema working memory, observational memory, or controlled input processors. This opt-in factory requires exactly `@mastra/core@1.67.0` and `@mastra/memory@1.30.0`, and a Kitaru server newer than 0.27.1 (see [Recording readiness](#recording-readiness) for what happens on an older server). For PostgreSQL storage, use `@mastra/pg` 1.25.x: `@mastra/pg` 1.26.0 and later require `@mastra/core` 1.68 or later. Kitaru tests memory replay against `@mastra/pg` 1.25.0. The existing `KitaruAgent` wrapper stays at the package root, keeps its history-only memory behavior, and does not require `@mastra/memory`.

```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.67.0 @mastra/memory@1.30.0 zod
```

The factory creates a fresh native agent for each invocation. A baseline uses your source storage and records the starting state before recall, then records the observer and reflector model outputs produced during that invocation. Replay restores the starting state into a separate in-memory store and runs the actor again. Memory changes during replay in two different ways:

- Working memory updates live. When the actor calls Mastra's working-memory tool, the tool runs and writes to the isolated store, so a changed prompt or model can produce different working memory.
- Observational memory (OM) is replayed. Kitaru hands the recorded observer and reflector outputs to native OM, which writes them to the isolated store as it did in production.

Replay never calls `sourceMemory()`, never writes to your source storage, and never calls an observer or reflector model.

Each replay OM call takes the unused recorded output with the same phase (observer or reflector), model method, and input. The input comparison ignores message times, dates, generated ids, and how an attachment is held: a declared URL, its captured reference, or its downloaded bytes. Replayed OM models accept captured references, so Mastra never tries to download one. Mastra also counts an attachment's tokens from its URL, sometimes by asking the provider, and those counts decide when OM observes. A baseline therefore records the tokens OM counted for each declared attachment, and replay reuses them instead of counting the captured reference or calling the provider. Mastra's number of OM calls depends on timing: a slow production observer merges buffering rounds that an instant replay makes separately, and it covers messages the actor produced while it ran. A buffered call (async observation or reflection) whose input matches no unused output therefore gets no result, because another window's output could describe messages the replay has not produced yet. Its messages stay in the actor's context, as they did in production while the observer ran, and a later buffered call usually matches the recorded window. A blocking call whose input matches no unused output takes the next unused output of its phase, and replay records an `om_input_mismatch` span. A blocking call after its phase's recorded outputs are used up fails replay with `KITARU_REPLAY_DIVERGED:mastra_om_call_order`, because an empty observation would drop the observed messages from the actor's context. So does any call whose phase has no recorded output at all. By default, no OM call reaches a provider. To let such a replay finish instead, set `missingObservationalMemoryResults: "live"` on `createMemoryReplayAgent`. A blocking call with no recorded output then calls the observer or reflector model that `resolveModel` returns for the recorded identity, with captured files sent as their recorded bytes, and recorded outputs still answer every other call. Buffered calls never go live. Each live call is recorded as an `llm_call` node named `om_observer_live_call` or `om_reflector_live_call`, and the replay session reports how many ran in `metadata.mastra_om_live_calls`, because part of its memory no longer comes from what production observed. Replay reports the other departures in an `om_call_divergence` span and in the session's `metadata.mastra_om_divergence` counts: `input_mismatches` (blocking calls that took another input's output), `surplus_calls` (buffered calls after their phase's outputs were used up), `unused_results`, and `live_calls` (blocking calls the live model answered). A baseline also records failed OM attempts, so a turn whose observer succeeded after Mastra retried it stays eligible, and its replay serves the successful output directly. When a failed blocking observation or an input processor's `abort()` ends the Mastra stream with a tripwire, the session still closes and the lease is released: a baseline becomes `ineligible` and a replay fails. Reusing recorded outputs lets you compare actor instruction/model changes, but does not measure how a fresh observer or reflector would respond to the changed conversation.

The following binding uses a process-local store. Supply your existing public memory storage domain and its complete configuration for a persistent application:

```ts
import { InMemoryStore } from "@mastra/core/storage";
import { Memory } from "@mastra/memory";
import {
  createMemoryReplayAgent,
  createProcessLocalMemoryAccess,
} from "@zenml-io/kitaru-mastra/memory";
import { z } from "zod";

const store = new InMemoryStore();
const sourceMemory = new Memory({
  storage: store,
  options: {
    semanticRecall: false,
    workingMemory: {
      enabled: true,
      scope: "thread",
      schema: z.object({ preference: z.string() }),
    },
  },
});
// Share this same instance with every writer, for the lifetime of the store.
const exclusiveAccess = createProcessLocalMemoryAccess();
const recorded = createMemoryReplayAgent(
  ({ memory }) => ({
    id: "support",
    name: "Support",
    memory,
    instructions: () => "Remember the user's preferences.",
    model: () => "openai/gpt-5-mini",
    defaultOptions: () => ({ maxSteps: 3 }),
  }),
  {
    agentId: process.env.KITARU_AGENT_ID!,
    requestedModelId: "openai/gpt-5-mini",
    allowedReplayModels: ["openai/gpt-5-mini"],
    sourceMemory: () => ({
      domain: store.stores.memory!,
      configuration: sourceMemory.getMergedThreadConfig(),
      settled: () => sourceMemory.settled(),
      memory: sourceMemory,
      exclusiveAccess,
    }),
    resolveModel: (id) => {
      if (id !== "openai/gpt-5-mini") throw new Error(`Unknown model: ${id}`);
      return "openai/gpt-5-mini";
    },
  },
);
const output = await recorded.stream("My preference is green.", {
  memory: { thread: "support-thread", resource: "customer-123" },
  context: [{ role: "system", content: "The customer is asking about preferences." }],
});
await output.consumeStream();
// Keep the application and source store alive while recording finalizes.
// Inspect session eligibility before shutting down or starting a replay.
```

Run this entrypoint with `KITARU_API_URL`, a Kitaru credential, an existing `KITARU_AGENT_ID`, and the model provider credential. Register the compiled command as the agent version's run specification to run it through a worker. The same command serves baseline and replay tasks; the worker supplies the recorded input and replay identity.

### Source ownership and supported configuration

All writers to a source thread or resource must participate in the same `MastraExclusiveMemoryAccess` implementation. The process-local helper works only when every writer shares that instance in one process. A new turn waits up to 100 ms for an earlier turn on the same thread or resource to release it; this limit is fixed and not configurable. A recorded turn holds both selectors until its memory writes, including delayed observational-memory work, have settled, or until `finalizationWaitMs` passes (60 seconds by default, after which the turn is ineligible). Kitaru then releases the selectors before it uploads the remaining evidence and the final session update. Turns that overlap on either selector still answer natively; only the overlapping turns become ineligible for replay, and later turns are unaffected. One overlap is common in chat and costs only the later turn: once a turn's native answer has finished and only its buffered observation or reflection is still running, a reply that starts on the same thread is ineligible with `om_work_unjoined`, and the earlier turn stays eligible. This holds only when that reply is another turn of this adapter using the same lease; any other overlapping writer still makes both turns ineligible. A turn never waits for buffered observational-memory work that no lease holds, such as work left by a turn that ran natively: it starts at once and is ineligible with `om_work_unjoined`. The selectors Kitaru coordinates on are the ones Mastra uses, including the reserved `mastra__threadId` and `mastra__resourceId` request-context keys. Pass the source `Memory` as `memory` so that its `settled()` also waits, for up to `finalizationWaitMs`, for the observational-memory work of recorded turns before you close storage. `settled()` does not provide exclusive access.

A multi-process or multi-server deployment must supply a backend using shared atomic storage; Kitaru does not include a production distributed lease backend. `acquire()` returns a callable release function with `verifyEligibility()`. The backend must atomically reserve both thread and resource IDs. When either ID is still held after `waitMs`, it must invalidate the current holders and return a lease that is not eligible but holds both IDs until it is released; that invalidation ends once every overlapping lease has been released. There is one exception, which the backend may leave out: after a turn's native answer finishes, Kitaru calls the lease's optional `markFinalizing()`. An acquisition with `cooperative: true`, which Kitaru passes for its own turns and their writes, must then not invalidate that holder when every holder still in the way is finalizing or already ineligible. It returns a lease that is not eligible, sets `overlapsFinalizingTurn: true`, and holds both IDs until it is released. A backend without `markFinalizing()` invalidates on every overlap, which is stricter but safe. `waitMs: 0` must not wait: Kitaru registers each write it makes outside its own lease this way and releases the registration when the write finishes. Kitaru never renews a lease, so the backend must also end a lease whose holder process died without releasing it: give each lease a time-to-live longer than your longest turn plus `finalizationWaitMs`, or tie it to a liveness check of the holder. `verifyEligibility()` must return false once a lease has expired. Without this, a server that stops mid-turn, for example during a rolling deploy, leaves every later turn on that thread and resource ineligible. `markUnsafeWrite()` is only for a write that could not register, because coordination failed or its selector is unknown, and that marker must survive process loss. Kitaru never calls `resetAfterQuiescence()`. Your application calls it for the marked selectors, or with no selector after an unknown-selector marker, once every process that might have written without registering has stopped or restarted. Coordination failure must prevent replay eligibility even when native writes continue.

The two ways a turn loses eligibility through the lease last for different times:

- Overlap invalidation from `acquire()` lasts only until every overlapping lease is released. The next turn after that is eligible again.
- A `markUnsafeWrite()` marker lasts until `resetAfterQuiescence()` clears it. Until then, every turn on the marked thread or resource, or on every thread after an unknown-selector marker, still answers natively but is recorded as ineligible with `memory_lease_conflict`. The process-local helper keeps its markers in memory, so they also end when that process restarts.

Validate these guarantees against your actual storage, deployment topology, and failure recovery before enabling production replay; the process-local example does not establish customer deployment readiness.

Schema working memory requires explicit `scope: "thread"`. An observational-memory configuration object may omit `scope`, using Mastra's implicit thread scope, or set it to `"thread"`. Supply explicit observer/reflector model identities, either shared through `observationalMemory.model` or in the phase configuration.

Some configurations make every turn ineligible:

- An `extract` list of `Extractor` instances in the observation or reflection configuration gives `om_config_unsupported`. An extractor runs application code, and the recorded configuration cannot carry code into replay. The built-in extractors that Mastra itself stores in OM records (`current-task`, `suggested-response`, `thread-title`) are recorded by name and supported.
- An observer or reflector model without a static identity, such as a function, or one that `resolveModel` cannot turn into a stream-capable model, also gives `om_config_unsupported`.
- Resource-scoped working memory or OM, semantic recall, automatic title generation, per-call `memory.options`, and memory options other than `readOnly`, `lastMessages`, `workingMemory`, `observationalMemory`, and `filterIncompleteToolCalls` give `memory_config_unsupported`.

Keep `memory.thread` and `memory.resource` consistent with reserved Mastra thread/resource IDs in `requestContext`. A mismatch cannot produce an eligible recording. Baseline callbacks receive the original live context. `captureRequestContext` selects only approved, replay-relevant JSON values; it does not remove values from the live context. Replay receives that recorded projection. A nonempty context without an explicit projection makes the recording ineligible. For example, return `{ locale: context.get("locale") }` when locale is the only value replay needs, or `{}` when none are needed.

Never include authentication tokens, credentials, or signed URLs in the projection. Credential-like context keys are rejected, and transport headers in recorded configuration make the envelope incomplete. These checks cannot identify every secret hidden in an arbitrary string; choose recorded fields explicitly. Use `resolveModel` to reconstruct model instances from locally configured credentials.

Dynamic `instructions`, `model`, and `defaultOptions` resolve during baseline setup; replay uses their recorded values. `resolveModel` must resolve the recorded actor, observer, and reflector model identifiers and any allowed actor override. OM identifiers must resolve to native stream-capable model objects, whose provider methods Kitaru intercepts to reuse recorded results during replay. A `system_prompt` override replaces only application instructions and retains recorded extra system context. Model and model-setting overrides affect the actor; observation and reflection retain their recorded configuration and outputs. Raw-input `prompt` overrides are rejected; record a new baseline to change invocation input.

### Recording readiness

Native output and replay readiness are separate. Consume the baseline stream normally. Once the actor finishes, Kitaru finalizes recording in the background: it joins native memory work, records OM results and memory evidence, verifies source ownership, and persists the final input. Keep the application and source storage alive until that finalization finishes. `consumeStream()` alone is not a recording-completion barrier for a baseline.

Inspect the baseline session's metadata and status:

- `mastra_replay_state: "pending"`: recording has not finalized; do not replay yet.
- `mastra_replay_state: "eligible"`: the complete version-3 input and evidence were persisted for replay.
- `mastra_replay_state: "ineligible"`: recording could not establish a complete, isolated baseline. Inspect `mastra_replay_reason` and `mastra_native_state` to distinguish recording failure from native execution failure, then record a new baseline after resolving the cause.

Recording-only problems do not replace the baseline's native answer. When the native answer succeeded but its recording cannot be used, the session is `completed` with the answer as its output and is marked `ineligible`; only a failed native turn produces a `failed` session, whose error is `KITARU_RECORDING_INCOMPLETE:<reason>`. A turn that ran natively because its recording could not be set up gets a session without steps, closed the same way once the native turn ends. `onRecordingError` receives the same code as `reason`, and `stage` is `"setup"` for these turns. `mastra_replay_reason` names the cause:

| Reason | What happened |
|---|---|
| `replay_input_too_large` | The thread's memory, or another part of the replay input, is over the replay size budget (16 MiB, 200,000 JSON values, or depth 64). |
| `credential_key_unsupported` | Memory, request context, or evidence has a credential-named key such as `token`, `password`, or `headers`. |
| `om_config_unsupported` | Observational memory uses `extract` extractors or a model without a static identity, such as a function. |
| `memory_config_unsupported` | The memory configuration uses options outside isolated replay, such as semantic recall or resource scope. |
| `agent_config_unsupported` | The agent or its run options use features outside isolated replay. |
| `model_identity_unsupported` | The actor model has no static identity, for example a fallback array. |
| `memory_store_shape_unsupported` | The memory store returned records Kitaru cannot represent or validate. |
| `om_work_unjoined` | Buffered observation or reflection from an earlier turn was still running when the turn started, including a reply that started while the earlier recorded turn was finalizing, or stored OM records show running work or a flag that was never cleared. |
| `memory_read_failed` | Reading the thread's memory from storage failed. |
| `memory_capture_timeout` | Reading the thread's memory did not finish in time. |
| `memory_lease_conflict` | Another writer overlapped the turn, or a write happened without the lease. |
| `memory_lease_unavailable` | The lease backend failed or did not answer in time. |
| `memory_mutation_failed` | A native memory write failed. |
| `om_tape_incomplete` | An observer or reflector result could not be recorded. |
| `om_settle_timeout` | Observational-memory work did not finish within `finalizationWaitMs`. |
| `request_evidence_incomplete` | The model request could not be recorded faithfully. |
| `recorded_evidence_unsupported` | Evidence contains a value the replay codec cannot represent, such as a function. |
| `context_unsupported`, `context_mutated_after_capture` | Request context could not be captured, or changed after capture. |
| `version_mismatch` | The installed Mastra packages are not the supported versions. |
| `file_capture_timeout` | Declared files did not download within `fileCaptureWaitMs`. |
| `file_capture_failed` | A thread history file that the turn resolved failed to download, or the turn's files exceed the capture limits. |
| `file_store_failed` | Kitaru could not store the turn's captured files as blobs on the Kitaru server. |
| `file_url_undeclared` | A file or image part in the input holds a network URL that `files` did not declare, thread history holds one and no `resolveFile` was supplied, or a processor passed the factory's `resolveFile` a URL that is neither declared nor in a file or image part of thread history. |
| `file_url_sent_to_model` | A file or image part reached the model as a URL instead of its content, so the provider or Mastra would fetch it outside `resolveFile`. |
| `recording_setup_timeout`, `recording_setup_failed` | Kitaru did not open the session in time, or could not open it. |
| `recording_step_failed`, `recording_evidence_failed`, `recording_flush_timeout` | Kitaru did not accept some evidence, or not in time. |
| `server_rejected_finalization` | The server refused the replay inputs, usually because it predates memory replay. |
| `native_run_failed` | The native turn itself failed. |

The remaining codes, such as `memory_evidence_incomplete`, `capture_setup_failed`, and `recording_finalization_failed`, cover causes the codes above do not name. A Kitaru outage can prevent even these diagnostics from being persisted; missing status updates are not evidence of successful recording.

The server stores two reasons of its own when a pending baseline is closed without a replay decision. Closing it as `failed` stores `abandoned`; this is how you clean up after a recorder that stopped mid-turn, because a plain `failed` session update is accepted. Closing it as `completed` stores `unfinalized`. A baseline still pending after 30 minutes is refused as `mastra_replay_abandoned` when replay is requested; this does not cancel a native turn or release a source lease. A baseline whose replay input lacks the recorded key order or turn start time, which early builds of this adapter did not store, is refused as `mastra_replay_recording_outdated`; record the turn again.

Memory replay needs a Kitaru server newer than 0.27.1. Kitaru 0.27.1 and earlier answer the final session update, which carries the replay input, with HTTP 422. The adapter then completes the session with its answer, marks it `ineligible` with `server_rejected_finalization`, and calls `onRecordingError`. The native answer is unaffected, but no turn recorded against such a server can be replayed.

A slow or unresponsive Kitaru server does not hold up the native answer. Before the model starts, a baseline turn waits up to `sessionSetupWaitMs` (2 seconds by default) for Kitaru to open its session. If Kitaru has not answered by then, the turn runs natively and is not recorded; a session that opens later is closed as ineligible with `recording_setup_timeout`. Model steps, memory changes, and other evidence upload in the background, in order, without delaying the stream. They must finish within twice `finalizationWaitMs` of the stream closing; otherwise Kitaru cancels the remaining uploads and closes the session as ineligible with `recording_flush_timeout`. The client `timeoutMs` bounds each background request to Kitaru, including the final session update.

The server refuses to replay a pending, ineligible, or incomplete memory baseline, and names the cause as `mastra_replay_<reason>`, for example `mastra_replay_pending` or `mastra_replay_memory_lease_conflict`:

- Creating a single replay of a refused baseline returns HTTP 409. CLI and MCP error details include the baseline's `session_id` next to the `reason`.
- An experiment run does not reject its whole cohort. Each refused baseline becomes a failed replay with no job, whose error is `Session <id>: mastra_replay_<reason>`, and the other baselines still run. A run in which every baseline is refused fails immediately.

Do not retry a pending session by supplying provisional inputs yourself.

### Create and inspect a memory replay

The SDK, CLI, and native MCP server support this workflow without a frontend. First inspect the baseline with `kitaru session get <baseline-session-id> --output json` and wait for `metadata.mastra_replay_state` to become `eligible`. Then create a replay using an existing evaluator:

```bash
kitaru replay create <baseline-session-id> \
  --evaluator your-evaluator@1 \
  --override '{"system_prompt":"Use the recorded preferences when answering."}' \
  --tool-policy '{"default":{"type":"history","scope":"baseline","on_miss":"fail"},"tools":{}}' \
  --output json
kitaru job watch <job-id>
kitaru replay get <replay-id> --output json
kitaru session get <result-session-id> --output json
kitaru session nodes <result-session-id> --include-payloads --output json
```

Read `result_session_id` from the replay, check the session's final status, and inspect its model-request and memory-mutation nodes. `session nodes` returns one page; pass a non-null `page.next_cursor` back with `--cursor` until it is null.

The Python SDK uses the same replay request. Given an authenticated `client`, a baseline UUID, and an existing evaluator:

```python
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import ReplayOverride, ToolPolicy

replay = await client.replays.create(
    ReplayCreateRequest(
        baseline_session_id=baseline_id,
        override=ReplayOverride(system_prompt="Use the recorded preferences."),
        tool_policy=ToolPolicy.model_validate(
            {
                "default": {"type": "history", "scope": "baseline", "on_miss": "fail"},
                "tools": {},
            }
        ),
        evaluators=[EvaluatorConfig(evaluator="your-evaluator", version=1)],
    )
)
```

Use `client.replays.get(replay.id)` to follow completion and obtain the result session. Fetch its inputs with `client.sessions.get()` and iterate nodes with `client.sessions.iter_nodes()` and `SessionNodeListParams(include_payloads=True)`.

The native MCP server starts these replays through an experiment. Use `kitaru_cohorts_manage` to create a cohort and a version containing the baseline session, `kitaru_experiments_manage` to configure the same override, policy, and evaluator, then `kitaru_workflow_start` with `operation: "experiment_run"`, the experiment ID, cohort-version ID, and agent-version ID. No separate MCP replay-creation tool is required.

Inspect the run with `kitaru_activity_read`: get `kind: "experiment_run"`, list `kind: "replay"` filtered by `experiment_run_id`, then get its result session. To read evidence, use `operation: "list_children"`, `kind: "session_nodes"`, `parent_id: "<result-session-id>"`, and `include_payloads: true`; follow the returned cursor until all pages have been read. A read-only MCP connection can inspect these results but cannot start experiments.

### Files, skills, and processors

Pass a static `inputProcessors` array in the factory configuration. File processors must use the factory's supplied `resolveFile`; declare every URL it may fetch in the adapter's `files` option and provide a baseline resolver returning `{ bytes: Uint8Array, mediaType: string }`. `files` is either a fixed list or a function that Kitaru calls once per recorded turn with `{ input, options }` (the call's input and stream options) and that returns the URLs for that turn. Use the function when URLs change per request. Declare every network URL in a file or image part of the input too. Replay would otherwise have to fetch it, so such a turn answers normally but is ineligible with `file_url_undeclared`. You do not need to declare URLs in file or image parts of thread history: they are part of the recorded conversation, so when you supply `resolveFile`, Kitaru captures each one when a processor passes it to the factory's `resolveFile`. A processor that re-reads recent attachments from history on every turn therefore works with `files: []`. Kitaru downloads nothing else from history, so older attachments that no processor reads add no download time, do not count toward the capture limits, and cannot make the turn ineligible, even when they were deleted. A history URL that reaches the model still as a URL, because no processor replaced it with bytes, makes the turn ineligible with `file_url_sent_to_model`: the provider or Mastra would fetch it outside `resolveFile`, and replay has only the recorded bytes. Declare history URLs that appear only in text, such as a link in a message, when a processor resolves them. When a processor passes the factory's `resolveFile` a URL that `files` did not declare, a baseline turn fetches it with your `resolveFile` as a native turn would, answers normally, and is ineligible with `file_url_undeclared`; a replay refuses it.

Kitaru fetches each declared file once per turn and records its bytes and media type. It starts every declared download before the model runs and waits up to `fileCaptureWaitMs` (10 seconds by default) for all of them. When a download has not finished by then, the turn runs natively and is recorded as ineligible with `file_capture_timeout`. A thread history file downloads when a processor resolves it, through your `resolveFile`, exactly as in a native turn, and Kitaru records the bytes it returns. When that download fails, the processor sees the error as it would natively, and the turn is ineligible with `file_capture_failed`; so is a turn whose resolved files exceed the limits below. Whenever a baseline turn falls back to a native run, the factory's `resolveFile` takes over the download Kitaru already started for a URL, running or finished, instead of fetching that URL again. In the recorded input and the recorded thread history, a value that is exactly a declared URL, as declared or in its `new URL(url).href` form, becomes a `kitaru-file://sha256/...` content reference; URLs inside text keep their text. Replay resolves these references from recorded content, verifies the hash, and does not fetch the original URL. A processor must pass the file reference from its current input or history to the injected resolver; do not close over the original signed URL. Undeclared URLs in the call's file and image parts, and missing or altered recorded content, fail instead of falling back to a network request. Capture accepts at most 64 distinct file URLs, declared and resolved from history together, and 16 MiB of file bytes in total; one file may use the whole 16 MiB. Once the native answer has finished, Kitaru stores each captured file as a blob on the Kitaru server. The server keeps one copy of identical content, and a process that stored a file on an earlier turn does not upload it again. The replay input keeps only each file's content reference, media type, length, SHA-256, and blob id, so file bytes do not count against the replay input limit below. A replay task downloads the blobs its replay input names and checks each one against its length, hash, and content reference. When the files cannot be stored, the turn is ineligible with `file_store_failed`. Deleting such a blob makes the turns that recorded it fail to replay.

Recorded data never keeps URL credentials. In recorded input, thread history, model requests, tool calls, memory changes, observational-memory results, and outputs, each URL keeps its text but its credentials become `REDACTED`: userinfo, credential-named query, fragment, and path parameters such as `token`, `key`, `sig`, `X-Amz-Signature`, or `client_secret` (including `&amp;`- and `\u0026`-escaped ones and ones percent-encoded, once or several times, inside another URL), JWT-shaped values, and webhook or bot secrets in the path. Pagination parameters such as `page`, `cursor`, or `pageToken` stay unchanged. URL credential redaction does not make a recording ineligible. A declared file's recorded entry holds only its content reference, media type, and stored bytes, not the original URL, so a download token in a declared URL is never stored and replay serves the file from the recorded bytes. Replay sees the redacted text, so a processor that must read such a file needs it declared in `files`.

The file guarantee covers only requests that go through the factory's `resolveFile`. When application code, such as a processor, a tool, or your own helper, calls `fetch()` or another HTTP client itself, Kitaru does not record the response and does not stop the request during replay. In replay, that code reads either a `kitaru-file://` reference or a URL whose credentials are `REDACTED`, so a direct request usually fails. Route every attachment download through the supplied `resolveFile`.

For skills, set `skillsDirectory` to the directory containing your skill folders and use the factory's supplied `workspace`. Kitaru reads skill files into an immutable native workspace and records their paths, sizes, and hashes. Deploy the same skill artifact with the replay command. The skills tree is limited to 1 MiB of file content and 10,000 files/directories. Changed files, missing files, and symlinks are rejected before replay execution.

The factory must use the supplied memory and workspace instances. Processors and tools are application code: their dependencies must use these supplied bindings for replay isolation. Kitaru does not sandbox arbitrary callbacks or prevent code from opening another database connection or making a network request. Workflows, subagents, provider-executed tools, approval/resume modes, dynamic tool inventories, `prepareStep`, output processors, and secondary structured-output models remain unsupported.

### Tool policies and evidence

Native memory tools execute against the isolated replay store, including under `history` with `on_miss: "fail"`. External tools, including tools added by a processor, follow the replay tool policy. A tool named `updateWorkingMemory` does not acquire the native-memory exemption by name. Use history with a failing miss when external tools must not execute.

Eligible session inputs contain a version-3 `mastra_memory_replay` envelope with the invocation, initial thread/resource/messages, observational state and buffers, effective configuration, approved request context, controlled file bytes, and ordered OM results (`omTape`). Supported dates and binary values retain their types; declared file URLs become content references. Database stores such as `@mastra/pg` return some OM buffer dates as ISO strings, and capture turns exact ISO timestamps back into dates. The envelope records whether the source was Mastra's `InMemoryStore` or a database store (`configuration.memoryStore`), and the isolated replay store copies that kind of store's behavior, for example returning copies of OM records and carrying the previous record's `lastObservedAt` into a new reflection. Older history-only snapshots cannot recover this state. OM recordings without recorded results must be recorded again with the factory. The envelope also records when the turn started (`turnStartedAt`) and each object's key order (`keyOrder`, with a SHA-256 of the recorded envelope). Storage such as PostgreSQL `jsonb` re-sorts object keys, so replay restores the recorded order, and tool arguments, tool results, schemas, and working-memory templates reach the model exactly as production sent them. Replay refuses an envelope whose restored content no longer matches that hash. Replay also computes observational memory's relative date labels ("today", "2 weeks ago") and its `activateAfterIdle` check from the recorded start time, advancing at wall-clock speed, so a turn replayed weeks later gets the same memory context production did.

Inputs remain bounded and self-contained. Unsupported values, credential-key redaction, or exceeding the 16 MiB serialized UTF-8 JSON limit make a recording ineligible. The replay input also has a 200,000-item budget and maximum depth of 64. Each binary value in memory is limited to 8 MiB before base64 encoding; encoded binary values and OM outputs consume the shared JSON budget, while captured files are stored as blobs outside it. These larger bounds apply to the memory replay input, not to every ordinary recorded node. The adapter reads the full initial thread history; `lastMessages` does not make recording unbounded or restrict that snapshot to the actor's recall window. A pre-turn capture that has not finished after five seconds becomes ineligible so a stalled storage read does not indefinitely delay the native answer. Large documents or long threads may therefore require a smaller baseline.

Unlike ordinary wrapper recording, this path records the effective actor prompt, tools, tool choice, and supported settings for each provider attempt, including failed retries. Request attributes include attempt identity, memory revision, source provenance, and evidence completeness. `memory_mutation` span nodes record ordered native storage changes and link them to the active actor attempt when one exists. This evidence describes the request sent at the adapter's model boundary, not a provider's internal processing.

Each request, mutation, or tool call node has the same budget as the replay input: 16 MiB of serialized JSON, 200,000 items, and 64 levels of nesting. A history tool policy serves only a result that was recorded whole, so a memory turn records tool arguments and results on this budget too, for example a list of 1,400 rows of 10 fields. When storage returns the messages it just saved, the result records each unchanged message as a `savedMessageRef` with its id and SHA-256 instead of a second copy. A node over the budget stores a degraded marker that names the exceeded bound. When you set `recordingLimits`, they also truncate each recorded request and tool payload on this path, and a truncated tool result cannot be served from history. Truncated or degraded evidence sets `request_evidence_truncated` or `evidence_truncated` and lists the exact reason in `request_incomplete_reasons` or `evidence_truncation_reasons`. It does not make the turn ineligible, because replay rebuilds memory and requests from the replay input, not from these nodes.

Consume replay streams through completion and inspect the replay session's final status and evidence completeness. Replay finalization waits for isolated memory work and closes its store. Mastra can settle a native stream after a policy failure, so native output alone does not establish replay success. Missing or incomplete starting state fails replay before model execution; a later OM call mismatch can fail after actor execution has begun. A failed or incomplete recording is not proof that all evidence was saved.

## Callback composition

Per-run Mastra hooks replace configured hooks. During replay, Kitaru evaluates its policy first. Passthrough calls then invoke an explicitly supplied configured hook followed by the caller's per-run hook. Kitaru-mocked calls do not invoke user tool hooks. Step recording completes before configured and caller `onStepFinish` callbacks.

Pass configured callbacks explicitly with `configuredOnStepFinish`, `configuredBeforeToolCall`, and `configuredAfterToolCall`. The wrapper never uses `getConfiguredToolHooks()`. Callbacks that are configured on the agent but not supplied to the wrapper cannot be preserved when replay must replace the same per-run hook.

Mastra merges per-run model settings with configured defaults. Kitaru can replace supplied keys but cannot remove configured keys it cannot inspect. Streaming preflight also resolves public default and tool functions. Those resolvers must be deterministic and side-effect-free for the request context: Kitaru and Mastra may invoke them more than once, with no exact invocation-count guarantee. Kitaru cannot detect every changing resolver through the public API.

## JSON boundary

Recorded payloads preserve JSON values, convert dates to ISO strings, bigints to decimal strings, and errors to `{name, message}`. Functions, symbols, circular references, and non-finite numbers are replaced with a marker instead of failing the run, because a recording problem must not break the agent. Replay tool inputs go through the same bounded converter that records them, so a history cache key computed during replay matches the key the server computed from the recorded call. Serialization never changes the Mastra result or tool output returned to the application.

## Existing wrapper scope

This experimental release supports `Agent.generate()` with Mastra `>=1.51.0 <1.68.0` and consumed `Agent.stream()` calls, including replay, on stable Mastra 1.67.x. Streaming supports local function tools and schema-only structured output. It rejects user `prepareStep` and input processors, approval and resume modes, background or `untilIdle` execution, and secondary structured-output models before native execution. Both replay entrypoints reject `prepareStep` and input processors because they can replace the model, prompt, or tools after preflight. Workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions, and LLM tool policy are intentionally not implemented.

Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or prefer static/history replay when execution must be suppressed.
