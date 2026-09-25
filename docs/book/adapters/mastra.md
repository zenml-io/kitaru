---
description: Record Mastra agent runs, import existing trace exports, and replay with recorded conversation context
icon: robot
---

# Mastra

The Kitaru Mastra adapter wraps an existing Mastra `Agent` and records `generate()` calls and supported streams as Kitaru [sessions](../concepts/agents-and-sessions.md). Mastra still runs the agent and Kitaru returns the native Mastra result unchanged. For thread-scoped working and observational memory, use the opt-in [isolated memory replay factory](#isolated-memory-replay).

{% hint style="warning" %}
`@zenml-io/kitaru-mastra` supports Node `>=22.22.0 <23 || >=26 <27`. `Agent.generate()` supports `@mastra/core >=1.51.0 <1.68.0`; recorded `Agent.stream()` calls require a stable Mastra 1.67.x release.
{% endhint %}

To bring in runs already recorded by Mastra, use [Import existing Mastra traces](#import-existing-mastra-traces). Importing an export does not require the original run to have used `KitaruAgent`.

## Install

{% tabs %}
{% tab title="pnpm" %}
```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.67.0
```
{% endtab %}

{% tab title="npm" %}
```bash
npm install @zenml-io/kitaru-mastra @mastra/core@1.67.0
```
{% endtab %}
{% endtabs %}

The adapter includes the framework-neutral `@zenml-io/kitaru` TypeScript package as a dependency.

## Wrap an agent

Create your Mastra agent as usual, then pass it to `KitaruAgent`:

```ts
import { Agent } from "@mastra/core/agent";
import { KitaruAgent } from "@zenml-io/kitaru-mastra";

const agent = new Agent({
  id: "support-agent",
  name: "Support agent",
  instructions: "Answer support requests using the available tools.",
  model: "openai/gpt-5-mini",
  tools,
});

const recordedAgent = new KitaruAgent(agent, {
  agentId: process.env.KITARU_AGENT_ID!,
  agentVersionId: process.env.KITARU_AGENT_VERSION_ID,
  requestedModelId: "openai/gpt-5-mini",
  allowedReplayModels: ["openai/gpt-5-mini", "openai/gpt-5"],
  resolveModel: (modelId) => modelRegistry[modelId],
});

const result = await recordedAgent.generate(messages, options);
console.log(result.text);
```

Configure the adapter subprocess with `KITARU_API_URL` and either the worker-provided `KITARU_API_TOKEN` or `KITARU_API_KEY`. A separate Node management driver can use [`createKitaruClient()`](../deploy/sdks.md) to reuse `kitaru login` without exporting a token. The wrapper calls the existing agent's public method. It does not recreate tools, inspect private agent fields, install model middleware, or replace the returned result.

`requestedModelId` is the Kitaru model identifier for the normal run. `allowedReplayModels` limits which replay model overrides the process will accept. When a replay selects another allowed model, `resolveModel` turns its Kitaru identifier into a Mastra model configuration. If no replay can change the model, `resolveModel` can be omitted.

## Stream an agent

On Mastra 1.67.x, call the public wrapper and consume its native text stream in the ordinary way:

```ts
const output = await recordedAgent.stream(messages, {
  structuredOutput: { schema: supportDecisionSchema },
});

for await (const chunk of output.textStream) {
  process.stdout.write(chunk);
}
```

Kitaru records completed model and local-tool steps plus the final resolved output. It does not store token-by-token events or introduce a Kitaru streaming protocol. The same entrypoint can replay a recorded stream, using the worker's replay configuration before Mastra starts. Replay returns Mastra's native stream object. Schema-only structured output is supported and stays available on `output.object`. A separate `structuredOutput.model` is not supported for streaming.

For ordinary streams, setup happens before Mastra starts and a setup failure rejects the initial `stream()` call. Memory-backed streams are different: Kitaru initializes from a public Mastra input processor after native recall so it can record the effective context. Mastra may return the stream object before that processor runs. A setup failure then rejects native aggregate consumption such as `getFullOutput()` and prevents model or tool execution; it does not necessarily reject the initial `stream()` promise.

Once native execution starts, a Kitaru step or completion write failure does not replace Mastra's chunks or aggregate result, and it does not disable later application tools. Observe it separately with the typed callback:

```ts
const recordedAgent = new KitaruAgent(agent, {
  agentId,
  requestedModelId,
  onRecordingError: ({ stage, sessionId }) => {
    console.error(`Kitaru recording failed at ${stage}`, { sessionId });
  },
});
```

The callback runs once. `stage` is `"setup"`, `"step"`, or `"complete"`, and `sessionId` is optional. `reason`, when present, is a short code for the failure; for a memory replay turn it is the session's `mastra_replay_reason`. Kitaru does not include prompts, outputs, credentials, or raw HTTP bodies in its default diagnostic. It does not await the callback's result, so a reporter that throws, rejects, or never settles cannot hold the application stream open.

Failed sessions store a bounded failure category rather than the raw callback message, which can contain request bodies or credentials. Provider errors from the AI SDK, and errors that carry an HTTP status, also keep the status and up to 500 characters of the provider's message, with URL credentials, `Bearer` and `Basic` values, and `sk-`-style keys replaced by `REDACTED`, so a rate limit, an outage, and a bad key read differently. The native Mastra error and caller callbacks remain unchanged.

Mastra 1.67 continues model execution in the background when the application leaves the stream unconsumed, exits a loop early, or cancels its reader. Kitaru records the eventual finish callback and completed result. It does not drain the returned reader itself or invent a final output. Kitaru marks the session failed when Mastra exposes an error or abort. User `prepareStep` and input processors are rejected before recording because they can replace tools or structured-output models after preflight. The adapter-owned memory-capture processor remains supported. After queued steps settle, the finish callback chooses the terminal status once. An error or abort observed before that decision records failure; a later abort cannot reverse completion because the API does not reopen terminal sessions.

Mastra default-option and tool resolvers must return the same value for the same request context and must have no side effects. Streaming preflight, tool inventory, and native execution can invoke them more than once. No exact invocation count is guaranteed, and Kitaru cannot detect every changing resolver through Mastra's public API.

## What Kitaru records

Each call creates isolated recording state and:

1. Creates a Kitaru session and an in-progress root node.
2. Records each completed Mastra step through the public `onStepFinish` callback.
3. Writes one LLM node followed by that step's local tool children.
4. Completes the same root node and session after Mastra succeeds, or records the failure when the run raises.

Each LLM node records the requested Kitaru model, the model and provider reported by Mastra, token usage, finish information, and provider metadata. Kitaru stores cost only when you provide a `costCalculator`; it does not calculate model prices on the server.

Ordinary `KitaruAgent` step nodes do not record model inputs because Mastra repeats the full prompt and message history in each provider request. Step outputs include the finish reason, text, tool calls, tool results, tripwire details, and warnings. Tool inputs are the arguments requested by the model, before a tool schema applies defaults or coercion.

Recording uses bounded JSON conversion. Tool strings are limited to 4096 characters, arrays and objects to 100 items, and nesting to 8 levels by default. Set larger limits on the wrapper when a tool needs its full arguments and result for history replay:

```ts
const recordedAgent = new KitaruAgent(agent, {
  agentId,
  requestedModelId,
  recordingLimits: { maxStringChars: 6_000, maxItems: 120, maxDepth: 10 },
});
```

Each setting must be a positive integer and cannot exceed 1,048,565 characters, 9,000 items, or 64 levels, respectively. A recorded value still has a 1 MiB and 9,000-item total budget; exceeding it produces an incomplete marker. These settings apply to dedicated tool-call nodes for both `generate()` and `stream()`; duplicate tool details in model-step summaries keep the default bounds. They do not change final text or provider metadata. Credential-shaped keys such as `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` remain redacted at every setting. The recorder marks truncated, degraded, or redacted tool values as incomplete. The recorder preserves final text until the whole serialized payload exceeds 1,048,576 characters, when it stores a degraded bounded marker rather than an unlimited transcript. This is a safety net, not a sensitive-data classifier. Do not put secrets or unnecessary personal data in prompts, tool inputs, tool outputs, or provider metadata.

The recorded node order reflects completed Mastra callbacks. It does not prove provider-side start order or wall-clock order among concurrent operations.

### Preserve configured callbacks

Mastra's per-run hooks replace configured hooks. When the agent already has callbacks that must still run, pass them explicitly as `configuredOnStepFinish`, `configuredBeforeToolCall`, and `configuredAfterToolCall` in the `KitaruAgent` options. During replay, Kitaru evaluates the tool policy first. A passthrough call then runs the configured hook followed by the caller's per-run hook; a mocked call runs neither user tool hook. Kitaru records a step before it calls configured and per-run `onStepFinish` callbacks.

The wrapper does not inspect `getConfiguredToolHooks()`. Configured callbacks that are not passed explicitly cannot be preserved when replay replaces the corresponding per-run hook. Mastra also merges per-run model settings with configured defaults, so Kitaru can replace supplied keys but cannot remove configured keys it cannot inspect.

## Replay behavior

A [replay](../concepts/replay.md) runs the same compiled command again. When the Kitaru worker sets `KITARU_REPLAY_ID`, both `generate()` and `stream()` fetch the replay configuration and apply supported overrides through public per-run Mastra options and tool hooks. Application code does not need a separate replay branch. Streaming replay executes a fresh Mastra stream; Kitaru does not play back the original text chunks.

The adapter can override:

- The run input. A valid JSON value in `KITARU_TASK_INPUTS` takes precedence over the messages passed by the caller. If a worker input is too large for that environment variable, the adapter uses `KITARU_TASK_ID` to fetch the task specification instead. Outside a worker task, it uses the caller's messages.
- System instructions. The override replaces per-run instructions and removes system messages from the effective input.
- The model. A replacement must appear in `allowedReplayModels` and resolve through `resolveModel`.
- Model settings: `temperature`, `topP`, `topK`, `maxOutputTokens`, `presencePenalty`, `frequencyPenalty`, `seed`, and `stopSequences`. Kitaru validates their types and bounds, then merges the changed settings with the caller's existing `modelSettings`.
- Local tool behavior through the policies below.

Replay overrides take precedence over the legacy `KITARU_OVERRIDE` fallback; the two are not merged.

## Tool policies

The Mastra adapter supports these [tool policies](../guides/tool-policies.md) for local executable tools:

| Policy | Replay behavior |
| --- | --- |
| `passthrough` | Calls the original tool. Any network request, database write, message, payment, or other side effect happens for real. |
| `static` | Returns the configured value without calling the tool. |
| `history` | Looks up a previous result using the tool name and JSON inputs. On a miss, `fail`, `passthrough`, and `error_result` behavior is supported. |
| `llm` | Rejected before the tool executes; this policy is not supported in 0.1.0. |

History matching uses the tool name and original JSON arguments. The Mastra importer preserves the raw exported arguments and result for this lookup, including arguments that a tool schema later coerces or fills with defaults. Other import formats or frameworks may serialize arguments differently, so matching logical calls alone does not guarantee a history match.

A completed history match replays its result, including `null`, without executing the live tool. A failed match throws `ToolPolicyError` with its stored error text and does not execute the live tool. A tool call whose stored arguments or result were explicitly marked incomplete is a history miss and follows `on_miss`; with `passthrough`, this executes the live tool. Older recordings without fidelity flags remain readable, but Kitaru cannot verify whether their tool results were truncated. Re-record them before relying on history replay. Imported trace payloads retain their original values, although the executing adapter must record complete arguments for the lookup to match.

Before a replay starts, `KitaruAgent` inventories configured tools, function-valued tools resolved from the run's `requestContext`, and per-run `clientTools` and `toolsets`. It rejects tools without a local `execute` function, approval-gated runs, sandboxed tools, and tool keys that Mastra would rename before exposing them to the model. Tools added only during execution and tools executed by a provider remain outside this preflight check and are not supported replay targets.

A tool-policy failure aborts the replay and records the session as failed. Replay forces `toolCallConcurrency: 1` and aborts Mastra's generation loop as soon as a tool hook fails, so a later model step or sibling tool cannot continue after the policy failure. Kitaru does not recreate the original exception class or convert a matched failure into a native tool-error result. On Mastra 1.67, a failed streaming policy may settle the native stream with no text instead of rejecting it; inspect the recorded replay session for the failure.

{% hint style="danger" %}
Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or choose static or history policies when replay must suppress execution.
{% endhint %}

## History-only memory with `KitaruAgent`

A supplied message array and recalled thread history are different inputs. An array contains only the messages the caller supplied; Mastra can still recall additional history when the invocation selects a memory thread.

For memory-dependent invocations, the adapter records a versioned conversation snapshot immediately before the first model step. Session inputs keep the supplied messages separately from the effective conversation, including its system messages and recalled history. The snapshot is tagged as memory-dependent; its message list is the combined effective input, not a separate recalled-only array. Replay uses that snapshot instead of recalling the thread again. This replays one invocation with its original context; it does not generate a new adaptive dialogue.

Replay removes per-run `memory`, `threadId`, `resourceId`, and `savePerStep` values, and removes Mastra's thread, resource, and internal memory keys from a copy of `requestContext`. It neither reads newer live history nor writes replay messages into the original thread. Default memory options remain unsupported because Mastra would merge them back after removal. Working memory, semantic recall, observational memory, and original invocations with user input processors or `prepareStep` are not replayable from these snapshots; they can add tools or change context beyond the first model step.

A missing, incomplete, or lossy snapshot produces an actionable unsupported-replay error before model execution. Record the invocation again with this adapter, or supply its complete recorded message array without live memory selectors. An explicit array without memory selectors continues to replay directly. Old recordings do not acquire missing history automatically. Their raw inputs do not identify whether memory was used, so removing memory settings from the replay entrypoint cannot establish that those inputs are complete. Record legacy memory-dependent invocations again before replaying them. Prompt and system-instruction overrides on conversation snapshots remain unsupported because replacing them can discard part of the recorded context; record a new invocation with the desired messages instead.

## Isolated memory replay

Import `createMemoryReplayAgent()` from `@zenml-io/kitaru-mastra/memory` when a consumed stream needs thread-scoped schema working memory, observational memory, or controlled input processors. This opt-in factory requires exactly `@mastra/core@1.67.0` and `@mastra/memory@1.30.0`, and a Kitaru server newer than 0.27.1 (see [Recording readiness](#recording-readiness) for what happens on an older server). For PostgreSQL storage, use `@mastra/pg` 1.25.x: `@mastra/pg` 1.26.0 and later require `@mastra/core` 1.68 or later. Kitaru tests memory replay against `@mastra/pg` 1.25.0. The existing `KitaruAgent` wrapper stays at the package root, keeps its history-only memory behavior, and does not require `@mastra/memory`.

```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.67.0 @mastra/memory@1.30.0 zod
```

The factory creates a fresh native agent for each invocation. A baseline uses your source storage and records the starting state before recall, then records the observer and reflector model outputs produced during that invocation. Replay restores the starting state into a separate in-memory store and runs the actor again. Memory changes during replay in two different ways:

- Working memory updates live. When the actor calls Mastra's working-memory tool, the tool runs and writes to the isolated store, so a changed prompt or model can produce different working memory.
- Observational memory (OM) is replayed. Kitaru hands the recorded observer and reflector outputs to native OM, which writes them to the isolated store as it did in production.

Replay never calls `sourceMemory()`, never writes to your source storage, and never calls an observer or reflector model.

Each replay OM call takes the unused recorded output with the same phase (observer or reflector), model method, and input. The input comparison ignores message times, dates, generated ids, and how an attachment is held: a declared URL, its captured reference, or its downloaded bytes. Replayed OM models accept captured references, so Mastra never tries to download one. Mastra also counts an attachment's tokens from its URL, sometimes by asking the provider, and those counts decide when OM observes. A baseline therefore records the tokens OM counted for each declared attachment, and replay reuses them instead of counting the captured reference or calling the provider. Mastra's number of OM calls depends on timing: a slow production observer merges buffering rounds that an instant replay makes separately, and it covers messages the actor produced while it ran. A buffered call (async observation or reflection) whose input matches no unused output therefore gets no result, because another window's output could describe messages the replay has not produced yet. Its messages stay in the actor's context, as they did in production while the observer ran, and a later buffered call usually matches the recorded window. A blocking call whose input matches no unused output takes the next unused output of its phase, and replay records an `om_input_mismatch` span. A blocking call after its phase's recorded outputs are used up fails replay with `KITARU_REPLAY_DIVERGED:mastra_om_call_order`, because an empty observation would drop the observed messages from the actor's context. So does any call whose phase has no recorded output at all. No OM call reaches a provider. Replay reports the other departures in an `om_call_divergence` span and in the session's `metadata.mastra_om_divergence` counts: `input_mismatches` (blocking calls that took another input's output), `surplus_calls` (buffered calls after their phase's outputs were used up), and `unused_results`. A baseline also records failed OM attempts, so a turn whose observer succeeded after Mastra retried it stays eligible, and its replay serves the successful output directly. When a failed blocking observation or an input processor's `abort()` ends the Mastra stream with a tripwire, the session still closes and the lease is released: a baseline becomes `ineligible` and a replay fails. Reusing recorded outputs lets you compare actor instruction/model changes, but does not measure how a fresh observer or reflector would respond to the changed conversation.

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

All writers to a source thread or resource must participate in the same `MastraExclusiveMemoryAccess` implementation. The process-local helper works only when every writer shares that instance in one process. A new turn waits up to 100 ms for an earlier turn on the same thread or resource to release it; this limit is fixed and not configurable. A recorded turn holds both selectors until its memory writes, including delayed observational-memory work, have settled, or until `finalizationWaitMs` passes (60 seconds by default, after which the turn is ineligible). Kitaru then releases the selectors before it uploads the remaining evidence and the final session update. Turns that overlap on either selector still answer natively; only the overlapping turns become ineligible for replay, and later turns are unaffected. For example, a reply sent while the previous turn's buffered reflection is still running makes both of those turns ineligible. A turn never waits for buffered observational-memory work that no lease holds, such as work left by a turn that ran natively: it starts at once and is ineligible with `om_work_unjoined`. The selectors Kitaru coordinates on are the ones Mastra uses, including the reserved `mastra__threadId` and `mastra__resourceId` request-context keys. Pass the source `Memory` as `memory` so that its `settled()` also waits, for up to `finalizationWaitMs`, for the observational-memory work of recorded turns before you close storage. `settled()` does not provide exclusive access.

A multi-process or multi-server deployment must supply a backend using shared atomic storage; Kitaru does not include a production distributed lease backend. `acquire()` returns a callable release function with `verifyEligibility()`. The backend must atomically reserve both thread and resource IDs. When either ID is still held after `waitMs`, it must invalidate the current holders and return a lease that is not eligible but holds both IDs until it is released; that invalidation ends once every overlapping lease has been released. `waitMs: 0` must not wait: Kitaru registers each write it makes outside its own lease this way and releases the registration when the write finishes. Kitaru never renews a lease, so the backend must also end a lease whose holder process died without releasing it: give each lease a time-to-live longer than your longest turn plus `finalizationWaitMs`, or tie it to a liveness check of the holder. `verifyEligibility()` must return false once a lease has expired. Without this, a server that stops mid-turn, for example during a rolling deploy, leaves every later turn on that thread and resource ineligible. `markUnsafeWrite()` is only for a write that could not register, because coordination failed or its selector is unknown, and that marker must survive process loss. Kitaru never calls `resetAfterQuiescence()`. Your application calls it for the marked selectors, or with no selector after an unknown-selector marker, once every process that might have written without registering has stopped or restarted. Coordination failure must prevent replay eligibility even when native writes continue.

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
| `om_work_unjoined` | Buffered observation or reflection from an earlier turn was still running when the turn started, or stored OM records show running work or a flag that was never cleared. |
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
| `file_capture_timeout` | Declared files or thread history files did not download within `fileCaptureWaitMs`. |
| `file_capture_failed` | A thread history file failed to download, or the turn's files exceed the capture limits. |
| `file_url_undeclared` | A file or image part in the input holds a network URL that `files` did not declare, thread history holds one and no `resolveFile` was supplied, or a processor passed an undeclared URL to the factory's `resolveFile`. |
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

Pass a static `inputProcessors` array in the factory configuration. File processors must use the factory's supplied `resolveFile`; declare every URL it may fetch in the adapter's `files` option and provide a baseline resolver returning `{ bytes: Uint8Array, mediaType: string }`. `files` is either a fixed list or a function that Kitaru calls once per recorded turn with `{ input, options }` (the call's input and stream options) and that returns the URLs for that turn. Use the function when URLs change per request. Declare every network URL in a file or image part of the input too. Replay would otherwise have to fetch it, so such a turn answers normally but is ineligible with `file_url_undeclared`. You do not need to declare URLs in file or image parts of thread history: they are part of the recorded conversation, so when you supply `resolveFile`, Kitaru declares them itself and captures them after it reads the thread at the start of each turn. A processor that re-reads recent attachments from history on every turn therefore works with `files: []`. Declare history URLs that appear only in text, such as a link in a message, when a processor resolves them. When a processor passes the factory's `resolveFile` a URL that `files` did not declare, a baseline turn fetches it with your `resolveFile` as a native turn would, answers normally, and is ineligible with `file_url_undeclared`; a replay refuses it.

Kitaru fetches each declared file once per turn and records its bytes and media type. It starts every declared download before the model runs and waits up to `fileCaptureWaitMs` (10 seconds by default) for all of them. When a download has not finished by then, the turn runs natively and is recorded as ineligible with `file_capture_timeout`. Thread history files download after Kitaru reads the thread and before the model runs, with their own `fileCaptureWaitMs` wait. A history download that fails or times out, or history files that exceed the limits below, leave the turn answering normally but ineligible, with `file_capture_failed` or `file_capture_timeout`. Kitaru downloads every history file each turn, so a thread with many or large attachments adds that download time before the model starts unless a processor fetches the same files anyway, in which case the processor reuses Kitaru's download. Whenever a baseline turn falls back to a native run, the factory's `resolveFile` takes over the download Kitaru already started for a URL, running or finished, instead of fetching that URL again. In the recorded input and the recorded thread history, a value that is exactly a declared URL, as declared or in its `new URL(url).href` form, becomes a `kitaru-file://sha256/...` content reference; URLs inside text keep their text. Replay resolves these references from recorded content, verifies the hash, and does not fetch the original URL. A processor must pass the file reference from its current input or history to the injected resolver; do not close over the original signed URL. Undeclared URLs in the call's file and image parts, and missing or altered recorded content, fail instead of falling back to a network request. Capture accepts at most 64 distinct file URLs, declared and from history together, and 16 MiB of file bytes in total, subject to the replay input limit below.

Recorded data never keeps URL credentials. In recorded input, thread history, model requests, tool calls, memory changes, observational-memory results, and outputs, each URL keeps its text but its credentials become `REDACTED`: userinfo, credential-named query, fragment, and path parameters such as `token`, `key`, `sig`, `X-Amz-Signature`, or `client_secret` (including `&amp;`- and `\u0026`-escaped ones and ones percent-encoded, once or several times, inside another URL), JWT-shaped values, and webhook or bot secrets in the path. Pagination parameters such as `page`, `cursor`, or `pageToken` stay unchanged. URL credential redaction does not make a recording ineligible. A declared file's recorded entry holds only its content reference, media type, and bytes, not the original URL, so a download token in a declared URL is never stored and replay serves the file from the recorded bytes. Replay sees the redacted text, so a processor that must read such a file needs it declared in `files`.

The file guarantee covers only requests that go through the factory's `resolveFile`. When application code, such as a processor, a tool, or your own helper, calls `fetch()` or another HTTP client itself, Kitaru does not record the response and does not stop the request during replay. In replay, that code reads either a `kitaru-file://` reference or a URL whose credentials are `REDACTED`, so a direct request usually fails. Route every attachment download through the supplied `resolveFile`.

For skills, set `skillsDirectory` to the directory containing your skill folders and use the factory's supplied `workspace`. Kitaru reads skill files into an immutable native workspace and records their paths, sizes, and hashes. Deploy the same skill artifact with the replay command. The skills tree is limited to 1 MiB of file content and 10,000 files/directories. Changed files, missing files, and symlinks are rejected before replay execution.

The factory must use the supplied memory and workspace instances. Processors and tools are application code: their dependencies must use these supplied bindings for replay isolation. Kitaru does not sandbox arbitrary callbacks or prevent code from opening another database connection or making a network request. Workflows, subagents, provider-executed tools, approval/resume modes, dynamic tool inventories, `prepareStep`, output processors, and secondary structured-output models remain unsupported.

### Tool policies and evidence

Native memory tools execute against the isolated replay store, including under `history` with `on_miss: "fail"`. External tools, including tools added by a processor, follow the replay tool policy. A tool named `updateWorkingMemory` does not acquire the native-memory exemption by name. Use history with a failing miss when external tools must not execute.

Eligible session inputs contain a version-3 `mastra_memory_replay` envelope with the invocation, initial thread/resource/messages, observational state and buffers, effective configuration, approved request context, controlled file bytes, and ordered OM results (`omTape`). Supported dates and binary values retain their types; declared file URLs become content references. Database stores such as `@mastra/pg` return some OM buffer dates as ISO strings, and capture turns exact ISO timestamps back into dates. The envelope records whether the source was Mastra's `InMemoryStore` or a database store (`configuration.memoryStore`), and the isolated replay store copies that kind of store's behavior, for example returning copies of OM records and carrying the previous record's `lastObservedAt` into a new reflection. Older history-only snapshots cannot recover this state. OM recordings without recorded results must be recorded again with the factory. The envelope also records when the turn started (`turnStartedAt`) and each object's key order (`keyOrder`, with a SHA-256 of the recorded envelope). Storage such as PostgreSQL `jsonb` re-sorts object keys, so replay restores the recorded order, and tool arguments, tool results, schemas, and working-memory templates reach the model exactly as production sent them. Replay refuses an envelope whose restored content no longer matches that hash. Replay also computes observational memory's relative date labels ("today", "2 weeks ago") and its `activateAfterIdle` check from the recorded start time, advancing at wall-clock speed, so a turn replayed weeks later gets the same memory context production did.

Inputs remain bounded and self-contained. Unsupported values, credential-key redaction, or exceeding the 16 MiB serialized UTF-8 JSON limit make a recording ineligible. The replay input also has a 200,000-item budget and maximum depth of 64. Each binary value is limited to 8 MiB before base64 encoding; encoded files and OM outputs consume the shared JSON budget. These larger bounds apply to the memory replay input, not to every ordinary recorded node. The adapter reads the full initial thread history; `lastMessages` does not make recording unbounded or restrict that snapshot to the actor's recall window. A pre-turn capture that has not finished after five seconds becomes ineligible so a stalled storage read does not indefinitely delay the native answer. Large documents or long threads may therefore require a smaller baseline.

Unlike ordinary wrapper recording, this path records the effective actor prompt, tools, tool choice, and supported settings for each provider attempt, including failed retries. Request attributes include attempt identity, memory revision, source provenance, and evidence completeness. `memory_mutation` span nodes record ordered native storage changes and link them to the active actor attempt when one exists. This evidence describes the request sent at the adapter's model boundary, not a provider's internal processing.

Each request, mutation, or tool call node has the same budget as the replay input: 16 MiB of serialized JSON, 200,000 items, and 64 levels of nesting. A history tool policy serves only a result that was recorded whole, so a memory turn records tool arguments and results on this budget too, for example a list of 1,400 rows of 10 fields. When storage returns the messages it just saved, the result records each unchanged message as a `savedMessageRef` with its id and SHA-256 instead of a second copy. A node over the budget stores a degraded marker that names the exceeded bound. When you set `recordingLimits`, they also truncate each recorded request and tool payload on this path, and a truncated tool result cannot be served from history. Truncated or degraded evidence sets `request_evidence_truncated` or `evidence_truncated` and lists the exact reason in `request_incomplete_reasons` or `evidence_truncation_reasons`. It does not make the turn ineligible, because replay rebuilds memory and requests from the replay input, not from these nodes.

Consume replay streams through completion and inspect the replay session's final status and evidence completeness. Replay finalization waits for isolated memory work and closes its store. Mastra can settle a native stream after a policy failure, so native output alone does not establish replay success. Missing or incomplete starting state fails replay before model execution; a later OM call mismatch can fail after actor execution has begun. A failed or incomplete recording is not proof that all evidence was saved.

## Structured output

Schema-only structured output is supported by both `generate()` and `stream()` and remains available on the returned Mastra result:

```ts
const result = await recordedAgent.generate(messages, {
  structuredOutput: { schema: supportDecisionSchema },
});

console.log(result.object);
```

A separate structuring model can be supplied only to `generate()` in the per-run options:

```ts
const result = await recordedAgent.generate(messages, {
  structuredOutput: {
    schema: supportDecisionSchema,
    model: "openai/gpt-5-nano",
  },
});
```

Kitaru records each secondary provider attempt as a separate model node with its own model identity, bounded input and output, usage, and failure status. Mastra still validates the schema and returns its native `result.object`. A successful provider call can be followed by a schema validation failure, in which case the model node contains the returned text and the run is marked failed.

Replay model and model-setting overrides affect the parent agent only. The secondary model stays configured in the entrypoint and executes again against the parent's new output. Agent-default secondary models, `useAgent: true`, and `errorStrategy: "warn"` or `"fallback"` remain unsupported and are rejected before execution. Move a default secondary model into the per-run options and use the default strict error strategy.

## Worker setup

Compile the agent into a Node command, register that command as the agent version's run specification, and run a [worker](../concepts/workers.md) that can execute it. Set `KITARU_AGENT_ID` in the run-spec environment. The worker supplies the task-scoped API URL and token, sets `KITARU_TASK_ID`, includes `KITARU_TASK_INPUTS` when it fits the environment boundary, and sets `KITARU_REPLAY_ID` for a replay.

The same entrypoint records a baseline session and executes replay jobs. Do not set replay environment variables manually around concurrent calls because environment variables are process-wide.

## Evaluation

Run native Mastra scorers against stored and replayed sessions with the [TypeScript evaluator bridge](../guides/typescript-evaluators.md). Supply an explicit mapping from the recorded session to your scorer input and deploy a pinned Node artifact on the worker.

## Supported boundary

The adapter supports:

- `Agent.generate()` calls on Mastra 1.51 through 1.67.
- Ordinary consumed `Agent.stream()` calls and replay on stable Mastra 1.67.x, with schema-only structured output.
- Opt-in isolated native memory replay through `createMemoryReplayAgent()` on exact Mastra core 1.67.0 and memory 1.30.0, with `@mastra/pg` 1.25.x for PostgreSQL storage, against a Kitaru server newer than 0.27.1.
- Local function tools, including function-valued tools resolved from the run's `requestContext`.
- Per-run model, system-instruction, model-setting, and input overrides.
- Passthrough, static, and same-adapter history tool policies.
- Schema-only structured output, plus per-run secondary structuring models with strict validation for `generate()`.

Streaming does not support approval or resume modes, background or `untilIdle` execution, or secondary structured-output models. The existing `KitaruAgent` wrapper does not support workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions, or LLM tool policy. Its replay path rejects `prepareStep` and input processors because they can replace the model, prompt, or tools after policy preflight. The opt-in memory factory supports the narrower dynamic-configuration and processor contract described in [Isolated memory replay](#isolated-memory-replay).

## Import existing Mastra traces

Use the Mastra importer when the run already exists in Mastra observability. Each full trace becomes one Kitaru session, with its source inputs, outputs, span hierarchy, model usage, and tool arguments and results. Invocations from the same thread remain separate sessions; `metadata.mastra.conversation_id` retains their shared identity. The importer does not join a conversation into one synthetic invocation.

### Export and register

Save the JSON response from Mastra's full `GET /observability/traces/{traceId}` endpoint, or serialize the storage `getTrace({traceId})` result. The verified format is Mastra core 1.51.0: an object with `traceId` and a `spans` array containing the root and descendants. To import several selected traces, save a JSON array of those complete responses. Trace-list summaries, `getTraceLight`, raw exporter events, and OpenTelemetry payloads are not accepted substitutes.

The importer is not registered automatically under `kitaru/`. From a Kitaru source checkout containing `plugins/packages/mastra-importer`, upload the parser script once to your selected server:

```bash
kitaru importer register mastra-export \
  --provider mastra \
  --script plugins/packages/mastra-importer/src/kitaru_mastra_importer/importer.py \
  --entrypoint parse
```

These commands use the server selected by `kitaru login`. Pass `--server URL` to select another server explicitly. Registration creates the importer and its first version; reuse that importer for subsequent uploads. A [worker](../concepts/workers.md) must be running to parse the file.

### Import for inspection or replay

Select an existing agent version that represents the exported run. For replay, its registered Node command must use the context-capable `KitaruAgent` described in [History-only memory with `KitaruAgent`](#history-only-memory-with-kitaruagent), with the same callable tool names and compatible schemas. An importer preserves the trace; it does not supply runnable agent code.

For inspection and evaluation, import the file without replay parameters:

```bash
kitaru session import mastra-traces.json \
  --importer mastra-export@latest \
  --agent support-agent@latest \
  --media-type application/json \
  --wait
```

Default imports preserve the raw invocation input and set `metadata.mastra.replay.eligible` to `false`. The root input alone may omit recalled history, so do not treat it as complete replay context.

For a known history-only memory invocation, choose the following mode **on the first import**:

```bash
kitaru session import mastra-traces.json \
  --importer mastra-export@latest \
  --agent support-agent@latest \
  --params '{"replay_context":"history-only","source_namespace":"support-production"}' \
  --media-type application/json \
  --wait
```

`replay_context` declares that the original agent used history-only memory, without working, semantic, or observational memory, custom input processors, or `prepareStep`. The export does not prove these configuration choices; use this mode only when you know them. It preserves the original invocation under `supplied_messages` and puts the initial full model messages, including system instructions and recalled history, in a versioned `mastra_conversation_context` snapshot. Missing or ambiguous context and unfinished spans make the snapshot incomplete; the adapter rejects it before model execution. Prompt and system-instruction overrides on these snapshots are unsupported.

List the imported sessions and inspect one before replaying:

```bash
kitaru session list --agent support-agent --origin imported --imported-from mastra
kitaru session get <session-id> --output json
```

Check `metadata.mastra.replay.eligible` and its reasons. Eligibility metadata is advisory, not a server-enforced ban on replay. For an eligible snapshot, create a [replay](../concepts/replay.md) with an existing evaluator and baseline tool history:

```bash
kitaru replay create <session-id> \
  --evaluator your-evaluator@latest \
  --tool-policy '{"default":{"type":"history","scope":"baseline","on_miss":"fail"}}' \
  --output json
```

The worker calls the model again with the saved context. A matching tool call returns its recorded result without executing the live tool; an unmatched call fails. Use the returned job ID with `kitaru job watch <job-id>` to follow completion.

### Identity and limits

Reimporting a trace skips the existing session rather than updating it. Keep `source_namespace` stable for one source deployment. It distinguishes deployments that might reuse trace IDs. Changing parameters alone does not upgrade a default import into a replay snapshot. If you already imported the trace without replay context, use a new explicit namespace to create a separate replay-ready copy.

The importer accepts selected files only; it does not fetch traces or live memory. It preserves usage reported by the export without counting generation totals twice, and imports monetary cost only when the source explicitly identifies USD. Missing usage and cost remain missing. Malformed traces produce isolated import failures while valid neighboring traces continue. See [Importing sessions](../guides/importing-sessions.md) for import counts and failure inspection.

## Runnable example

The [Mastra support-triage example](https://github.com/zenml-io/kitaru/tree/main/examples/typescript/mastra_support_triage) includes two entry points. Its existing worker command records a real `generate()` call and replays it with prompt, instruction, model-setting, and history-policy overrides. Its `stream` command uses a provider-free deterministic Mastra model and local order lookup to print two native text chunks while Kitaru records the final run.

Use Node 22 or Node 26 and a running Kitaru API backed by PostgreSQL. The deterministic stream needs an existing agent ID and no provider credential:

```bash
pnpm install --frozen-lockfile
pnpm build
KITARU_API_URL='https://your-kitaru-server.example.com' \
KITARU_API_KEY='your-kitaru-key' \
KITARU_AGENT_ID='your-agent-id' \
pnpm --filter @zenml-io/kitaru-example-mastra-support-triage stream
```

The generate-and-replay workflow calls OpenAI:

```bash
pnpm install --frozen-lockfile
pnpm build
OPENAI_API_KEY='your-openai-key' uv run python -m examples.typescript.mastra_support_triage.demo
```

See the example README for the complete environment and validation steps.
