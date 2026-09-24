# `@zenml-io/kitaru-mastra`

Experimental recording and replay support for Mastra. `generate()` supports `@mastra/core >=1.51.0 <1.68.0`; recorded and replayed `stream()` calls require a stable `@mastra/core 1.67.x` release.

For native working and observational memory, use the opt-in [isolated memory replay factory](#isolated-memory-replay) on exact Mastra core 1.67.0 and memory 1.30.0.

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

`stage` is `"step"` or `"complete"`, and `sessionId` is optional. The callback runs once and its return value is not awaited, so it cannot delay native completion. A thrown, rejected, or never-settling reporter does not change the Mastra result.

Failed sessions store a bounded failure category rather than the raw provider or callback message, which can contain request bodies or credentials. The native Mastra error and caller callbacks remain unchanged.

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

Import `createMemoryReplayAgent()` from `@zenml-io/kitaru-mastra/memory` when a consumed stream needs thread-scoped schema working memory, observational memory, or controlled input processors. This opt-in factory requires exactly `@mastra/core@1.67.0` and `@mastra/memory@1.30.0`. The existing `KitaruAgent` wrapper stays at the package root, keeps its history-only memory behavior, and does not require `@mastra/memory`.

```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.67.0 @mastra/memory@1.30.0 zod
```

The factory creates a fresh native agent for each invocation. A baseline uses your source storage and records the starting state before recall, then records the observer and reflector model outputs produced during that invocation. Replay restores the starting state into a separate in-memory store, runs the actor again, and supplies those recorded outputs to native observational memory (OM). Working-memory tools and native OM storage updates therefore run against isolated state. Replay never calls `sourceMemory()` or makes fresh observer/reflector provider calls.

Recorded OM outputs match by call order, phase (observer or reflector), and model method. A missing, extra, reordered, or unused call fails replay with `KITARU_REPLAY_DIVERGED:mastra_om_call_order`. Changed OM input alone does not fail replay: Kitaru reuses the recorded output and records an `om_input_mismatch` span. This lets you compare actor instruction/model changes with recorded OM results, but does not measure how a fresh observer or reflector would respond to the changed conversation.

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

All writers to a source thread or resource must participate in the same `MastraExclusiveMemoryAccess` implementation. The process-local helper works only when every writer shares that instance in one process. A new turn waits up to 100 ms for an earlier turn on the same thread or resource to release it. A recorded turn holds both selectors until its memory writes, including delayed observational-memory work, have settled, or until `finalizationWaitMs` passes (60 seconds by default, after which the turn is ineligible). Kitaru then releases the selectors and sends the final session update. Turns that overlap on either selector still answer natively; only the overlapping turns become ineligible for replay, and later turns are unaffected. Pass the source `Memory` as `memory` so that its `settled()` also waits for the observational-memory work of recorded turns before you close storage. `settled()` does not provide exclusive access.

A multi-process or multi-server deployment must supply a backend using shared atomic storage; Kitaru does not include a production distributed lease backend. `acquire()` returns a callable release function with `verifyEligibility()`. The backend must atomically reserve both thread and resource IDs. When either ID is still held after `waitMs`, it must invalidate the current holders and return a lease that is not eligible but holds both IDs until it is released; that invalidation ends once every overlapping lease has been released. `waitMs: 0` must not wait: Kitaru registers each write it makes outside its own lease this way and releases the registration when the write finishes. `markUnsafeWrite()` is only for a write that could not register, because coordination failed or its selector is unknown, and that marker must survive process loss. Kitaru never calls `resetAfterQuiescence()`. Your application calls it for the marked selectors, or with no selector after an unknown-selector marker, once every process that might have written without registering has stopped or restarted. Coordination failure must prevent replay eligibility even when native writes continue. Validate these guarantees against your actual storage, deployment topology, and failure recovery before enabling production replay; the process-local example does not establish customer deployment readiness.

Schema working memory requires explicit `scope: "thread"`. An observational-memory configuration object may omit `scope`, using Mastra's implicit thread scope, or set it to `"thread"`. Supply explicit observer/reflector model identities, either shared through `observationalMemory.model` or in the phase configuration. Resource-scoped state, semantic recall, automatic title generation, and per-call `memory.options` remain unsupported.

Keep `memory.thread` and `memory.resource` consistent with reserved Mastra thread/resource IDs in `requestContext`. A mismatch cannot produce an eligible recording. Baseline callbacks receive the original live context. `captureRequestContext` selects only approved, replay-relevant JSON values; it does not remove values from the live context. Replay receives that recorded projection. A nonempty context without an explicit projection makes the recording ineligible. For example, return `{ locale: context.get("locale") }` when locale is the only value replay needs, or `{}` when none are needed.

Never include authentication tokens, credentials, or signed URLs in the projection. Credential-like context keys are rejected, and transport headers in recorded configuration make the envelope incomplete. These checks cannot identify every secret hidden in an arbitrary string; choose recorded fields explicitly. Use `resolveModel` to reconstruct model instances from locally configured credentials.

Dynamic `instructions`, `model`, and `defaultOptions` resolve during baseline setup; replay uses their recorded values. `resolveModel` must resolve the recorded actor, observer, and reflector model identifiers and any allowed actor override. OM identifiers must resolve to native stream-capable model objects, whose provider methods Kitaru intercepts to reuse recorded results during replay. A `system_prompt` override replaces only application instructions and retains recorded extra system context. Model and model-setting overrides affect the actor; observation and reflection retain their recorded configuration and outputs. Raw-input `prompt` overrides are rejected; record a new baseline to change invocation input.

### Recording readiness

Native output and replay readiness are separate. Consume the baseline stream normally. Once the actor finishes, Kitaru finalizes recording in the background: it joins native memory work, records OM results and memory evidence, verifies source ownership, and persists the final input. Keep the application and source storage alive until that finalization finishes. `consumeStream()` alone is not a recording-completion barrier for a baseline.

Inspect the baseline session's metadata and status:

- `mastra_replay_state: "pending"`: recording has not finalized; do not replay yet.
- `mastra_replay_state: "eligible"`: the complete version-3 input and evidence were persisted for replay.
- `mastra_replay_state: "ineligible"`: recording could not establish a complete, isolated baseline. Inspect `mastra_replay_reason` and `mastra_native_state` to distinguish recording failure from native execution failure, then record a new baseline after resolving the cause.

Recording-only problems do not replace the baseline's native answer. Diagnostics use `KITARU_RECORDING_INCOMPLETE:<reason>`, such as `memory_mutation_failed`, `context_mutated_after_capture`, or `memory_evidence_incomplete`. A Kitaru outage can prevent even these diagnostics from being persisted; missing status updates are not evidence of successful recording. A pending baseline older than 30 minutes is reported as `mastra_replay_abandoned` when replay is requested; this does not cancel a native turn or release a source lease.

The server rejects pending, ineligible, or incomplete memory baselines before scheduling a replay or experiment run. CLI and MCP conflict responses expose a safe `mastra_replay_*` reason. Do not retry a pending session by supplying provisional inputs yourself.

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

Pass a static `inputProcessors` array in the factory configuration. File processors must use the factory's supplied `resolveFile`; declare every allowed URL in the adapter's `files` list and provide a baseline resolver returning `{ bytes: Uint8Array, mediaType: string }`.

Kitaru replaces declared file URLs, including signed URLs, with `kitaru-file://sha256/...` content references in recorded input and records the bytes and media type. Replay resolves these references from recorded content, verifies the hash, and does not fetch the original URL. A processor must pass the file reference from its current input to the injected resolver; do not close over the original signed URL. Undeclared URLs and missing or altered recorded content fail instead of falling back to a network request. Capture accepts at most 64 distinct file URLs and 16 MiB of file bytes in total, subject to the replay input limit below.

For skills, set `skillsDirectory` to the directory containing your skill folders and use the factory's supplied `workspace`. Kitaru reads skill files into an immutable native workspace and records their paths, sizes, and hashes. Deploy the same skill artifact with the replay command. The skills tree is limited to 1 MiB of file content and 10,000 files/directories. Changed files, missing files, and symlinks are rejected before replay execution.

The factory must use the supplied memory and workspace instances. Processors and tools are application code: their dependencies must use these supplied bindings for replay isolation. Kitaru does not sandbox arbitrary callbacks or prevent code from opening another database connection or making a network request. Workflows, subagents, provider-executed tools, approval/resume modes, dynamic tool inventories, `prepareStep`, output processors, and secondary structured-output models remain unsupported.

### Tool policies and evidence

Native memory tools execute against the isolated replay store, including under `history` with `on_miss: "fail"`. External tools, including tools added by a processor, follow the replay tool policy. A tool named `updateWorkingMemory` does not acquire the native-memory exemption by name. Use history with a failing miss when external tools must not execute.

Eligible session inputs contain a version-3 `mastra_memory_replay` envelope with the invocation, initial thread/resource/messages, observational state and buffers, effective configuration, approved request context, controlled file bytes, and ordered OM results (`omTape`). Supported dates and binary values retain their types; declared file URLs become content references. Older history-only snapshots cannot recover this state. OM recordings without recorded results must be recorded again with the factory.

Inputs remain bounded and self-contained. Unsupported values, redaction, or exceeding the 16 MiB serialized UTF-8 JSON limit make a recording ineligible. The replay input also has a 200,000-item budget and maximum depth of 64. Each binary value is limited to 8 MiB before base64 encoding; encoded files and OM outputs consume the shared JSON budget. These larger bounds apply to the memory replay input, not to every ordinary recorded node. The adapter reads the full initial thread history; `lastMessages` does not make recording unbounded or restrict that snapshot to the actor's recall window. A pre-turn capture that has not finished after five seconds becomes ineligible so a stalled storage read does not indefinitely delay the native answer. Large documents or long threads may therefore require a smaller baseline.

Unlike ordinary wrapper recording, this path records the effective actor prompt, tools, tool choice, and supported settings for each provider attempt, including failed retries. Request attributes include attempt identity, memory revision, source provenance, and evidence completeness. `memory_mutation` span nodes record ordered native storage changes and link them to the active actor attempt when one exists. This evidence describes the request sent at the adapter's model boundary, not a provider's internal processing.

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
