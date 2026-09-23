---
description: Record Mastra agent runs, import existing trace exports, and replay with recorded conversation context
icon: robot
---

# Mastra

The Kitaru Mastra adapter wraps an existing Mastra `Agent` and records `generate()` calls and supported streams as Kitaru [sessions](../concepts/agents-and-sessions.md). Mastra still runs the agent and Kitaru returns the native Mastra result unchanged.

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

The callback runs once. `stage` is `"step"` or `"complete"`, and `sessionId` is optional. Kitaru does not include prompts, outputs, credentials, or raw HTTP bodies in its default diagnostic. It does not await the callback's result, so a reporter that throws, rejects, or never settles cannot hold the application stream open.

Failed sessions store a bounded failure category rather than the raw provider or callback message, which can contain request bodies or credentials. The native Mastra error and caller callbacks remain unchanged.

Mastra 1.67 continues model execution in the background when the application leaves the stream unconsumed, exits a loop early, or cancels its reader. Kitaru records the eventual finish callback and completed result. It does not drain the returned reader itself or invent a final output. Kitaru marks the session failed when Mastra exposes an error or abort. User `prepareStep` and input processors are rejected before recording because they can replace tools or structured-output models after preflight. The adapter-owned memory-capture processor remains supported. After queued steps settle, the finish callback chooses the terminal status once. An error or abort observed before that decision records failure; a later abort cannot reverse completion because the API does not reopen terminal sessions.

Mastra default-option and tool resolvers must return the same value for the same request context and must have no side effects. Streaming preflight, tool inventory, and native execution can invoke them more than once. No exact invocation count is guaranteed, and Kitaru cannot detect every changing resolver through Mastra's public API.

## What Kitaru records

Each call creates isolated recording state and:

1. Creates a Kitaru session and an in-progress root node.
2. Records each completed Mastra step through the public `onStepFinish` callback.
3. Writes one LLM node followed by that step's local tool children.
4. Completes the same root node and session after Mastra succeeds, or records the failure when the run raises.

Each LLM node records the requested Kitaru model, the model and provider reported by Mastra, token usage, finish information, and provider metadata. Kitaru stores cost only when you provide a `costCalculator`; it does not calculate model prices on the server.

Step nodes do not record model inputs because Mastra repeats the full prompt and message history in each provider request. Step outputs include the finish reason, text, tool calls, tool results, tripwire details, and warnings. Tool inputs are the arguments requested by the model, before a tool schema applies defaults or coercion.

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

Before a replay starts, the adapter inventories configured tools, function-valued tools resolved from the run's `requestContext`, and per-run `clientTools` and `toolsets`. It rejects tools without a local `execute` function, approval-gated runs, sandboxed tools, and tool keys that Mastra would rename before exposing them to the model. Tools added only during execution and tools executed by a provider remain outside this preflight check and are not supported replay targets.

A tool-policy failure aborts the replay and records the session as failed. Replay forces `toolCallConcurrency: 1` and aborts Mastra's generation loop as soon as a tool hook fails, so a later model step or sibling tool cannot continue after the policy failure. Kitaru does not recreate the original exception class or convert a matched failure into a native tool-error result. On Mastra 1.67, a failed streaming policy may settle the native stream with no text instead of rejecting it; inspect the recorded replay session for the failure.

{% hint style="danger" %}
Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or choose static or history policies when replay must suppress execution.
{% endhint %}

## Memory behavior

A supplied message array and recalled thread history are different inputs. An array contains only the messages the caller supplied; Mastra can still recall additional history when the invocation selects a memory thread.

For memory-dependent invocations, the adapter records a versioned conversation snapshot immediately before the first model step. Session inputs keep the supplied messages separately from the effective conversation, including its system messages and recalled history. The snapshot is tagged as memory-dependent; its message list is the combined effective input, not a separate recalled-only array. Replay uses that snapshot instead of recalling the thread again. This replays one invocation with its original context; it does not generate a new adaptive dialogue.

Replay removes per-run `memory`, `threadId`, `resourceId`, and `savePerStep` values, and removes Mastra's thread, resource, and internal memory keys from a copy of `requestContext`. It neither reads newer live history nor writes replay messages into the original thread. Default memory options remain unsupported because Mastra would merge them back after removal. Working memory, semantic recall, observational memory, and original invocations with user input processors or `prepareStep` are not replayable from these snapshots; they can add tools or change context beyond the first model step.

A missing, incomplete, or lossy snapshot produces an actionable unsupported-replay error before model execution. Record the invocation again with this adapter, or supply its complete recorded message array without live memory selectors. An explicit array without memory selectors continues to replay directly. Old recordings do not acquire missing history automatically. Their raw inputs do not identify whether memory was used, so removing memory settings from the replay entrypoint cannot establish that those inputs are complete. Record legacy memory-dependent invocations again before replaying them. Prompt and system-instruction overrides on conversation snapshots remain unsupported because replacing them can discard part of the recorded context; record a new invocation with the desired messages instead.

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
- Local function tools, including function-valued tools resolved from the run's `requestContext`.
- Per-run model, system-instruction, model-setting, and input overrides.
- Passthrough, static, and same-adapter history tool policies.
- Schema-only structured output, plus per-run secondary structuring models with strict validation for `generate()`.

Streaming does not support approval or resume modes, background or `untilIdle` execution, or secondary structured-output models. The adapter does not support workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions, or LLM tool policy. `prepareStep` and input processors are rejected during replay because they can replace the model, prompt, or tools after policy preflight.

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

Select an existing agent version that represents the exported run. For replay, its registered Node command must use the context-capable `KitaruAgent` described in [Memory behavior](#memory-behavior), with the same callable tool names and compatible schemas. An importer preserves the trace; it does not supply runnable agent code.

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
