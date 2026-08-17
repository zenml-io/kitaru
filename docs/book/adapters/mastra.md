---
description: Record and replay non-streaming Mastra 1.51 agent runs with the Kitaru TypeScript adapter
icon: robot
---

# Mastra

The Kitaru Mastra adapter wraps an existing Mastra `Agent` and records each non-streaming `generate()` call as a Kitaru [session](../concepts/agents-and-sessions.md). Mastra still runs the agent and Kitaru returns the native Mastra result unchanged.

{% hint style="warning" %}
`@zenml-io/kitaru-mastra` 0.1.0 is the initial stable package release for Node `>=22.22.0 <23` and `@mastra/core >=1.51.0 <1.52.0`. It supports non-streaming `Agent.generate()` only.
{% endhint %}

## Install

{% tabs %}
{% tab title="pnpm" %}
```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.51.0
```
{% endtab %}

{% tab title="npm" %}
```bash
npm install @zenml-io/kitaru-mastra @mastra/core@1.51.0
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

Configure the adapter subprocess with `KITARU_API_URL` and either the worker-provided `KITARU_API_TOKEN` or `KITARU_API_KEY`. A separate Node management driver can use [`createKitaruClient()`](../deploy/sdks.md) to reuse `kitaru login` without exporting a token. The wrapper calls the existing agent's public `generate()` method. It does not recreate tools, inspect private agent fields, install model middleware, or replace the returned result.

`requestedModelId` is the Kitaru model identifier for the normal run. `allowedReplayModels` limits which replay model overrides the process will accept. When a replay selects another allowed model, `resolveModel` turns its Kitaru identifier into a Mastra model configuration. If no replay can change the model, `resolveModel` can be omitted.

## What Kitaru records

Each call creates isolated recording state and:

1. Creates a Kitaru session and an in-progress root node.
2. Records each completed Mastra step through the public `onStepFinish` callback.
3. Writes one LLM node followed by that step's local tool children.
4. Completes the same root node and session after Mastra succeeds, or records the failure when the run raises.

Each LLM node records the requested Kitaru model, the model and provider reported by Mastra, token usage, finish information, and provider metadata. Kitaru stores cost only when you provide a `costCalculator`; it does not calculate model prices on the server.

Step nodes do not record model inputs because Mastra repeats the full prompt and message history in each provider request. Step outputs include the finish reason, text, tool calls, tool results, tripwire details, and warnings. Tool inputs are the arguments requested by the model, before a tool schema applies defaults or coercion.

Recording uses bounded JSON conversion. Credential-shaped keys such as `authorization`, `token`, `secret`, `password`, `api_key`, `apikey`, and `cookie` are replaced with `[redacted]`, and oversized or unsupported values are truncated or marked. This is a safety net, not a sensitive-data classifier. Do not put secrets or unnecessary personal data in prompts, tool inputs, tool outputs, or provider metadata.

The recorded node order reflects completed Mastra callbacks. It does not prove provider-side start order or wall-clock order among concurrent operations.

### Preserve configured callbacks

Mastra's per-run hooks replace configured hooks. When the agent already has callbacks that must still run, pass them explicitly as `configuredOnStepFinish`, `configuredBeforeToolCall`, and `configuredAfterToolCall` in the `KitaruAgent` options. During replay, Kitaru evaluates the tool policy first. A passthrough call then runs the configured hook followed by the caller's per-run hook; a mocked call runs neither user tool hook. Kitaru records a step before it calls configured and per-run `onStepFinish` callbacks.

The wrapper does not inspect `getConfiguredToolHooks()`. Configured callbacks that are not passed explicitly cannot be preserved when replay replaces the corresponding per-run hook. Mastra also merges per-run model settings with configured defaults, so Kitaru can replace supplied keys but cannot remove configured keys it cannot inspect.

## Replay behavior

A [replay](../concepts/replay.md) runs the same compiled command again. When the Kitaru worker sets `KITARU_REPLAY_ID`, the wrapper fetches the replay configuration and applies supported overrides through public per-run Mastra options and tool hooks. Application code does not need a separate replay branch.

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

History matching is guaranteed only when both the recording and replay use this Mastra adapter. Another framework may apply schema defaults, coercion, or serialization differently, which changes the history key even when the logical tool call looks equivalent.

A found history value of `null` fails closed. The current lookup response cannot distinguish a successful `null` result from a failed recorded call, so the adapter neither treats it as a mocked success nor executes the real tool.

Before a replay starts, the adapter inventories configured tools, function-valued tools resolved from the run's `requestContext`, and per-run `clientTools` and `toolsets`. It rejects tools without a local `execute` function, approval-gated runs, sandboxed tools, and tool keys that Mastra would rename before exposing them to the model. Tools added only during execution and tools executed by a provider remain outside this preflight check and are not supported replay targets.

A tool-policy failure aborts the replay and records the session as failed. Replay forces `toolCallConcurrency: 1` and aborts Mastra's generation loop as soon as a tool hook fails, so a later model step or sibling tool cannot continue after the policy failure.

{% hint style="danger" %}
Replay is execution, not a transaction. A passthrough tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll it back. Use application-level idempotency keys for side-effecting tools, or choose static or history policies when replay must suppress execution.
{% endhint %}

## Memory behavior

Replay deliberately stays off live Mastra memory threads. The adapter removes per-run `memory`, `threadId`, `resourceId`, and `savePerStep` values and removes Mastra's reserved thread and resource keys from `requestContext`. It rejects agents whose default options would add those memory settings back.

The replay therefore neither reads messages added to a production thread after the recording nor writes replay messages into that thread. The consequence is that a session originally recorded with thread history replays without that history.

## Structured output

Schema-only structured output is supported and remains available on the returned Mastra result:

```ts
const result = await recordedAgent.generate(messages, {
  structuredOutput: { schema: supportDecisionSchema },
});

console.log(result.object);
```

`structuredOutput.model` is rejected before execution. Mastra 1.51 implements that option with a second internal model call which is not exposed through the parent agent's public callbacks, so Kitaru cannot record it completely.

## Worker setup

Compile the agent into a Node command, register that command as the agent version's run specification, and run a [worker](../concepts/workers.md) that can execute it. Set `KITARU_AGENT_ID` in the run-spec environment. The worker supplies the task-scoped API URL and token, sets `KITARU_TASK_ID`, includes `KITARU_TASK_INPUTS` when it fits the environment boundary, and sets `KITARU_REPLAY_ID` for a replay.

The same entrypoint records a baseline session and executes replay jobs. Do not set replay environment variables manually around concurrent calls because environment variables are process-wide.

## Supported boundary

Version 0.1.0 supports:

- Non-streaming `Agent.generate()` calls.
- Local function tools, including function-valued tools resolved from the run's `requestContext`.
- Per-run model, system-instruction, model-setting, and input overrides.
- Passthrough, static, and same-adapter history tool policies.
- Schema-only structured output.

It does not support streaming, workflows, subagents, MCP tools, provider-native tool replay, dynamic instructions, `prepareStep`, input processors, LLM tool policy, or TypeScript evaluators. `prepareStep` and input processors are rejected during replay because they can replace the model, prompt, or tools after policy preflight.

## Runnable example

The [Mastra support-triage example](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/mastra_support_triage) records a real Mastra agent, runs the compiled Node command through a job-scoped Kitaru worker, then replays it with prompt, instruction, model-setting, and history-policy overrides. Its side-effecting `queueRefundReview` tool is answered from history during replay, so the example's append-only outbox remains unchanged.

Use Node 22 and a running Kitaru API backed by PostgreSQL:

```bash
pnpm install --frozen-lockfile
pnpm build
OPENAI_API_KEY='your-openai-key' uv run python -m v2_examples.mastra_support_triage.demo
```

See the example README for the complete environment and validation steps.
