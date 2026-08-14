---
description: Record and replay AI SDK 7 ToolLoopAgent and generateText calls with the Kitaru Vercel AI SDK adapter.
icon: bolt
---

# Vercel AI SDK Adapter

`@zenml-io/kitaru-vercel-ai` adds Kitaru recording and replay to the Vercel [AI SDK](https://ai-sdk.dev) 7 `ToolLoopAgent` and `generateText` APIs. Use `createKitaruToolLoopAgent(...)` for an AI SDK Agent object or `createKitaruGenerateText(...)` for the direct function API. Both return native AI SDK results.

{% hint style="info" %}
Version `0.1.0-rc.3` is a pre-1.0 compatibility preview. Kitaru records non-streaming Agent `generate()` and direct `generateText` calls. Agent `stream()` remains available outside replay as a native, recording-free passthrough; standalone `streamText` is not wrapped.
{% endhint %}

## Install

Use Node 22.22 or later in the Node 22 release line. Install the adapter with AI SDK 7 and the provider package used by your agent. This OpenAI example uses the versions verified in the repository:

```bash
pnpm add @zenml-io/kitaru-vercel-ai@0.1.0-rc.3 ai@7.0.65 @ai-sdk/openai@4.0.20 zod@4.4.3
```

The adapter includes `@zenml-io/kitaru`, the framework-neutral TypeScript SDK, as a dependency.

## Record an Agent

Pass native `ToolLoopAgent` settings and Kitaru configuration to `createKitaruToolLoopAgent`:

```ts
import { openai } from "@ai-sdk/openai";
import { createKitaruToolLoopAgent } from "@zenml-io/kitaru-vercel-ai";
import { tool } from "ai";
import { z } from "zod";

const agentId = process.env.KITARU_AGENT_ID;
if (!agentId) {
  throw new Error("KITARU_AGENT_ID is required");
}

const agent = createKitaruToolLoopAgent(
  {
    id: "support-agent",
    instructions: "Investigate the request before answering.",
    model: openai("gpt-5-nano"),
    tools: {
      lookupOrder: tool({
        description: "Look up an order",
        inputSchema: z.object({ orderId: z.string() }),
        execute: async ({ orderId }) => findOrder(orderId),
      }),
    },
  },
  { agentId },
);

const result = await agent.generate({
  prompt: "Why is order ord-123 delayed?",
});

console.log(result.text);
```

The returned object implements AI SDK's public `Agent` interface. `version`, `id`, typed `tools`, all four Agent generic parameters, call-option validation, `prepareCall`, callbacks, runtime context, structured output, retries, timeouts, abort signals, and the native result object remain available. Each overlapping `generate()` invocation gets an independent Kitaru recorder and tool state.

AI SDK's `Agent` interface also requires `stream()`. During ordinary execution, the adapter delegates that method directly to a native `ToolLoopAgent` without starting a Kitaru session. This preserves compatibility with native consumers, but Kitaru does not record the stream. When `KITARU_REPLAY_ID` is set, `stream()` rejects before provider or tool execution because streaming replay is unsupported.

## Record a direct generation

Register an agent in Kitaru, then pass its ID to the adapter:

```ts
import { openai } from "@ai-sdk/openai";
import { createKitaruGenerateText } from "@zenml-io/kitaru-vercel-ai";

const agentId = process.env.KITARU_AGENT_ID;
if (!agentId) {
  throw new Error("KITARU_AGENT_ID is required");
}

const generateText = createKitaruGenerateText({ agentId });

const result = await generateText({
  model: openai("gpt-5-nano"),
  prompt: "Triage this support request",
});

console.log(result.text);
```

Configure the adapter subprocess with `KITARU_API_URL` and either `KITARU_API_TOKEN` or `KITARU_API_KEY`. You can also pass `apiUrl` and `apiKey` to `createKitaruGenerateText`. A separate Node management process can use [`createKitaruClient()`](../typescript-sdk.md#reuse-a-developer-login) to reuse `kitaru login` without exporting a token.

The direct wrapper calls AI SDK's public `generateText`, callbacks, and local tool `execute` functions. It does not reproduce the AI SDK generation loop. Native options, callbacks, generic types, and return behavior therefore remain available. The direct wrapper sets `maxRetries` to `0` so Kitaru records one provider attempt rather than hiding retries inside a node. The Agent API preserves native retry settings.

Each run creates a Kitaru session. It records one `llm_call` node per model step and one `tool_call` node per local tool execution, including model identity, provider, tokens, tool arguments, tool results, failures, and optional estimated cost. The adapter deliberately records `null` for LLM-node inputs rather than copying provider request data. Session inputs retain the effective prompt or messages, and the completed session summary retains the generated text and other bounded result metadata.

## Structured output

AI SDK structured output works through the native `output` option. Read it from the native `result.output` property:

```ts
import { jsonSchema, Output } from "ai";

const schema = jsonSchema<{ decision: string }>({
  additionalProperties: false,
  properties: { decision: { type: "string" } },
  required: ["decision"],
  type: "object",
});

const result = await generateText({
  model: openai("gpt-5-nano"),
  output: Output.object({ schema }),
  prompt: "Return a decision",
});

console.log(result.output.decision);
```

When AI SDK produces the object, Kitaru includes it in the session summary. If generation ends without a usable structured object, such as a length stop, the adapter still completes the session and omits the object from the summary.

## Replay

The same program records ordinary runs and executes replays. When a [worker](../concepts/workers.md) starts the registered command, it injects the task-scoped Kitaru connection, task ID, replay ID, and baseline inputs. The adapter resolves an Agent's `prepareCall` first, then:

1. replaces the caller's prompt or messages with the replay input;
2. applies supported prompt, instruction, model-setting, and allowlisted model overrides;
3. runs the native Agent or `generateText` call again; and
4. answers each local tool call according to the replay's [tool policy](../guides/tool-policies.md).

Model replacement is opt-in. Set `allowedReplayModels` and provide `resolveModel` to map each allowed Kitaru model ID to an AI SDK `LanguageModel`:

```ts
const replayOptions = {
  agentId,
  allowedReplayModels: ["openai/gpt-5-nano"],
  resolveModel: (modelId: string) => {
    if (modelId === "openai/gpt-5-nano") {
      return openai("gpt-5-nano");
    }
    return undefined;
  },
};
```

Pass `replayOptions` as the Kitaru options to either factory. Unsafe or unallowlisted overrides fail before a model call begins.

### Tool policies

Replay supports local executable tools under these policies:

| Policy | Replay behavior |
| --- | --- |
| `history` | Looks up the recorded result by tool name and arguments. The original `execute` function is not called on a hit. |
| `static` | Returns the configured matching value. The original `execute` function is not called. |
| `passthrough` | Calls the original `execute` function with the current input and AI SDK execution options. |

The `llm` tool policy is not supported. A replay that configures it fails with a tool-policy error.

History compatibility is guaranteed only when both the baseline and replay use this Vercel AI SDK adapter. Frameworks can validate, default, or serialize the same logical arguments differently, so history recorded through another adapter is not a compatibility promise. If a baseline calls the same tool more than once with identical arguments, replay resolves every matching call to the last recorded result for that pair and warns once that the trajectory may differ.

Static and history results are validated against the tool's `outputSchema` when one is declared. A schema created with `jsonSchema()` must include its optional runtime `validate` callback for replay to enforce it; otherwise replay fails closed. A configured `error_result` is an error sentinel rather than a successful tool value, so it bypasses output-schema validation and records a failed tool node.

{% hint style="warning" %}
`passthrough` is live execution, not a transaction. A tool can complete an external side effect before a later model or recording failure, and Kitaru cannot roll that effect back. Use application-level idempotency keys for side-effecting tools, or use `static` or `history` when the replay must suppress execution.
{% endhint %}

Baseline execution retains AI SDK tool concurrency. During replay, the adapter runs local tools one at a time in model-output order. It registers the complete ordered set before local execution starts, so an earlier policy failure prevents later queued tools from producing side effects.

## Run replays with a worker

Compile the TypeScript entrypoint and register its Node command. Registration creates the agent and its first version; retrieve the new ID, then register a replay-ready version that stores that ID in its run environment. The TypeScript adapter requires the script to pass `agentId` explicitly.

```bash
pnpm build

kitaru agent register support-agent \
  --command "node dist/agent.js" \
  --working-dir "$PWD"

export KITARU_AGENT_ID="$(
  kitaru --output json agent get support-agent | jq -r '.item.id'
)"

kitaru agent version register support-agent \
  --command "node dist/agent.js" \
  --working-dir "$PWD" \
  --env KITARU_AGENT_ID="$KITARU_AGENT_ID"
```

Start a worker with access to the compiled program, its Node dependencies, model credentials, and any systems used by passthrough tools:

```bash
kitaru worker start
```

The program should call the wrapped `generateText` function normally. It does not need a replay branch. See [Workers](../concepts/workers.md) for the task lifecycle and [Replay](../concepts/replay.md) for creating and running a replay.

## Supported boundary

Version `0.1.0-rc.3` supports:

- AI SDK `>=7.0.60 <8.0.0`;
- non-streaming `ToolLoopAgent.generate()` with the public AI SDK Agent type;
- native, recording-free Agent `stream()` passthrough outside replay;
- non-streaming `generateText`;
- prompt strings and message arrays;
- local tools with an `execute` function;
- native structured output;
- `history`, `static`, and `passthrough` replay policies; and
- bounded prompt, instruction, model-setting, and allowlisted model replacement during replay.

It does not record streaming generation and does not wrap standalone `streamText`. Agent `stream()` rejects when replay is active. Provider-executed tools, dynamic tools, tool approval, sandboxed replay, per-step overrides through `prepareStep`, async-iterable tools during replay, and the `llm` tool policy are unsupported in replay. Async-iterable local tools remain native during baseline recording, but cannot be replayed.

Manual approval is a two-call workflow in AI SDK, but Kitaru cannot persist a waiting Agent run without server and worker changes. When baseline `generate()` returns an unresolved manual approval request, the adapter returns the native result unchanged and marks the Kitaru session failed with `manual_approval_continuation_unsupported`. Automatic approval decisions complete normally. Agent replay rejects approval configuration and approval messages before any provider or tool side effect.

## Runnable examples

- [Vercel AI SDK support triage](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_support_triage) is the smaller adapter-focused example. It shows a support agent, local tools, model replacement, and optional cost calculation.
- [Vercel AI SDK ticket resolver](https://github.com/zenml-io/kitaru/tree/develop/v2_examples/vercel_ai_ticket_resolver) is the full end-to-end walkthrough. It records a deterministic ten-ticket baseline, reviews failures, creates an evaluator and cohorts, and runs target and control replays through a worker. Its synthetic tools make passthrough safe within that example only.
