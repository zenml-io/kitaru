---
description: Run TypeScript evaluation code and native Mastra scorers through versioned Kitaru evaluators.
icon: chart-line
---

# TypeScript and Mastra evaluators

Run existing TypeScript evaluation code on recorded or replayed sessions by registering a small Python evaluator that invokes a compiled Node entrypoint. Kitaru sends the complete `SessionView` and evaluator parameters to Node, validates the returned results, and stores them through the same evaluation tasks used by [Python evaluators](write-an-evaluator.md).

The framework-neutral `runEvaluator` helper is exported from `@zenml-io/kitaru/evaluator`. For native Mastra scorers, `createMastraEvaluator` from `@zenml-io/kitaru-mastra` maps their numeric `score` and optional `reason` to Kitaru's `score` and `explanation`. Keys in the scorer record become evaluation names.

## Make the recorded input explicit

A `SessionView` contains the session record and every session node, including their recorded payloads. It does not imply a particular conversation format. You must supply `mapInput` to turn that recording into the native input your Mastra scorer expects.

For example, adopt this application-specific fixture contract: `session.inputs.conversation` is a nonempty, chronologically ordered array of messages with string `role` and `content` fields. Preserve additional message fields rather than extracting only the latest user prompt. The session output remains available separately, and tool nodes retain their full recorded inputs and outputs.

```json
{
  "conversation": [
    {"role": "user", "content": "What is the return window?"},
    {"role": "assistant", "content": "Which item are you returning?"},
    {"role": "user", "content": "A book delivered yesterday."}
  ]
}
```

This is a fixture schema you arrange to record, not an automatic format conversion by the Mastra adapter. If the conversation is absent or incompatible, fail the evaluation. Generating a substitute conversation would evaluate invented evidence. Neither helper can recover history or tool payloads that were omitted or reduced before storage.

## Write a native Mastra evaluator

Save this as `evaluator.ts`. It uses a deterministic custom scorer, so trying it needs no model credentials. The check demonstrates access to the complete supplied transcript; replace its criterion with your own.

```typescript
import { createScorer } from "@mastra/core/evals";
import { createMastraEvaluator } from "@zenml-io/kitaru-mastra";
import { runEvaluator } from "@zenml-io/kitaru/evaluator";

type Message = { role: string; content: string };
type ConversationInput = {
  conversation: Message[];
  toolNodes: unknown[];
};

function readConversation(inputs: unknown): Message[] {
  if (typeof inputs !== "object" || inputs === null ||
      !("conversation" in inputs) ||
      !Array.isArray(inputs.conversation) ||
      inputs.conversation.length === 0) {
    throw new Error("Expected a nonempty recorded conversation");
  }
  return inputs.conversation.map((message: unknown) => {
    if (typeof message !== "object" || message === null ||
        !("role" in message) || typeof message.role !== "string" ||
        !("content" in message) || typeof message.content !== "string") {
      throw new Error("Invalid recorded conversation message");
    }
    return { ...message, role: message.role, content: message.content };
  });
}

const evaluator = createMastraEvaluator({
  scorers: () => ({
    "multiple-user-turns": createScorer<ConversationInput, unknown>({
      id: "multiple-user-turns",
      description: "Check that the recorded conversation has multiple user turns",
    })
      .generateScore(({ run }) => {
        if (!run.input) throw new Error("Missing mapped conversation");
        return run.input.conversation.filter((message) => message.role === "user")
          .length >= 2 ? 1 : 0;
      })
      .generateReason(() => "Checked every supplied conversation message"),
  }),
  mapInput: (view) => ({
    input: {
      conversation: readConversation(view.session.inputs),
      toolNodes: view.nodes.filter((node) => node.node_type === "tool_call"),
    },
    output: view.session.outputs,
  }),
});

await runEvaluator(evaluator);
```

For native `type: "agent"` scorers, your mapper must return Mastra's agent input structure with `inputMessages`, `rememberedMessages`, `systemMessages`, and `taggedSystemMessages`, plus an `output` array of `MastraDBMessage` objects. Construct these from your recorded schema and preserve message order, identifiers, content parts, and tool payloads. The generic custom scorer above accepts its own input shape instead.

For model-based judges, construct the native scorers inside `scorers: (params) => ({ ... })`, passing their normal judge model and configuration options from `params`. Configure provider credentials in the worker environment. The scorer factory receives parameters on each invocation; use a model parameter such as `judge_model` so evaluation configuration records the chosen judge. Use the native scorer's own configuration API rather than expecting the bridge to choose a model or prompt.

For TypeScript code that does not use Mastra, pass your own callback directly to `runEvaluator`. It receives `(view, params)` and can return Kitaru evaluation results including numeric or boolean `score`, string `value`, `passed`, `explanation`, and applicable scale fields. Mastra conversion supplies numeric results; the generic callback supports the full Kitaru result contract.

## Build and pin the worker artifact

Use Node 22 and install dependencies during your worker image build. Include `@zenml-io/kitaru`, `@zenml-io/kitaru-mastra`, `@mastra/core`, and your chosen compiler or bundler in a package manifest, pin their versions, and commit the package-manager lockfile. Build the TypeScript entrypoint as a Node-compatible ES module and deploy it at a stable absolute path, for example `/opt/evaluators/conversation/evaluator.mjs`.

For example, with `esbuild` pinned as a development dependency, build your entrypoint and local scorer modules together:

```bash
pnpm install --frozen-lockfile
pnpm exec esbuild evaluator.ts --bundle --platform=node --format=esm \
  --target=node22 --packages=external --outfile=dist/evaluator.mjs
```

This command leaves npm package imports external, so install their locked runtime dependencies alongside the deployed artifact. Bundle any custom scorer source into the entrypoint. After copying the build into the worker image, record its SHA-256 digest:

```bash
shasum -a 256 /opt/evaluators/conversation/evaluator.mjs
```

The digest pins only the entrypoint's bytes. It does not pin Node, external imports, model providers, or other runtime files. Deploy an immutable worker image with a fixed Node version and dependencies installed from the lockfile; preserve the image identifier with your deployment records. Kitaru does not install npm packages or compile TypeScript when an evaluation task starts.

Save the following as `conversation_evaluator.py`, replacing the digest with the 64-character value from the build. Keep the path and digest in the uploaded wrapper, not in user-supplied evaluator parameters.

```python
from pathlib import Path
from typing import Any

from kitaru.task.evaluator import EvaluationResult, SessionView
from kitaru.task.typescript import run_typescript_evaluator


async def evaluate(session: SessionView, **params: Any) -> list[EvaluationResult]:
    return await run_typescript_evaluator(
        session,
        artifact=Path("/opt/evaluators/conversation/evaluator.mjs"),
        sha256="REPLACE_WITH_THE_ARTIFACT_SHA256",
        params=params,
        node="node",
        timeout_seconds=60,
    )
```

The worker must have the Python Kitaru package, Node executable, artifact, and runtime dependencies available. `artifact` must be an absolute path. `node` selects the executable; use an absolute executable path when your worker's `PATH` is not sufficient.

## Register and run

Register the Python wrapper using the ordinary evaluator CLI:

```bash
kitaru evaluator register conversation-quality \
  --script conversation_evaluator.py --entrypoint evaluate
```

Evaluate stored sessions, using the version returned by registration. For example, if registration created version 1:

```bash
kitaru session evaluate --tag imported-baseline \
  --evaluator conversation-quality@1 \
  --evaluator-params 'conversation-quality@1={}' --wait
```

For a judge configured to read `judge_model`, the parameter argument can instead be `--evaluator-params 'conversation-quality@1={"judge_model":"gpt-5-nano"}'`. The deterministic example does not call a model.

Select the same evaluator version and parameters in a replay or [experiment](regression-suite.md). Baseline and replay sessions use the same evaluation route; each evaluation invocation receives one stored session, not every session in an external conversation thread. Any whole-conversation criterion requires that the relevant history was recorded in that session.

Register a new evaluator version when its wrapper or artifact changes:

```bash
kitaru evaluator version register conversation-quality \
  --script conversation_evaluator.py --entrypoint evaluate
```

Stored evaluation provenance links the registered evaluator version to the uploaded Python wrapper and its hardcoded artifact digest. Parameters record the judge model and other configuration you supply. Keep the corresponding immutable worker deployment available to reproduce the runtime dependencies as well.

## Failure behavior

The Python helper checks the artifact digest, starts Node, and sends a version-1 JSON request containing the `SessionView` and parameters over standard input. `runEvaluator` reads that request and writes the result envelope to standard output. Keep standard output reserved for the protocol. The complete response is limited to 1 MiB across all results, including explanations; exceeding this limit fails the evaluation task.

A nonzero process exit, malformed response, invalid result, duplicate evaluation name, or timeout fails that evaluation task. Kitaru validates the entire returned list before storing results, so a failure does not leave partial evaluation rows from that invocation. Other evaluation tasks can still complete. Child-process diagnostics are suppressed in propagated errors to avoid exposing credentials or session contents; reproduce failures locally with controlled fixture data when debugging.
