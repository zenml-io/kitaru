# Adaptive Mastra conversation

This example lets a deterministic simulated user choose its next message from the target agent's preceding response. Raw Mastra calls generate a fresh dialogue, and one Kitaru session records its ordered transcript. An existing Python evaluator then checks the transcript's output contract.

The scenario uses only fictional parcel information. The target has no tools or persistent memory. If it asks for an order reference, the simulator supplies `FIXTURE-42`; otherwise the simulator asks it to clarify. Each run creates a new target agent and explicit message history.

## Run the example

From the repository root, use Node 22.22 or newer within Node 22, pnpm 10.33, uv, and local PostgreSQL reachable by the repository's [devtools harness](../../../devtools/AGENTS.md). Docker can provide PostgreSQL when it is not already running.

```bash
uv sync --frozen --extra server --extra worker --extra cli
pnpm install --frozen-lockfile
pnpm run build:packages
pnpm --filter @zenml-io/kitaru-example-mastra-adaptive-conversation build
```

Set `OPENAI_API_KEY` in your environment, then run:

```bash
uv run python examples/typescript/mastra_adaptive_conversation/demo.py
```

The command starts an isolated temporary local server, registers the example as an executable agent, and runs two conversations through job-scoped workers using `openai/gpt-5-nano`. It checks that each job produces exactly one distinct session with a fresh opening message, then evaluates both sessions using `kitaru/output-contract`. It prints the session IDs, branch choices, stop reasons, and evaluation verdicts. The temporary server and database are removed when the command exits, including on failure.

To retain the sessions for inspection on an existing development server, set `KITARU_API_KEY` if required and pass its URL:

```bash
uv run python examples/typescript/mastra_adaptive_conversation/demo.py --api-url http://localhost:8000
```

This mode keeps the registered agent, sessions, and evaluations on that server. The target makes paid model calls; the deterministic simulator and Python contract evaluator make none.

## What the session means

Outputs contain a versioned transcript, the simulator branches, effective target settings, independently identified simulator and evaluator configuration, observed token usage, and the stop reason. Checkpoints preserve the completed conversation prefix if a later target call fails. There are no automatic target retries.

The example bounds the conversation to three target turns, one generation step per call, 2,000 output tokens per call, a 45-second call deadline, a 120-second conversation deadline, and 12,000 observed total target tokens. Token accounting is checked after each response, so the last call can exceed the aggregate token budget. This is not a hard dollar cap. Missing provider usage fails the run rather than treating unknown usage as zero.

The Python evaluator checks required output fields and types. A passing contract means the transcript has the expected structure; it does not establish that the target's advice is correct or useful. Evaluator parameters belong to the evaluation job and remain separate from target overrides.

These are transcript-only sessions. They do not contain complete model/tool telemetry, support faithful tool replay, or establish automatic cost comparisons. The example generates a new dialogue each time. It does not introduce a production conversation runner or change the Mastra adapter recorder.

## Target variants and worker inputs

The executable accepts a versioned worker input object:

```json
{"scenario_version":"parcel-fixture-v1","prompt":"Ask for the order reference first, then suggest contacting support about the fictional delay."}
```

Here `prompt` is the target's instructions. The simulated user's opening message is fixed by the scenario. The command resolves worker inputs once, including the task-spec API fallback when the worker cannot put the payload in its environment.

Replay `prompt` and `system_prompt` overrides change only target instructions; `system_prompt` takes precedence. Model overrides allow `openai/gpt-5-nano` and `openai/gpt-5-mini`; the demo uses nano. The only supported model parameter override is `maxOutputTokens`, from 1 to 2,000. Simulator behavior and evaluator parameters do not change with target variants. Replay tool policies other than the default passthrough policy are rejected because the example has no tools.

`scenario_complete`, `turn_limit`, `usage_limit`, and overall `time_limit` are completed bounded stops. `call_timeout` and `runtime_failure` fail the session and executable, retaining the last recorded prefix. Usage on a failed or timed-out call may be unavailable. A hard process kill or recording-server failure can prevent the final checkpoint, so worker/task status remains authoritative for interrupted runs.

## Evaluate the complete generated conversation

The example also provides custom Mastra scorers through Kitaru's existing TypeScript evaluator bridge. After the build above, run:

```bash
uv run python examples/typescript/mastra_adaptive_conversation/check_scorers.py
```

This command generates a baseline conversation and an adaptive rerun with `openai/gpt-5-nano`, then evaluates both stored sessions. The rerun starts a fresh dialogue: the simulator reacts to its new answers rather than playing back the baseline history. It makes at most six target calls and two judge calls, with no provider retries. The command removes its temporary server, worker state, and database on exit.

`src/scorers.ts` explicitly maps `session.outputs.messages` in recorded order. It does not use `session.inputs.prompt` as a user message, concatenate only the final exchange, or assume an ordinary Mastra agent judge reads every historical turn. Both custom Mastra scorers receive the same complete transcript:

- `conversation_evidence` checks the fictional parcel request, lexical cues that the earlier assistant requested a reference, the later user's reference, and the final assistant's response. This scenario-specific keyword check can miss valid paraphrases; it is not a semantic judgment or a general measure of answer quality.
- `conversation_judge` sends every user and assistant message to an independent judge with an explicit rubric. Its explanation is stored with its named result. The live judge uses `gpt-5-nano`, with a bounded request and no retries.

Judge model and instructions come from evaluator job parameters. Target overrides, simulator configuration, and the transcript's original `judge` description do not configure this evaluator. Persisted evaluation rows identify the registered evaluator version and exact parameters. The registered Python wrapper checks compiled artifact digests; Node and imported packages still need the pinned workspace dependencies installed before workers run.

The mapping accepts the version-1, tool-free parcel transcript with complete alternating user/assistant turns and a supported completed stop. Failed, unfinished, unsupported, or tool-bearing sessions fail evaluation. It creates no synthetic tool records. If either scorer fails, the bridge fails the evaluation task without storing a partial result batch.
