# Mastra support triage

This demo records and replays a real Mastra `Agent` using OpenAI `gpt-5-nano`. The agent investigates a delayed order and suspected duplicate charge with three local tools:

- `lookupAccount` and `lookupOrder` read versioned fixtures.
- `queueRefundReview` appends one line on every real call.

The TypeScript driver uses `@zenml-io/kitaru/node` to read the login selected by the Python `kitaru` CLI. It registers the agent command and evaluator, creates a session-run job, and starts `kitaru worker start --job-id JOB_ID` as a dedicated subprocess. It then creates and runs the replay job through another exact-job worker. There is no TypeScript CLI and no token-export step.

During replay, Kitaru returns the original `queueRefundReview` result from history, so the append-only outbox stays at one line. The replay also replaces the input, system instructions, and `maxOutputTokens`, then runs three Python evaluations against the result session.

Every LLM node records `openai/gpt-5-nano` as the requested model and the model ID that the provider served. Kitaru does not price a call itself, so `agent.ts` supplies a `costCalculator` that converts recorded token usage into dollars.

## Run

Use Node 22 and a Kitaru server backed by PostgreSQL. Log in once with the Python CLI, then install, build, and run the TypeScript driver:

```bash
kitaru login https://your-kitaru-server.example.com
pnpm install --frozen-lockfile
pnpm build
OPENAI_API_KEY='your-openai-key' pnpm --filter @zenml-io/kitaru-example-mastra-support-triage demo
```

The driver reads the selected server and credential without printing or copying the credential. Before creating remote resources, it verifies that the dedicated worker command is installed and can make an authenticated, read-only request to the selected server. The worker can use the same stored login, or an explicit `KITARU_API_KEY` or `KITARU_API_TOKEN` supplied to the driver.

The command prints the run state directory, session and replay IDs, both outbox counts, the mocked history action, and evaluation scores.

## Isolation and recovery

Each invocation creates an owner-only `.state/<run-id>/` directory. Its outbox, worker caches, exact remote IDs, account and server identity, cancellation records, and operation journal are isolated from every other invocation. Each worker started by this driver restricts its own claims to the job ID recorded for that run, so it cannot take work from another demo invocation.

Job scoping does not reserve the task on the server. An already-running broad worker can still claim the task before this exact-job worker does. Stop broad workers, or scope them away from these jobs, before running the demo on a shared server. Preventing that race completely requires a server-side claim-isolation primitive that the current API does not provide.

Resume an interrupted run by passing the printed state directory:

```bash
pnpm --filter @zenml-io/kitaru-example-mastra-support-triage demo -- --resume v2_examples/mastra_support_triage/.state/RUN_ID
```

Completed jobs and completed runs are read back instead of executed again. The driver therefore does not deliberately repeat a paid provider call during recovery. This is not a global exactly-once guarantee against a competing broad worker or server-side task retry.

Every non-idempotent create is recorded before the request. If the server may have accepted a request whose response was lost, the operation becomes `ambiguous` and the driver stops. Inspect `run-manifest.json` and the target server, then choose one explicit recovery action.

If the resource exists, adopt its exact UUID:

```bash
pnpm --filter @zenml-io/kitaru-example-mastra-support-triage demo -- --resume v2_examples/mastra_support_triage/.state/RUN_ID --adopt create_agent=RESOURCE_UUID
```

The driver reads the resource back and validates its type, account ownership where the API exposes it, and parent IDs before committing it to the manifest. Valid operation keys are `create_agent`, `create_agent_version`, `create_evaluator`, `upload_evaluator_source`, `create_evaluator_version`, `create_initial_job`, and `create_replay`.

If inspection proves that no resource was created, explicitly accept the duplicate-create risk and retry that exact operation:

```bash
pnpm --filter @zenml-io/kitaru-example-mastra-support-triage demo -- --resume v2_examples/mastra_support_triage/.state/RUN_ID --retry create_agent
```

The driver records the explicit retry decision before issuing another create. It rejects recovery for a different operation, a changed request fingerprint, or both actions at once.

If the exact committed initial job or replay job is terminal `failed` or `canceled`, use `--retry create_initial_job` or `--retry create_replay` to replace it. The driver reads back and verifies the old job and its parent IDs, records the replacement authorization before creating anything, and then updates the manifest with the new exact IDs. It never creates a replacement automatically, because doing so can repeat paid model work.

One process holds an exclusive lock on the run directory from before the first manifest read until the workflow exits. A concurrent resume fails before it can call the server. The owner-only lock records its host, process ID, and unique token. A dead local process's stale lock is replaced atomically, while a live local or remote-host owner remains protected. Normal release removes only the caller's token.

If the dedicated worker fails to start or exits unsuccessfully, the driver verifies the exact job, account, kind, and agent version before sending one cancellation request. A `409` or lost cancellation response triggers one exact job read. Terminal state or `cancel_requested_at` confirms the result; any other result remains ambiguous in the manifest. The original worker failure remains the primary error.

The JSON fixtures are never changed.
