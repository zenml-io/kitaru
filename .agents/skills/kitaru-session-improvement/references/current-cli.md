# Current CLI operations

Run commands from the repository or example directory required by the project. Export the local environment once per terminal when the repository uses an `.env` file:

```bash
set -a; source .env; set +a
```

Coding-agent tool calls usually start fresh shells, so the exported state does not persist between calls. For automated operations, either load `.env` inside every shell invocation or use `uv run --env-file .env kitaru ...`. Apply the same rule to every command started in parallel.

## Preflight pattern

```bash
uv run kitaru status
uv run kitaru worker list
uv run kitaru importer list
uv run kitaru evaluator get cost
uv run kitaru evaluator get latency
uv run kitaru evaluator get tool-call-patterns
uv run kitaru evaluator list
```

Run independent reads as one bounded preflight. Record the selected server and exact agent dashboard link before creating resources.

Start a missing worker in a second terminal:

```bash
set -a; source .env; set +a
uv run kitaru worker start --name WORKER_NAME
```

Seed bundled development plugins only on a fresh local server:

```bash
uv run python ../../scripts/seed_default_plugins.py
```

## Register an agent version

```bash
uv run kitaru agent register \
  AGENT_NAME \
  --command "AGENT_COMMAND" \
  --description "AGENT_DESCRIPTION" \
  --display-version BASELINE_LABEL \
  --working-dir REPOSITORY_ROOT \
  --timeout-seconds 180 \
  --tool TOOL_NAME
```

## Import local traces

Replace `BASELINE_VERSION` with the exact discovered version number:

```bash
uv run kitaru session import \
  TRACE_PATH \
  --importer IMPORTER_REFERENCE \
  --agent "AGENT_NAME@${BASELINE_VERSION}" \
  --tag BASELINE_TAG \
  --params '{"source_instance":"SOURCE_NAME"}' \
  --media-type application/x-ndjson \
  --wait
```

If the import skips all traces because the same source IDs already exist under another agent, preserve the source file and use an importer-supported source namespace or a temporary remapping utility supplied by the repository. Do not delete existing sessions to resolve a collision.

```bash
uv run python PATH_TO_REPOSITORY_REMAP_SCRIPT TRACE_PATH TEMP_PATH --namespace UNIQUE_NAMESPACE
```

Use `kitaru --output json` for read operations when an MCP response fails output validation. Continue to use exact IDs and versions from the structured result.

If replay-list or run-child reads are incompatible, do not use an unfiltered global job list as evidence. List replay sessions through the exact candidate agent-version ID, `origin=replay`, and the run's time window, then verify each session's replay and experiment-run metadata before joining evaluations.

## Create, test, and register an evaluator

Create the evaluator only after the behavior brief is approved:

```bash
uv run kitaru evaluator scaffold EVALUATOR_NAME --path EVALUATOR_PATH
```

```bash
uv run kitaru evaluator test EVALUATOR_PATH --entrypoint evaluate
```

Register when the evaluator is absent. Create a new immutable version when the approved behavior differs from the existing source:

```bash
uv run kitaru evaluator register \
  EVALUATOR_NAME \
  --script EVALUATOR_PATH \
  --entrypoint evaluate \
  --description "APPROVED_CRITERION" \
  --display-version DISPLAY_VERSION
```

Use structured output when a CLI result must be passed into later reasoning:

```bash
uv run kitaru --output json COMMAND
```
