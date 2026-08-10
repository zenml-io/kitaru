---
description: Export a frozen Kitaru cohort, agent source, and pinned evaluators for Harbor 0.20 or Verifiers v1
icon: box-archive
---

# Export Experiments

Use `kitaru experiment export` when you want to run an existing experiment outside Kitaru as a Harbor dataset or a Verifiers v1 environment. The export combines one immutable cohort version with one exact agent version, the local source for that agent, and every evaluator version pinned by the experiment.

The command reads Kitaru and your local source tree. It does not create a run or change server state.

## Prepare the export

You need:

* an experiment whose evaluator selections use exact versions;
* an immutable cohort version for the same agent;
* an exact agent version with a run specification;
* the local directory containing the source used by that agent version; and
* one evaluator result to turn into the primary numeric reward.

Write the reward selector as `EVALUATOR:RESULT:score` or `EVALUATOR:RESULT:passed`. A `score` must be numeric. A `passed` value becomes `1.0` or `0.0`.

Start with a dry run. This resolves the selected resources, checks the cohort and evaluator versions, and inventories the source without writing the destination:

```bash
kitaru experiment export checkout-eval \
  --cohort-version checkout-errors@3 \
  --agent checkout-agent@7 \
  --format verifiers-v1 \
  --source-root "$PWD" \
  --destination ./exports/checkout-verifiers \
  --primary-reward correctness:exact-match:score \
  --dry-run
```

Remove `--dry-run` to write the bundle. Add `--archive` if you also want a deterministic ZIP beside the destination directory. The destination parent must already exist, and the command refuses to replace an existing destination or archive.

The generated `kitaru-export.json` records the exact Kitaru IDs, evaluator version IDs, source digest, file digests, exclusions, target version, and validation result. The built-in validation is structural. It checks the generated files and target metadata without importing or executing the agent or evaluators.

## Export to Harbor

Harbor exports target Harbor 0.20.0 and task schema 1.3. The agent command must write a complete trace inside the sandbox. Declare either:

* `atif` for an `ATIF-v1.7` trace; or
* `kitaru` for a Kitaru full-session JSON document with multiple nodes.

The trace path must be an absolute POSIX path inside the sandbox. The adapter supplies the same path to the agent as `KITARU_TRACE_PATH` and supplies the frozen session inputs as `KITARU_TASK_INPUTS`.

```bash
kitaru experiment export checkout-eval \
  --cohort-version checkout-errors@3 \
  --agent checkout-agent@7 \
  --format harbor \
  --source-root "$PWD" \
  --destination ./exports/checkout-harbor \
  --primary-reward correctness:exact-match:score \
  --trace-format atif \
  --trace-path /workspace/trajectory.json \
  --required-env TOOLS_API_KEY
```

Run the commands from the generated directory. Its README supplies the concrete 12-character image digest in place of `DIGEST`:

```bash
docker build -t kitaru-export:DIGEST agent_image
harbor run -p dataset --agent agent.kitaru_agent:KitaruAgent
```

Harbor supplies the task sandbox. The generated adapter runs the registered agent command and working directory unchanged, then copies the declared trace to Harbor's agent logs. The verifier converts that trace into an evaluator-facing Kitaru session, runs all pinned evaluators, writes the selected reward to `/logs/verifier/reward.txt`, and writes numeric results to `/logs/verifier/metrics.json`.

There is no fallback reward. A missing or malformed trace, a failed evaluator, a missing selected result, or an invalid selected reward fails the task.

## Export to Verifiers v1

Verifiers exports target the v1 authoring API in Verifiers 0.3.0. No trace options are needed because the generated harness uses the Verifiers runtime trace.

```bash
kitaru experiment export checkout-eval \
  --cohort-version checkout-errors@3 \
  --agent checkout-agent@7 \
  --format verifiers-v1 \
  --source-root "$PWD" \
  --destination ./exports/checkout-verifiers \
  --primary-reward correctness:exact-match:score \
  --required-env TOOLS_API_KEY
```

Run the exact commands emitted in the generated README:

```bash
uv sync
uv run eval kitaru-verifiers-v1 --model MODEL --env.agent.runtime.type docker --env.agent.harness.forward-env TOOLS_API_KEY
```

Omit the final `--env.agent.harness.forward-env` option when the export has no required environment names. Repeat it once for each name that you supplied with `--required-env`.

The generated harness copies the agent source into the runtime, sets `KITARU_TASK_INPUTS`, `KITARU_MCP_URLS`, `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL`, and runs the registered command. The reward function converts the completed Verifiers v1 trace, runs all pinned evaluators, records their numeric outputs as metrics, and returns the selected reward.

## Runtime environment and secrets

`--required-env NAME` records and forwards an environment variable name. Repeat the option for multiple names. The bundle does not contain the current values of those variables.

The exporter does not resolve Kitaru secret IDs from the registered run specification into secret values or target-specific environment variable names. Supply each variable that the exported agent or evaluator needs with `--required-env`, then provide its value to Harbor or Verifiers at runtime.

The source inventory excludes common secret files and generated directories, including `.env*`, private-key files, credential files, `.git`, virtual environments, build output, and `node_modules`. File symlinks must resolve within the source root; directory symlinks are not supported. Review `kitaru-export.json` before distributing an artifact because source files outside those exclusions are copied into it.

## Export through MCP

The standard-mode MCP server exposes `kitaru_experiment_export`. Unlike the CLI, the MCP tool accepts exact UUIDs only. It can read and write only beneath directories listed in `KITARU_MCP_WORKSPACE_ROOTS`. On macOS and Linux, separate multiple roots with `:`.

```bash
KITARU_MCP_MODE=standard \
KITARU_MCP_WORKSPACE_ROOTS=/absolute/project:/absolute/exports \
kitaru-mcp
```

Call the tool with a required `request` object:

```json
{
  "request": {
    "experiment_id": "10000000-0000-0000-0000-000000000001",
    "cohort_version_id": "20000000-0000-0000-0000-000000000002",
    "agent_version_id": "30000000-0000-0000-0000-000000000003",
    "format": "verifiers-v1",
    "source_root": "/absolute/project",
    "destination": "/absolute/exports/checkout-verifiers",
    "primary_reward": "correctness:exact-match:score",
    "required_environment_names": ["TOOLS_API_KEY"],
    "archive": false,
    "dry_run": true
  }
}
```

For Harbor, set `format` to `harbor` and add `trace_format` and `trace_path`. Remove `dry_run` or set it to `false` only after reviewing the receipt.

## Current limits

This export path supports passthrough tool policies only. It rejects experiments with replay overrides, agent versions without a run specification, unpinned evaluator selections, package evaluators without one exact `==` version, mismatched agents, incomplete cohort membership, and session inputs larger than 32 KiB. One source file may be at most 100 MiB and the included source tree may be at most 1 GiB.

Script-backed evaluators are copied into the bundle. Package-backed evaluators remain exact package requirements, so building or installing the exported environment must be able to fetch them. The Verifiers harness copies agent source into its runtime but does not invent a dependency-install command; the registered command must work with that source and the selected runtime.

The local source is a caller-supplied snapshot. Kitaru checks it for unsafe paths, size limits, and changes while copying, but it cannot prove that it is the same checkout used to register the agent version. Choose the source root deliberately and keep the resulting digest with your experiment records.
