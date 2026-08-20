---
description: Export a frozen Kitaru cohort as a ready Harbor 0.20 or Verifiers 0.3 project
icon: box-archive
---

# Export Experiments

Use `kitaru experiment export` to take a reviewed benchmark from Kitaru into Harbor or Verifiers. One export combines an immutable cohort version, an exact agent version and its local source, and every evaluator version pinned by the experiment.

Choose the target that matches what you want to do next:

* **Harbor:** build a Harbor 0.20 project with one task per cohort session, then run it with Harbor's normal dataset and agent workflow.
* **Verifiers:** build one Verifiers 0.3 plugin containing the whole benchmark, run it with the bundled agent Harness or another compatible Harness, then use the same environment as a PrimeRL 0.8 training source.

The export is local and read-only with respect to Kitaru. It does not start an experiment run or change server state.

## Before you export

You need an experiment with exact evaluator versions, an immutable cohort version for the same agent, an exact agent version with a run specification, and the local source tree for that agent version.

Select one evaluator result as the numeric reward. Write the selector as `EVALUATOR:RESULT:score` for a numeric score or `EVALUATOR:RESULT:passed` to map pass and fail to `1.0` and `0.0`.

Start with `--dry-run`. It resolves the current Kitaru resources, attached secrets, source, dependencies, policies, and target options without writing an artifact. Run the real export after you review the receipt. The real export resolves everything again and produces the authoritative manifest.

## Path 1: run the cohort in Harbor

Harbor exports target Harbor 0.20.0 and task schema 1.3. Your registered agent command must write a complete `ATIF-v1.7` or Kitaru full-session trace to an absolute path inside the task sandbox.

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
  --dry-run
```

Review the receipt, remove `--dry-run`, and run the command again. Then enter the generated directory and follow its README:

```bash
cd ./exports/checkout-harbor
docker build -t kitaru-export:DIGEST agent_image
harbor run -p dataset --agent agent.kitaru_agent:KitaruAgent
```

The generated README contains the actual image digest. `dataset` contains one Harbor task for each session in the cohort. `agent.kitaru_agent:KitaruAgent` starts the exported agent source and registered command, receives the frozen session inputs through `KITARU_TASK_INPUTS`, and copies the declared trace into Harbor's logs. The verifier converts that trace to a Kitaru session, runs the pinned evaluators, and writes the selected reward and numeric metrics through Harbor's normal result files.

A missing or malformed trace, an evaluator failure, a missing selected result, or an invalid selected reward fails the task. There is no fallback reward.

## Path 2: evaluate or train from Verifiers

Verifiers exports target the v1 authoring API in Verifiers 0.3.0. The cohort becomes one benchmark Taskset with N Tasks, one per cohort session. The project also contains one shared evaluator bridge, one replaceable default Harness for the selected agent version, `eval.toml`, and a PrimeRL 0.8.0 training-source configuration.

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

Review the receipt, remove `--dry-run`, and run the command again. Then enter the generated directory and use the checked-in configuration:

```bash
cd ./exports/checkout-verifiers
uv sync
uv run eval @ eval.toml --model MODEL
```

Replace `MODEL` with the model identifier to evaluate. `eval.toml` selects the generated Taskset, bundled Harness, and Docker runtime. The Harness copies the agent source into Docker, installs its dependencies, sets the task inputs and model endpoint variables, and runs the registered command. Historical outputs, reasoning, tool results, evaluator evidence, reward selection, and scoring code remain private to the task and are not placed in Harness-visible TaskData or the rollout container.

### Use another Harness

The manifest and `eval.toml` name the generated plugin. Its Taskset and bundled default Harness share one artifact-specific plugin ID, so multiple exports can be installed side by side.

To evaluate the same benchmark and scoring behavior with an independently installed Harness, change only `env.agent.harness.id` in `eval.toml`:

```toml
[env.agent.harness]
id = "my-compatible-harness"
```

This selects a different agent program. A compatible Harness must load under Verifiers 0.3 and consume the exported prompt, tools, and runtime contract. An unknown or incompatible ID fails before rollout; Verifiers does not fall back to the bundled Harness.

### Continue to PrimeRL

`prime-rl.toml` contains the tested PrimeRL 0.8.0 training-source configuration for the same Taskset, Harness, Docker runtime, timeouts, and forwarded environment names. Add that source to your PrimeRL trainer configuration, then choose the model, optimizer, hardware, and other trainer settings in PrimeRL.

Kitaru's target CI installs the exact Harbor, Verifiers, and PrimeRL versions and runs provider-free task, Harness, rollout, trace, reward, loader, and training-source lifecycle checks on Python 3.12. The PrimeRL check stops before trainer, optimizer, GPU, or model execution; it does not train a model.

## Choose what the artifact contains

The default preserves the experimental evidence engineers normally need to reproduce evaluation behavior: prompts, session outputs, model, tool, subagent, and span payloads, visible reasoning, metadata, diagnostics, usage, cost, task inputs, and ordinary registered environment configuration.

Use `--omit-content` when a destination should receive less evidence. Repeat the option for any of these categories:

| Category | Removed content |
| --- | --- |
| `session_outputs` | Final session outputs |
| `model_payloads` | Model-call inputs, outputs, selectors, and model parameters |
| `tool_payloads` | Tool-call inputs, outputs, and selectors |
| `subagent_payloads` | Subagent-call inputs, outputs, and selectors |
| `span_payloads` | Span inputs, outputs, selectors, and attributes |
| `visible_reasoning` | Node reasoning |
| `metadata` | Session and node metadata |
| `diagnostic_details` | Errors, external IDs, trace IDs, and cache keys |
| `usage_and_cost` | Token usage and cost |

For example:

```bash
kitaru experiment export checkout-eval \
  --cohort-version checkout-errors@3 \
  --agent checkout-agent@7 \
  --format verifiers-v1 \
  --source-root "$PWD" \
  --destination ./exports/checkout-verifiers-minimal \
  --primary-reward correctness:exact-match:score \
  --omit-content visible_reasoning \
  --omit-content usage_and_cost
```

Task inputs and structural trace fields are always retained because the targets need them to construct and score tasks. Every selected omission is recorded in the dry-run receipt, final receipt, manifest, and generated README. The receipt also warns that removing evidence can change evaluator behavior.

## Environment values and protected material

Current attached-secret values are never written to the artifact. Kitaru resolves them only long enough to remove exact matches from exported sessions and evaluator inputs, reject them in executable source or dependency declarations, and record their variable names as runtime requirements. Protected credential and local-state paths such as `.env*`, private keys, credential files, `.kitaru`, `.git`, virtual environments, build output, and `node_modules` are excluded from the source snapshot.

Ordinary values from the registered `RunSpec.env` are included by default. Use `--environment-mode runtime_only` to omit those values too and turn their names into runtime requirements:

```bash
kitaru experiment export checkout-eval \
  --cohort-version checkout-errors@3 \
  --agent checkout-agent@7 \
  --format verifiers-v1 \
  --source-root "$PWD" \
  --destination ./exports/checkout-verifiers-runtime-env \
  --primary-reward correctness:exact-match:score \
  --environment-mode runtime_only
```

The manifest separates Verifiers requirements into `task_private`, used by Kitaru's scoring task, and `bundled_harness`, used by the exported agent. It also records their union as `all`. Supply the active names through the target's environment or secret mechanism; do not put values into the bundle. When you select another Harness, that Harness and Verifiers define any additional requirements for the new agent program.

Use `--required-env NAME` for an additional evaluator-side runtime variable. Repeat it for more names. Attached agent secrets and registered values moved by `runtime_only` are discovered from the agent version automatically. Harbor places all required names in its shared task environment; Verifiers keeps evaluator-side names private to scoring and forwards agent-side names through the bundled Harness.

The exporter cannot discover transformed or historical sensitive text that no longer exactly matches a currently resolvable attached-secret value. If you do not want that experimental content in the target environment, omit its content category or exclude its source path explicitly.

## Source and dependency choices

The source snapshot excludes generated directories by default. Use `--include-source relative/path` to add a required file from a generated directory, or `--exclude-source relative/path` to remove more source. Protected paths cannot be included. Source links, special files, and files with multiple hard links are rejected.

Both targets install standard Python projects from `pyproject.toml` and optional `uv.lock`, or from `requirements.txt`. The receipt and manifest report one dependency status:

| Status | Meaning |
| --- | --- |
| `locked` | The snapshot contains `pyproject.toml` plus `uv.lock`, or every `requirements.txt` entry has an artifact hash. The generated runtime enforces the lock or hashes. |
| `declared` | Dependencies are valid and installable, but the snapshot does not fully lock their resolved versions or artifacts. |

Relative workspace dependencies must stay inside the source root. Remote artifacts must have an immutable SHA-256 identity, VCS requirements must select an exact commit, and dependency URLs cannot contain credentials.

Kitaru reads and retains one stable source snapshot for the export and records its digest. You choose the source root; Kitaru cannot prove that it is the same checkout used when the agent version was registered.

## Read the assurance and provenance receipt

The receipt and `kitaru-export.json` separate three levels of evidence:

| Level | What it proves |
| --- | --- |
| `preflight` | Kitaru resolved and validated the current request inputs. A dry run stops here. |
| `structural_validation` | The written files, manifest, target metadata, limits, and digests are internally valid. |
| `release_compatibility` | Whether this exact artifact ran the pinned target lifecycle. A normal user export reports `not_performed`; Kitaru's release CI separately proves the exporter contract. |

For Verifiers, provenance also records separate `benchmark_digest`, `default_harness_digest`, `runtime_bundle_digest`, and composed `artifact_digest` values, plus the native plugin, distribution, and module names. Each Task records its Kitaru session UUID and content digest.

Add `--archive` to write a deterministic ZIP beside the directory. The destination parent must already exist. Export never replaces an existing destination or archive and never appends to one.

## Grow a Verifiers benchmark

A cohort version is the benchmark identity. To add or remove cases, create a new immutable cohort version from the exact previous version UUID, then export it to a new destination:

```bash
BASELINE_ID="$(
  kitaru --output json cohort version get checkout-errors@3 \
  | jq -r '.item.id'
)"

NEW_COHORT_VERSION_ID="$(
  kitaru --output json cohort version create checkout-errors \
    --baseline "$BASELINE_ID" \
    --add-session NEW_SESSION_UUID \
    --display-version expanded-benchmark \
  | jq -r '.item.id'
)"

kitaru experiment export checkout-eval \
  --cohort-version "$NEW_COHORT_VERSION_ID" \
  --agent checkout-agent@7 \
  --format verifiers-v1 \
  --source-root "$PWD" \
  --destination ./exports/checkout-verifiers-v4 \
  --primary-reward correctness:exact-match:score
```

Keep the returned cohort-version reference or UUID. The previous artifact remains runnable and attributable. V1 deliberately has no partial-component, evaluator-only, or in-place append mode.

## Export through MCP

The standard-mode MCP server exposes the same policies through `kitaru_experiment_export`. MCP uses exact UUIDs and confines source, destination, staging, archive, and cleanup paths to `KITARU_MCP_WORKSPACE_ROOTS`.

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
    "content_policy": {"omit": []},
    "environment_policy": {"mode": "include"},
    "source_policy": {"include": [], "exclude": []},
    "archive": false,
    "dry_run": true
  }
}
```

For Harbor, set `format` to `harbor` and add `trace_format` and `trace_path`. Remove `dry_run` only after reviewing the receipt.

## Supported v1 boundary

The export requires a supported Python project and shell-free registered command, exact evaluator versions, passthrough tool policies, complete cohort membership, and a run specification for the selected agent version. It rejects replay overrides, unpinned or ambiguous evaluator packages, unsafe dependency sources, and inputs or artifacts outside the documented resource limits. The failure names the unsupported contract before publication.
