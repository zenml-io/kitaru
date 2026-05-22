---
name: kitaru-replay-lab-model-matrix
description: >-
  Coordinate the generic Replay Lab model-matrix kit for the live LangGraph
  requirements-triage example. Use when the user wants to compare multiple
  Kitaru model aliases, seed or reuse a requirements-triage manifest, run the
  matrix, render the HTML verdict report, or explain replay drift and rankings.
---

# Kitaru Replay Lab: Model Matrix

Use this skill for the generic model-matrix kit under:

- `examples/end_to_end/replay_lab/MODEL_MATRIX_KIT.md`
- `examples/end_to_end/replay_lab/langgraph_requirements_triage/`

This skill coordinates the existing Replay Lab skills. It does not replace them.

## Mental model

Explain the comparison as a concrete lane story:

1. **Observed lane** — the original live requirements-triage execution.
2. **Baseline replay lane** — the same execution replayed from `requirements_triage_langgraph_call` with no candidate change.
3. **Candidate lanes** — the same replay point, but with each candidate's `model` alias applied.

The safe comparison is candidate lane versus baseline replay. Observed versus candidate can be misleading because replay itself may change cost, latency, or output.

## First files to read

1. `examples/end_to_end/replay_lab/MODEL_MATRIX_KIT.md`
2. `examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json`
3. `examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py`
4. `examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py`

If the user is still learning Replay Lab, route them through the deterministic README first:

- `examples/end_to_end/replay_lab/README.md`

## Coordinate the workflow

### 1. Check aliases

Run or ask the user to run:

```bash
uv run kitaru model list
```

The committed matrix expects local aliases named:

- `cheap`
- `balanced`
- `quality`

Do not commit provider model names. If aliases are missing, tell the user to register aliases locally with this shape:

```bash
uv run kitaru model register <alias> --model <provider/model>
```

For the first two-candidate walkthrough, the user needs `balanced` for observed seeding plus `cheap` and `balanced` for candidate replay. The expanded run also needs `quality`.

### 2. Seed or reuse a manifest

If no live manifest exists, seed one:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --small \
  --model balanced
```

Default manifest path:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json
```

If the user already has a manifest, inspect its path and case count before running the matrix.

### 3. Run the matrix

First run: use two candidates so the report is easy to read.

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --matrix-path examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json \
  --candidate-limit 2
```

Expanded run: remove `--candidate-limit 2` to include all candidates from the matrix.

Power-user path: repeated `--candidate-path` inputs are valid, but never combine them with `--matrix-path`.

### 4. Evaluator descriptor

The live script uses this descriptor by default:

```json
{
  "target": "examples.end_to_end.replay_lab.langgraph_requirements_triage.evaluator:evaluate_requirements_triage",
  "id": "requirements_triage_v1",
  "on_error": "warn",
  "precedence": "override"
}
```

Explain it plainly: after each lane finishes, this deterministic evaluator reads the final answer and checks whether required sections are present. It writes details under `metrics.evaluation`; it does not overwrite runtime facts such as cost or latency.

### 5. Render HTML

Render generated JSON to a static HTML verdict report:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py \
  --json-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.json \
  --output-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.html
```

The committed sample report can also be rendered or inspected:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.html
```

These named sample files are the only tracked report artifacts. Generated manifests and generated reports should stay untracked unless the user explicitly sanitizes and allowlists them.

## How to explain the report

Use this order:

1. **Overall recommendation** — ship, caution, or hold.
2. **Replay drift** — did baseline replay stay close to observed production?
3. **Ranking** — which alias performed best in the cohort?
4. **Candidate effect** — cost, latency, and quality deltas versus baseline replay.
5. **Changed outputs** — which cases said something materially different?
6. **Evaluator evidence** — what did `metrics.evaluation` check?
7. **Cases to inspect** — concrete case IDs or rows the user should read.

Concrete phrasing to prefer:

> The cheap alias saved latency on both cases, but it dropped the Missing information section on one case. Because that changed the answer shape, this is a caution result, not a clean ship result.

Avoid saying:

> Cheap wins on aggregate metrics.

The user needs to know what actually changed and why it matters.

## Route to other Replay Lab skills

- Use `kitaru-replay-lab-investigate` when the user needs help choosing or building a cohort manifest.
- Use `kitaru-replay-lab-compare` for a single-candidate deterministic support comparison.
- Use `kitaru-replay-lab-report` when the user already has JSON/Markdown/HTML reports and wants interpretation.

Stay in this skill when the task is specifically about the requirements-triage model matrix, aliases, evaluator descriptor, candidate ranking, or live HTML verdict report.

## Guardrails

- Keep committed docs and skills generic. Do not add private organization, customer, or partner names.
- Do not hardcode provider model names into committed matrix files or docs except as placeholder syntax.
- Do not treat `ship` as automatic deployment approval. It means safe enough from this replay cohort.
- If replay drift is high, say the comparison is weaker evidence and name the cases to inspect.
- Do not commit generated manifests or live reports. Only the named sample JSON/HTML files are intended to be tracked.
