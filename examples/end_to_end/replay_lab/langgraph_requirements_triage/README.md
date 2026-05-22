# LangGraph requirements-triage model matrix

This is the live Replay Lab example for comparing several model aliases on the same LangGraph agent.

The example is intentionally small. A product team brings a few messy requirements requests to an assistant. The assistant is not supposed to solve the whole product problem. Its job is to triage the request: summarize it, list known requirements, point out missing information, name risks, and recommend the next action.

That gives Replay Lab something concrete to measure. Model swaps often fail in boring, easy-to-miss ways. A cheaper model might sound fluent but skip the missing approval rule. A faster model might keep the right headings but stop warning about risk. Replay Lab replays the same cases so you can compare those differences side by side.

## What is inside

```text
langgraph_requirements_triage/
├── candidates/
│   └── model_matrix.example.json
├── manifests/
│   └── .gitkeep
├── reports/
│   ├── requirements-triage-sample.json
│   └── requirements-triage-sample.html
├── evaluator.py
├── render_report.py
├── requirements_cases.py
├── requirements_flow.py
├── run_replay_lab.py
└── seed_observed.py
```

The main files are:

- `requirements_cases.py`: three synthetic requirements requests, with stable case IDs and labels.
- `requirements_flow.py`: the live Kitaru + LangGraph flow. It uses the runner name `requirements_triage`, so the replay anchor is `requirements_triage_langgraph_call`.
- `seed_observed.py`: runs the live flow to create observed executions, then writes a Replay Lab manifest.
- `candidates/model_matrix.example.json`: three candidate descriptors using model aliases: `cheap`, `balanced`, and `quality`.
- `run_replay_lab.py`: replays the manifest against the candidate matrix and writes JSON/Markdown reports.
- `evaluator.py`: a deterministic evaluator that checks the answer shape. It looks for the expected sections and false-certainty language.
- `render_report.py`: turns a Replay Lab JSON report into a single HTML report.
- `reports/requirements-triage-sample.json` and `.html`: sanitized sample artifacts you can inspect without making live model calls.

## How the replay works

Each case has three kinds of lanes:

1. **Observed**: the original live run created by `seed_observed.py`.
2. **Baseline replay**: the same execution replayed from `requirements_triage_langgraph_call` with no candidate change.
3. **Candidate replay**: the same replay point, but with a candidate model alias passed in.

The baseline lane is the control. If replay alone changes the answer, cost, or quality, the report lowers its confidence. The candidate lanes are then judged against that baseline, not against the original observed run directly.

In plain terms, Replay Lab first asks: did replay itself move the target? Then it asks: what did each candidate change beyond that?

## Before running the live path

Install the local and LangGraph extras from the repository root:

```bash
uv sync --extra local --extra langgraph --extra langgraph-openai
uv run kitaru login
uv run kitaru init
```

The committed files use model aliases, not provider model names. Register aliases that make sense for your own environment:

```bash
uv run kitaru model register cheap --model <provider/model-for-cheap-alias>
uv run kitaru model register balanced --model <provider/model-for-balanced-alias>
uv run kitaru model register quality --model <provider/model-for-quality-alias>
uv run kitaru model list
```

For the first live run, you need at least:

- `balanced` for observed seeding
- `cheap` and `balanced` for the first two-candidate comparison

## First: inspect the sample report

You can read the sample report without credentials or live model calls:

```text
reports/requirements-triage-sample.html
```

To regenerate it from the sanitized JSON fixture:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py \
  --json-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json \
  --output-path /tmp/requirements-triage-sample.html
```

Open `/tmp/requirements-triage-sample.html`. Look for:

- the overall recommendation
- replay trust
- candidate ranking
- changed output sections
- evaluator details
- cases the report says to inspect

The sample lets you understand the report before spending money on live model calls.

## Run the live example

Seed two observed live runs:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --small \
  --model balanced
```

This writes:

```text
manifests/requirements_triage.json
```

Run the first two-candidate matrix:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --matrix-path examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json \
  --candidate-limit 2
```

Render the HTML report:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py \
  --json-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.json \
  --output-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.html
```

Then open:

```text
reports/requirements-triage-langgraph-demo.html
```

If that run looks good, remove `--candidate-limit 2` to include the `quality` alias as well.

## Useful command variants

Seed all three committed cases:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --model balanced
```

Seed a specific case:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --case onboarding-workflow-access \
  --model balanced
```

Use repeated candidate files instead of the matrix file:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --candidate-path /path/to/cheap.json \
  --candidate-path /path/to/quality.json
```

Do not pass `--matrix-path` and `--candidate-path` together. The script rejects that so you do not accidentally compare the wrong candidates.

## Reading the verdict

The HTML report uses three verdict words:

- `ship` means the candidate looks safe enough on this cohort to consider moving forward.
- `caution` means the candidate may be useful, but the report found cases you should read before deciding.
- `hold` means the comparison is not strong enough shipping evidence.

The most important warning is replay drift. If baseline replay already changes the answer too much before a candidate is applied, the candidate result is weaker evidence. The report can still be useful, but it is telling you to inspect the case instead of trusting the ranking blindly.

## Generated files and git hygiene

Only these report artifacts are tracked on purpose:

```text
reports/requirements-triage-sample.json
reports/requirements-triage-sample.html
```

Everything else in `manifests/` and `reports/` is local output from your run and should stay untracked unless you intentionally sanitize and allowlist it.

That rule matters. Live reports can contain model output, execution IDs, and details from your environment. Treat generated reports as evidence from your machine, not as source files.

## Where to go next

For the broader Replay Lab walkthrough, including the deterministic no-key path, see:

```text
../MODEL_MATRIX_KIT.md
```

Use this folder when you specifically want to inspect or run the live LangGraph requirements-triage matrix.
