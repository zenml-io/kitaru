# LangGraph requirements-triage model matrix

A product team already has a requirements-triage agent in production. The agent reads messy requests, summarizes the ask, lists known requirements, points out missing information, names risks, and recommends the next action.

Later, the team wants to change something: maybe a cheaper model, a faster model, or a prompt/model combination that should preserve the same behavior. The danger is not dramatic failure. The danger is boring, easy-to-miss failure: the answer still sounds fluent, but it drops the missing approval rule; it keeps the headings, but stops warning about deadline risk.

This live Replay Lab example is about gathering evidence for that decision. It replays the same requirements cases through candidate model aliases so you can inspect whether a replacement preserves the behavior that matters and whether any cost or latency savings are worth the risk. It is not a universal model leaderboard.

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
- `candidates/model_matrix.example.json`: three replacement-candidate descriptors using model aliases: `cheap`, `balanced`, and `quality`.
- `run_replay_lab.py`: replays the manifest against the candidate matrix and writes JSON/Markdown reports.
- `evaluator.py`: a deterministic evaluator that checks the answer shape. It looks for the expected sections and false-certainty language.
- `render_report.py`: turns a Replay Lab JSON report into a single HTML report.
- `reports/requirements-triage-sample.json` and `.html`: sanitized sample artifacts you can inspect without making live model calls.

## How the replay works

Each case has three kinds of lanes:

1. **Observed**: the original live run created by `seed_observed.py`.
2. **Baseline replay**: the same execution replayed from `requirements_triage_langgraph_call` with no candidate change.
3. **Candidate replay**: the same replay point, but with a replacement-candidate model alias passed in.

The baseline lane is the control. If replay alone changes the answer, cost, or quality, the report lowers its confidence. The candidate lanes are then judged against that baseline, not against the original observed run directly.

In plain terms, Replay Lab first asks: did replay itself move the target? Then it asks: what did each proposed replacement change beyond that?

## Before running the live path

Install the local and LangGraph extras from the repository root:

```bash
uv sync --extra local --extra langgraph --extra langgraph-openai
uv run kitaru login
uv run kitaru init
```

The committed files use model aliases, not provider model names. Register aliases that make sense for your own environment. Use `current` for the model that produced the observed lane, and use the other aliases for possible replacements:

```bash
uv run kitaru model register current --model <provider/model-for-current-production-alias>
uv run kitaru model register cheap --model <provider/model-for-cheap-alias>
uv run kitaru model register balanced --model <provider/model-for-balanced-alias>
uv run kitaru model register quality --model <provider/model-for-quality-alias>
uv run kitaru model list
```

For the first live run, you need at least:

- `current` for observed seeding
- `cheap` and `balanced` for the first two-candidate replacement comparison

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
- candidate decision evidence
- changed output sections
- evaluator details
- cases the report says to inspect

The sample lets you understand the report before spending money on live model calls.

## Run the live example

Seed two observed live runs:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --small \
  --model current
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

If that run looks good, remove `--candidate-limit 2` to include the `quality` replacement candidate as well.

## Useful command variants

Seed all three committed cases:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --model current
```

Seed a specific case:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --case onboarding-workflow-access \
  --model current
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

The HTML report uses three verdict words. Read them as decision evidence for this replay cohort, not as a universal ranking of models:

- `ship` means the candidate looks safe enough on this cohort to consider moving forward.
- `caution` means the candidate may be useful, but the report found cases you should read before deciding.
- `hold` means the comparison is not strong enough shipping evidence.

The most important warning is replay drift. If baseline replay already changes the answer too much before a candidate is applied, the candidate result is weaker evidence. The report can still be useful, but it is telling you to inspect the case instead of treating the ordered candidates as a winner board.

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
