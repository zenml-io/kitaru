# Replay Lab model-matrix kit

Imagine a production requirements-triage agent is already helping your team make small but important product decisions. It reads messy requests, points out missing details, names risks, and recommends the next action. Later, someone asks the natural question: can we change the model or prompt to save money or improve behavior without losing the judgment the team relies on?

This kit shows how Replay Lab turns that question into evidence. It replays the same production-like cases through candidate changes and helps you inspect whether the replacement preserves important behavior, where it changed the answer, and whether any savings are worth the risk.

It has two paths:

1. **Deterministic support demo** — no provider key, no live model calls, cheap and repeatable. Start here if you want to learn the measurement shape.
2. **Live requirements-triage model matrix** — opt-in LangGraph path that calls models through Kitaru aliases. Use this when you want to compare a small set of real model choices.

The story is intentionally practical. You have a few requirements requests that made your current assistant look shaky: one request is missing important access details, another has unclear approval rules, another has deadline risk. You do not want to swap models because a leaderboard says one model is cheaper or “better.” You want to replay the same cases, compare candidate replacements side by side, and see what changed in the concrete answers.

Replay Lab gives you three kinds of lanes. Think of them as camera angles on the same case:

- **Observed lane** — what happened in the original run.
- **Baseline replay lane** — the same case replayed from the chosen checkpoint with no candidate change.
- **Candidate lanes** — the same case replayed from the same checkpoint with candidate-specific inputs, such as a replacement model alias.

The baseline lane is the control. If observed cost was high, but baseline replay is already cheaper before any candidate is applied, the candidate should not get credit for the whole drop. Candidate lanes tell you what each candidate changed beyond normal replay effects.

## Before you start

From the repository root:

```bash
uv sync --extra local --extra langgraph --extra langgraph-openai
uv run kitaru login
uv run kitaru init
```

The deterministic path does not need provider credentials. The live LangGraph path needs whichever provider credentials your local Kitaru model aliases use.

If your aliases use a different LangGraph provider integration, install the matching extra instead of `langgraph-openai`.

## Path A: run the deterministic no-key Replay Lab first

This is the safe first run. It teaches the lane layout without live model variance.

```bash
uv run python examples/end_to_end/replay_lab/seed_observed.py --small
uv run python examples/end_to_end/replay_lab/run_replay_lab.py
uv run python examples/end_to_end/replay_lab/render_report.py
```

Open the generated HTML report:

```text
examples/end_to_end/replay_lab/reports/support-replay-lab-demo.html
```

What happened:

1. `seed_observed.py` created deterministic observed executions and wrote a manifest.
2. `run_replay_lab.py` replayed each case as baseline and candidate.
3. `render_report.py` turned the JSON report into a single HTML file.

This path is intentionally boring in the best way: you can rerun it without spending money or needing an API key.

## Path B: inspect the committed live sample report

Before running live models, inspect the sample artifacts that are safe to keep in the public repo:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.html
```

These two named files are sanitized examples. They are the only tracked report artifacts for the live model-matrix kit. Generated manifests and generated reports from your own runs stay untracked.

To regenerate the sample HTML from the sample JSON:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py \
  --json-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json \
  --output-path /tmp/requirements-triage-sample.html
```

Open `/tmp/requirements-triage-sample.html` and read it like a reviewer would:

- Start with the overall recommendation.
- Check the replay trust indicator.
- Look at the candidate decision evidence.
- Open the case rows that changed output.
- Read evaluator details before trusting a quality score.

## Register model aliases for the live path

The live matrix uses Kitaru aliases, not provider model names, inside committed files.

That means the repository can safely say “current”, “cheap”, “balanced”, and “quality” without exposing or prescribing your actual provider choices. You decide locally what each alias points to.

Use `current` for the model that produced the observed lane. Use the candidate aliases for possible replacements. Keeping those names separate prevents a confusing report where the current production model appears to be a winning replacement candidate.

Example shape:

```bash
uv run kitaru model register current --model <provider/model-for-current-production-alias>
uv run kitaru model register cheap --model <provider/model-for-cheap-alias>
uv run kitaru model register balanced --model <provider/model-for-balanced-alias>
uv run kitaru model register quality --model <provider/model-for-quality-alias>
uv run kitaru model list
```

Use aliases that make sense for your environment. For a quick first run, you need at least `current` for observed seeding and `cheap` plus `balanced` for the first two-candidate matrix.

The live example folder has its own README with a shorter map of the files and commands:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/README.md
```

The example matrix lives here:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json
```

It contains three candidates:

- `cheap`
- `balanced`
- `quality`

The first walkthrough uses only the first two with `--candidate-limit 2`. The third alias is there for the expanded matrix.

## Seed live observed requirements-triage executions

Now create the observed lane for a small live cohort. This calls your `current` alias: the model you want to treat as today’s production behavior.

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py \
  --small \
  --model current
```

This writes:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json
```

Think of that manifest as the frozen replay set. It says, “these are the original live executions we want to compare candidates against.”

Useful variants:

```bash
# Default: seed three cases
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py --model current

# Custom case count
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/seed_observed.py --count 3 --model current
```

## Run the first two-candidate matrix

Run Replay Lab against the matrix, but only use the first two candidates on the first pass:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --matrix-path examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json \
  --candidate-limit 2
```

This compares:

- observed `current` run,
- baseline replay with no candidate change,
- candidate replay for `cheap`,
- candidate replay for `balanced`.

That does **not** mean Replay Lab is ranking your current production model against itself. The observed and baseline lanes are the control group. The candidate lanes are proposed replacements.

The script also attaches a deterministic evaluator descriptor:

```json
{
  "target": "examples.end_to_end.replay_lab.langgraph_requirements_triage.evaluator:evaluate_requirements_triage",
  "id": "requirements_triage_v1",
  "on_error": "warn",
  "precedence": "override"
}
```

The evaluator is deliberately transparent. It checks whether the final answer has the expected sections:

- Summary
- Known requirements
- Missing information
- Risks
- Recommended next action

A team can replace this later with a richer evaluator, such as a domain-specific function, a human review step, or an LLM judge. The important part is that the evaluator result lands in the report under `metrics.evaluation`, so the HTML can show why a quality score was assigned.

## Render the live HTML verdict report

The live comparison writes JSON and Markdown reports under:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/
```

The report filename is based on the manifest name. With the default manifest, render it like this:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/render_report.py \
  --json-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.json \
  --output-path examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-langgraph-demo.html
```

When you open the HTML, read it in this order:

1. **Overall recommendation** — a summary judgment across candidates.
2. **Replay trust** — whether baseline replay stayed close enough to observed production.
3. **Candidate decision evidence** — which alias looks safest for this specific cohort, and which cases make that evidence weaker.
4. **Cost, latency, and quality deltas** — what changed compared with baseline replay.
5. **Changed output sections** — where a candidate said something materially different.
6. **Evaluator scorecard** — why the evaluator did or did not trust the answer shape.
7. **Cases to inspect** — concrete examples to read before making a decision.

Verdict words are intentionally conservative:

- `ship` means “safe enough from this replay cohort,” not “deploy blindly.”
- `caution` means “promising, but inspect the named cases.”
- `hold` means “do not use this comparison as shipping evidence.”

If replay drift is high, stop and inspect. In plain terms: if the baseline replay already changed the answer or quality too much before any candidate was applied, the replay setup itself moved the target. Candidate results may still be useful clues, but they are weaker evidence.

## Optional: expand to the three-candidate matrix

Once the two-candidate run behaves as expected, remove the candidate limit:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --matrix-path examples/end_to_end/replay_lab/langgraph_requirements_triage/candidates/model_matrix.example.json
```

This adds the `quality` alias to the comparison.

Power users can also pass individual candidate descriptor files instead of a matrix:

```bash
uv run python examples/end_to_end/replay_lab/langgraph_requirements_triage/run_replay_lab.py \
  --manifest-path examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/requirements_triage.json \
  --candidate-path /path/to/cheap.json \
  --candidate-path /path/to/quality.json
```

Do not pass `--matrix-path` and `--candidate-path` in the same run. The script rejects that because otherwise it is too easy to compare the wrong set.

## Git hygiene

Replay Lab creates useful local artifacts, but most of them are evidence from your machine, not source files.

Tracked on purpose:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.html
```

Untracked by default:

```text
examples/end_to_end/replay_lab/manifests/*
examples/end_to_end/replay_lab/reports/*
examples/end_to_end/replay_lab/langgraph_requirements_triage/manifests/*
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/*
```

The `.gitkeep` files keep empty output directories in the repo. The named sample JSON/HTML files are special public-safe fixtures. Do not commit generated manifests, generated JSON reports, generated Markdown reports, or generated HTML reports from a private/live run unless you intentionally sanitize and allowlist them first.

## Replacing the synthetic setup

To adapt this kit to your own workflow:

1. Replace the requirements cases with your own small cohort.
2. Keep the manifest concrete: case ID, observed execution ID, reason, labels, and replay checkpoint.
3. Keep candidate descriptors small and explicit.
4. Use aliases for model choices instead of committing provider model names.
5. Start with two candidates so the report is easy to inspect.
6. Add a deterministic evaluator before trying a subjective one.
7. Treat the HTML report as a review aid, not a deployment approval button or universal model leaderboard.

The useful habit is the same in every domain: freeze a few real cases, replay them from the same point, compare candidate effects against baseline replay, and read the cases that changed before you ship.
