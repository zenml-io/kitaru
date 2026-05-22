# Replay Lab end-to-end demo

Replay Lab shows the workflow behind this product idea:

> Use observability to see what happened. Use Kitaru to replay the bad cases,
> compare a candidate, and decide whether the fix is safe to ship.

This demo is deterministic by default. It does **not** call a live LLM provider,
so it is cheap, repeatable, and safe to run in a video or review session.

## The story

Monday morning, Sarah gets two signals at once.

First, support tickets from the last week show a messy pattern: refund answers
are slower for enterprise customers, a regulated medical claim response feels
risky, and shipping-status cases keep looping through tools. Then finance says
the agent is also spending more per case.

Sarah does not yet know whether these are the same problem. She needs more than
a dashboard. She needs a small production-like history she can rerun, a candidate
fix to compare, and a clear answer about whether the candidate actually helps.

Sarah has Claude Code connected to the Kitaru MCP server. Instead of clicking
through traces by hand, she asks Claude to investigate and run Replay Lab.

## Setup

From the repository root:

```bash
uv sync --extra local
uv run kitaru login      # starts/connects to a local Kitaru server
uv run kitaru init       # one-time project marker, safe to skip if it exists
```

You do **not** need `OPENAI_API_KEY` or any provider credentials for the default
path.

## Optional: live model-matrix kit

This README keeps the deterministic support-agent walkthrough as the first path
because it is the easiest way to understand Replay Lab without spending money or
needing provider credentials.

If you want the opt-in live LangGraph version, use the generic model-matrix kit:

```text
examples/end_to_end/replay_lab/MODEL_MATRIX_KIT.md
```

That kit walks through registering local model aliases, seeding live
requirements-triage executions, running a two-candidate first matrix, optionally
expanding to three candidates, using the deterministic evaluator descriptor, and
reading the HTML verdict report.

## Step 1: create the observed production-like executions

Sarah first needs executions that stand in for last week's production cases. In
the real product, these would already exist. In the demo, we seed a deterministic
12-case production-like history: the three original cases plus three synthetic
history variants for each one.

```bash
uv run python examples/end_to_end/replay_lab/seed_observed.py
```

This runs the current champion support agent on deterministic synthetic cases
and writes the v0 replay set manifest:

```text
examples/end_to_end/replay_lab/manifests/support_demo.json
```

Think of this manifest as the prototype version of a replay pack: a frozen list
of cases Sarah wants to reuse whenever she tests a candidate. The generated
variant IDs are stable, for example `regulated-medical-claim--hist-02`, so replay
can reconstruct the same case later from the manifest.

If you want the old quick path with only the three base cases, run:

```bash
uv run python examples/end_to_end/replay_lab/seed_observed.py --small
```

You can also cap the richer deterministic history while experimenting:

```bash
uv run python examples/end_to_end/replay_lab/seed_observed.py --count 5
```

All of these paths remain cheap and deterministic. They do not call a live LLM
provider.

## Step 2: ask Claude Code to investigate with Kitaru MCP

Once the observed executions exist, Sarah can use Claude Code as the operator
surface. Claude has access to Kitaru through MCP, including the
`kitaru_replay_lab_compare` tool.

Copy-paste prompt:

```text
Use the Kitaru MCP server to investigate the Replay Lab demo.

We had complaints after the latest support-agent change. Use the cohort manifest
at examples/end_to_end/replay_lab/manifests/support_demo.json and compare the
candidate descriptor at examples/end_to_end/replay_lab/candidates/cheaper_support_agent.json.

Please run Replay Lab with baseline replay and candidate replay, then read the
generated report and tell me:

- where the candidate helped
- where it changed output
- whether the comparison is trustworthy
- which cases I should inspect before shipping
```

Under the hood, Claude can use Kitaru MCP tools to inspect executions, run the
Replay Lab comparison, and explain the generated reports. That is the product
story: Kitaru owns the runtime and replay machinery; the coding agent uses MCP
to operate it.

## Step 3: run the same comparison manually

If you want the fully explicit command path, run:

```bash
uv run python examples/end_to_end/replay_lab/run_replay_lab.py
```

For every case in the manifest, Kitaru creates three lanes:

1. **Observed production** — what happened in the original seeded execution.
2. **Baseline replay** — the same execution replayed from `draft_response` with
   no candidate changes.
3. **Candidate replay** — the same execution replayed from `draft_response` with
   the cheaper candidate descriptor applied.

The command writes:

```text
examples/end_to_end/replay_lab/reports/support-replay-lab-demo.json
examples/end_to_end/replay_lab/reports/support-replay-lab-demo.md
```

## Step 4: render the shareable HTML report

```bash
uv run python examples/end_to_end/replay_lab/render_report.py
```

Open:

```text
examples/end_to_end/replay_lab/reports/support-replay-lab-demo.html
```

This is the artifact Sarah can attach to a PR or show in a review. It is not a
Kitaru frontend page; it is a generated report for the prototype.

## Why baseline replay matters

The tempting comparison is:

| Metric | Observed production | Candidate replay | Naive story |
|---|---:|---:|---|
| Cost | $0.42 | $0.27 | "36% cheaper" |

That can lie. Candidate replay runs under the replay harness, while observed
production happened in the original production-like run.

Replay Lab inserts a control lane:

| Metric | Observed production | Baseline replay | Candidate replay | Candidate effect |
|---|---:|---:|---:|---:|
| Cost | $0.42 | $0.29 | $0.27 | -7% |
| Quality | 0.91 | 0.91 | 0.79 | -0.12 |

If baseline replay is already cheaper than observed production, the candidate
should not get credit for that whole difference. Baseline replay tells Sarah
what replay changed by itself. Candidate replay tells her what the candidate
changed beyond replay effects.

## What to look for in the report

The expected demo outcome is deliberately mixed:

- The candidate is cheaper and faster on most cases.
- The baseline replay column shows whether the comparison is trustworthy.
- The regulated medical cases change output and may lose quality.
- The report should therefore not say "ship it blindly".

That last point is the whole reason Replay Lab is useful. Cost savings do not
matter if the candidate changed an answer that a customer or reviewer cares
about.

Every row includes execution IDs that you can inspect with the CLI, MCP tools,
or the Kitaru dashboard. For example:

```bash
uv run kitaru executions get <execution-id-from-report>
```

## Files

- `support_flow.py` — deterministic Kitaru flow with stable checkpoints.
- `scenarios.py` — base support cases, deterministic history variants, and
  scoring rules.
- `candidates/cheaper_support_agent.json` — v0 candidate descriptor.
- `seed_observed.py` — runs observed champion executions and writes a manifest.
- `run_replay_lab.py` — runs baseline and candidate replay, then writes JSON and
  Markdown reports.
- `render_report.py` — turns the JSON report into a static HTML artifact.
- `MODEL_MATRIX_KIT.md` — generic walkthrough for the opt-in live LangGraph
  requirements-triage model-matrix kit.
- `langgraph_requirements_triage/` — live model-matrix example using local
  Kitaru model aliases, a deterministic evaluator descriptor, and a static HTML
  verdict report.
- `manifests/` and `reports/` — generated-output directories. Generated files
  should stay untracked unless explicitly allowlisted as sanitized samples.

## Candidate descriptor shape

The candidate descriptor is deliberately small:

```json
{
  "label": "Cheaper deterministic support agent",
  "flow_inputs": {
    "agent_profile": "candidate"
  },
  "checkpoint_overrides": {},
  "notes": "Replay the same support cases from draft_response..."
}
```

Replay Lab does not need to know what `agent_profile` means. It only passes the
candidate's supported replay changes into Kitaru replay. In this example, the
flow uses that input to choose the deterministic candidate behavior.

## Optional: live requirements-triage model matrix

The default demo is deterministic on purpose. It makes the baseline replay
column easy to trust.

The opt-in live variant is documented in `MODEL_MATRIX_KIT.md`. It uses a
LangGraph requirements-triage flow and compares local Kitaru model aliases such
as `cheap`, `balanced`, and `quality`. The committed matrix does not contain
provider model names; you register those aliases locally before running it.

Start with the deterministic path in this README, then move to the model-matrix
kit when you want realistic live model calls.

## Keeping generated files out of git

`seed_observed.py`, `run_replay_lab.py`, and `render_report.py` write generated
files under `manifests/` and `reports/`. These artifacts are useful locally, but
should not be committed by default.

The live model-matrix kit has exactly two tracked report artifacts, both
sanitized samples:

```text
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.json
examples/end_to_end/replay_lab/langgraph_requirements_triage/reports/requirements-triage-sample.html
```

Generated manifests, generated JSON reports, generated Markdown reports, and
generated HTML reports from local or live runs should stay untracked unless they
are intentionally sanitized and allowlisted first. The `.gitkeep` files only keep
empty output directories present in the repository.
