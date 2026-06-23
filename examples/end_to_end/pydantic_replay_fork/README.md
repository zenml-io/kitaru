# PydanticAI replay demo — debug a production run, then prove the fix across a cohort

You built a support agent with **PydanticAI** and made each run a durable Kitaru flow, so every run is
a durable execution made of checkpoints. Something looks wrong in a production run. This example shows
the whole loop, in code — using **plain Kitaru SDK primitives**, no wrapper:

1. **run** — your agent runs in production (durable, checkpointed).
2. **reproduce** — replay that run from an intermediate checkpoint with *no* edits (cached head, live
   tail) to confirm you can faithfully reproduce it.
3. **edited replay** — replay from the same checkpoint *with* a config change (cheaper model + looser
   prompt) and see whether the decision moved.
4. **cohort** — apply that *same* change across your last N production runs and measure whether it's an
   improvement: cheaper, faster, and quality-no-worse — or what regressed.

There is only one replay concept here — the SDK's `flow.replay(...)`. "Reproduce" and "edited replay"
are the **same call**: with no overrides it reproduces faithfully; with overrides it re-runs the tail
under new config.

## The story, in one screen

```python
from support_copilot import support_copilot_flow

# A production run — the one SDK call that starts a durable execution.
handle = support_copilot_flow.run(prompt, customer, "openai:gpt-5-mini", "baseline")
handle.wait()                 # block until the flow reaches a terminal state
exec_id = handle.exec_id      # its execution id

# Reproduce from the `decide` checkpoint — NO edits. gather_context is served
# from cache; decide + finalize re-run live.
reproduced = support_copilot_flow.replay(exec_id, from_="decide", cache=False)
reproduced.wait()

# The SAME call, now WITH edits — a cheaper model + looser prompt override the
# flow inputs, so decide + finalize re-run under the new config.
edited = support_copilot_flow.replay(
    exec_id, from_="decide", cache=False,
    model="openai:gpt-5-nano", prompt_profile="trimmed_permissions",
)
edited.wait()
```

Read `demo.py` top to bottom for the full arc — it reads almost like a notebook, with each step in its
own function so you can run just one. The cohort runner (`cohort.py`) loops the same
`support_copilot_flow.replay(...)` over recent runs and applies bring-your-own metrics.

## Run it

The agent calls OpenAI through PydanticAI, so set your key (the demo does not load `.env` for you):

```bash
cd examples/end_to_end/pydantic_replay_fork
export OPENAI_API_KEY=sk-...        # or: set -a && source .env && set +a

uv run python demo.py run-all       # the full narrated arc
```

Individual commands (each maps to a function in `demo.py` you can read on its own):

| command | what it does |
|---|---|
| `uv run python demo.py run` | run the agent once; print the exec id + decision |
| `uv run python demo.py replay <EXEC-ID>` | reproduce from `decide` (no edits), then replay it edited under `gpt-5-nano` + looser prompt; write `replay_vs_rerun.html` |
| `uv run python demo.py cohort` | apply the change to the last N runs; print metric deltas + regressions |

## Running on a remote stack (Kubernetes)

The same demo runs unchanged on a containerized stack — the flow declares its image
needs (`@flow(image=ImageSettings(...))` in `support_copilot.py`): it installs
`pydantic-ai` into the image and pulls `OPENAI_API_KEY` into the pod from a Kitaru
secret named `openai-creds`.

Create that secret once (the key must be named `OPENAI_API_KEY` so pydantic-ai picks
it up automatically):

```bash
kitaru secrets set openai-creds --private --OPENAI_API_KEY=sk-...
```

Then point at your stack and run as usual:

```bash
kitaru stack use <your-k8s-stack>
uv run python demo.py run-all
```

> **Troubleshooting:** if you hit `ApiClient.call_api() got an unexpected keyword
> argument 'response_type'` when submitting to Kubernetes, your local `kubernetes`
> client is too new for the stack's connector — pin it: `uv pip install "kubernetes<26"`.

## The same operation from the CLI

`demo.py` tells the SDK story. The exact same replay is a first-class Kitaru CLI command — the SDK's
`flow.replay(...)` and the CLI are two surfaces over one concept:

```bash
# reproduce (no edits): replay from the decide checkpoint — cached head, live tail
kitaru executions replay --from decide <EXEC-ID>

# edited replay: re-run decide + finalize under a new config
kitaru executions replay --from decide <EXEC-ID> \
    --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'
```

Kitaru serves every checkpoint before `decide` from cache and re-runs `decide` onward live; `--args`
overrides the flow inputs so the re-run steps pick up the new config. Add `--output json` to capture
the new execution id programmatically.

## The agent

A three-step flow, each step a plain `pydantic_ai.Agent`; the `@checkpoint` boundaries make the steps
durable and addressable for replay:

```
gather_context  →  decide  →  finalize
                   ↑ the CUT
```

- `decide` is the intermediate step you replay from (`CUT = "decide"`). Its prompt is what changes
  between the `baseline` profile (permission/SSO/admin changes are `needs_review`) and the
  `trimmed_permissions` profile (answer directly). Reconfiguring this step is what flips the decision.
- Output is a typed `SupportDecision` (policy label, risk status, required action, summary).
- "Did the decision move?" is judged on the decision fields (`risk_status`, `required_action`); the
  free-text `policy_label`/`summary` are reworded by the model each call, so they don't count as drift.

Config (`model` + `prompt_profile`) travels as flow inputs, so the steps rebuild their agents in any
process — that's why the SDK *and* the `kitaru executions replay` CLI both reproduce a run from a fresh
process with no in-memory state.

## Cohort metrics

`run_cohort(exec_ids, baseline_model=..., variant_model=..., variant_prompt_profile=..., metrics=[...],
repeats=N)` replays each case twice — once with no edits (baseline) and once with the variant config —
skipping any case without the `decide` checkpoint, and compares them with **bring-your-own metrics** —
a metric is just a callable `metric(baseline, variant) -> MetricDelta` where `baseline`/`variant` are
plain `ReplayRun` records (`exec_id`, `decision`, `model`). Three are provided in `utils`:

- `cost` — `display_cost_usd` from Kitaru's usage tracking (lower is better)
- `latency` — wall-clock seconds (lower is better)
- `quality_judge` — an LLM judge scoring the answer 1–5 (higher is better)

`report.summary()` prints per-metric baseline-vs-variant means and an `improvement` verdict (every
metric no-worse). `report.regressions()` returns just the metrics (and decision changes) that got
worse. `repeats` averages the variant over N runs to smooth out nondeterminism.

## Files

| file | purpose |
|---|---|
| `agent.py` | the PydanticAI support agent: typed outputs + per-step prompt profiles |
| `support_copilot.py` | the durable flow: the three `@checkpoint` steps + `@flow`, plus `recent_exec_ids()` |
| `demo.py` | the walkthrough: `run` / `replay` / `cohort` as plain functions over SDK primitives |
| `cohort.py` | `run_cohort(...) -> Report` with `summary()` / `regressions()` |
| `utils.py` | analysis helpers: cost/latency/quality metrics, the judge, decision extraction, `ReplayRun` |
| `comparison_html.py` | the reproduce-vs-edited HTML report |

## Validating

This example is validated by **real runs** — a real model and the real Kitaru backend, not mocks.
Set `OPENAI_API_KEY` (and your Kitaru connection), then:

```bash
uv run python demo.py run-all     # exercises run → replay → compare → cohort end to end
```
