# PydanticAI replay demo — debug a production run, then prove the fix across a cohort

You built a support agent with **PydanticAI** and wrapped it with **Kitaru**, so every run is a
durable execution made of checkpoints. Something looks wrong in a production run. This example shows
the whole loop, in code:

1. **run** — your agent runs in production (durable, checkpointed).
2. **rerun** — reproduce that run from an intermediate step with *no* changes (cached head, live tail)
   to confirm you can faithfully reproduce it. This is the Kitaru **CLI**.
3. **replay** — re-run from the same step *with* a config change (cheaper model + looser prompt) and
   see whether the decision moved.
4. **cohort** — apply that *same* change across your last N production runs and measure whether it's an
   improvement: cheaper, faster, and quality-no-worse — or what regressed.

The agent is a normal PydanticAI agent. Everything around it is a thin Kitaru wrapper.

## The story, in one screen

```python
from support_copilot import KitaruAdapterPA
from utils import cost, latency, quality_judge
from cohort import cohort

agent   = KitaruAdapterPA(model="openai:gpt-5-mini")     # wrap your agent — no rewrite
exec_id = agent.run(prompt, customer)                     # a production run

rerun   = agent.rerun(exec_id)                            # reproduce: cached head, live tail, no edits
replay  = agent.replay(exec_id, at="decide",              # re-run the decision step under a new config
                       model="openai:gpt-5-nano",
                       prompt_profile="trimmed_permissions")
replay.diff(rerun)                                        # did the change move the decision?

report = cohort(agent.last_executions(10)).experiment(    # apply the SAME change across recent runs
    agent, variant=replay.recipe,
    metrics=[cost, latency, quality_judge], repeats=1)
report.summary()        # aggregate deltas + "is it an improvement?"
report.regressions()    # the ship-blocker: what got worse
```

`rerun` reproduces (no edits); `replay` is the same operation **with** edits. `replay.recipe` captures
the change so the cohort applies exactly that change to every run.

## Run it

The agent calls OpenAI through PydanticAI, so set your key (the demo does not load `.env` for you):

```bash
cd examples/end_to_end/pydantic_replay_fork
export OPENAI_API_KEY=sk-...        # or: set -a && source .env && set +a

uv run python demo.py run-all       # the full narrated arc
```

Individual commands:

| command | what it does |
|---|---|
| `uv run python demo.py run` | run the agent once; print the exec id + decision |
| `uv run python demo.py rerun <EXEC-ID>` | reproduce from the `decide` step via the Kitaru CLI; show drift (should be none) |
| `uv run python demo.py replay <EXEC-ID>` | re-run `decide` under `gpt-5-nano` + looser prompt; write `replay_vs_rerun.html` |
| `uv run python demo.py cohort` | apply the change to the last N runs; print metric deltas + regressions |

## The CLI-native rerun

Reproducing a run is a first-class Kitaru CLI operation — the `rerun` command actually shells out to it:

```bash
kitaru executions replay --from decide <EXEC-ID> --output json
```

Kitaru serves every checkpoint before `decide` from cache and re-runs `decide` onward live. The demo
then diffs the reproduced decision against the original to confirm it reproduced faithfully.

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

## Cohort metrics

`cohort(cases).experiment(agent, variant=recipe, metrics=[...], repeats=N)` reruns (baseline) and
replays (variant) each case from the cut, skipping any case without the cut, and compares them with
**bring-your-own metrics** — a metric is just a callable
`metric(baseline, variant) -> MetricDelta`. Three are provided in `utils`:

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
| `support_copilot.py` | the story surface: `KitaruAdapterPA` (`run`/`rerun`/`replay`/`last_executions`) + the checkpoint flow |
| `cohort.py` | `cohort(cases).experiment(...) -> Report` with `summary()` / `regressions()` |
| `utils.py` | boilerplate: cost/latency/quality metrics, the judge, decision extraction, `Recipe` |
| `demo.py` | the `click` CLI |
| `comparison_html.py` | the rerun-vs-replay HTML report |

## Tests

The suite uses PydanticAI's `TestModel` (deterministic, no API key) and runs from the repo root:

```bash
uv run pytest tests/test_pydantic_replay_fork.py tests/test_pydantic_demo_cli.py -v
```
