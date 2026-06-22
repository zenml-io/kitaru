# PydanticAI demo — SIMPLIFY refactor (2026-06-22)

Supersedes the R1–R5 surface. Behavior is unchanged and already tested; this restructures the
example so the story file reads like the promise and all boilerplate (incl. the judge + metrics)
moves to `utils`. No SDK changes.

## Verbs (user-locked 2026-06-22)
- **`rerun`** — re-execute from the cut with NO config change (cached head, live tail). (was `reproduce`)
- **`replay`** — re-execute from the cut WITH a config change (new model/prompt at the `decide` step
  + global). (was `experiment`/`fork`)
- "fork" is dropped entirely.

## Target story (what the demo code should read like)
```python
from support_copilot import KitaruAdapterPA
from utils import cost, latency, quality_judge
from cohort import cohort

agent  = KitaruAdapterPA(model="openai:gpt-5-mini")            # wrap your PydanticAI agent — no rewrite
exec_id = agent.run(prompt, customer)                           # a production run (durable checkpoints)

rerun  = agent.rerun(exec_id)                                   # no edits: cached head, live tail
replay = agent.replay(exec_id, at="decide",                     # WITH edits: reconfigure the decide step
                      model="openai:gpt-5-nano",
                      prompt_profile="trimmed_permissions")
replay.diff(rerun)                                              # compare -> DriftReport

report = cohort(agent.last_executions(10)).experiment(          # apply the SAME change across a cohort
    agent, variant=replay.recipe,
    metrics=[cost, latency, quality_judge], repeats=1)
report.summary()        # aggregate deltas (cost / latency / quality / decision changes)
report.regressions()    # the ship-blocker: what got worse
```
`rerun`/`replay` EXECUTE and return a result handle (no `.run()` ceremony). `at="decide"` defaults to
`CUT`. Do NOT build speculative `skip=` / per-call `edits=[at(id,…)]` — global model/prompt + the
`decide` step is the validated mechanism; don't overfit the sketch.

## File layout (`examples/end_to_end/pydantic_replay_fork/`)
- `agent.py` — the PydanticAI support agent (domain). Unchanged.
- `support_copilot.py` — THE STORY SURFACE. The module-level `@checkpoint` steps + `@flow`, the
  `ContextVar` `_activate` machinery, and `KitaruAdapterPA` with ONLY: `__init__(model, prompt_profile)`,
  `run`, `rerun`, `replay`, `last_executions`, and the small `RunHandle` returned by rerun/replay
  (`.exec_id`, `.decision`, `.recipe`, `.diff(other)->DriftReport`). Reads top-to-bottom like the promise.
- `utils.py` — ALL boilerplate: cost/latency extraction; artifact/decision reading; `Recipe` dataclass
  (the captured edit set: model/prompt_profile/at); the **judge** (`QualityScore`, `build_judge`,
  `quality_judge` metric); the built-in metrics `cost`/`latency`; cohort row/aggregate internals shared
  with cohort.py. The CUT constant.
- `cohort.py` — `cohort(cases) -> Cohort`; `Cohort.experiment(agent, *, variant: Recipe, metrics, repeats=1)
  -> Report`; `Report.summary()` (aggregate deltas + decision-change count + improvement verdict) and
  `Report.regressions()` (per-metric/decision items that got worse). BYO metric = a plain callable
  `metric(baseline: RunHandle, variant: RunHandle) -> MetricDelta(name, baseline, variant, lower_is_better)`.
- `demo.py` — click CLI adapted to the new verbs: `run`, `rerun`, `replay`, `cohort`, `run-all`
  (narrated). `rerun` stays CLI-native (`kitaru executions replay --from decide`). No "fork"/"experiment".
- `comparison_html.py` — adapt to render `replay` vs `rerun`.

## Tests
- Migrate the existing pipeline tests to the new module/verb names (rerun/replay), keep their assertions
  (incl. the reproduce-after-experiment race test → now rerun-after-replay; head-cached proof; replay
  flips decision; improvement-logic unit test). Add cohort `summary()`/`regressions()` tests with
  synthetic deltas + a TestModel cohort. CLI smoke test updated to the new command set. All green.

## Out of scope
- SDK changes; `skip=`/per-call edits; the LangGraph demo (this is pydantic-only).
