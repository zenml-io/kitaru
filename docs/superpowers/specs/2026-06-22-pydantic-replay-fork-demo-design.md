# PydanticAI replay & fork demo — design

**Date:** 2026-06-22
**Branch:** feat/langgraph-replay-fork (worktree)
**Scope:** A self-contained example that tells the Kitaru "durability + evaluation" story for **PydanticAI** agents (the LangGraph half is covered by `examples/end_to_end/replay_fork_demo/`; the LangFuse-import half is Alex's). Target: a narrated demo for the **AI Engineer Summit (Friday)**.

## Why

From the replay strategy meetings (2026-06-17, "Let's talk replay" / "Replay"): Kitaru is positioned as an **eval/experimentation environment** — you *wrap your production agent with a Kitaru adapter* to debug and evaluate it, you don't rewrite it in Kitaru. The team is deliberately **leaning into PydanticAI users and de-emphasizing LangGraph**, so the headline demo for the summit should be PydanticAI. The arc the team wants (Alexej's "debugging → shipping"): reproduce a real run → experiment (swap model/prompt, mock a tool) → compare → scale to a cohort/regression.

## Decisions (from brainstorming)

- **Mechanism: native-wrap.** Wrap the PydanticAI agent with `KitaruAgent(agent, checkpoint_strategy="calls")` so it runs **as a durable Kitaru execution** from the start (each model/tool call is a checkpoint). No external-trace reconstruction (that's the LangGraph/LangFuse track). Reproduce is then the existing `client.executions.replay(exec_id, from_=…)`.
- **Agent: support-copilot, mirrored.** Rebuild the reference support-copilot as a PydanticAI agent (typed `SupportDecision` output, a couple of tools, deps). Both demo halves share one relatable scenario (the permission case) and differ only by framework — "same eval story, LangGraph or PydanticAI."
- **Fork edit: cheaper model + looser prompt.** Swap `gpt-5-mini → gpt-5-nano` and a looser permission prompt → surfaces the `needs_review → safe` permission regression (same as the LangGraph demo).
- **Model: real** `gpt-5-mini`/`gpt-5-nano` for the recorded demo; **`TestModel`** (PydanticAI's deterministic fake) for CI tests.
- **Scope: spine + small cohort.** Single-case spine first, then a cohort over the agent's real history.
- **Cohort source: the agent's last 10 Kitaru executions** (`client.executions.list` filtered to this agent), not a hand-authored scenario set. For each, fork/reproduce **from the same fixed intermediate checkpoint, skipping any execution that lacks it.**
- **New self-contained example folder**, not bolted onto the LangGraph one. Reuse the existing `_drift` comparator and `comparison_html` report.

## The central bet (and fallback)

`KitaruAgent` **blocks per-run `model=` overrides** by design (it raises). So a fork can't just "replay with a different model." The intended path — the native analog of the LangGraph fork — is:

> **fork = construct a new `KitaruAgent` wrapping a different-model / different-prompt agent, then replay the baseline execution from a cut with it** (cached head reused, live tail re-runs under the forked config).

This is **unproven for a wrapped PydanticAI agent** and is the make-or-break risk. Therefore:

- **The implementation plan's first task is a validation spike**: confirm that replaying a recorded wrapped-agent execution from a checkpoint, using a *separately-constructed* fork agent, re-runs the tail under the new config and yields a comparable execution.
- **Fallback if the spike fails:** fork = **re-run the fork agent fresh on the same root input** and compare to the baseline run (no cut / no cached head). Less elegant (re-runs everything live, costs more), bulletproof, and still tells the full reproduce→fork→compare→cohort story. The choice is made from the spike result, not assumed.

Reproduction is **not** at risk — it's the existing native replay.

## Architecture

```
wrap:        KitaruAgent(support_agent, checkpoint_strategy="calls")   # durable native execution
run:         agent.run_sync(scenario)  -> Kitaru execution (per-call checkpoints, in the dashboard)
reproduce:   client.executions.replay(exec_id, from_=CUT)             # cached head, live tail, no edits
fork:        new KitaruAgent(nano + looser-prompt agent); replay exec_id from CUT  (spike) | re-run fresh (fallback)
compare:     semantic-field drift (reuse _drift.compare_decisions / DriftReport)
cohort:      executions.list(agent) -> last 10 -> reproduce+fork each from CUT (skip if absent) -> aggregate
report:      replay_vs_fork.html (reuse comparison_html) + an aggregate cohort summary
```

`CUT` is a fixed, stable checkpoint selector for the decision step (the model call that produces `SupportDecision`). Resolved via the existing `replay.py` selector logic; the cohort skips executions where it can't be resolved.

## Components (`examples/end_to_end/pydantic_replay_fork/`, self-contained)

- **`agent.py`** — the PydanticAI support-copilot: `Agent[SupportDeps, SupportDecision]` on a real model, 1–2 tools (customer lookup / permission check), baseline vs `trimmed_permissions` instructions, a `build_agent(model, prompt_profile)` factory.
- **`utils.py`** — the only domain glue: `build_agent`, the fork-agent factory, the fixed `CUT` selector, a typed-decision extractor, the cohort query (`last_executions(agent_name, n=10)`), and the HTML report writer (delegating to `comparison_html`).
- **`replay_fork.py`** (or fold into utils) — the thin facade over `KitaruAgent` + `client.executions.replay`: `run`, `reproduce(exec_id)`, `fork(exec_id, model, prompt_profile)`, `diff`, `cohort()`. Reuses `kitaru.adapters.langgraph.replay._drift` for the comparator (framework-agnostic) — or a small shared `_drift` if cleaner.
- **`demo.py`** — a `click` CLI mirroring the LangGraph one: `run`, `reproduce`, `fork`, `cohort`, `run-all` (narrated: production run → reproduce → fork → compare → cohort).
- **bundled support data** (tools' local data) kept inside the folder for self-containment.
- **A local `.env`** (gitignored) for `OPENAI_API_KEY`.

## The demo (`run-all`, narrated)

1. "Your PydanticAI agent ran in production — wrapped with Kitaru, every call is a durable checkpoint." → run a scenario, print exec id + dashboard URL.
2. "Reproduce it from the decision step — cached head, live tail." → reproduction drift: False.
3. "Fork it before shipping: gpt-5-nano + looser permissions." → fork the run.
4. "Compare → did the change move the decision?" → fork drift: `needs_review → safe`; write `replay_vs_fork.html`.
5. "Now across your last 10 runs." → cohort reproduce+fork from `CUT`, print "K/10 regressed" + per-case lines.

## Testing

- **Unit/integration with `TestModel`** (no API key, deterministic): wrap → run → reproduce (drift≈0) → fork (scripted nano model flips the decision) → compare; cohort over a few seeded executions.
- **The validation spike** gets its own test proving the chosen fork mechanism re-runs the tail under the forked config.
- **Real-model path** is exercised manually for the recording (guarded by `OPENAI_API_KEY`), not in CI.
- Follow the repo's flow-test convention (`primed_zenml` fixture; isolated config) used by the existing replay tests.

## Out of scope

- External-trace import / LangFuse for PydanticAI (Alex's track).
- Tool-result mocking during replay (the fork is model+prompt; tool-mock is a later "mock tools" feature).
- Per-call surgical edits / call-level fork beyond the fixed `CUT`.
- A new shared cross-framework adapter abstraction — reuse what exists; generalize later only if a third framework needs it.

## Affected / new files

- New: `examples/end_to_end/pydantic_replay_fork/{agent.py, utils.py, demo.py, __init__?, .env(gitignored), data}`.
- Reuse (no change expected): `kitaru.adapters.langgraph.replay._drift`, `examples/.../replay_fork_demo/comparison_html.py` (or copy a slim renderer in for self-containment).
- Possibly touch `src/kitaru/adapters/pydantic_ai/` only if the spike shows the fork mechanism needs a small, well-scoped hook (decided from the spike, not assumed).

## Amendments — 2026-06-22 (after the replay-reframe discussion + multi-step spike)

These supersede the matching parts above.

- **One primitive, reframed around replay.** Drop "fork" as a distinct verb. *Reproduce* = `replay` from a cut with no edits (this is `kitaru executions replay --from …`, CLI-native). *Experiment* = `replay` from the cut **re-running the chosen step under a new config** + a global model/prompt swap. Both are "replay with/without edits."
- **Multi-step agent, realized as a Kitaru `@checkpoint` flow.** A single `KitaruAgent` making multiple calls under `checkpoint_strategy="calls"` yields sibling terminal checkpoints (`_MultipleTerminalStepsOutputError`) — confirmed by the multi-step spike (`docs/superpowers/notes/2026-06-22-pydantic-multistep-spike.md`). The reliable shape is the agent composed as a flow of explicit `@checkpoint` steps (`gather → decide → finalize`), each running the PydanticAI agent (a raw `pydantic_ai.Agent`; `KitaruAgent` is a passthrough inside an explicit `@checkpoint`). This gives a real intermediate step with a cached head. The single-step `KitaruAgent` one-liner is NOT used (it can't expose an intermediate step). Tasks 2–4's single-step agent is reworked to this multi-step flow.
- **Reconfigure = kind #2 (re-run the step under a new config), NOT output-override.** The chosen intermediate step re-executes under a different model/prompt/params, plus a global config change. Mechanism: replay from that step's checkpoint with a reconfigured agent (the head is cached, the step + tail re-run under the new config). `CUT` = the intermediate step's checkpoint; per the "first invocation" clarification, address it by `invocation_id`/`call_id` (a step invoked multiple times → the first invocation's `call_id`).
- **Cohort metrics = improvement, not just drift.** Over the agent's last 10–100 executions, compute per-run + aggregate **cost + latency** (from Kitaru's tracked usage) and an **LLM-judge quality score**, plus how many decisions changed. "Is it an improvement?" = cheaper/faster/at-least-as-good across the cohort.
- **CLI split.** Reproduce/replay uses the existing `kitaru executions replay`. The experiment (reconfigured-agent replay), cohort, and metrics live as a thin example layer around the agent (they need the agent to re-run). The single-run inspect/reconfigure is a frontend action; the example demonstrates the same capability headlessly.
