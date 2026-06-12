# Replayability demo (CLI) — engineering requirements triage

Built for a LangGraph/requirements-triage audience. Shows the one thing the
dashboard can't: **replay a case from a checkpoint with DIFFERENT params and
watch the output diverge** — here, a cheaper model silently drops a mandatory
safety sign-off. Everything is via the **CLI/SDK**, deterministic (no API keys,
no live model, identical every run), and needs no dashboard.

## One command (run this; or run the beats below by hand)

```bash
bash examples/end_to_end/replay_lab/requirements_triage_lab/run_demo.sh
```

## Or type the beats live

```bash
cd $(git rev-parse --show-toplevel)
export ZENML_DISABLE_CLIENT_SERVER_MISMATCH_WARNING=True
RL=examples/end_to_end/replay_lab/requirements_triage_lab

# BEAT 1 — seed a few "production" requirements-triage executions.
uv run python $RL/seed_observed.py
#   -> note the bracket-load-signoff execution id it prints.

# BEAT 2 — THE PAYOFF: replay that case from the draft step with a cheaper model.
kitaru executions replay <BRACKET_EXEC_ID> --from draft_response \
  --args '{"agent_profile": "candidate"}'
#   -> a new execution; checkpoints before draft_response are reused, the rest re-run.

# Show the divergence (original vs replay):
uv run python $RL/show_divergence.py <BRACKET_EXEC_ID> <REPLAY_EXEC_ID>

# BEAT 3 — Replay Lab over the whole cohort -> ship/caution/HOLD verdict.
uv run python $RL/run_replay_lab.py
uv run python $RL/render_panel.py
```

## What lands on screen

- **Original (current model):** *"…the design needs an independent sign-off
  before manufacturing…"* — quality 1.0.
- **Replay (cheaper model):** *"…the design is approved to proceed; no further
  review needed."* — quality 0.42, **DROPPED: independent sign-off**.
- **Cohort verdict:** 1 HOLD (the bracket), 2 caution (cost wins), recommendation
  **HOLD**. ~51% cheaper, but it dropped a safety rule on a load-bearing part.

## Talk track

> *"In production you have hundreds of these triage runs. I want to switch to a
> cheaper model. So I replay this exact case — from the draft step — but with the
> cheaper model. Watch: the current model required an independent sign-off on a
> load-bearing part. The cheaper one drops it and says 'approved to proceed.'
> Replay Lab flags that as a hold across the whole cohort. You caught a silent
> safety regression before it ever shipped — and this is all your code, your
> stack, replayed from real checkpoints. The same wrapping works on your
> LangGraph agents via our adapter."*

## Why this is reliable (the anti-disaster notes)

- **No dashboard.** Replay-with-different-params is a CLI/SDK capability; the
  dashboard does not do it. Don't demo it from the UI.
- **Deterministic.** champion vs candidate are fixed behaviors, so the divergence
  and the verdict are identical every run — nothing flaky, no live LLM.
- **Pre-run once** before the call so a warm result is ready; then re-run live for
  effect (it's ~60–90s for the full script).
- The agent here is deterministic on purpose. For a real model swap (live LLM),
  the sibling `pydantic_support` lab does the same with `current` vs `cheap`
  model aliases.
