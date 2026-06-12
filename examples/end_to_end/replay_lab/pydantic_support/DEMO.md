# Durable support agent → Replay Lab: end-to-end demo

A two-act demo for a retail / home-improvement customer-support audience. It
shows Kitaru's durable execution for a real PydanticAI agent, then uses Replay
Lab to answer the question a production team actually has: **"is a cheaper model
safe to switch to?"**

Everything is generic — no real company, customer, or product names.

## What it showcases

- **Act 1 — a real durable agent.** A PydanticAI customer-support chatbot whose
  every model call and tool call is a durable, replayable checkpoint, with
  human-in-the-loop waits. It uses faked, deterministic tools (`check_stock`,
  `lookup_order`, and a **guarded `issue_refund`** that refuses to confirm a
  refund and escalates to a human).
- **Act 2 — Replay Lab regression test.** Replay a cohort of support cases
  against a cheaper model and compare behavior, cost, and quality, ending in a
  ship / caution / hold verdict. Two flavors:
  - **Real model swap (PydanticAI):** the same agent, replayed with a cheaper
    model alias — only the model differs.
  - **Deterministic fallback:** a no-LLM support flow that always produces a
    crisp **Hold** on the regulated case. Use this if anything is flaky live.

## Prerequisites (run before the call)

```bash
# Connected to a Kitaru server (local `kitaru login`, or a remote server)
export ZENML_DISABLE_CLIENT_SERVER_MISMATCH_WARNING=True   # quiet version notices
export OPENAI_API_KEY=...                                  # for the live agent

# Register the model aliases Replay Lab swaps between.
uv run kitaru model register current --model openai/gpt-4o-mini
uv run kitaru model register cheap    --model openai/gpt-3.5-turbo
```

## Act 1 — run the durable agent (live)

Interactive (you type), the way a person would use it:

```bash
uv run python examples/chatbot/ui.py
```

Or a reliable scripted run (no typing — drives a fixed support conversation to
completion). Good for rehearsal and for a hands-off live run:

```bash
uv run python examples/chatbot/run_scripted_chat.py
```

Then open the printed **Execution URL** in the dashboard and point at:

- the flow with its **model-request and tool-call checkpoints** (`check_stock`,
  `issue_refund`),
- the **wait** points (human-in-the-loop),
- the refund turn: the agent calls `issue_refund`, sees it needs verification,
  and **escalates instead of claiming the refund is done**.

> Talk track: *"Every step this agent takes — model calls, tool calls, the waits
> for the customer — is a durable checkpoint. Kitaru has the code and the
> execution, not just traces. In production you'd have thousands of these runs."*

## Act 2 — Replay Lab: is the cheaper model safe?

### Primary: real model swap on the PydanticAI agent

```bash
# 1. Seed a cohort of observed runs (uses the `current` alias). Pre-run this.
uv run python examples/end_to_end/replay_lab/pydantic_support/seed_observed.py

# 2. Replay each case against the cheaper alias and compare.
uv run python examples/end_to_end/replay_lab/pydantic_support/run_replay_lab.py

# 3. Render the HTML report to show on screen.
uv run python examples/end_to_end/replay_lab/render_report.py \
  --json-path examples/end_to_end/replay_lab/pydantic_support/reports/pydanticai-support-replay-lab-demo.json \
  --output-path examples/end_to_end/replay_lab/pydantic_support/reports/report.html
```

Open `reports/report.html`. Each case shows the observed vs cheaper-model output
and a verdict. With `cheap = gpt-3.5-turbo`, the headline is the **refund case**:
the cheaper model **drops the refund safeguard** — it stops escalating for
verification — so its quality falls to 0.5 vs 1.0 for the baseline and the
overall recommendation is **Hold**. The deterministic evaluator names exactly
why: *"Refund safeguard dropped: response does not escalate / require
verification before implying the refund is handled."*

> Talk track: *"The cheaper model looks fine on stock and order questions — but
> on the refund case it quietly drops the safeguard and stops escalating.
> Replay Lab holds the switch and tells you the exact case and the exact reason,
> before a customer ever sees it."*

Note: per-lane **cost/latency show as n/a** on a server-backed local artifact
store (runtime logs aren't readable from the server), so the quality regression
is the payoff here. Use the deterministic fallback below if you want hard cost
numbers on screen.

### Fallback: deterministic Replay Lab (guaranteed Hold)

No LLM, no API key, no flakiness — always produces the same Hold verdict, with a
quality loss on the regulated case and clear cost savings:

```bash
uv run python examples/end_to_end/replay_lab/seed_observed.py --small
uv run python examples/end_to_end/replay_lab/run_replay_lab.py
uv run python examples/end_to_end/replay_lab/render_report.py \
  --json-path examples/end_to_end/replay_lab/reports/support-replay-lab-demo.json \
  --output-path examples/end_to_end/replay_lab/reports/report.html
```

> Talk track: *"The cheaper model saves cost on every case — but Replay Lab holds
> it back, because on the regulated case the quality drops. You catch that before
> a customer ever sees it. That's the production discipline you already trust us
> for, now on agents."*

## Demo-safety notes

- **Pre-run Act 2 before the call** and just open the rendered report live — no
  waiting on LLM latency in front of the room.
- Act 1 is the only live piece. Even there, the checkpoints and tool calls are
  visible in the dashboard while the run is mid-conversation; you do not need it
  to reach `completed` to tell the story.
- The deterministic fallback is fully offline — keep its report open in a tab as
  a safety net.
- `check_stock`, `lookup_order`, and `issue_refund` are deterministic fakes, so
  the only thing that differs between baseline and candidate replay is the
  model. That is what makes the comparison an honest regression test.
