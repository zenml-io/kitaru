# Synera — design-partner strategy & demo notes

Internal notes for the Synera (Ruben Bertelo) conversation. Pairs with the
runnable demo in this folder. Grounded in the two Grain calls (May 20 discovery,
June 9 technical deep-dive with Hamza).

## Who / where they are

- **Synera** — low-code platform for mechanical engineers (CAD/FEA workflow
  automation). Customers: BMW, Airbus, NASA, Volkswagen, incl. **defense**.
- Building an **agentic layer**: LangGraph chat agents that help engineers go
  from request → CAD geometry → FEA-validated result.
- **Ruben Bertelo** — Tech Lead, ~4 months in. Team of 8, several still
  onboarding; key engineer joins end of month. Prior ZenML user (personal
  projects). Prefers **email**; internal comms on Teams.
- Their tooling-assessment window closes **~June 23**.

## Their stack (what the demo must match)

| Layer | Synera |
|---|---|
| Language | Python |
| Agent framework | **LangGraph** (LangChain ecosystem) — multiple chat agents |
| Observability | **LangFuse** (callback handler; not LangSmith) |
| Serving | FastAPI, **in-process** execution |
| Deployment | Mostly **on-prem / customer infra** (data sovereignty); versioned releases |

## The pain (his words)

1. **Evaluation is greenfield** — manual, shallow, only a user-feedback loop.
2. Wants **white-box** eval: tool selection, routing, summarization — at
   trajectory *and* per-step level.
3. **No regression safety net** — can't catch model/quality drift before a
   release reaches BMW.
4. **The reproduction nightmare (emotional core):** a customer hits an edge case
   on *their* infra; it reaches Synera third-hand. *"It's impossible for me to
   know with any confidence what is happening."*

## His dream (stated unprompted)

> Customer hits an unknown edge case → Synera gets the LangFuse trace → replays
> it in their own environment to reproduce → diagnoses (swap model / prompt /
> harness) → promotes it to a **regression test case** → datasets and ground
> truth grow over time. Across many scenarios, many customers.

That is a product spec. Kitaru Replay Verify is the implementation.

## Colleague's questions, answered

**Which stack does this run on?**
- Design-partner phase: **self-hosted, on their infra**. Start with the
  script/durable demo locally (a `local` or `local_remote` Kitaru stack); the
  durable verifier runs as a Kitaru flow they can host via Docker. Production
  orchestration target later is **Kubernetes** (their customers bring K8s).
- Nothing leaves their walls — the whole demo runs offline on fabricated traces.

**Which adapter do they use?**
- **LangGraph → `KitaruGraphRunner`** + the **LangFuse trace import** path. This
  is unambiguous from the calls. (Hamza floated PydanticAI as "easier"; Ruben
  said no — it must be LangGraph. The demo is built on LangGraph for exactly
  this reason.)
- Checkpoint strategy: `graph_call` to start (one checkpoint per invoke);
  `calls` mode (via `KitaruLangGraphMiddleware`) later for true per-step
  white-box replay, which is what his tool-selection eval ultimately wants.

**How do we give them the best experience?**
1. **Open with scan mode.** `run_synera_scan` answers "are my traces even
   replayable?" with zero setup and nothing shared — it defuses his #1 blocker
   (data sensitivity + "do my traces have enough metadata?").
2. **Then the verify demo** on his domain: the cheaper config silently skips FEA
   validation; Replay Verify catches it as Drift → Don't ship. He sees his own
   use case.
3. **Then durable mode**: the regression pack that grows, with baseline lanes
   cached across candidate iterations (no repeated baseline spend) — his "grow
   our datasets/ground truth" dream + cost control.
4. **Self-hosted Docker** framing throughout. Defense customers = on their infra.

## Why Kitaru is uniquely positioned (the wedge)

- **LangFuse alone** = observability; it records, it can't *replay with changes*.
- **Generic eval frameworks** = black-box input/output scoring; no concept of
  replaying from step N of a LangGraph with a different model.
- **Build it themselves** = 5 years of ZenML orchestration + the Kitaru SDK; an
  8-person team that just deprioritized eval won't.
- Kitaru's moat is the triangle: **orchestration × LangFuse trace-ingestion ×
  replay-with-mutation** — and it's *shipping code* (PR #412), not a pitch.

Lead with the single most painful, most reproducible win (replay one trace,
catch the regression, lock it in), not the platform vision.

## Next steps (from the calls)

- Hamza to ship a running demo + a PR with the LangFuse adapter, tailored once
  Ruben shares the *shape* of his LangFuse traces (the callback-handler
  instrumentation) — the one thing blocking a fully tailored cut.
- Shared Slack channel (Ruben's personal account) + email. NDA to proceed.
