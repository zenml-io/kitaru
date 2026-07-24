# Replay Verify — Synera design assistant (LangGraph)

A Synera-tailored cut of the Replay Verify demo. Same engine as
[`../replay_verify_imported_cases`](../replay_verify_imported_cases), but the
agent under test is a real **LangGraph** mechanical-engineering assistant —
Synera's stack — and the cohort is fabricated mechanical-engineering traces (no
customer data, no IP).

## The story it tells

Synera ships LangGraph chat agents that help engineers go from a request to CAD
geometry to an FEA-validated result, for customers like BMW, Airbus, and defense
primes. Someone proposes a cheaper / faster agent config to cut cost. Can it
ship? You can't just re-run it against past customer sessions — those ran on the
customer's infra and touch sensitive IP.

Replay Verify gives the missing step: **prove a cheaper or changed agent won't
regress, using imported LangFuse traces, with nothing leaving your machine.**

The planted regression here is realistic: the "cheaper" candidate config silently
**stops running FEA validation** on simulation requests. The engine catches it.

## Run it (no credentials, no server, no login)

```bash
uv run python -m examples.replay_verify_synera_langgraph.run_synera_demo \
  --report-dir ./synera-reports
```

```text
Replay Verify — Synera design assistant (LangGraph), candidate vs baseline
──────────────────────────────────────────────────────────────────────────
  7 imported cases
  ✓ 3 Match     (cheaper config agrees with the current agent)
  ✗ 2 Drift     (cheaper config diverged — see report)
  ⤼ 2 Skipped   (couldn't test safely — missing evidence / stale corpus)

  RECOMMENDATION:  Don't ship  (2 cases drifted)

  Full report:  open ./synera-reports/verification_report.html
```

- **Drift** is the catch: on both simulation cases the candidate dropped
  `run_fea_simulation` from its tool trajectory and flipped `risk_status` from
  `safe` to `needs_review`. That is white-box, per-step tool-selection drift —
  not just a final-text diff. Open `verification_report.html` for the
  field-by-field baseline-vs-candidate comparison.
- **Skipped** cases fail closed: one trace had no recorded output, one retrieved
  from a stale standards corpus. The engine refuses to grade what it can't
  faithfully replay, and says exactly why.
- **Don't ship** — the recommendation, in plain language.

This uses a deterministic rule-based router as an LLM stand-in, so it runs
offline and reproducibly. The agent is a genuine compiled LangGraph `StateGraph`
(`synera_agent.py`); only the model is stubbed.

## Scan mode — "are my traces even replayable?"

The opener for a first conversation. Point the same validators at *uninstrumented*
LangFuse traces and get a checklist instead of an error — how many are
verifiable today, and exactly which fields unlock the rest. Zero setup, nothing
shared.

```bash
uv run python -m examples.replay_verify_synera_langgraph.run_synera_scan \
  --report-dir ./synera-scan
```

```text
Synera trace scan — can these LangFuse traces be replayed yet?
────────────────────────────────────────────────────────────
  0 of 5 traces are verifiable as-is
  Checklist (which fields unlock the rest):  open ./synera-scan/scan_checklist.md
```

The honest answer for raw chat traces is "0 of 5" — the checklist then ranks the
missing fields (`available_tools`, `tool_calls`, `model`, `prompt_or_config`, …)
so there's a concrete instrumentation to-do instead of a vague "add more
logging."

## What's here

| File | Purpose |
|---|---|
| `synera_agent.py` | The LangGraph mechanical-eng assistant (router → tools → finalize). Baseline vs candidate differ only by a `skip_fea_validation` config flag. |
| `synera_runner.py` | Plugs the LangGraph agent into the Replay Verify engine (the `ImportedRunnerCallable` contract). |
| `generate_synera_cohort.py` | Writes the fabricated imported-case cohort (JSONL). |
| `run_synera_demo.py` | The verify run: baseline vs candidate, Match/Drift/Skipped, HTML report. |
| `run_synera_scan.py` | The scan: fidelity checklist for uninstrumented traces. |
| `fixtures/` | The cohort + uninstrumented observations (all fabricated). |

## Maps to Synera's stack

- **Framework:** LangGraph (`StateGraph`) — their actual agent framework.
- **Observability:** LangFuse-shaped imported traces (the `scan` path mirrors a
  real LangFuse export).
- **Deployment:** runs as a plain script today; the durable variant
  (`../replay_verify_imported_cases/durable_verify_flow.py`) runs the same engine
  as a Kitaru flow with cohort + reports persisted as artifacts and baseline
  lanes cached across candidate iterations — deployable on their own infra.

## Honesty contract

Same as the parent demo: this is **imported-input fresh execution, not
deterministic checkpoint replay**. The runner builds outputs from imported
input, imported tool availability, and imported retrieval metadata only — it
never reads `observed_output` to flatter a candidate. Field comparison is on
`policy_label` (discipline), `risk_status`, `tool_names`, and
`retrieval_document_ids`; free-text responses are not scored.
