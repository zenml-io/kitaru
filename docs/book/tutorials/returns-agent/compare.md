---
description: Compare baseline and replay evidence and state the result at the size the cohort supports.
icon: code-compare
---

# 5. Compare the paired evidence

**Observe → Judge → Define → Replay → Compare**

A conclusive improvement or regression claim requires every expected original-and-replay comparison under the same evaluator versions. An incomplete run is still useful evidence for diagnosing an inconclusive result. In this final phase, you will inspect run health, read the available pairs, preserve failures and missing results, and state the narrow conclusion supported by your reviewed cohort.

## Confirm the run completed

List experiment runs and inspect the exact run receipt:

```bash
uv run kitaru experiment run list --size 20
uv run kitaru experiment run get "$RUN_ID"
uv run kitaru experiment run jobs "$RUN_ID" --size 100
```

Confirm that the run attempted every member of `$COHORT_REFERENCE` and that every expected replay has an explicit terminal state. A failed, canceled, or missing replay makes the result incomplete. Do not silently remove it and reduce the denominator.

## Inspect replay sessions and evaluations

```bash
uv run kitaru session list \
  --agent returns-resolver \
  --origin replay \
  --size 20

uv run kitaru evaluation list --size 100
```

For every cohort member, put the imported baseline and replay beside one another. Compare:

- the result from `$BEHAVIOR_EVALUATOR`;
- the accepted terminal tool calls and their results;
- the final structured output;
- tool-health and timing measurements;
- cost and token use; and
- replay, evaluation, or job failures.

Open [http://localhost:8000](http://localhost:8000) to inspect paired traces. The evaluator tells you whether its encoded rule passed. The trace shows how the agent reached the outcome and whether the measurement missed important evidence.

## Classify each transition

Use the human-reviewed role of each session when interpreting the transition:

| Baseline | Replay | Interpretation to investigate |
| --- | --- | --- |
| Fail | Pass | The target case may have improved. Check the trace and operational measurements. |
| Pass | Pass | The reviewed behavior was preserved for this case. Check for other regressions. |
| Pass | Fail | The candidate regressed on this reviewed case. |
| Fail | Fail | The candidate did not fix this case, or the evaluator still lacks required evidence. |
| Known result | Unknown or missing | The comparison is inconclusive for this case. |

Do not force every change into pass or fail. A candidate can improve the primary behavior while increasing tool failures, latency, or cost enough to create a real trade-off.

## State the conclusion at the right size

Use one overall evidence conclusion:

| Conclusion | What the evidence says | What to do next |
| --- | --- | --- |
| **Improved** | Target cases improved and reviewed counterexamples remained acceptable. | Expand the reviewed population or preserve this cohort as a regression check. |
| **Regressed** | A target or counterexample became worse. | Inspect the paired traces, revise the change, and register a new agent version. |
| **Trade-off** | One important measure improved while another became worse. | Decide whether the trade-off is acceptable or change the candidate. |
| **Inconclusive** | A replay failed, required evidence is missing, or the population cannot support the needed claim. | Repair execution or add reviewed evidence before deciding. |

Your statement should name the exact cohort and behavior. A defensible form is:

> On `$COHORT_REFERENCE`, candidate `$CANDIDATE_AGENT` [improved, regressed, traded off, or produced inconclusive evidence for] the reviewed behavior measured by `$BEHAVIOR_EVALUATOR`. This result applies to the frozen reviewed sessions; it does not establish general safety or production readiness.

The ten supplied traces are a small synthetic population. Your selected worklist is also adaptive: you chose it partly because the traces looked interesting. Do not infer production prevalence or general agent quality from this experiment.

## If the result is inconclusive

Inconclusive is not a near-pass. Preserve the reason:

- If a replay failed, inspect its child job and rerun only after correcting the execution problem.
- If tool evidence is missing, change the replay policy or instrumentation rather than guessing the outcome.
- If the evaluator is wrong, register a new evaluator version and apply it consistently to both sides.
- If the cohort lacks a necessary counterexample, create a new cohort version with reviewed membership.

Keep the old versions. Their immutability makes it possible to explain why two experiment runs reached different conclusions.

## Optional: generate fresh traces

The supplied export makes setup repeatable, but its model outputs are not an answer key. To create a new export, create `.env` in the example directory with valid `OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, and `LANGFUSE_SECRET_KEY`, then run:

```bash
./generate.sh
```

The script makes ten paid agent runs, waits for the Langfuse observations, and replaces `traces/langfuse-traces.jsonl`. Model behavior varies. Import the new file, inspect what actually happened, and build a new review worklist from that evidence.

For your own agent, keep collecting traces where you already collect them and use [Import your traces](../../getting-started/import-your-traces.md) to select the matching importer. Historical investigation does not require the original code to remain runnable. Replay does require a compatible registered candidate and a worker that can execute it.

## Clean up

Stop the worker in Terminal 2 with `Ctrl-C`, then disconnect the CLI:

```bash
uv run kitaru logout
```

For a CLI-managed local workspace, logout stops its containers but keeps the PostgreSQL data volume.

## What you completed

You followed the full evidence chain:

1. **Observed** a trace population before assigning labels.
2. **Judged** selected sessions and stored human reasoning beside exact evidence.
3. **Defined** one behavior with a frozen reviewed cohort and evaluator version.
4. **Replayed** one candidate under an explicit tool policy.
5. **Compared** complete baseline and replay evidence without hiding failures or uncertainty.

The durable result is not a predetermined passing demo. It is an auditable claim about one reviewed behavior, one frozen population, and one candidate.

## Where to go next

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Use kitaru-investigation</strong></td><td>Apply this method to the agent in your own repository.</td><td><a href="../../agent-native/setup.md">../../agent-native/setup.md</a></td></tr><tr><td><strong>Build a regression suite</strong></td><td>Grow reviewed evidence into a reusable comparison.</td><td><a href="../../guides/regression-suite.md">../../guides/regression-suite.md</a></td></tr><tr><td><strong>Replay and overrides</strong></td><td>Control models, tools, history, and replay safety.</td><td><a href="../../guides/replay-and-overrides.md">../../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Write an evaluator</strong></td><td>Design and calibrate a domain-specific evaluator.</td><td><a href="../../guides/write-an-evaluator.md">../../guides/write-an-evaluator.md</a></td></tr></tbody></table>
