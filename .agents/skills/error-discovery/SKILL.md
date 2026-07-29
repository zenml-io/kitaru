---
name: error-discovery
description: Guide a human through the bounded Kitaru baby-vp error-discovery workflow. Use when the user asks to inspect the preloaded agent traces, discover failure modes, build a cohort, draft a narrow scorer, or validate that scorer in the error-discovery MCP App.
---

# Error discovery

Keep the roles separate:

- The human notices and judges.
- This skill organizes the investigation and compiles accepted judgments.
- The MCP App makes full trace evidence legible.
- Suggestions remain provisional until the human accepts or dismisses them.

Do not silently label the traces, name a taxonomy before review, or substitute
your judgment for the user's.

## 1. Start the review

1. Call `start_error_discovery`.
2. Tell the user the first pass is open coding, not category assignment.
3. Ask them to read each trace backward:
   - inspect the final response;
   - decide whether it follows from the evidence and execution;
   - expand grouped tool calls and results;
   - find the first upstream decision that made the outcome wrong, risky, or
     unsupported.
4. Ask for editable free-text observations. Treat
   acceptable/problematic/uncertain as optional holistic judgments.

The fixture is already loaded. Do not ask about imports, credentials, Langfuse,
or trace generation.

## 2. Alternate breadth and depth

Begin in breadth mode. Encourage review of different scenarios and agent
configurations. Do not synthesize until the App accepts a finished batch, which
requires at least four non-empty observations across three scenarios.

When an observation suggests a failure:

1. Let the user request a related trace.
2. Describe every related-trace suggestion as a high-recall proposal.
3. Let the user compare or dismiss it.
4. Move into depth around the possible failure.
5. Return to breadth and inspect a dissimilar counterexample.

If the user says “I finished this batch,” “done reviewing,” or similar, call
`read_review_state`. Respect the server's minimum-review gate. Do not count a
judgment without a free-text observation.

## 3. Propose hypotheses

Use only annotations returned by `read_review_state`. Do not use unreviewed
traces as supporting evidence.

Propose one to three hypotheses. For each include:

- a short provisional title;
- an observable binary definition;
- reviewed evidence trace IDs;
- reviewed counterexample trace IDs;
- the main unresolved ambiguity.

Call `save_provisional_hypotheses`. Say plainly that these are agent
suggestions, not accepted failure modes.

Ask the user to revisit cited examples in the App. Later examples may change
the criterion. Encourage editing earlier observations and re-reviewing examples
rather than defending the first hypothesis.

## 4. Require human acceptance

Ask the user to choose, edit, or reject one binary failure-mode definition.
Require:

- one observable behavior;
- at least one reviewed in-scope example;
- at least one reviewed out-of-scope example from a different scenario;
- short reasons for both;
- no claim about the agent's hidden intent.

Prefer the App's exact-draft confirmation control. It produces a one-use token.
When the host context reports that token, call
`commit_accepted_failure_mode` with the reported session ID, revision, and
token. Do not alter the confirmed draft.

If the App confirmation control is unavailable, ask the user to reopen the App
and use it. Do not mint confirmation from model-authored chat arguments. Never
interpret silence or continued conversation as acceptance.

## 5. Compile one narrow scorer

After acceptance, draft one binary scorer for that failure mode only. Include:

- a single observable criterion;
- an explicit `Pass` definition;
- an explicit `Fail` definition;
- two to four human-reviewed discovery examples;
- at least one Pass and one Fail example;
- a short explanation for each example.

Do not use held-out traces as demonstrations. Do not generate or execute scorer
code.

Have the human edit and confirm the exact rubric in the App. Commit only the
returned token with `commit_scorer_rubric`. The accepted rubric hash is the
identity of the scorer run.

## 6. Run the blinded held-out check

1. Call `get_unlabeled_heldout_traces` with the accepted scorer ID.
2. Apply the accepted rubric yourself, as the Claude host model.
3. Produce exactly one `Pass` or `Fail` and a short trace-grounded rationale for
   each of the six trace IDs.
4. Call `record_scorer_run` once with all six predictions, the scorer ID, and
   the exact rubric hash.
5. Only after that call succeeds, call `reveal_validation_results` with the
   immutable scorer-run ID.

Never read `fixtures/private-heldout-labels.json`, ask another tool or agent to
inspect it, or infer labels from source variant names. The held-out tool
contract intentionally omits labels and expected outcomes.

## 7. Review disagreements

Show agreements, false passes, and false fails. For each disagreement, compare
the scorer rationale with the newly revealed human rationale.

Question the possible causes in this order:

1. Is the rubric ambiguous or too broad?
2. Are the discovery examples unrepresentative or contradictory?
3. Did the scorer ignore observable trace evidence?
4. Is the human label itself questionable?

Do not rationalize the scorer after seeing the label. If the criterion changes,
state that the old scorer run no longer validates the revised rubric.

End by saying that six deliberately selected held-out traces are useful for
debugging the rubric, but cannot establish production accuracy or
generalization.

## Boundaries

Do not add imports, provider connections, replay, prompt editing, live trace
generation, background labeling, clustering, generic taxonomy management, or
production persistence. Keep the work inside this Act 3 loop.
