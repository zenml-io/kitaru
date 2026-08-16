---
description: Turn accepted human evidence into one frozen cohort and evaluator version.
icon: list-check
---

# 3. Define one behavior to test

**Observe → Judge → Define → Replay → Compare**

A verdict says what a reviewer concluded about one complete session. A repeatable test needs a more precise behavior definition, a frozen population, and a measurement that reads observable trace evidence.

## Accept one observable behavior

Use only the persisted annotations and confirmed verdicts from your investigation. Write one binary definition that answers:

1. Under which observable conditions does the behavior matter?
2. Which recorded agent action passes?
3. Which recorded agent action fails?
4. Which tool or external outcome evidence is required?
5. What result should the evaluator return when evidence is missing?
6. Which reviewed counterexamples limit the definition?

For example, “the agent should handle refunds correctly” is too broad. A usable definition names the required recorded conditions and distinguishes an accepted action from a claim in the final response.

Keep agent behavior separate from a tool or provider failure. If a trace lacks the external evidence required to judge an outcome, record that uncertainty instead of turning absence into a pass.

## Freeze the reviewed population

A [**cohort**](../../concepts/cohorts.md) is a named population of sessions. A **cohort version** freezes one exact membership list so later experiment runs use the same evidence.

Before creating it, list the exact reviewed target cases that exercise the behavior you want to change and the reviewed counterexamples that could expose overcorrection. Confirm the membership, then create the cohort:

```bash
uv run kitaru cohort create returns-regression \
  --agent returns-resolver \
  --description "Human-reviewed sessions for one accepted returns behavior." \
  --display-version initial-review \
  --session YOUR_REVIEWED_SESSION_UUID \
  --session YOUR_COUNTEREXAMPLE_SESSION_UUID
```

Verify the immutable version and its members:

```bash
uv run kitaru cohort version get returns-regression@1
uv run kitaru session list --cohort returns-regression@1 --size 20

COHORT_REFERENCE="returns-regression@1"
```

The cohort should contain only sessions whose role in this behavior is supported by the review. Testing only problematic sessions can make a blunt change look successful. Counterexamples test whether nearby behavior that was already acceptable remains acceptable.

Create a new cohort version when membership changes. Existing versions remain unchanged. Set `COHORT_REFERENCE` to the exact accepted version before continuing.

## Select or create an evaluator

Inspect the installed [evaluator](../../concepts/evaluators.md) catalog before writing code:

```bash
uv run kitaru evaluator list
```

Use an installed evaluator when it expresses the accepted behavior. Pin its exact version and parameters, then save the reference for the remaining pages:

```bash
BEHAVIOR_EVALUATOR="NAME@VERSION"
```

If no installed evaluator fits, scaffold a narrow deterministic evaluator:

```bash
uv run kitaru evaluator scaffold \
  returns-behavior \
  --path evaluator.py
```

Replace the scaffold with code that implements the behavior you accepted during review. The canonical example README includes a [generic terminal-action consistency evaluator](https://github.com/zenml-io/kitaru/tree/develop/examples/pydantic_ai_ticket_resolver#7-select-or-create-an-evaluator) to demonstrate the `SessionView` and `EvaluationResult` contracts. Adapt the rule, required evidence, and missing-evidence result to your accepted behavior.

If you want coding-agent help, ask it to implement only the accepted behavior from the persisted investigation and show you how each branch follows from recorded evidence. Tell it not to read or use the example's test-only expected outcomes. Review the resulting code before registering it.

Do not map ticket or session identifiers to expected answers. Do not search the customer reply for words such as `refund` when tool results provide stronger evidence. A useful evaluator distinguishes, for example, an accepted refund from a claimed refund, multiple accepted terminal actions from one, and missing action evidence from a pass.

Validate and register the implementation:

```bash
uv run kitaru evaluator test \
  evaluator.py \
  --entrypoint evaluate

uv run kitaru evaluator register \
  returns-behavior \
  --script evaluator.py \
  --entrypoint evaluate \
  --description "Evaluate one human-reviewed returns behavior from trace evidence." \
  --display-version initial-review
```

Kitaru assigns the first version the reference `returns-behavior@1`. The version pins the evaluator code and parameters used by later comparisons.

Save that reference:

```bash
BEHAVIOR_EVALUATOR="returns-behavior@1"
```

## Calibrate against human evidence

Apply the evaluator to the frozen baseline cohort:

```bash
uv run kitaru session evaluate \
  --cohort "$COHORT_REFERENCE" \
  --evaluator "$BEHAVIOR_EVALUATOR" \
  --wait

uv run kitaru evaluation list --size 100
```

Compare each evaluation with the investigation's annotations and verdicts. Report agreement, disagreement, and unknown results. A script that loads successfully is not necessarily a valid measurement, and agreement on a small reviewed sample does not make the evaluator production-ready.

When the evaluator disagrees with a human judgment, inspect the trace and the rule. The correct response may be to fix the evaluator, refine the behavior, mark the case uncertain, or create a new cohort version. Register changed evaluator code as a new version, then update `BEHAVIOR_EVALUATOR`. Update `COHORT_REFERENCE` whenever you accept a newer cohort version. Do not change the expected label merely to make the metric pass.

## Checkpoint

You now have:

- one precise behavior accepted from persisted human evidence;
- `COHORT_REFERENCE`, set to the exact accepted cohort version;
- `BEHAVIOR_EVALUATOR`, set to the exact installed or custom evaluator version; and
- baseline evaluations checked against the human review.

No agent or model has run yet. Continue to [4. Replay one bounded change](replay.md).
