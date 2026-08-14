---
description: The whole loop on real traffic — import sessions, review what actually happened, freeze the evidence into a cohort, and prove a change against it with an experiment.
icon: route
---

# The full loop, end to end

The [quickstart](quickstart.md) takes one run and forks it. This guide
takes a *population*: you start from traces you already have, work out
what is actually going wrong, freeze that evidence, and then prove a fix
against it. That is the loop Kitaru exists for, and it is the shape of
every real improvement.

It runs on the canonical example, which is checked in and tested in CI —
so if a command here drifts, that example is the source of truth:

```bash
cd examples/pydantic_ai_ticket_resolver
```

A returns agent that sometimes refunds when it should escalate. Nine
steps, about half an hour.

{% hint style="info" %}
Prefer to be walked through it? `kitaru-investigation` is an
[agent skill](../agent-native/skills.md) that runs this journey
conversationally, choosing the review batch for you and stopping at the
same checkpoints. This page is the same loop by hand.
{% endhint %}

## 1. A server and a worker

```bash
kitaru login --local     # Docker-backed local server, becomes your active server
kitaru agent register returns-resolver --command "python -m examples.pydantic_ai_ticket_resolver.agent"
kitaru worker start
```

The worker matters more than it looks. Nothing you do from here
*executes* your agent — the server records the intent, and a worker you
control runs the code, in your environment, with your keys. Keep it
running in its own terminal.

## 2. Get sessions in

Two ways in, and they converge on the same object. Either wrap your
agent with an [adapter](../adapters/README.md) so runs record natively,
or import traces you already collect:

```bash
kitaru session import traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --media-type application/x-ndjson \
  --wait
```

Tag the import. That tag is how you address this population later, and
it is much harder to reconstruct after the fact. Importers ship for
Langfuse, LangSmith, Braintrust, OpenTelemetry and JSONL — see
[Import any trace format](../guides/importing-sessions.md).

## 3. Let the cheap evaluators describe the population

Before deciding anything, run the built-in descriptive evaluators. They
do not know what *good* means for your business, but they will tell you
where cost, latency and tool behavior are unusual:

```bash
kitaru session evaluate --tag returns-baseline \
  --evaluator kitaru/cost@latest \
  --evaluator kitaru/latency@latest \
  --evaluator kitaru/tool-call-patterns@latest --wait

kitaru evaluation list --size 100
```

This is a search aid, not a verdict. It narrows where to look.

## 4. Review what actually happened

Now the part no evaluator can do for you. Open an
[investigation](../concepts/investigations.md), work through the
sessions, and record what you find as annotations pinned to the exact
node that shows it:

```bash
kitaru annotation create \
  --investigation-session "$INVESTIGATION_SESSION_ID" \
  --question-key outcome \
  --selector "{\"node_id\":\"$REFUND_NODE_ID\"}" \
  --value '{"judgment":"problematic","reason":"Amount exceeds the automatic approval threshold."}'

kitaru investigation session verdict "$INVESTIGATION_ID" "$SESSION_ID" problematic
```

Answers and the verdict are separate on purpose: the answers are the
per-question data you will calibrate against later, the verdict settles
the session as a whole.

Resist naming the failure category before you have looked. A taxonomy
invented up front is the most common way to review fifty sessions and
learn nothing.

## 5. Say what "good" means, once

You now have a behavior you can state in a sentence. Turn it into an
[evaluator](../concepts/evaluators.md) so it can be checked
automatically forever after:

```bash
kitaru evaluator scaffold returns-policy
kitaru evaluator test returns-policy --session "$SESSION_ID"   # offline, no worker
kitaru evaluator register returns-policy --script returns_policy.py --entrypoint evaluate
```

Test offline before registering. An evaluator that disagrees with your
own labels is worse than none — see
[Write an evaluator](../guides/write-an-evaluator.md) for calibrating it
against the annotations from step 4.

## 6. Freeze the evidence

A [cohort](../concepts/cohorts.md) is the reviewed sessions, frozen. Two
of them, because a change that fixes the broken cases while wrecking the
healthy ones is not a fix:

```bash
kitaru cohort create unsafe-refund-baseline --agent returns-resolver \
  --description "Refunded despite a rule requiring escalation." \
  --session "$TICKET_004" --session "$TICKET_007"

kitaru cohort create safe-refund-control --agent returns-resolver \
  --description "Correctly handled refunds — the regression guard." \
  --session "$TICKET_001" --session "$TICKET_002"
```

Cohort versions are immutable. `unsafe-refund-baseline@1` means the same
sessions next month as it does today, which is the whole point.

## 7. Make the change

Register the fix as a new agent version. Code changes need no override —
the new version *is* the change:

```bash
kitaru agent version register returns-resolver \
  --command "python -m examples.pydantic_ai_ticket_resolver.agent" \
  --env RETURNS_POLICY_MODE=strict
```

## 8. Run the experiment

An [experiment](../concepts/experiments.md) replays a whole cohort
against the change, with the evaluators pinned so both sides are scored
identically:

```bash
kitaru experiment create improve-returns-policy --agent returns-resolver \
  --evaluator returns-policy@1 \
  --evaluator kitaru/cost@latest \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}'

kitaru experiment run start improve-returns-policy \
  --cohort-version "$TARGET_COHORT_VERSION_ID" \
  --agent returns-resolver@2 \
  --evaluate-baselines --wait --timeout 1800
```

`--evaluate-baselines` scores the original sessions too. Without it you
have new numbers and nothing to compare them to.

Run it twice — once for the target cohort, once for the control. The
control is what turns "it fixed the bad cases" into "it fixed the bad
cases and left the good ones alone."

## 9. Read the result honestly

```bash
kitaru experiment run get "$RUN_ID"
```

Four outcomes are possible, and only one of them is "ship it": improved,
regressed, traded off (better on one evaluator, worse on another), or
inconclusive. Inconclusive is a real result on a small cohort — the
answer is more evidence, not a rounder number.

The cohort that caught this failure is now a regression suite. Replay it
against the next change too; that is how the loop compounds. See
[Build a regression suite from production](../guides/regression-suite.md).

## Where this goes next

* Automate the boring half with the [agent skills](../agent-native/skills.md) —
  `kitaru-investigation` for steps 2–6, `kitaru-replay-experiment` for 8–9.
* Wire the cohort into CI so a regression blocks the merge.
* Widen the population: this ran on a handful of sessions, and the same
  loop runs on a week of traffic without changing shape.
