# Cohort discovery interview

Use this interview after importing sessions and running broad evaluators. Learn which behavior matters before creating cohorts or a custom evaluator.

## Interview rules

- Ask one question at a time.
- Explain unfamiliar terms immediately before use.
- Refer to sessions by case label or description, not UUID.
- Use trace evidence to make each question concrete.
- Accept uncertainty and offer a recommendation grounded in observed patterns.
- Do not turn cost, latency, or tool counts into cohorts without a user goal that makes them relevant.
- Persist every per-session answer before moving on.
- Keep the main path short when the answers are clear.

## Start with the outcome

Ask: "What is the most important outcome for this agent to get right?"

If this is too broad, offer examples derived from the agent's actual purpose, outputs, tools, and failure modes.

## Choose a bounded review set

Use the answer plus broad evaluator signals to choose likely problems, nearby successes, and counterexamples. The user should judge behavior, not select session IDs.

Create fixed questions for every selected session:

1. Is the outcome acceptable, problematic, or uncertain, and why?
2. What should the agent have done in this case?
3. What trace evidence or domain condition determines that judgment?

The third question may be specialized to the stated goal. Keep question keys stable across the investigation.

## Review each session

Show the curated evidence first. Inspect the final output, relevant tool calls, and the earliest decision that produced the outcome. Ask one question, persist its answer as an annotation, then continue. Use exact node selectors for evidence-specific answers.

Alternate likely problems with comparison cases. Before accepting a rule, test it against at least one counterexample or nearby successful case.

## Form a hypothesis

State the emerging hypothesis with one observable behavior, supporting annotations, counterexample annotations, and the main unresolved ambiguity. Let the user correct it.

Ask which successful behavior they are most worried about breaking. This answer defines the control rule.

## Define the decision rule

Ask: "What evidence would make you comfortable using the new version?"

Turn the answer into a primary measure and guardrails. Keep global shipping criteria in the approved brief. Investigation annotations store the per-session judgments and evidence.

## Behavior brief

Present this summary before cohort creation:

| Field | Agreed meaning |
|---|---|
| Goal | The business behavior to improve. |
| Failure rule | The observable behavior that counts as wrong. |
| Expected behavior | What the agent should do instead. |
| Observations | Reviewed cases, annotations, selectors, and reasons. |
| Hypothesis | One rule, supporting cases, counterexamples, and remaining ambiguity. |
| Target cohort | Sessions that exhibit the failure or risk condition. |
| Control cohort | Similar successful sessions that must remain correct. |
| Primary measure | The evaluator result that determines success. |
| Guardrails | Other regressions to review. |
| Missing evidence | How the evaluator handles an unjudgeable session. |

Ask: "Does this capture what you want to improve?" Do not create cohorts or evaluator code until the user approves or corrects it.
