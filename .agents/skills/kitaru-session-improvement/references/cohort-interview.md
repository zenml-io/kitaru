# Cohort discovery interview

Use this interview after importing sessions and running broad evaluators. Its purpose is to learn which behavior matters before creating cohorts.

## Interview rules

- Ask one question at a time.
- Explain unfamiliar terms before using them.
- Refer to sessions by case label or a short description, not by UUID.
- Use trace evidence to make questions concrete.
- Accept uncertainty. Offer a recommendation when the user does not know.
- Summarize only when the answer changes the emerging behavior rule. Do not repeat every answer back to the user.
- Avoid yes-or-no questions when more than one earlier option could be confirmed.
- Do not turn cost, latency, or tool counts into cohorts without a user goal that makes them relevant.
- Keep the main path to three or four domain questions when the answers are clear. Ask follow-ups only for unresolved behavior choices.

## Review loop

For each likely behavior, inspect the final output, the evidence used to reach it, and the relevant tool calls. Identify the first earlier decision that made the result wrong, risky, or unsupported.

Capture a plain-language observation with:

- case label and exact session ID;
- optional status: acceptable, problematic, or uncertain;
- observed behavior and supporting trace evidence;
- the earlier decision or tool action that matters;
- the user's reason.

Persistent observations are a future feature. Keep the current observations in the investigation brief and do not claim they were stored on the server.

Alternate focused and broad review. After one observation, show a related session and a counterexample before accepting a general rule.

## Question sequence

### 1. Learn the outcome

Ask: "What is the most important outcome for this agent to get right?"

If the question is too broad, offer examples grounded in the agent: selecting the correct action, obeying safety or approval rules, controlling cost, responding quickly, or writing an acceptable reply.

### 2. Define unacceptable behavior

Show two or three observed patterns and ask: "Which of these would make you stop or roll back this agent?"

Ask for the rule behind the answer. Capture thresholds, exceptions, escalation conditions, or required tool evidence. Inspect the first earlier decision that violated the rule, not only the final response.

### 3. Resolve concrete cases

For each likely failure pattern, ask what the agent should have done. Do not infer the answer from an existing evaluator.

For the canonical returns example, useful prompts are:

- "A $280 defective-item refund exceeds the $200 automatic approval threshold. Should the agent refund, reject, or escalate?"
- "A valid defect claim has an account-takeover risk flag. Should the agent refund or escalate for review?"

### 4. Test the hypothesis with comparison cases

Explain: "A control cohort is a saved group of cases that already work and should remain correct after the change."

Ask: "Which nearby successful behavior are you most worried about breaking?"

For the canonical policy path, recommend valid refunds that exercise different reasoning paths: an ordinary in-window defect, an over-refund request capped at the amount paid, and a defect found after retrying lookup by email.

State the emerging hypothesis with:

- one observable behavior;
- supporting sessions;
- counterexample sessions;
- the main unresolved ambiguity.

Let the user correct the hypothesis before defining the shipping evidence.

### 5. Define the decision rule

Ask: "What evidence would make you comfortable using the new version?"

Turn the answer into a primary measure and guardrails. A canonical answer is: every risky refund escalates, every valid-refund control still refunds the correct amount, no replay fails, and cost or latency changes remain visible for review.

## Investigation brief template

Present this summary before cohort creation:

| Field | Agreed meaning |
|---|---|
| Goal | The business behavior to improve. |
| Failure rule | The behavior that counts as wrong. |
| Expected behavior | What the agent should do instead. |
| Observations | Reviewed cases, supporting trace evidence, and why they matter. |
| Hypothesis | One observable rule, supporting sessions, counterexamples, and remaining ambiguity. |
| Target cohort | Sessions that exhibit the failure or risk condition. |
| Control cohort | Similar successful sessions that must remain correct. |
| Primary measure | The evaluator result that determines success. |
| Guardrails | Cost, latency, tool behavior, replay failures, or other regressions to review. |

Ask: "Does this capture what you want to improve?" Do not create a cohort until the user approves or corrects it.

## Canonical example outcome

When the user chooses refund-policy safety and gives the expected escalation answers, propose:

| Cohort | Cases | Rationale |
|---|---|---|
| `unsafe-refund-baseline` | tickets 004 and 007 | The baseline refunded despite an approval or risk rule that requires escalation. |
| `safe-refund-control` | tickets 001, 009, and 010 | These are valid refunds using ordinary, capped-amount, and lookup-retry paths that must remain refunds. |

Use the broad evaluator results as supporting columns in the proposal. Do not use their ranking as the membership rule.
