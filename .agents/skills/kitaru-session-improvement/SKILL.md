---
name: kitaru-session-improvement
description: Guide a first-time Kitaru user from imported agent traces to an evidence-backed improvement. Use to inspect sessions, interview a domain expert in plain language, persist review answers as investigation annotations, create behavioral cohorts and an evaluator, replay a candidate agent version, and compare the result. Explain Kitaru concepts only when they become useful and operate Kitaru resources for the user.
---

# Kitaru Session Improvement

Lead one complete improvement loop. The user supplies domain judgment. Handle Kitaru concepts, identifiers, filters, code, and workflow operations for them.

## Load the contracts

Read these references before acting:

- [references/current-cli.md](references/current-cli.md) before registration, import, investigation, or local evaluator commands
- [references/current-mcp.md](references/current-mcp.md) before the first MCP call
- [references/cohort-interview.md](references/cohort-interview.md) before asking the first improvement question
- [references/current-investigations.md](references/current-investigations.md) before creating an investigation or annotation

Inspect the agent repository, its trace source, and any walkthrough named by the user. Do not assume a particular agent, ticket set, cohort, evaluator, or improvement goal.

Do not look for a completed custom evaluator before the interview. The evaluator is an output of the approved behavior.

## Keep the experience understandable

- Reach the first business question after one bounded preflight and baseline summary. Do not front-load a glossary.
- Ask one plain-language question at a time.
- Refer to cases and observed actions in user-facing text. Carry UUIDs and version references yourself.
- Explain each Kitaru concept in one or two sentences immediately before using it.
- Batch independent reads and cache exact identities.
- Use standard-mode MCP for supported reads, investigation review, cohort and experiment management, and workflow starts. Use the CLI for local file upload, agent registration, and evaluator scaffold, test, and registration.
- After one capability or response-validation failure for a resource family, use the documented fallback for that family.
- Do not require the user to choose session IDs, write evaluator code, or translate their goals into Kitaru objects.
- Treat coding-agent shell calls as fresh processes. Load the environment in each automated call.

Use these explanations when each concept becomes necessary:

- Session: "A session is one recorded run of your agent."
- Built-in evaluator: "An evaluator applies the same check to every session. The built-in checks measure cost, latency, and tool use, which help us choose what to inspect."
- Investigation: "An investigation is a bounded review of selected sessions. It stores the questions, progress, and answers so the review can resume later."
- Annotation: "An annotation stores one review answer and can point to the exact trace node that supports it."
- Cohort: "A cohort is a saved group of sessions that show the same behavior. It lets us replay and compare those cases together."
- Custom evaluator: "I will turn the behavior we agreed on into a repeatable check for baseline and replayed sessions."
- Replay: "A replay runs the same recorded input through the candidate agent. This can make paid model calls."
- Experiment: "An experiment keeps evaluator versions together so baseline and candidate are measured by the same rules."

## Respect the product boundary

- Do not depend on replay-readiness metadata. Prove compatibility by completing a replay.
- Do not request server score sorting. Sort bounded results locally when needed.
- Persist per-session review answers and evidence as investigation annotations. Keep investigation-wide shipping criteria in the approved brief because investigations currently have no global-answer field.
- Do not edit Kitaru core or importers.
- Do not edit the agent until the user asks for the proposed improvement.
- Do not start paid model calls without approval.
- Ask before cohort writes when the user requested that boundary.
- Treat approval of the behavior brief as approval to create, test, and register its evaluator. Ask again only when implementation requires a missing behavior choice.
- Use exact IDs and immutable versions after discovery.
- Read remote state after an uncertain mutation before retrying.
- Never print secrets.

## Keep an evidence ledger

Track the exact baseline agent version, session IDs and case labels, built-in evaluator versions and results, investigation identity and questions, annotations and node selectors, approved cohort memberships, evaluator version, candidate version, experiment runs, replay sessions, and comparison evidence. Do not create a repository state file.

## 1. Inspect and preflight

Describe the agent's purpose, input, output, state, tools, and side effects in plain language. Confirm whether actions are real or mocked.

Confirm the server, exact agent identity, worker, importer, bundled evaluators, and trace count. Show the selected server and dashboard link before creating resources. Give the shortest recovery command for a missing worker or bundled plugin.

## 2. Resolve the baseline

Resolve or register the exact baseline version. Import traces only when the expected set is absent. List sessions by exact agent-version identity plus source attributes such as origin and tag. Never use a shared tag alone for later selection.

## 3. Gather broad signals

Run cost, latency, and tool-call-pattern evaluators against the bounded baseline set through `kitaru_workflow_start` when available. Poll with `kitaru_activity_read`. Join results to sessions and summarize the range, repeated tool paths, terminal actions, failures, and case families. These signals help select review cases but do not define good behavior.

Ask: "What is the most important outcome for this agent to get right?"

## 4. Select and create an investigation

The investigation must choose its own bounded review set. The user does not choose session IDs.

Use the stated goal, broad evaluator signals, final outputs, errors, and trace patterns to select a small diverse set containing likely problems, nearby successes, and counterexamples. Explain the selection using case labels.

Before creating the investigation, define a fixed ordered question set that can be asked for every selected session:

1. an outcome judgment such as acceptable, problematic, or uncertain, with a reason;
2. the expected behavior for this case;
3. when useful, one goal-specific evidence question derived from the user's outcome.

Create an investigation with those questions, ordered sessions, and curated views. Every curated view item must contain a non-empty selector that points to exact session evidence. Follow [references/current-investigations.md](references/current-investigations.md).

## 5. Interview and persist answers

Work through one investigation session at a time. Show its curated evidence, ask one fixed question at a time, and persist each answer immediately as an annotation. Use a node selector when the answer concerns a specific call, output, error, or metadata value. Mark the investigation session complete only after every question has an answer. Skip only when the case cannot be judged and record why.

Alternate likely problems with comparison cases. Find the earliest decision or tool action that made a result wrong, risky, or unsupported. Resume an existing investigation by reading its status, linked sessions, and annotations rather than repeating answered questions.

After the bounded review, summarize one to three behavior hypotheses. Each must include an observable behavior, supporting annotations, counterexamples, and unresolved ambiguity.

## 6. Approve the behavior brief

Prepare a brief containing the improvement goal, reviewed observations, failure rule, expected behavior, target inclusion rule, control inclusion rule, primary success measure, guardrails, examples, counterexamples, and missing-evidence behavior. Ask the user to correct or approve it. This is the behavior decision.

## 7. Create target and control cohorts

Map the approved rules to exact reviewed sessions. Propose separate target and control cohorts with purpose, included case labels, annotation evidence, counterexamples, and excluded near-misses. After approval, create exact immutable versions and verify their membership. Reuse only an exact matching version.

## 8. Author the evaluator

Turn one approved behavior into a binary observable rubric with explicit pass and fail definitions, representative examples, explanations, and missing-data behavior. Invoke `$kitaru-evaluator-authoring` in autonomous mode with the approved brief and representative target and control sessions. The evaluator must use evidence available in both imported and replayed sessions.

Report the plain-language criterion, generated file, behavioral tests, immutable evaluator version, and baseline distribution. Revisit the brief if the measured distribution disagrees with reviewed evidence.

## 9. Select and replay the candidate

Explain which approved failure the proposed code or configuration change targets and which control behavior could regress. Resolve or register an exact candidate version. Ask for approval before paid replay.

Create or reuse an experiment with exact evaluator versions. Include the custom evaluator as the primary measure and only relevant guardrails. Set the replay tool policy explicitly. Start target and control runs through `kitaru_workflow_start`, then poll runs and child jobs through `kitaru_activity_read` until terminal.

## 10. Decide from evidence

Compare baseline and candidate by case label. Report terminal action, primary evaluation transition, comparable guardrail deltas, tool-path change, replay status, and exact resource identities. Keep failed replays visible. Mark a metric unavailable when baseline and replay do not record comparable evidence.

Answer the approved decision rule: did every target failure become correct, did every control remain correct, did a guardrail regress, and is the evidence sufficient? Recommend use, revision, or more evidence with a short reason.

## Completion criteria

Finish only when the baseline is understood, investigation answers and exact evidence are persisted, the behavior brief is approved, meaningful target and control cohorts exist, the evaluator matches the approved rubric, paid replay approval has been handled, both runs are terminal, and the recommendation traces back to exact evidence.
