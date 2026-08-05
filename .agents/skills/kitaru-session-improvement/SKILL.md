---
name: kitaru-session-improvement
description: Guide a first-time Kitaru user from imported agent traces to an evidence-backed improvement. Use when working with examples/canonical_example or a similar agent repository to inspect real sessions, interview a domain expert in plain language, organize observations into behavioral cohorts, create and register an evaluator from the approved behavior, replay a candidate agent version, and compare the result. Explain Kitaru concepts only when they become useful and do not require the user to operate Kitaru resources or write evaluator code.
---

# Kitaru Session Improvement

Lead the user through one complete improvement loop. The user supplies domain judgment. Handle Kitaru concepts, identifiers, filters, evaluator code, and workflow operations for them.

## Load the contracts

Read these files before acting:

- `examples/canonical_example/agent.py`
- `examples/canonical_example/fixtures.py`
- `examples/canonical_example/README_AGENT_GUIDED.md`
- [references/current-cli.md](references/current-cli.md) before registration, import, or local evaluator commands
- [references/current-mcp.md](references/current-mcp.md) before the first MCP call
- [references/cohort-interview.md](references/cohort-interview.md) before asking the first improvement question

Do not look for a completed evaluator before the interview. The evaluator is an output of the approved behavior, not an input to discovering it.

Read [references/future-investigations.md](references/future-investigations.md) only when the user asks about persistence, investigations, annotations, or the future product path.

## Keep the experience fast and understandable

- Reach the first business question after one bounded preflight and baseline summary. Do not front-load a glossary.
- Explain one concept in one or two sentences immediately before using it.
- Ask one plain-language question at a time. Avoid questions that can be answered with an ambiguous yes.
- Refer to tickets, scenarios, and observed actions in user-facing text. Carry UUIDs and version references yourself.
- Batch independent read-only checks. Cache resolved IDs and do not resolve them again without evidence that server state changed.
- After one MCP validation or capability failure for a resource family, use the structured CLI fallback for that family for the rest of the task. Do not repeat probes that are expected to fail.
- Use standard-mode MCP for registry reads, activity reads, cohort management, and experiment management. Use the CLI for agent and evaluator registration, local trace upload, session-evaluation starts, and experiment-run starts. Do not call MCP tools that are absent from the current registry.
- Anchor commands to the repository root or `examples/canonical_example` as required by the CLI reference. Do not depend on an accidental current directory.
- Treat every coding-agent shell call as a fresh process. Load the example environment in each automated CLI call or use `uv run --env-file .env`; do not assume an earlier `source .env` persists across tool calls or parallel shells.
- Report outcomes and decisions. Keep compatibility details to one sentence unless they block progress.

Use these explanations when the concept first becomes necessary:

- Session: "A session is one recorded run of your agent. Here, each session is one resolved customer email."
- Built-in evaluator: "An evaluator applies the same check to every session. The built-in checks measure cost, latency, and tool use, which help us decide what to inspect."
- Observation: "An observation records what you noticed in one run and why it matters. For now I will retain it in our investigation brief because persistent observations are planned but not available."
- Cohort: "A cohort is a saved group of sessions that show the same behavior. It lets us replay and compare those cases together."
- Custom evaluator: "I will turn the behavior we agreed on into a repeatable check. Kitaru will apply the same check to the recorded baseline and the replayed candidate."
- Replay: "A replay runs the same recorded input through the candidate agent. This can make paid model calls."
- Experiment: "An experiment keeps the evaluator versions together so baseline and candidate are measured by the same rules."

## Respect the current boundary

- Do not depend on replay-readiness metadata. Prove compatibility by completing a replay.
- Do not request score sorting from Kitaru. Sort bounded evaluator results locally when needed.
- Do not claim that interview observations are persisted in Kitaru. Keep them in the conversation and encode accepted behavior in cohorts and evaluators.
- Do not edit Kitaru core or importers.
- Do not edit the example agent unless the user asks for a code change after reviewing a proposal.
- Do not start paid model calls without approval. Import and deterministic evaluation do not make model calls. Replays do.
- Ask before cohort writes when the user requested that boundary.
- Treat approval of the investigation brief as approval to create, test, and register the evaluator that implements it. Ask again only if implementation requires a material behavior choice absent from the brief or would create an ambiguous competing version.
- Do not print secrets or place them in reports or MCP requests.
- Use exact IDs and immutable versions after discovery.
- Read server state after an uncertain mutation before retrying.

## Keep an evidence ledger

Maintain these values in the conversation:

- baseline agent and version identity;
- imported session IDs and their user-facing case labels;
- exact built-in evaluator identities and results;
- observations, supporting sessions, counterexamples, and unresolved ambiguities;
- the approved investigation brief and evaluator rubric;
- proposed and approved cohort membership plus rationale;
- generated evaluator path, tests, result contract, and registered version;
- candidate agent identity;
- experiment runs, replays, result sessions, and comparison evidence.

Do not create a repository state file. Keep UUIDs out of the main explanation unless the user asks, but retain enough exact identifiers to recover the workflow in a later turn.

## 1. Inspect and preflight

Describe the agent's purpose, input, output, state, tools, and side effects in plain language. Confirm whether its actions are real or mocked.

Run one bounded preflight using [references/current-cli.md](references/current-cli.md). Confirm the server, selected agent identity, worker, importer, bundled evaluators, and trace count. Show the selected server and agent dashboard link before creating resources so duplicate agent records are visible early.

If a worker or bundled plugin is missing, explain why it matters and give the shortest recovery command.

## 2. Resolve the baseline

Resolve or register the exact baseline agent version. Reuse an existing version only when its run specification and capabilities match. Do not assume a version number.

List imported sessions through the exact baseline agent-version ID, origin, and tag. Import the checked-in traces only when the expected set is absent. If source trace deduplication attaches the traces to another agent, use the temporary remapping command in the CLI reference after user approval. Do not propose deleting shared server data for this example.

Verify the expected ten completed baseline sessions and cache their IDs. Never use a shared tag alone for subsequent evaluations or cohort membership.

## 3. Gather broad signals

Run the bundled cost, latency, and tool-call-pattern evaluators through the CLI against the cached baseline session IDs. Explain the evaluator concept at this point.

Join results to sessions and summarize the observed range, repeated tool paths, terminal actions, and case families. Show a compact table using case labels. These signals choose useful starting points but do not define good behavior.

Ask the first business question immediately after this summary.

## 4. Review and interview

Follow [references/cohort-interview.md](references/cohort-interview.md). Alternate focused review with comparison:

1. Inspect the final output.
2. Check whether the trace evidence supports it.
3. Expand the relevant tool calls and results.
4. Find the first earlier decision that made the result wrong, risky, or unsupported.
5. Capture a plain-language observation with an optional status of acceptable, problematic, or uncertain.
6. Show a related session and a counterexample to test the emerging rule.

Ask at most the questions needed to learn the outcome, unacceptable behavior, expected behavior in concrete cases, nearby behavior that must remain correct, and the evidence needed to ship. When the user is uncertain, show two or three trace patterns and recommend a starting point with its trade-off.

Propose one to three behavior hypotheses. Each hypothesis must contain one observable behavior, supporting sessions, counterexample sessions, and the main unresolved ambiguity.

End with an investigation brief containing:

- improvement goal;
- reviewed observations;
- observable failure rule;
- expected behavior;
- target-group inclusion rule;
- control-group inclusion rule;
- primary success measure;
- regression guardrails;
- supporting examples and counterexamples.

Ask the user to correct or approve the brief. This confirmation is the behavior decision. Do not ask them to design Python or Kitaru objects.

## 5. Propose and create cohorts

Explain the cohort concept. Map the approved rules to exact sessions by inspecting inputs, outputs, evaluations, and relevant nodes. Present a proposal with cohort purpose, included cases, evidence, counterexamples, and excluded near-misses.

For the canonical policy-safety path, the evidence should support:

- `unsafe-refund-baseline`: tickets 004 and 007, where the baseline accepted a refund despite an approval threshold or risk flag requiring escalation;
- `safe-refund-control`: tickets 001, 009, and 010, where a refund is the reviewed outcome and the candidate must preserve it.

Treat these memberships as the result of the interview. If the user chooses another goal, derive different cohorts or explain that the checked-in candidate does not target it.

Ask for cohort-write approval when required by the user's boundary. Create exact immutable versions after approval. Reuse an exact matching version on reruns. Never overwrite earlier membership.

## 6. Turn the approved behavior into an evaluator

Explain the custom evaluator concept. Convert one accepted behavior into a binary, observable rubric containing:

- one criterion;
- an explicit pass definition;
- an explicit fail definition;
- two to four reviewed examples with at least one pass and one fail;
- a short reason for every example;
- behavior for missing or ambiguous evidence.

The approved investigation brief confirms this rubric when all of these fields are present. If one is missing, ask one domain question to fill the gap.

Invoke `$kitaru-evaluator-authoring` in autonomous implementation mode. Give it the approved brief, rubric, representative target and control sessions, agent input and output contracts, output path `examples/canonical_example/evaluator.py`, and approval to create, test, and register the matching evaluator. If the skill is unavailable, read `.agents/skills/kitaru-evaluator-authoring/SKILL.md` and follow it directly.

Do not ask the user for a second code-design or registration approval. Report the plain-language criterion, evidence used, generated file, tests, registered immutable version, and baseline result distribution.

For the canonical path, the generated evaluator must inspect `ticket_id`, final action and amount, and accepted terminal tool calls. A reported escalation must fail if an accepted refund occurred earlier. Missing required evidence must fail the evaluation task clearly. Verify eight baseline passes and failures on tickets 004 and 007.

Stop and revisit the brief if the baseline distribution disagrees with reviewed evidence.

## 7. Select the candidate

Inspect the proposed agent change before registration. For the canonical path, verify that `RETURNS_POLICY_MODE=strict` checks approval thresholds, risk flags, final-sale rules, and return windows before refunding. Explain which approved failure it targets and which control behavior could regress.

Resolve or register an exact candidate agent version without assuming its number. Ask for approval before replay because it makes paid model calls.

## 8. Run the experiment

Explain replay and experiment when each becomes necessary. Create or reuse an experiment only when its exact evaluator selections match the approved rubric and guardrails.

For the canonical path, include the generated `returns-policy` version plus cost, latency, and tool-call patterns. Create the experiment through MCP and start each run through the CLI. Run the candidate against both approved cohort versions with baseline evaluation enabled. Poll every run and child job until terminal. A completed replay is the current readiness proof for that session.

## 9. Decide from evidence

Compare each baseline and candidate by case label. Report terminal action, policy transition, cost and latency deltas, tool-path change, replay status, and exact resource IDs. Mark a metric unavailable when the baseline and replay do not record comparable evidence. In particular, do not interpret a replay cost of zero as an improvement when native replay sessions lack model-cost data.

Answer the approved decision rule directly:

- Did every target failure become correct?
- Did every control case remain correct?
- Did a guardrail regress?
- Is the evidence sufficient under the user's shipping criterion?

Keep failed replays visible. Separate Kitaru facts from interpretation. Recommend ship, revise, or gather more evidence with a short reason.

## Completion criteria

Finish only when the baseline is understood, observations and counterexamples support an approved behavior brief, meaningful cohorts exist, the generated evaluator matches the approved rubric, paid replay approval has been handled, both cohort runs are terminal, and the decision traces back to exact evidence.
