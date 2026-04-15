# Compliance review example

This directory is the scaffold for Kitaru's canonical **company document compliance review** example.

The finished example will show how to wrap a Claude Agent SDK document-review agent with Kitaru so that useful agent turns become durable, observable, replayable workflow checkpoints.

## Current status

This folder currently contains the **Phase 0 foundation plus the first four runnable stages**:

- the example directory layout
- the Stage 1 single-turn compliance check
- the Stage 2 sequential multi-domain audit
- the Stage 3 memory-backed HR + IT audit
- the Stage 4 conversational wait/resume review
- a placeholder entrypoint for Stage 5
- deterministic JSON-backed retrieval helpers in `tools.py`
- shared Claude Agent SDK helper wiring in `claude_agent.py`
- an example-local dependency declaration
- synthetic JSON company, standard, and document data
- model-free unit coverage for the retrieval layer

It does **not** yet contain:

- deployment guidance

That piece is planned for the final polish stage.

## Intended stage sequence

| Stage | File | Status |
|---|---|---|
| 1. Single-turn compliance check | `stage_1_single_turn.py` | First runnable flow |
| 2. Multi-domain audit | `stage_2_multi_domain.py` | Runnable sequential audit with saved report artifact |
| 3. Memory-backed audit | `stage_3_memory.py` | Runnable HR + IT audit with flow-level memory |
| 4. Conversational wait/resume loop | `stage_4_conversational.py` | Runnable wait/resume conversation |
| 5. Deploy guidance/example | `stage_5_deploy.py` | Placeholder |

Stage 1 is intentionally narrow: one Kitaru checkpoint wraps one Claude Agent SDK turn that reviews Acme Corp's IT security policy against the SOC 2 data retention requirement.

Stage 2 repeats the Stage 1 pattern four times — HR, IT security, vendor contracts, and insurance — then runs one synthesis checkpoint. The synthesis checkpoint saves the final markdown report as `compliance_report.md` with `kitaru.save()`.

Replay story: because each domain is its own checkpoint, a replay from a later failed checkpoint can reuse earlier durable results. For example, if the insurance checkpoint or synthesis checkpoint fails after HR, IT security, and vendor contracts have completed, replay can resume from the failed checkpoint instead of re-running all earlier Claude turns:

```bash
kitaru executions replay <exec-id> --from check_insurance
```

Stage 3 narrows the audit back to HR + IT and adds cross-run continuity through Kitaru memory. It uses **flow-level memory**, so the `audit_with_memory` flow remembers its own prior findings across runs instead of publishing them into a broader named namespace.

| Setting | Value |
|---|---|
| Scope | the `audit_with_memory` flow ID discovered from execution metadata |
| Scope type | `flow` |
| IT finding key | `findings/it_security` |
| HR finding key | `findings/hr_compliance` |
| Last-run key | `audit/last_run` |

First run: flow memory is empty, so Claude performs a fresh HR + IT audit. After the checkpoints complete, the flow writes the latest findings into its flow-level memory. Second run: the flow reads those prior findings before the checkpoints and passes them into Claude as normal checkpoint inputs, so the agent can say whether the previous gaps are still present, resolved, or changed.

Useful memory inspection and maintenance commands:

```bash
# See available memory scopes, including flow scopes
kitaru memory scopes

# Find the latest Stage 3 execution and copy its flow_id from the JSON output
kitaru executions list --flow audit_with_memory --limit 1 --output json

# List stored entries for this audit flow
kitaru memory list --scope <flow-scope-id> --scope-type flow

# Seed a known prior IT finding for this audit flow before a demo run
kitaru memory set findings/it_security '{"status":"known_gap","summary":"Data retention schedule missing"}' --scope <flow-scope-id> --scope-type flow

# Compact accumulated history for a key after repeated runs
kitaru memory compact --scope <flow-scope-id> --scope-type flow --key findings/it_security --source-mode history
```

Stage 4 turns the compliance review into a durable conversation. The important shape is:

1. run one Claude turn in the `run_claude_agent` checkpoint;
2. pause in the `conversational_compliance_review` flow body with `kitaru.wait()`;
3. accept the human's next message as wait input;
4. run the next checkpoint with `resume=<previous Claude session ID>` so the same Claude session continues.

The flow returns the latest `ClaudeAgentResult`; Stage 4 does **not** introduce a new conversation result object. To finish the conversation, provide `/done`, `/exit`, or `/quit` as the wait input.

## Data layout

The planned synthetic data layout is:

```text
data/
  company.json
  standards/
    labor_law_requirements.json
    soc2_controls.json
    contract_clause_requirements.json
    insurance_coverage_standards.json
  documents/
    employee_handbook.json
    it_security_policy.json
    vendor_contract_alpha.json
    vendor_contract_beta.json
    insurance_policy.json
    financial_statements_2024.json
    data_privacy_policy.json
    disaster_recovery_plan.json
```

The JSON files are synthetic and self-contained. Each document includes stable metadata, section IDs, section text, and a `known_planted_findings` array that records the intended pass/gap outcome for the example.

## Retrieval helpers

`tools.py` provides a small deterministic retrieval surface over the local JSON files:

- `list_documents()` returns the planned company document catalog.
- `get_company_info()` returns `data/company.json`.
- `read_document(doc_id)` returns readable text for a company document, standard, or company profile.
- `read_document(doc_id, section="...")` and `read_section(doc_id, section)` return one named section.
- `search_documents(query)` performs simple case-insensitive token search across document sections and standard requirements.

There are no model calls, embeddings, vector databases, external services, or Kitaru runtime initialization in this layer.

## Setup story

This example has its own `pyproject.toml` because it depends on the Claude Agent SDK in addition to Kitaru. That file is an **example-local setup surface**; it does not mean the whole repository needs to install these dependencies in every CI lane.

From this directory, the intended setup will be:

```bash
cd examples/compliance_review
uv sync
kitaru init
```

The local dependency surface is intentionally small:

- `kitaru[local]` for Kitaru flows, checkpoints, local runtime support, and the CLI
- `claude-agent-sdk` for the agent/tool loop used by the later runnable stages

If you are developing this example inside the Kitaru repository and need the current checkout rather than the published `kitaru` package, install the repository root into the example environment in editable mode after syncing:

```bash
uv pip install -e '../..[local]'
```

Stage 1 uses the Claude Agent SDK, so set Anthropic credentials in the way expected by the SDK before running it. For the normal API-key path:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

Then run the first real flow from this directory:

```bash
uv run stage_1_single_turn.py
```

To run the Stage 2 multi-domain audit:

```bash
uv run stage_2_multi_domain.py
```

To run the Stage 3 memory-backed HR + IT audit:

```bash
uv run stage_3_memory.py
```

To run the Stage 4 conversational wait/resume review:

```bash
uv run stage_4_conversational.py
```

After each Claude turn, the flow waits for your next message. In an interactive local terminal, Kitaru may prompt directly. For a non-interactive or remote run, use the printed execution ID in a second terminal:

```bash
# Continue with a follow-up. The value is a JSON string, so keep the inner quotes.
kitaru executions input <exec-id> --value '"Please explain the highest-priority remediation."'
kitaru executions resume <exec-id>

# Finish and return the latest ClaudeAgentResult.
kitaru executions input <exec-id> --value '"/done"'
kitaru executions resume <exec-id>
```

Or, from Python/tests, import and call:

```python
from examples.compliance_review.stage_1_single_turn import run_workflow

result = run_workflow()
print(result.result)
```

For Stage 2:

```python
from examples.compliance_review.stage_2_multi_domain import run_workflow

result = run_workflow()
print(result.result)
```

For Stage 3:

```python
from examples.compliance_review.stage_3_memory import run_workflow

result = run_workflow()
print(result.result)
```

For Stage 4:

```python
from examples.compliance_review.stage_4_conversational import run_workflow

result = run_workflow()
print(result.result)
```

The focused Phase 0 retrieval verification command from the repository root is:

```bash
uv run pytest tests/test_compliance_review_tools.py
```

The focused Stage 1 boundary verification command is:

```bash
uv run pytest tests/test_phase1_compliance_review_stage1.py
```

That Stage 1 test stubs the Claude turn and does not call Anthropic.

The focused Stage 2 multi-checkpoint verification command is:

```bash
uv run pytest tests/test_phase2_compliance_review_stage2.py
```

That Stage 2 test stubs the five Claude turns, runs the real decorated flow, and verifies the saved report artifact.

The focused Stage 3 memory verification command is:

```bash
uv run pytest tests/test_phase3_compliance_review_stage3.py
```

That Stage 3 test stubs Claude, runs the real decorated flow twice, and verifies flow-level memory continuity through Kitaru's memory inspection API.

The focused Stage 4 wait/resume verification command is:

```bash
uv run pytest tests/test_phase4_compliance_review_stage4.py
```

That Stage 4 test stubs Claude, runs the real decorated flow through two wait points, provides follow-up input through the Kitaru client API, and verifies the second Claude turn resumes the first turn's session ID.

## Credentials

Stage 1 and later Claude-backed stages require Anthropic credentials in the environment expected by the Claude Agent SDK. The retrieval helpers and Phase 0 unit test do not require credentials.
