# Compliance review example

This directory is the scaffold for Kitaru's canonical **company document compliance review** example.

The finished example will show how to wrap a Claude Agent SDK document-review agent with Kitaru so that useful agent turns become durable, observable, replayable workflow checkpoints.

## Current status

This folder currently contains the **Phase 0 foundation plus the first Phase 1 runnable flow**:

- the example directory layout
- the Stage 1 single-turn compliance check
- placeholder entrypoints for Stages 2–5
- deterministic JSON-backed retrieval helpers in `tools.py`
- shared Claude Agent SDK helper wiring in `claude_agent.py`
- an example-local dependency declaration
- synthetic JSON company, standard, and document data
- model-free unit coverage for the retrieval layer

It does **not** yet contain:

- multi-domain audit flow
- memory-backed audit flow
- conversational wait/resume flow
- deployment guidance

Those pieces are planned for later stages.

## Intended stage sequence

| Stage | File | Status |
|---|---|---|
| 1. Single-turn compliance check | `stage_1_single_turn.py` | First runnable flow |
| 2. Multi-domain audit | `stage_2_multi_domain.py` | Placeholder |
| 3. Memory-backed audit | `stage_3_memory.py` | Placeholder |
| 4. Conversational wait/resume loop | `stage_4_conversational.py` | Placeholder |
| 5. Deploy guidance/example | `stage_5_deploy.py` | Placeholder |

Stage 1 is intentionally narrow: one Kitaru checkpoint wraps one Claude Agent SDK turn that reviews Acme Corp's IT security policy against the SOC 2 data retention requirement.

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

Or, from Python/tests, import and call:

```python
from examples.compliance_review.stage_1_single_turn import run_workflow

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

## Credentials

Stage 1 and later Claude-backed stages require Anthropic credentials in the environment expected by the Claude Agent SDK. The retrieval helpers and Phase 0 unit test do not require credentials.
