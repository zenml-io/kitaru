# Replay Verify imported-input demo

This example shows the next Replay Verify story in one local command:

1. load imported case records from neutral JSONL,
2. validate whether each case has enough evidence to compare safely,
3. run eligible cases through local baseline and candidate support-copilot code,
4. stop broken or unsafe cases before the candidate code runs,
5. write JSON and Markdown reports.

The most important thing to notice is not a perfect score. The important thing is
that broken cases fail closed. If a case is missing required evidence, has unsafe
write-like behavior, or has incomplete RAG metadata, Replay Verify keeps it in
the report but does not run the candidate on it.

## What this proves

This demo proves a narrow workflow:

> Well-shaped imported cases can drive fresh local baseline/candidate execution,
> and Replay Verify can report which cases were safe to compare and which cases
> were stopped.

The support-copilot runner builds outputs from:

- the imported `root_input`,
- the imported `available_tools`,
- mocked/read-only tool results,
- imported retrieval metadata for RAG cases.

It does **not** decide success by reading a manifest `expected_output`. The
fixture does not contain an `expected_output` field.

## What this does not prove

This demo does **not** prove any of these larger claims:

- arbitrary Langfuse traces can be replayed exactly,
- Langfuse traces become Kitaru checkpoint replays,
- recorded model/tool responses are replayed with zero live calls,
- any PydanticAI production run is automatically comparable,
- 0% drift is expected for customer traces,
- a public Kitaru CLI or MCP command exists for this path.

The local support-copilot app is deterministic so you can run the demo without
provider credentials. It is useful for showing the imported-input verifier path,
but it is not a live LLM quality benchmark.

## Run it

From the repository root:

```bash
uv run python examples/replay_verify_imported_cases/run_langfuse_pydanticai_demo.py \
  --source jsonl \
  --case-file examples/replay_verify_imported_cases/fixtures/support_copilot_imported_cases.jsonl \
  --report-dir ./replay-verify-reports
```

You do not need `OPENAI_API_KEY`, Langfuse credentials, a Kitaru server, or a
Kitaru login for this checked-in JSONL path.

## Expected output files

The command writes four files to the report directory:

```text
replay-verify-reports/
  imported_cases.jsonl
  fidelity_report.json
  verification_report.json
  verification_report.md
```

What each file means:

- `imported_cases.jsonl` is the neutral case cohort after loading and normalizing
  the input JSONL.
- `fidelity_report.json` shows validation results: recovered fields, missing
  fields, eligibility, safety status, and stop reasons.
- `verification_report.json` shows the full run summary and per-case comparison
  results.
- `verification_report.md` is the human-readable version to open first.

The Markdown report should say:

```text
Execution mode: imported_input_fresh_execution_not_deterministic_checkpoint_replay
Recorded-response control: unavailable
Candidate executions for stopped cases: 0
Unsafe live executions: 0
```

## Fixture cohort

The default JSONL file contains eight cases:

| Case | What it represents | Expected behavior |
|---|---|---|
| `rv-model-only-eligible` | model-only support answer | candidate runs |
| `rv-read-only-tool-eligible` | read-only subscription lookup | candidate runs |
| `rv-mocked-write-eligible` | write-like ticket creation, mocked | candidate runs |
| `rv-rag-eligible` | RAG answer with complete retrieval metadata | candidate runs |
| `rv-missing-output-stopped` | no observed output/evaluator signal | candidate stops |
| `rv-missing-tools-stopped` | no imported `available_tools` list | candidate stops |
| `rv-unsafe-live-write-stopped` | live write-like email behavior | candidate stops |
| `rv-incomplete-rag-stopped` | stale/incomplete RAG metadata | candidate stops |

A good demo moment is opening `verification_report.md` and pointing at the stopped
cases. The report should preserve each bad case with a concrete reason instead
of dropping it silently.

## Safe claims to use in a demo

You can safely say:

- Replay Verify can load neutral imported-input cases from JSONL.
- It validates evidence before executing candidate code.
- It runs local baseline and candidate callables for eligible cases.
- It holds incomplete, unsafe, or non-comparable cases before candidate
  execution.
- This checked-in path uses imported-input fresh execution, not deterministic
  checkpoint replay.

## Risky claims to avoid

Avoid saying:

- “Replay Verify replays arbitrary Langfuse traces exactly.”
- “Foreign traces become Kitaru checkpoints.”
- “Recorded responses are replayed.”
- “This proves customer traces will have no drift.”
- “There is already a public CLI/MCP command for imported-case verification.”

## Known limitations

- The checked-in source mode is JSONL only. Langfuse fetching belongs to the
  source adapter work, not this runnable fixture path.
- The support-copilot code is deterministic and local. It stands in for a
  PydanticAI app but does not call a live provider.
- Report comparison is field-based: `policy_label`, `risk_status`, `tool_names`,
  and `retrieval_document_ids`.
- Recorded-response control is unavailable. The report states this explicitly.
- This example intentionally does not add a public Kitaru CLI or MCP command.
