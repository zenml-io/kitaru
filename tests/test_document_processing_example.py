"""Contract tests for the document processing example."""

import importlib
import json
import sys
from pathlib import Path

from kitaru_importer_langfuse import parse
from pydantic_ai import BinaryContent

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ParsedSession
from kitaru.task.plugins import load_plugin_entrypoint
from kitaru.worker.process import parse_inline_dependencies

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "document_processing"
sys.path.insert(0, str(REPOSITORY_ROOT))
agent_module = importlib.import_module("examples.document_processing.agent")
corpus_module = importlib.import_module("examples.document_processing.corpus")
evaluator_module = importlib.import_module("examples.document_processing.evaluator")
extractor_module = importlib.import_module("examples.document_processing.extractor")
main_module = importlib.import_module("examples.document_processing.__main__")
CASES = corpus_module.CASES
PREPARED_TRACE_PATH = main_module.PREPARED_TRACE_PATH
DocumentInput = agent_module.DocumentInput
evaluate = evaluator_module.evaluate
get_document_input = agent_module.get_document_input
build_prompt = extractor_module.build_prompt


def test_corpus_uses_three_pinned_nist_pdfs() -> None:
    """Keep the source corpus public, immutable, and aligned with labels."""
    assert len(CASES) == 3
    assert len({case.document_id for case in CASES}) == 3
    assert all(case.url.startswith("https://nvlpubs.nist.gov/") for case in CASES)
    assert all(len(case.sha256) == 64 for case in CASES)
    assert {
        case.document_id: case.expected.model_dump(mode="json") for case in CASES
    } == evaluator_module.EXPECTED


def test_build_prompt_attaches_the_complete_pdf(tmp_path: Path) -> None:
    """Send PDF bytes as a named PydanticAI document input."""
    pdf = tmp_path / "standard.pdf"
    pdf.write_bytes(b"%PDF-1.7\nexample")

    prompt = build_prompt(pdf)

    assert prompt[0] == "Extract the catalog record for this document."
    assert isinstance(prompt[1], BinaryContent)
    assert prompt[1].data == pdf.read_bytes()
    assert prompt[1].media_type == "application/pdf"
    assert prompt[1].identifier == pdf.name


def test_candidate_unwraps_a_langfuse_session_turn() -> None:
    """Project the importer's multi-turn envelope into one PDF input."""
    expected = {
        "document_id": "nist-ai-rmf-1.0",
        "pdf_path": "examples/document_processing/documents/nist-ai-rmf-1.0.pdf",
    }

    value = get_document_input(
        {
            "schema_version": 1,
            "turns": [
                {
                    "source_trace_id": "trace-1",
                    "inputs": expected,
                    "outputs": {"title": "baseline"},
                }
            ],
        }
    )

    assert value == DocumentInput.model_validate(expected)


def test_langfuse_trace_export_is_importable() -> None:
    """Import a trace row shaped like the current Langfuse trace API."""
    trace = {
        "id": "trace-1",
        "timestamp": "2026-07-31T12:00:00Z",
        "name": "standards-document-extraction",
        "sessionId": "nist-ai-rmf-1.0",
        "input": {
            "document_id": "nist-ai-rmf-1.0",
            "pdf_path": ("examples/document_processing/documents/nist-ai-rmf-1.0.pdf"),
        },
        "output": {"title": "baseline"},
        "environment": "baseline",
        "version": "prompt-v1",
        "observations": [
            {
                "id": "root",
                "traceId": "trace-1",
                "type": "SPAN",
                "name": "extract-document",
                "startTime": "2026-07-31T12:00:00Z",
                "endTime": "2026-07-31T12:00:02Z",
                "input": {"document_id": "nist-ai-rmf-1.0"},
                "output": {"title": "baseline"},
                "metadata": {"sdk_span_type": "agent run"},
            },
            {
                "id": "generation",
                "traceId": "trace-1",
                "parentObservationId": "root",
                "type": "GENERATION",
                "name": "chat gpt-5-nano",
                "startTime": "2026-07-31T12:00:00Z",
                "endTime": "2026-07-31T12:00:01Z",
                "model": "gpt-5-nano",
                "input": [{"role": "user", "content": "Extract the record"}],
                "output": {"title": "baseline"},
                "metadata": {
                    "attributes": {
                        "gen_ai.provider.name": "openai",
                        "gen_ai.request.model": "gpt-5-nano",
                        "sdk_span_type": "model request",
                    }
                },
                "usageDetails": {"input": 100, "output": 20},
            },
        ],
    }

    sessions = list(
        parse(
            (json.dumps(trace) + "\n").encode(),
            {"source_instance": "nist-standards"},
        )
    )

    assert len(sessions) == 1
    session = sessions[0]
    assert isinstance(session, ParsedSession)
    assert session.external_id == "nist-standards:nist-ai-rmf-1.0"
    assert session.inputs["turns"][0]["inputs"]["document_id"] == ("nist-ai-rmf-1.0")
    assert session.nodes[0].children[0].node_type is NodeType.LLM_CALL
    assert session.nodes[0].children[0].provider == "openai"


def test_prepared_trace_corpus_forms_useful_cohorts() -> None:
    """Keep controls, extraction failures, and telemetry failures represented."""
    sessions = [
        item
        for item in parse(
            PREPARED_TRACE_PATH.read_bytes(),
            {"source_instance": "nist-standards"},
        )
        if isinstance(item, ParsedSession)
    ]
    tags = {
        session.external_id: set(session.metadata["langfuse.tags"])
        for session in sessions
    }

    assert len(sessions) == 12
    assert sum("control" in value for value in tags.values()) == 3
    assert sum("extraction-edge" in value for value in tags.values()) == 7
    assert sum("telemetry-edge" in value for value in tags.values()) == 2

    retry = next(
        session for session in sessions if session.external_id.endswith("ai-rmf-retry")
    )
    assert len(retry.inputs["turns"]) == 2

    failed = next(
        session
        for session in sessions
        if session.external_id.endswith("genai-provider-timeout")
    )
    assert failed.status.value == "failed"
    assert failed.nodes[0].status is NodeStatus.FAILED

    partial = next(
        session
        for session in sessions
        if session.external_id.endswith("csf-missing-parent")
    )
    readiness = partial.metadata["replay_readiness"]
    assert readiness["level"] == "partial"
    assert readiness["graph_complete"] is False
    assert "missing parent" in " ".join(readiness["reasons"])
    generation = next(
        node for node in partial.nodes if node.node_type is NodeType.LLM_CALL
    )
    assert generation.tokens is not None
    assert generation.tokens.input_tokens == 1755
    assert generation.tokens.output_tokens == 79


def test_prepared_baselines_contain_measurable_failures() -> None:
    """Keep every extraction edge case useful for field-level evaluation."""
    sessions = [
        item
        for item in parse(
            PREPARED_TRACE_PATH.read_bytes(),
            {"source_instance": "nist-standards"},
        )
        if isinstance(item, ParsedSession)
    ]
    extraction_edges = [
        session
        for session in sessions
        if "extraction-edge" in session.metadata["langfuse.tags"]
    ]
    scores = []
    for session in extraction_edges:
        result = evaluate(
            SessionView(
                session=SessionResponse.model_construct(
                    inputs=session.inputs,
                    outputs=session.outputs,
                ),
                nodes=[],
            )
        )
        scores.append(result.score)

    assert len(scores) == 7
    assert all(score < 1 for score in scores)
    assert set(scores) == {0.75}


def test_evaluator_scores_each_labeled_field() -> None:
    """Report fractional accuracy for native and imported sessions."""
    case = CASES[0]
    expected = case.expected.model_dump(mode="json")
    partial = {**expected, "title": "Short title"}
    imported_inputs = {
        "schema_version": 1,
        "turns": [{"inputs": case.replay_input().model_dump(mode="json")}],
    }
    passing = SessionView(
        session=SessionResponse.model_construct(
            inputs=case.replay_input().model_dump(mode="json"),
            outputs=expected,
        ),
        nodes=[],
    )
    failing = SessionView(
        session=SessionResponse.model_construct(
            inputs=imported_inputs,
            outputs=json.dumps(partial),
        ),
        nodes=[],
    )

    assert evaluate(passing).score == 1.0
    assert evaluate(passing).passed is True
    assert evaluate(failing).score == 0.75
    assert evaluate(failing).passed is False
    assert "title" in evaluate(failing).explanation


def test_registered_plugins_are_worker_compatible() -> None:
    """Keep both uploaded service artifacts dependency-free."""
    assert parse_inline_dependencies(EXAMPLE_DIR / "evaluator.py") == []
    importer_path = (
        REPOSITORY_ROOT
        / "plugins"
        / "langfuse"
        / "src"
        / "kitaru_importer_langfuse"
        / "importer.py"
    )
    assert parse_inline_dependencies(importer_path) == []
    assert callable(load_plugin_entrypoint(importer_path, "parse", "Importer"))


def test_runner_uses_the_public_cli_for_the_improvement_loop() -> None:
    """Keep the canonical orchestration on supported CLI commands."""
    runner = (EXAMPLE_DIR / "run.sh").read_text()

    for command in (
        "importer test",
        "agent register",
        "importer register",
        "worker start",
        "session import",
        "session list",
        "cohort create",
        "cohort version create",
        "evaluator test",
        "evaluator register",
        "experiment create",
        "experiment run start",
    ):
        assert command in runner
    assert '--agent "${agent_name}"' in runner
