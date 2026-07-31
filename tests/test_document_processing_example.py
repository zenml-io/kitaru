"""Contract tests for the document processing example."""

import importlib
import json
import sys
from pathlib import Path

from kitaru_importer_langfuse import parse
from pydantic_ai import BinaryContent

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeType
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
CASES = corpus_module.CASES
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
