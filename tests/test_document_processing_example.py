"""Contract tests for the document processing example."""

import importlib
import json
import sys
from pathlib import Path

from pydantic_ai import BinaryContent

from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import NodeType
from kitaru.importers.langfuse import parse
from kitaru.task.evaluator import SessionView
from kitaru.task.importer import ParsedSession, flatten_nodes
from kitaru.task.plugins import load_plugin_entrypoint
from kitaru.worker.process import parse_inline_dependencies

REPOSITORY_ROOT = Path(__file__).parents[1]
EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "document_processing"
CANONICAL_EXAMPLE_DIR = REPOSITORY_ROOT / "examples" / "canonical_example"
sys.path.insert(0, str(REPOSITORY_ROOT))
agent_module = importlib.import_module("examples.document_processing.agent")
corpus_module = importlib.import_module("examples.document_processing.corpus")
evaluator_module = importlib.import_module("examples.document_processing.evaluator")
extractor_module = importlib.import_module("examples.document_processing.extractor")
CASES = corpus_module.CASES
TRACE_PATH = EXAMPLE_DIR / "traces" / "langfuse-traces.jsonl"
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


def test_real_trace_corpus_is_replay_ready() -> None:
    """Keep one real imported baseline for each pinned document."""
    sessions = [
        item
        for item in parse(
            TRACE_PATH.read_bytes(),
            {"source_instance": "nist-standards"},
        )
        if isinstance(item, ParsedSession)
    ]

    assert len(sessions) == 3
    assert {session.external_id.rsplit(":", 1)[-1] for session in sessions} == {
        case.document_id for case in CASES
    }
    assert all(session.inputs["turns"] for session in sessions)
    assert all(
        any(
            node.node_type is NodeType.LLM_CALL for node in flatten_nodes(session.nodes)
        )
        for session in sessions
    )


def test_real_baselines_contain_measurable_differences() -> None:
    """Keep the checked-in real export useful for comparison."""
    sessions = [
        item
        for item in parse(
            TRACE_PATH.read_bytes(),
            {"source_instance": "nist-standards"},
        )
        if isinstance(item, ParsedSession)
    ]
    scores = []
    for session in sessions:
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

    assert any(score < 1.0 for score in scores)
    assert len(set(scores)) > 1


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
    importer_path = REPOSITORY_ROOT / "src" / "kitaru" / "importers" / "langfuse.py"
    assert parse_inline_dependencies(importer_path) == []
    assert callable(load_plugin_entrypoint(importer_path, "parse", "Importer"))


def test_trace_generation_is_a_standalone_example_step() -> None:
    """Keep real trace generation separate and retain its JSONL export."""
    generator = (EXAMPLE_DIR / "generate.sh").read_text()

    assert "examples.document_processing.corpus" in generator
    assert "examples.document_processing.langfuse_capture" in generator
    assert "langfuse-traces.jsonl" in generator
    assert 'env_file="${example_dir}/.env"' in generator
    env_example = (EXAMPLE_DIR / ".env.example").read_text()
    assert "KITARU_API_URL=http://localhost:8000" in env_example


def test_example_readme_teaches_the_complete_cli_journey() -> None:
    """Keep all Kitaru operations visible in the local guide."""
    readme = (EXAMPLE_DIR / "README.md").read_text()

    assert "# Improve a document agent with the Kitaru CLI" in readme
    for command in (
        "kitaru status",
        "kitaru evaluator test",
        "kitaru agent register",
        "kitaru evaluator register",
        "worker start",
        "kitaru session import",
        "kitaru session list",
        "kitaru session evaluate",
        "kitaru cohort create",
        "kitaru cohort version create",
        "kitaru experiment create",
        "kitaru experiment run start",
    ):
        assert command in readme
    assert "--importer langfuse@latest" in readme
    assert "kitaru importer register" not in readme
    assert "docker compose -f ../../docker-compose.yml up" in readme
    assert "run.sh" not in readme


def test_canonical_example_uses_bundled_starting_point_evaluators() -> None:
    """Teach explicit bounded evaluation without plugin registration."""
    readme = (CANONICAL_EXAMPLE_DIR / "README.md").read_text()

    assert "kitaru evaluator list" in readme
    assert "--sessions-file /tmp/kitaru-document-session-ids.txt" in readme
    for evaluator in ("cost@1", "latency@1", "tool-call-patterns@1"):
        assert f"--evaluator {evaluator}" in readme
    assert "kitaru evaluator register" not in readme
