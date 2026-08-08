"""Contract tests for provider-local importer normalization."""

from types import ModuleType

import pytest

from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode
from kitaru_braintrust_importer import importer as braintrust
from kitaru_langfuse_importer import importer as langfuse
from kitaru_langsmith_importer import importer as langsmith
from kitaru_opentelemetry_importer import importer as otlp

IMPORTERS = (braintrust, langfuse, langsmith, otlp)


@pytest.mark.parametrize("importer", IMPORTERS)
def test_json_pointer_tokens_are_escaped(importer: ModuleType) -> None:
    """Escape slash and tilde characters in generated pointer tokens."""
    assert importer._child_selector("", "a/b~c") == "/a~1b~0c"


@pytest.mark.parametrize("importer", IMPORTERS)
def test_normalizes_node_selectors_and_visible_reasoning(importer: ModuleType) -> None:
    """Keep each provider plugin responsible for its normalized node fields."""
    node = ImportedNode(
        node_type=NodeType.LLM_CALL,
        name="model request",
        status=NodeStatus.COMPLETED,
        inputs={
            "messages": [
                {"role": "system", "content": "Follow policy."},
                {"role": "user", "content": "Where is my order?"},
            ]
        },
        outputs={
            "messages": [{"role": "assistant", "content": "It shipped."}],
            "reasoning": "The tracking event says shipped.",
        },
        attributes={},
    )
    later_node = ImportedNode(
        node_type=NodeType.LLM_CALL,
        name="later model request",
        status=NodeStatus.COMPLETED,
        inputs={"messages": [{"role": "system", "content": "New policy."}]},
        outputs={"role": "assistant", "content": "Done."},
        attributes={},
    )

    importer._populate_node_fields([node, later_node])

    assert node.input_text_selector == "/messages/1/content"
    assert node.output_text_selector == "/messages/0/content"
    assert node.system_prompt_selector == "/messages/0/content"
    assert node.reasoning == "The tracking event says shipped."
    assert later_node.system_prompt_selector == "/messages/0/content"


@pytest.mark.parametrize("importer", IMPORTERS)
def test_output_selector_skips_reasoning_and_tool_parts(importer: ModuleType) -> None:
    """Select visible assistant text instead of hidden model output parts."""
    output = {
        "role": "assistant",
        "parts": [
            {"type": "thinking", "content": "Inspect private evidence."},
            {"type": "tool_call", "content": "lookup_order"},
            {"type": "text", "content": "The order shipped."},
        ],
    }

    assert importer._output_text_selector(output) == "/parts/2/content"


@pytest.mark.parametrize("importer", IMPORTERS)
def test_output_selector_is_empty_for_non_visible_parts(importer: ModuleType) -> None:
    """Leave the output selector empty when no visible assistant text exists."""
    output = {
        "role": "assistant",
        "parts": [
            {"type": "reasoning", "content": "Inspect private evidence."},
            {"type": "function_call", "content": "lookup_order"},
        ],
    }

    assert importer._output_text_selector(output) is None


@pytest.mark.parametrize("importer", IMPORTERS)
def test_detects_framework_locally(importer: ModuleType) -> None:
    """Keep framework detection inside each provider plugin."""
    assert importer._detect_framework({"otel.scope.name": "pydantic_ai"}) == (
        "pydantic-ai"
    )
    assert importer._detect_framework(["langgraph", "pydantic-ai"]) is None
