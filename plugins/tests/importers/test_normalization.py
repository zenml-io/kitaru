"""Contract tests for provider-local importer normalization."""

from types import ModuleType

import pytest

from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode
from kitaru_importer_braintrust import importer as braintrust
from kitaru_importer_langfuse import importer as langfuse
from kitaru_importer_langsmith import importer as langsmith
from kitaru_importer_opentelemetry import importer as otlp

IMPORTERS = (braintrust, langfuse, langsmith, otlp)


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

    system_prompt = importer._populate_node_fields([node, later_node])

    assert system_prompt == "Follow policy."
    assert node.input_text_selector == '$["messages"][1]["content"]'
    assert node.output_text_selector == '$["messages"][0]["content"]'
    assert node.system_prompt_selector == '$["messages"][0]["content"]'
    assert node.reasoning == "The tracking event says shipped."
    assert later_node.system_prompt_selector == '$["messages"][0]["content"]'


@pytest.mark.parametrize("importer", IMPORTERS)
def test_detects_framework_locally(importer: ModuleType) -> None:
    """Keep framework detection inside each provider plugin."""
    assert importer._detect_framework({"otel.scope.name": "pydantic_ai"}) == (
        "pydantic-ai"
    )
    assert importer._detect_framework(["langgraph", "pydantic-ai"]) is None
