"""Contract tests for provider-local importer normalization."""

from types import ModuleType

import pytest

from importers import braintrust, langfuse, langsmith, otlp
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode

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

    system_prompt = importer._populate_node_fields([node])

    assert system_prompt == "Follow policy."
    assert node.input_text_selector == '$["messages"][1]["content"]'
    assert node.output_text_selector == '$["messages"][0]["content"]'
    assert node.system_prompt_selector == '$["messages"][0]["content"]'
    assert node.reasoning == "The tracking event says shipped."


@pytest.mark.parametrize("importer", IMPORTERS)
def test_detects_framework_locally(importer: ModuleType) -> None:
    """Keep framework detection inside each provider plugin."""
    assert importer._detect_framework({"otel.scope.name": "pydantic_ai"}) == (
        "pydantic-ai"
    )
    assert importer._detect_framework(["langgraph", "pydantic-ai"]) is None
