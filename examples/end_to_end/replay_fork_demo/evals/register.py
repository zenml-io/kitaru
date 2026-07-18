"""Expose the production PydanticAI agent as a registered Kitaru agent."""

import os

from reference_agent.agent import build_support_agent
from reference_agent.config import load_variant

from kitaru import ExecutionEvidence, Score

AGENT_NAME = "support-agent"
AGENT_VERSION = os.getenv("SUPPORT_AGENT_VERSION", "v2.2")

# A deployment or candidate checkout selects its own configuration. Kitaru
# fingerprints the entrypoint and configuration when ``register`` is called.
VARIANT_NAME = os.getenv("SUPPORT_AGENT_VARIANT", "baseline")
kagent = build_support_agent(load_variant(VARIANT_NAME), name=AGENT_NAME)


@kagent.protection("completed-execution", capability="pure")
def completed_execution(evidence: ExecutionEvidence) -> Score:
    """Require every candidate replay to finish before it can pass."""
    return Score(value=1.0 if evidence.status == "completed" else 0.0)
