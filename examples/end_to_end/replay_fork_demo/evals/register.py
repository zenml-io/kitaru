"""Expose the production PydanticAI agent as a registered Kitaru agent."""

import os

from reference_agent.agent import build_support_agent
from reference_agent.config import load_variant

AGENT_NAME = "support-agent"
AGENT_VERSION = os.getenv("SUPPORT_AGENT_VERSION", "v2.2")

# A deployment or candidate checkout selects its own configuration. Kitaru
# fingerprints the entrypoint and configuration when ``register`` is called.
VARIANT_NAME = os.getenv("SUPPORT_AGENT_VARIANT", "baseline")
kagent = build_support_agent(load_variant(VARIANT_NAME), name=AGENT_NAME)
