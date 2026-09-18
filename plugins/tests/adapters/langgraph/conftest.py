"""Shared fixtures for LangGraph adapter tests."""

# Import deepagents before any test patches langchain.agents.create_agent.
# Its first import copies create_agent into its own namespace, so importing it
# under a patch would keep the patched function for the rest of the session.
import deepagents  # noqa: F401

from .fixtures import fake_client

__all__ = ["fake_client"]
