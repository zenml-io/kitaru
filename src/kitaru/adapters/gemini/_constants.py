"""Shared constants for the Gemini Interactions adapter."""

ADAPTER_ID = "gemini_interactions"
ANTIGRAVITY_AGENT_ID = "antigravity-preview-05-2026"
INTERACTIONS_CONTRACT_ERROR_MESSAGE = (
    "Installed google-genai does not expose the Gemini Interactions preview API "
    "required by kitaru.adapters.gemini. Upgrade/install with "
    "`uv sync --extra gemini` or `pip install 'kitaru[gemini]'`. Expected "
    "google.genai.Client, Interactions "
    "FunctionCallStep/FunctionResultStep types, and callable "
    "client.interactions.create and client.interactions.get methods."
)

GEMINI_EVENTS_METADATA_KEY = "gemini_interactions_events"
GEMINI_RUN_SUMMARIES_METADATA_KEY = "gemini_interactions_run_summaries"
