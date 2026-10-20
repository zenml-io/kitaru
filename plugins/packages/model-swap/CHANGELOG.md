# Changelog

## Unreleased

- Add the `kitaru/model-swap` analyzer. It sends each recorded model call of an import, as it was recorded, to OpenAI-compatible candidate models and reports tool-call and answer agreement, failure categories, latency, token usage and projected cost per candidate. It supports a dry run that only estimates calls and spend, a spend cap, and tool schemas supplied from the agent's code when the trace lacks them.
