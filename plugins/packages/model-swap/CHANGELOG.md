# Changelog

## Unreleased

- Add the `kitaru/model-swap` analyzer for choosing the right model for an agent. It sends each recorded model call of an import, rebuilt from the recording, to OpenAI-compatible candidate models and names the best candidate per goal (closest to today, cheapest, fastest, best open model). It reports tool-call and answer agreement, provider failure categories, latency, token usage, openness and projected cost per candidate, against the current model as a reference for run-to-run noise. It supports a dry run, a spend cap and a time budget that are both strict, an optional judge, tool schemas supplied from the agent's code, and a rendered section for `kitaru insight report`.
