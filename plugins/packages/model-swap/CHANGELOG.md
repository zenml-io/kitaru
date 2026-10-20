# Changelog

## Unreleased

- Accept decision models as the judge: TypeSafe Jev, Cloudflare Clef on Workers AI, and OpenAI's Decisions API (`"judge": {"kind": "jev" | "clef" | "openai_decisions"}`). The judge's probability that two answers are equivalent is reported per disagreement, rulings are counted as equivalent, different, or unsure, and disagreements are ordered for review with likely different answers first.
- Add the `kitaru/model-swap` analyzer for choosing the right model for an agent. It sends each recorded model call of an import, rebuilt from the recording, to OpenAI-compatible candidate models and names the best candidate per goal (closest to today, cheapest, fastest, best open model). It reports tool-call and answer agreement, provider failure categories, latency, token usage, openness and projected cost per candidate, against the current model as a reference for run-to-run noise. It supports a dry run, a spend cap and a time budget that are both strict, an optional judge, tool schemas supplied from the agent's code, and a rendered section for `kitaru insight report`.
