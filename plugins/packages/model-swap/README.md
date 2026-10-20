# Kitaru model swap

Find out what changes if an agent's model calls move to another model. The `kitaru/model-swap` analyzer sends each recorded model call of an import, exactly as it was recorded, to OpenAI-compatible candidate endpoints (open models on a provider, a local Ollama model, or a cheaper closed tier) and compares the answers with the recording.

| Catalog name | Entrypoint | Configuration |
|---|---|---|
| `kitaru/model-swap` | `kitaru_model_swap.analyzer:analyze_model_swap` | `candidates` parameter; API keys come from environment variables each candidate names |

It runs on your worker with your keys. It reports, per candidate:

- tool-call agreement (same tool, same arguments after JSON normalization) and answer agreement for text replies, with per-argument agreement for each tool;
- provider failures (timeouts, rate limits, HTTP errors, empty replies, tool-call parse errors, refusals), counted apart from disagreement;
- latency and input, cached-input, output and reasoning tokens;
- cost per call, projected per session and per month at the import's observed traffic, using prices from the `price` parameter or the `genai-prices` table;
- coverage: calls seen, sampled, replayable and skipped, with reasons.

A dry run (`"dry_run": true`) only estimates calls and spend, and `max_spend_usd` stops cleanly before the estimate would pass the cap. Recorded reasoning parts are not sent to candidates; the report says how many calls this affects.

The `model-swap-summary` insight carries the full report as JSON under `kitaru.model_swap/v1` and a rendered HTML section under `kitaru.report_section/v1`. `kitaru insight report --import-id IMPORT_ID --out report.html` embeds that section in a self-contained HTML file. Session links point at the dashboard URL the server reports when the analysis runs.

See "Find out what changes if you switch models" in the [Kitaru documentation](https://docs.zenml.io/kitaru) for parameters and a walkthrough.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
