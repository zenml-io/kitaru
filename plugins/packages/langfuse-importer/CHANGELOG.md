# Changelog

## Unreleased

- Fetch traces directly from the Langfuse API by trace id or time window, through the `adapter` extra.
- Import traces oldest first and grouped by session, fixing later traces in a session being dropped as duplicates.

## 0.2.0

- Add the Langfuse importer-backed adapter, installed through the `adapter` extra.
- Isolate invalid numeric values, model fields, and unserializable payloads without discarding unrelated sessions; preserve explicit zero costs.
- Bound observation depth and nested tool JSON scanning, validate before tool-link inference, and use iterative tree flattening.

## 0.1.1

- Resolve legacy ingestion updates independently of JSONL row order and preserve explicit zero token counts.

## 0.1.0

- Promote the Langfuse importer, including inferred tool-call links and normalized tool outputs, to stable.

## 0.1.0rc3

- Nest matched tool calls under requesting LLM calls, retain their source parent as a secondary link, decode JSON tool-call arguments, and select structured final-answer text.

## 0.1.0rc2

- Align the Langfuse importer release candidate with Kitaru 0.22.0rc9.

## 0.1.0rc1

- Add causal links from tool spans to the model calls that requested them, with an option to disable inference.

## 0.1.0rc0

- Initial release candidate for the Langfuse trace importer.
