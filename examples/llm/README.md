# Tracked LLM example

This example shows how `kitaru.llm()` behaves inside a flow: prompt/response
artifacts are captured, and usage metadata is attached automatically.

Run it with the stable flat entrypoint:

```bash
uv run -m examples.flow_with_llm
```

For the full catalog, see [../README.md](../README.md).

| Example | Requires | What it demonstrates | Test |
|---|---|---|---|
| [flow_with_llm.py](flow_with_llm.py) | `uv sync --extra local` plus a model alias / provider credentials | Tracked model calls with captured metadata and credential resolution | [../../tests/test_phase12_llm_example.py](../../tests/test_phase12_llm_example.py) |

For the credential setup walkthrough, see the hosted guide:
[Tracked LLM Calls](https://kitaru.ai/docs/getting-started/llm-calls).
