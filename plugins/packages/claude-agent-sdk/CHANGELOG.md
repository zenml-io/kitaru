# Changelog

## 0.1.0rc0

- Add one-shot string query recording through the public Claude Agent SDK message stream.
- Add fresh root-input replay with prompt, system-prompt, and model overrides.
- Add bounded static, history, error-result, and passthrough policies for adapter-wrapped in-process SDK MCP tools.
- Preserve native Claude messages while rejecting replay configurations that the public adapter boundary cannot enforce safely.
