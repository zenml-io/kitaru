# Changelog

## 0.1.0rc0

- Add one-shot string query recording through the public Claude Agent SDK message stream.
- Add fresh root-input replay with prompt, system-prompt, and model overrides.
- Add bounded static, history, and passthrough tool policies for adapter-wrapped in-process SDK MCP tools, with fail, error-result, or passthrough behavior on a miss.
- Preserve native Claude messages while rejecting replay configurations that the public adapter boundary cannot enforce safely.
- Record one assistant turn as one model node even when the Claude CLI splits that turn across several messages, so a tool call lands under the model call that requested it and the run is no longer ended by a recording error after Claude has already run.
- Keep everything a split turn produced: reasoning text and answer text recorded from an earlier part of the turn are no longer erased when the next part arrives.
- Record the most readable failure cause the terminal message carries rather than the word `success`, and keep the HTTP status of a failing provider call in the session's terminal metadata as `api_error_status`.
- Add the difference between Claude's terminal token totals and the partial per-call counts to the root node, the way the run's total cost is already recorded there, and record Claude's thinking tokens in Kitaru's reasoning-token field.
- Record `effective_prompt` on the session's root node, the prompt text actually sent to Claude, so a replay that overrides the prompt keeps the overridden question next to the answer it produced.
- Log a warning on the `kitaru_claude_agent_sdk.runner` logger when a consumer stops the stream early and Kitaru cannot finalize the session or close the Claude iterator, because closing the generator discards notes attached to the exception.
