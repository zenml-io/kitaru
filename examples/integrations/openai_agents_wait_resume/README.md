# OpenAI Agents wait/resume

This is the smallest OpenAI Agents SDK example that shows Kitaru's durable
`wait()`/resume path.

The flow does three things:

1. Ask an OpenAI agent to draft a customer-facing reply.
2. Pause with `kitaru.wait()` for human approval and optional notes.
3. Resume the same execution and either reject the draft or ask OpenAI to revise
   it using the notes.

## Run it

```bash
uv sync --extra local --extra openai-agents
uv run kitaru init
export OPENAI_API_KEY='sk-...'
uv run examples/integrations/openai_agents_wait_resume/openai_wait_resume.py
```

When the flow reaches the review wait, answer from another terminal:

```bash
kitaru executions input <EXECUTION_ID> --value '{"decision": "approve", "notes": "Make it warmer."}'
kitaru executions resume <EXECUTION_ID>
```

To reject the draft:

```bash
kitaru executions input <EXECUTION_ID> --value '{"decision": "reject", "notes": "Needs legal review."}'
kitaru executions resume <EXECUTION_ID>
```

## What to look for

- `reply_writer_openai_runner_call` captures the OpenAI draft.
- `approve_openai_reply` is the durable wait point.
- A second `reply_writer_openai_runner_call` appears only when approval notes ask
  OpenAI to revise the draft.
- `final_reply` is the final named artifact.

The point is the pause: the process can release compute while waiting, and the
same execution continues once input is supplied.
