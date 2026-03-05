# Kitaru

Durable execution for AI agents, built on [ZenML](https://zenml.io).

Kitaru makes agent workflows **persistent, replayable, and observable** using a small set of Python primitives. No graph DSL, no framework lock-in — just decorators on your existing code.

## Quick example

```python
import kitaru

@kitaru.checkpoint(type='llm_call')
def research(topic: str) -> str:
    ...

@kitaru.checkpoint(type='llm_call')
def write_draft(notes: str) -> str:
    ...

@kitaru.saga
def content_pipeline(topic: str) -> str:
    notes = research(topic)
    draft = write_draft(notes)

    approved = kitaru.wait(
        event="webhook",
        name="approve_draft",
        schema=bool,
        prompt="Publish this draft?",
    )

    if not approved:
        return "Cancelled"
    return draft
```

Every checkpoint is persisted and replayable. `wait()` suspends at zero compute cost until a webhook resumes it.

## Development

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ty check
```

### Claude Code skills

Install the official Astral skills for ty, ruff and uv:

```shell
/plugin marketplace add astral-sh/claude-code-plugins
/plugin install astral@astral-sh
```
