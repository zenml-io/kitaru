---
description: "Turn a pattern you'd otherwise eyeball across sessions into a versioned analyzer: the entrypoint contract, a runnable example, and how it runs on import."
icon: chart-pie
---

# Write an analyzer

An [analyzer](../concepts/analyzers.md) writes insights about a set of sessions: named, typed observations rather than per-session verdicts. This guide takes you from a question about a batch of sessions to a registered analyzer running on your imports.

## The analyzer contract

An analyzer is a callable, a single Python file or an installable package, that receives every session ID in the set and fetches the data it needs:

```python
# session_outcomes_analyzer.py
from collections import Counter
from uuid import UUID

from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    InsightInput,
)
from kitaru.client.api_client import KitaruAPIClient


async def analyzer(session_ids: list[UUID], **params) -> InsightInput:
    counts: Counter[str] = Counter()
    async with KitaruAPIClient() as client:
        for session_id in session_ids:
            session = await client.sessions.get(session_id)
            counts[session.status] += 1
    return InsightInput(
        name="session_outcomes",
        title="Session outcomes",
        description="How the imported sessions finished.",
        data=CategoricalInsightData(
            values=[
                CategoryValue(label=status, value=count)
                for status, count in counts.items()
            ]
        ),
    )
```

The first argument is `list[UUID]`. `KitaruAPIClient()` uses the server URL and credentials supplied to the task process. `client.sessions.get(session_id)` returns [session](../concepts/agents-and-sessions.md) metadata; `client.sessions.get_with_nodes(session_id)` includes its model and tool calls with payloads. Fetch only what the analysis needs, and process traces one at a time to avoid retaining the whole import in memory. Return one `InsightInput` or a list, including an empty list when there are no findings. Each returned item becomes one stored insight. Both synchronous and asynchronous callables are supported. `params` are per-run knobs, passed when you name the analyzer on an import.

This example needs no provider credentials: it reads session status through Kitaru's task credentials. An analyzer that judges the set instead of just counting it, for example one that reads every node and summarizes what went wrong across the batch, calls a model inside `analyzer` the same way an [LLM judge](write-an-evaluator.md) does inside `evaluate`.

## Register it

```bash
kitaru analyzer register session-outcomes \
  --script session_outcomes_analyzer.py --entrypoint analyzer
```

Analyzers are versioned like evaluators: registering the next version with `kitaru analyzer version register session-outcomes --script ...` creates version 2, and every insight remembers exactly which version wrote it. There is no `--agent-id` option: analyzers are global plugins, never scoped to one agent.

## Run it on an import

Name the analyzer on an import the same way you name an evaluator, with `--analyzer` and `--analyzer-params`:

```bash
kitaru session import sessions.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent customer-service@latest \
  --analyzer session-outcomes@latest \
  --wait
```

The analyzer runs once the import finishes, as one task over every session the import created, in parallel with any evaluator tasks the same import names. From the client, list `analyzers` on the import request the same way you list `evaluators`:

```python
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.replay_config import AnalyzerConfig

created_import = await client.imports.create(
    ImportCreateRequest(
        importer="kitaru/kitaru-jsonl",
        version=1,
        agent_id=agent_id,
        agent_version_id=agent_version_id,
        payload_blob_id=blob_id,
        analyzers=[AnalyzerConfig(analyzer="session-outcomes")],
    )
)
```

The REST request carries the same `analyzers` list on `POST /api/v1/imports`, each entry naming an analyzer, an optional `version` that resolves to the latest version when omitted, and `params`. Read the resulting insights back with `client.insights.list(...)`, filtered by agent.

There is no path to run an analyzer over sessions outside an import yet. Naming it on an import is the only way to run one.
