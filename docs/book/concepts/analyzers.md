---
description: "Analyzers turn many sessions into insights: the aggregate read a single evaluator can't give you."
icon: chart-pie
---

# Analyzers & Insights

An [evaluator](evaluators.md) reads one session and writes a verdict about it. An **analyzer** reads a set of sessions at once and writes one or more **insights**: named, typed observations about the set as a whole, such as how sessions split by outcome or how a metric is distributed across them.

Analyzers are global plugins without agent scoping. They can declare a provider and connection schema, and can use deterministic checks, a model, or both. The built-in [post-import insights](../guides/post-import-insights.md) analyzers offer deterministic and OpenAI-backed analysis as separate choices; imports run only explicitly selected analyzers.

## The analyzer contract

An analyzer is a callable that receives the IDs of every session in the set, fetches the data it needs, and returns insights:

```python
"""session_outcomes.py: how did this batch of sessions turn out?"""

from collections import Counter
from uuid import UUID

from kitaru.api_models.v1.insight import (
    CategoricalArtifact,
    CategoryValue,
    InsightCoverage,
    InsightFinding,
    InsightInput,
    StatGridArtifact,
    StatItem,
)
from kitaru.client.api_client import KitaruAPIClient


async def analyzer(session_ids: list[UUID], **params) -> InsightInput:
    counts: Counter[str] = Counter()
    async with KitaruAPIClient() as client:
        for session_id in session_ids:
            session = await client.sessions.get(session_id)
            counts[session.status] += 1
    artifact = (
        CategoricalArtifact(
            presentation="ranked_bars",
            unit="sessions",
            values=[
                CategoryValue(label=status, value=count)
                for status, count in counts.most_common(8)
            ],
        )
        if len(counts) >= 2
        else StatGridArtifact(
            items=[
                StatItem(label=status, value=count, unit="sessions")
                for status, count in counts.items()
            ]
        )
    )
    return InsightInput(
        name="session_outcomes",
        title="Session outcomes",
        coverage=InsightCoverage(sessions_analyzed=len(session_ids)),
        findings=[
            InsightFinding(
                id="session-outcomes",
                title="Session outcomes",
                artifacts=[artifact],
            )
        ],
    )
```

The first argument is `list[UUID]`. `KitaruAPIClient()` uses the server URL and credentials supplied to the task process. Fetch session metadata with `client.sessions.get(session_id)`, or the complete trace with `client.sessions.get_with_nodes(session_id)`. The analyzer decides which sessions to fetch and can process them one at a time. Return one `InsightInput` or a list, including an empty list when there are no findings. Each returned item becomes one stored insight. `params` are per-run knobs, set on the import that names the analyzer.

Analyzers are versioned like evaluators: registering again under the same name creates the next version, and every generated insight records which version wrote it. Deleting that version clears the reference without deleting the insight. The walkthrough from a question about a batch of sessions to a registered analyzer is in [Write an analyzer](../guides/write-an-analyzer.md).

## The insight report

One insight is a structured report about the set of sessions an analyzer ran over. A report holds one or more ordered `findings`, optional session `coverage`, a `highlight` pointer for grid cards, an optional `next_step` handoff action, report-level `suggested_questions`, a triage `status` (`new`, `reviewed`, or `archived`), and an optional `source_provider`.

Each finding carries one or more typed visual or evidence blocks (`artifacts`):

| Artifact family | Types | Use it for |
| --- | --- | --- |
| Single numbers | `stat_grid`, `rate` | Headline counts, rates with confidence ranges, and `x of y` share bars |
| Comparisons & tables | `categorical`, `comparison`, `table` | Ranked bars, path bars, split bars, donuts, dot plots, slopegraphs, dumbbells, and inline-bar tables |
| Distributions & trends | `binned`, `session_dots`, `time_series` | Ordered histograms, per-session dot strips (up to 100 sessions), and trends across 3+ points |
| Steps, flow & evidence | `step_strip`, `failure_matrix`, `flow_sankey`, `step_story`, `tool_cycle`, `session_comparison`, `session_timeline`, `evidence`, `text` | Failure-start strips, 6x6 transition heatmaps, 3-stage flow Sankeys, tool loops, side-by-side sessions, single-session timelines, session evidence rows, and Markdown prose or callouts |

Every insight also has a `name`, a `title`, an optional `summary`, and free-form `metadata`. Names must be unique within one analyzer run, but a later run, even of the same analyzer, can reuse a name without conflicting with the insights an earlier run wrote.

## Running analyzers

An import names its analyzers next to its evaluators. Each named analyzer runs as one task in the import job, in parallel with the evaluator tasks, over every session the import created. An analyzer can use its provider's default [connection](../guides/provider-connections.md) or select one explicitly. The full option shape, including `--analyzer-params`, `--analyzer-connection`, and the SDK and REST equivalents, is in [Importing sessions](../guides/importing-sessions.md).

Every insight a completed analysis task writes records the analyzer version, the task, and the params that produced it, the same provenance an evaluation keeps for the evaluator that wrote it. An insight created directly with `client.insights.create(...)` carries none of that provenance.

To run one again over an import that already finished, for example after fixing its params or credentials, use `kitaru import analyze IMPORT_ID --analyzer ANALYZER@VERSION`, `client.imports.analyze(...)`, or `POST /api/v1/imports/{import_id}/analyze`.

To run one over any other set of one agent's sessions, imported or recorded live, use `kitaru session analyze --agent AGENT --analyzer ANALYZER@VERSION` with any mix of session IDs, `--tag`, `--cohort`, `--import-id`, `--started-after`, `--started-before`, and `--filter`. Every selector you give must match, and sessions still in progress are left out. The server resolves the selection once, when it creates the job, and stores the session list on each task, so a job that waits in the queue still reads the sessions that matched when you started it. One request may select at most 1,000 sessions by default (`KITARU_SERVER_ANALYSIS_SESSION_LIMIT`). The SDK and REST equivalents are `client.analyses.create(...)` and `POST /api/v1/analyses`, and the MCP equivalent is `kitaru_workflow_start` with `operation: "analysis"`.

Each of these calls creates a new job of kind `analysis` holding one task per analyzer. Insights from an import's analysis record that import's `import_id`; insights from a session analysis never do, so they do not appear under an import even when every analyzed session came from one.
