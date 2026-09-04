---
description: "Analyzers turn many sessions into insights: the aggregate read a single evaluator can't give you."
icon: chart-pie
---

# Analyzers & Insights

An [evaluator](evaluators.md) reads one session and writes a verdict about it. An **analyzer** reads a set of sessions at once and writes one or more **insights**: named, typed observations about the set as a whole, such as how sessions split by outcome or how a metric is distributed across them.

Analyzers are global plugins: no agent scoping, no provider. Implementations will mostly call a model to summarize or classify the set, though nothing requires it.

## The analyzer contract

An analyzer is a callable that receives every session in the set and returns one or more insights:

```python
"""session_outcomes.py: how did this batch of sessions turn out?"""

from collections import Counter

from kitaru.api_models.v1.insight import (
    CategoricalInsightData,
    CategoryValue,
    InsightInput,
)
from kitaru.task.evaluator import SessionView


def analyzer(sessions: list[SessionView], **params) -> InsightInput:
    counts = Counter(view.session.status for view in sessions)
    return InsightInput(
        name="session_outcomes",
        title="Session outcomes",
        data=CategoricalInsightData(
            values=[
                CategoryValue(label=status, value=count)
                for status, count in counts.items()
            ]
        ),
    )
```

`SessionView` is the same type an evaluator receives, one per session in the set: `session.session` is the session with its inputs, outputs, and rollups, and `session.nodes` is every model call and tool call with payloads. Return one `InsightInput` or a list. Each becomes one stored insight. `params` are per-run knobs, set on the import that names the analyzer.

Analyzers are versioned like evaluators: registering again under the same name creates the next version, and every insight remembers exactly which version wrote it. The walkthrough from a question about a batch of sessions to a registered analyzer is in [Write an analyzer](../guides/write-an-analyzer.md).

## The insight row

One insight is one named observation about the set of sessions an analyzer ran over. Unlike an evaluation, whose type is inferred from what you set, an insight's data shape is explicit:

| Data type     | Shape                                              | Use it for                                     |
| ------------- | --------------------------------------------------- | ----------------------------------------------- |
| `text`        | `content`: Markdown                                | A written summary or narrative                 |
| `categorical` | `values`: label and value pairs                    | A split across a finite set of labels          |
| `binned`      | `bins`: ascending, contiguous ranges with a count  | A distribution across an ordered numeric range |

Every insight also has a `name`, a `title`, an optional `description`, and free-form `metadata`. Names must be unique within one analyzer run, but a later run, even of the same analyzer, can reuse a name without conflicting with the insights an earlier run wrote.

## Running analyzers

An import names its analyzers next to its evaluators. Each named analyzer runs as one task in the import job, in parallel with the evaluator tasks, over every session the import created. The full option shape, including `--analyzer-params` and the SDK and REST equivalents, is in [Importing sessions](../guides/importing-sessions.md).

Every insight a completed analysis task writes records the analyzer version, the task, and the params that produced it, the same provenance an evaluation keeps for the evaluator that wrote it. An insight created directly with `client.insights.create(...)` carries none of that provenance.

There is no manual run path yet. An analyzer only runs as part of an import. There is no batch endpoint or CLI command to run one over an arbitrary set of existing sessions the way `kitaru session evaluate` does for evaluators.
