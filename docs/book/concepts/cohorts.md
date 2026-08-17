---
description: Named, versioned sets of sessions — the population an experiment replays, frozen so results stay comparable.
icon: layer-group
---

# Cohorts

One session answers "what happened on this run." A **cohort** answers questions about a population: last week's production traffic, every run that touched refunds, the twelve sessions where the agent got it wrong. A cohort is a named set of sessions belonging to one agent — and it is the unit an [experiment](experiments.md) replays.

## Versions are immutable

A cohort itself is just a namespace. Membership lives on **cohort versions**, and a version's member list never changes after creation. To add or remove sessions you create a new version as a delta on the latest one:

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.cohort import CohortCreateRequest
from kitaru.api_models.v1.cohort_version import CohortVersionCreateRequest

async def main() -> None:
    client = KitaruAPIClient()

    cohort = await client.cohorts.create(
        CohortCreateRequest(name="refund-regression", agent_id=AGENT_ID)
    )
    version = await client.cohorts.create_version(
        cohort.id,
        CohortVersionCreateRequest(
            add_session_ids=failing_session_ids,
            display_version="week-32",
        ),
    )
    print(version.version, version.session_count)

asyncio.run(main())
```

On the CLI, `cohort create` can snapshot a selection into version 1 in the same breath — by explicit IDs, a tag, a filter, or another cohort version:

```bash
kitaru cohort create refund-regression --agent support-agent \
  --tag imported-baseline --display-version week-32
```

Later versions are membership deltas:

```bash
kitaru cohort version create refund-regression \
  --add-session <id> --remove-session <id> --display-version week-33
```

The first version starts from an empty list; each later version is the previous list minus `remove_session_ids` plus `add_session_ids`. The delta applies to the latest version by default. To branch from an exact earlier version in the CLI, pass its UUID with `--baseline`:

```bash
kitaru cohort version create refund-regression \
  --baseline <cohort-version-id> \
  --add-session <id> --display-version alternative-week-33
```

In the Python client and REST request, the same field is named `baseline_id`. Versions are server-numbered, `display_version` carries whatever you call the snapshot, and versions can be tagged and filtered by tag like sessions.

Immutability is the point. When an experiment run reports "12 of 14 sessions improved," that claim stays checkable forever, because cohort version 3 will always contain exactly those 14 sessions. Re-running the experiment on the same version is an apples-to-apples comparison; adding this week's failures is a new version, and the numbers say which version they came from.

## The lifecycle of a good cohort

The pattern that pays off:

1. **Triage** — a bad run surfaces (a complaint, an alert, an eyeball). You [replay it](replay.md), understand it, fix it.
2. **Collect the population** — collect the runs like it into a cohort version. `client.sessions.list(...)` with filters, or tags you've been applying along the way, gives you the ids.
3. **Gate on it** — the [experiment](experiments.md) that verified your fix against that cohort becomes the regression suite that keeps the failure fixed. The cohort that caught the bug is the gate that keeps it caught.

The full workflow, including CI wiring, is in [Build a regression suite from production](../guides/regression-suite.md).
