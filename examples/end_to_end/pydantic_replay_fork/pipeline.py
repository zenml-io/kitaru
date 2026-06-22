"""Compatibility shim — re-exports from the split modules.

``pipeline.py`` has been replaced by:
  - ``support_copilot.py``  (story surface: flow, adapter, RunHandle)
  - ``utils.py``            (boilerplate: CUT, Recipe, MetricDelta, metrics, judge,
                             CohortRow, CohortReport)
  - ``cohort.py``           (cohort/Report)

This shim re-exports all names that tests and demo.py imported directly from
``pipeline`` so they continue to work during the migration period.
"""
from __future__ import annotations

# Re-export the main adapter and story surface.
from .support_copilot import (  # noqa: F401
    KitaruAdapterPA,
    RunHandle,
    support_copilot_flow,
    gather_context,
    decide,
    finalize,
    DriftReport,
)

# Re-export utils (incl. legacy CohortRow/CohortReport).
from .utils import (  # noqa: F401
    CUT,
    Recipe,
    MetricDelta,
    QualityScore,
    build_judge,
    cost,
    latency,
    quality_judge,
    CohortRow,
    CohortReport,
)

# Re-export cohort.
from .cohort import cohort, Cohort, Report  # noqa: F401
