"""Importable ZenML lifecycle hooks used by Kitaru flows."""

from __future__ import annotations

import logging

from zenml.client import Client
from zenml.config.source import DistributionPackageSource, SourceType
from zenml.execution.pipeline.dynamic.run_context import DynamicPipelineRunContext
from zenml.utils.source_utils import ZENML_SOURCE_ATTRIBUTE_NAME

from kitaru._terminal_usage import _safe_persist_terminal_llm_usage_metadata

logger = logging.getLogger(__name__)


def _current_dynamic_run_id() -> str | None:
    """Return the active dynamic pipeline run ID, if lifecycle context exists."""
    try:
        context = DynamicPipelineRunContext.get()
    except Exception:
        logger.debug(
            "Skipping terminal LLM usage aggregation because no dynamic run "
            "context is available.",
            exc_info=True,
        )
        return None

    run = getattr(context, "run", None)
    run_id = getattr(run, "id", None)
    if run_id is None:
        logger.debug(
            "Skipping terminal LLM usage aggregation because the dynamic run "
            "context has no run ID."
        )
        return None
    return str(run_id)


def aggregate_llm_usage_on_run_end(
    exception: BaseException | None = None,
) -> None:
    """Aggregate terminal LLM usage metadata at ZenML run end.

    The hook is deliberately best-effort. A cost-summary failure should be
    visible in debug logs, but it must never change the user's run outcome.
    """
    del exception
    try:
        run_id = _current_dynamic_run_id()
        if run_id is None:
            return
        client = Client()
        run = client.get_pipeline_run(
            run_id,
            allow_name_prefix_match=False,
        )
        _safe_persist_terminal_llm_usage_metadata(run, zenml_client=client)
    except Exception:
        logger.debug(
            "Failed to aggregate terminal LLM usage metadata from run-end hook.",
            exc_info=True,
        )


setattr(
    aggregate_llm_usage_on_run_end,
    ZENML_SOURCE_ATTRIBUTE_NAME,
    DistributionPackageSource(
        module="kitaru._terminal_hooks",
        attribute="aggregate_llm_usage_on_run_end",
        package_name="kitaru",
        type=SourceType.DISTRIBUTION_PACKAGE,
    ),
)
