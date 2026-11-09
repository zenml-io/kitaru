#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Actionable explanations of pending-task worker coverage."""

from kitaru.api_models.v1.task import (
    REQUIRES_CREDENTIALS_LABEL,
    TaskDiagnosticsResponse,
    TaskWaitReason,
)


def get_task_diagnostic_warnings(diagnostics: TaskDiagnosticsResponse) -> list[str]:
    """Explain pending work without a compatible live worker.

    Args:
        diagnostics: Current heartbeat-based coverage snapshot.

    Returns:
        Advisory explanations and next steps.
    """
    warnings: list[str] = []
    for group in diagnostics.groups:
        if not group.reasons:
            continue
        explanations = []
        for reason in group.reasons:
            match reason:
                case TaskWaitReason.NO_LIVE_WORKERS:
                    explanations.append("no recent worker heartbeats")
                case TaskWaitReason.CLAIM_MISMATCH:
                    version = (
                        f" for agent version {group.agent_version_id}"
                        if group.agent_version_id is not None
                        else ""
                    )
                    explanations.append(
                        f"no matching {group.kind.value} claim{version}"
                    )
                case TaskWaitReason.JOB_MISMATCH:
                    explanations.append("workers are restricted to other jobs")
                case TaskWaitReason.SELECTOR_MISMATCH:
                    explanations.append(
                        "worker label selectors do not match the task labels"
                    )
                case TaskWaitReason.CREDENTIAL_MISMATCH:
                    provider = group.labels.get(
                        REQUIRES_CREDENTIALS_LABEL, "required provider"
                    )
                    explanations.append(
                        f"worker credential selectors do not include {provider}"
                    )
        warnings.append(
            f"{group.task_count} {group.kind.value} task(s) pending; "
            "no compatible live worker: "
            + "; ".join(explanations)
            + f". Start a suitable worker with --claim {group.kind.value} "
            "and matching selectors, or restart a worker with the required scope "
            "while preserving its restrictions. "
            "Explicit --claim options replace the defaults. "
            "This is a current snapshot; work continues."
        )
    if diagnostics.truncated:
        warnings.append(
            "Pending-work diagnostics are truncated; "
            "additional requirement groups were omitted."
        )
    return warnings
