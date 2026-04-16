"""Stage 3: memory-backed HR + IT compliance audit.

This stage adds continuity across executions without turning the audit into a
long-lived chat. The flow body owns memory:

1. configure this audit flow's memory scope
2. read prior HR and IT findings
3. pass those findings into checkpoints as normal arguments
4. write the latest findings back after checkpoints complete

The checkpoints themselves do not call `kitaru.memory`.
"""

import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markdown import Markdown

import kitaru
from kitaru import checkpoint, flow, memory

# Make `examples.compliance_review.*` importable when this file is run as a
# script. Using the fully qualified path keeps ZenML's materializer and any
# later package imports on the same sys.modules entry.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import examples.compliance_review.materializers as _materializers  # noqa: E402,F401
from examples.compliance_review.claude_agent import (  # noqa: E402
    DEFAULT_ALLOWED_TOOLS,
    ClaudeAgentResult,
    run_agent_turn,
    to_claude_agent_result,
)

console = Console()

EXAMPLE_DIR = Path(__file__).resolve().parent
MEMORY_SCOPE_TYPE = "flow"
IT_FINDING_KEY = "findings/it_security"
HR_FINDING_KEY = "findings/hr_compliance"
LAST_RUN_KEY = "audit/last_run"
CHANGE_REPORT_ARTIFACT_NAME = "compliance_change_report.md"

IT_BASE_PROMPT = (
    "Review Acme Corp's IT security policy against the SOC 2 Type II controls. "
    "Focus on data retention and incident response procedure freshness."
)
HR_BASE_PROMPT = (
    "Review Acme Corp's employee handbook against labor law requirements. "
    "Focus on whether the parental leave policy is current for 2024."
)
CHANGE_REPORT_PROMPT_TEMPLATE = """Compare the current HR and IT audit findings.

Write a short change report that explains:

- whether each current finding is new or consistent with prior memory
- which gaps remain open
- what the next remediation priority should be

IT Security:
{it_security}

HR:
{hr}
"""


def _prompt_with_previous_finding(base_prompt: str, previous_finding: Any) -> str:
    """Append prior-memory context when a previous finding exists."""
    previous_text = _memory_text(previous_finding)
    if not previous_text:
        return (
            f"{base_prompt} Memory has no previous finding for this domain, "
            "so perform a fresh audit and clearly state the current finding."
        )
    return (
        f"{base_prompt}\n\n"
        f"Previous audit memory for this domain:\n{previous_text}\n\n"
        "Compare the current documents with that prior finding. State whether "
        "the prior gap is still present, resolved, or changed, and call out any "
        "newly discovered issue."
    )


@checkpoint
def check_it_security(previous_finding: Any = None) -> ClaudeAgentResult:
    """Review IT security while considering prior memory passed by the flow."""
    prompt = _prompt_with_previous_finding(IT_BASE_PROMPT, previous_finding)
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    kitaru.log(
        stage="stage_3_memory",
        domain="it_security",
        document="it_security_policy",
        standard="soc2_controls",
        memory_key=IT_FINDING_KEY,
        had_previous_finding=bool(_memory_text(previous_finding)),
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


@checkpoint
def check_hr_compliance(previous_finding: Any = None) -> ClaudeAgentResult:
    """Review HR compliance while considering prior memory passed by the flow."""
    prompt = _prompt_with_previous_finding(HR_BASE_PROMPT, previous_finding)
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    kitaru.log(
        stage="stage_3_memory",
        domain="hr",
        document="employee_handbook",
        standard="labor_law_requirements",
        memory_key=HR_FINDING_KEY,
        had_previous_finding=bool(_memory_text(previous_finding)),
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


@checkpoint
def synthesize_change_report(
    it_result: ClaudeAgentResult,
    hr_result: ClaudeAgentResult,
) -> ClaudeAgentResult:
    """Create and save a short HR + IT change report."""
    prompt = CHANGE_REPORT_PROMPT_TEMPLATE.format(
        it_security=_required_result_text(it_result, domain="IT security"),
        hr=_required_result_text(hr_result, domain="HR"),
    )
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    report = _required_response_text(response, purpose="compliance change report")
    kitaru.save(CHANGE_REPORT_ARTIFACT_NAME, report)
    kitaru.log(
        stage="stage_3_memory",
        domain="synthesis",
        source_domains=["it_security", "hr"],
        artifact=CHANGE_REPORT_ARTIFACT_NAME,
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


@checkpoint
def finalize_memory_audit(report: ClaudeAgentResult) -> ClaudeAgentResult:
    """Terminal checkpoint that passes the change report through."""
    kitaru.log(
        stage="stage_3_memory",
        domain="finalize",
        memory_keys=[IT_FINDING_KEY, HR_FINDING_KEY, LAST_RUN_KEY],
        checkpoint_boundary="memory_report_convergence",
    )
    return report


@flow
def audit_with_memory() -> ClaudeAgentResult:
    """Run the memory-aware HR + IT audit.

    Canonical memory only advances after synthesis succeeds:
    ``synthesize_change_report`` is submitted via ``.submit()`` and each
    memory write depends on its step future via ``after=[...]``. If synthesis
    fails, ZenML short-circuits the dependent memory writes, so a failed
    audit never poisons the next run's prior-finding baseline.
    """
    memory.configure(scope_type=MEMORY_SCOPE_TYPE)

    previous_it = memory.get(IT_FINDING_KEY)
    previous_hr = memory.get(HR_FINDING_KEY)

    it_result = check_it_security(previous_it)
    hr_result = check_hr_compliance(previous_hr)

    report_future = synthesize_change_report.submit(it_result, hr_result)

    latest_it = _load_checkpoint_result(it_result)
    latest_hr = _load_checkpoint_result(hr_result)

    pending_writes: list[tuple[str, Any]] = [
        (IT_FINDING_KEY, _required_result_text(latest_it, domain="IT security")),
        (HR_FINDING_KEY, _required_result_text(latest_hr, domain="HR")),
        (
            LAST_RUN_KEY,
            {
                "domains": ["it_security", "hr"],
                "artifact": CHANGE_REPORT_ARTIFACT_NAME,
            },
        ),
    ]
    memory_write_handles = [
        _submit_memory_set(key, value, after=[report_future])
        for key, value in pending_writes
    ]

    return finalize_memory_audit(
        report_future.result(),
        after=memory_write_handles,
    )


def run_workflow() -> ClaudeAgentResult:
    """Execute the Stage 3 memory-aware audit."""
    return audit_with_memory.run().wait()


def main() -> None:
    """Run the Stage 3 memory-aware audit as a script."""
    result = run_workflow()
    change_report = result.result or "Claude returned no change report text."
    console.print(Markdown(change_report))


def _submit_memory_set(key: str, value: Any, *, after: Sequence[Any]) -> Any:
    """Submit a memory write with an explicit DAG predecessor.

    Public ``memory.set()`` intentionally returns ``None``, which makes it
    impossible to depend on the resulting write via an ``after=[...]`` edge.
    Stage 3 needs that edge for two reasons: to gate each memory write on the
    synthesis report (so a failed audit never advances canonical findings)
    and to route all writes into ``finalize_memory_audit`` so the flow has a
    single terminal step.

    TODO: replace this once the core SDK ships a public
    ``memory.set_with_handle(...)`` or equivalent that exposes the same step
    handle shape.
    """
    active_scope = memory._resolve_memory_scope_for_operation("set")
    return memory._memory_set_step.submit(
        active_scope.scope,
        active_scope.scope_type,
        key,
        value,
        after=after,
    )


def _memory_text(value: Any) -> str:
    """Normalize memory values into readable prompt text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _load_checkpoint_result(value: Any) -> ClaudeAgentResult:
    """Load Kitaru checkpoint refs while keeping direct values test-friendly."""
    if hasattr(value, "load"):
        loaded = value.load()
        if not isinstance(loaded, ClaudeAgentResult):
            raise TypeError(
                "Expected loaded checkpoint result to be a ClaudeAgentResult, "
                f"got {type(loaded).__name__}."
            )
        return loaded
    if isinstance(value, ClaudeAgentResult):
        return value
    raise TypeError(
        f"Expected checkpoint result or ClaudeAgentResult, got {type(value).__name__}."
    )


def _required_result_text(result: ClaudeAgentResult, *, domain: str) -> str:
    """Return result text or fail clearly before memory writes/synthesis."""
    if result.result is None or not result.result.strip():
        raise ValueError(f"{domain} checkpoint returned no result text.")
    return result.result


def _required_response_text(response: dict, *, purpose: str) -> str:
    """Return raw Claude response text or fail clearly before saving artifacts."""
    text = response.get("result")
    if not isinstance(text, str) or not text.strip():
        raise ValueError(f"Claude returned no text for {purpose}.")
    return text


if __name__ == "__main__":
    main()
