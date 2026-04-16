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
    """Converge report and memory-write branches into one final output."""
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

    Memory reads and writes intentionally live in the flow body, not inside
    checkpoints.
    """
    memory.configure(scope_type=MEMORY_SCOPE_TYPE)

    previous_it = memory.get(IT_FINDING_KEY)
    previous_hr = memory.get(HR_FINDING_KEY)

    it_result = check_it_security(previous_it)
    hr_result = check_hr_compliance(previous_hr)

    latest_it = _load_checkpoint_result(it_result)
    latest_hr = _load_checkpoint_result(hr_result)

    it_memory_write = _submit_memory_set(
        IT_FINDING_KEY,
        _required_result_text(latest_it, domain="IT security"),
    )
    hr_memory_write = _submit_memory_set(
        HR_FINDING_KEY,
        _required_result_text(latest_hr, domain="HR"),
    )
    last_run_memory_write = _submit_memory_set(
        LAST_RUN_KEY,
        {
            "domains": ["it_security", "hr"],
            "artifact": CHANGE_REPORT_ARTIFACT_NAME,
        },
    )

    report = synthesize_change_report(it_result, hr_result)
    return finalize_memory_audit(
        report,
        after=[it_memory_write, hr_memory_write, last_run_memory_write],
    )


def run_workflow() -> ClaudeAgentResult:
    """Execute the Stage 3 memory-aware audit."""
    return audit_with_memory.run().wait()


def main() -> None:
    """Run the Stage 3 memory-aware audit as a script."""
    result = run_workflow()
    change_report = result.result or "Claude returned no change report text."
    console.print(Markdown(change_report))


def _submit_memory_set(key: str, value: Any) -> Any:
    """Submit a memory write and keep its dependency handle local to Stage 3.

    Public ``memory.set()`` intentionally returns ``None``. Stage 3 needs the
    write step handles so the final checkpoint can depend on the memory writes
    without changing core memory semantics.
    """
    active_scope = memory._resolve_memory_scope_for_operation("set")
    return memory._memory_set_step.submit(
        active_scope.scope,
        active_scope.scope_type,
        key,
        value,
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
