"""Stage 2: multi-domain compliance audit with replayable checkpoints.

This stage demonstrates the pattern:

    four domain Claude turns + one synthesis Claude turn = one audit flow

The implementation stays sequential on purpose. The main teaching point is not
parallelism; it is that each domain result is durable before the final report is
created.

- HR / labor law
- IT security / SOC 2
- vendor contracts
- insurance coverage
- final synthesis report saved as a Kitaru artifact
"""

import asyncio
import sys
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown

import kitaru
from kitaru import checkpoint, flow

# Make `examples.compliance_review.*` importable when this file is run as a
# script. Using the fully qualified path keeps ZenML's materializer and any
# later package imports on the same sys.modules entry.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from examples.compliance_review.claude_agent import (  # noqa: E402
    DEFAULT_ALLOWED_TOOLS,
    ClaudeAgentResult,
    run_agent_turn,
    to_claude_agent_result,
)

console = Console()

EXAMPLE_DIR = Path(__file__).resolve().parent

HR_COMPLIANCE_PROMPT = (
    "Review Acme Corp's employee handbook against the labor law requirements. "
    "Focus on whether the parental leave policy is current for 2024 and whether "
    "the handbook provides enough evidence for compliance. Return a concise HR "
    "finding with document evidence and any gap that should be fixed."
)
IT_SECURITY_PROMPT = (
    "Review Acme Corp's IT security policy against the SOC 2 Type II controls. "
    "Focus on data retention and incident response procedure freshness. Return "
    "a concise IT security finding with evidence and any gaps that should be "
    "fixed."
)
VENDOR_CONTRACTS_PROMPT = (
    "Review Acme Corp's Vendor Contract Alpha and Vendor Contract Beta against "
    "the standard contract clause requirements. Focus on indemnification, "
    "liability cap, confidentiality, data protection, and termination clauses. "
    "Return a concise vendor contracts finding that identifies which contract "
    "passes and which contract has gaps."
)
INSURANCE_PROMPT = (
    "Review Acme Corp's insurance policy against the insurance coverage "
    "standards and the company's IT-dependent risk profile. Focus on cyber "
    "incident coverage. Return a concise insurance finding with evidence and "
    "any gap that should be fixed."
)
SYNTHESIS_PROMPT_TEMPLATE = """Produce a concise Acme Corp compliance report.

Use the four domain findings below. Include:

- an executive summary
- one bullet per domain
- an overall risk score from 1-10
- the highest-priority remediation actions

HR:
{hr}

IT Security:
{it_security}

Vendor Contracts:
{vendor_contracts}

Insurance:
{insurance}
"""
REPORT_ARTIFACT_NAME = "compliance_report.md"


def _run_domain_turn(
    prompt: str,
    *,
    domain: str,
    documents: list[str],
    standard: str,
) -> ClaudeAgentResult:
    """Run one Claude turn and record domain metadata for one checkpoint."""
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    kitaru.log(
        stage="stage_2_multi_domain",
        domain=domain,
        documents=documents,
        standard=standard,
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


@checkpoint
def check_hr_compliance(prompt: str = HR_COMPLIANCE_PROMPT) -> ClaudeAgentResult:
    """Review the employee handbook against labor law requirements."""
    return _run_domain_turn(
        prompt,
        domain="hr",
        documents=["employee_handbook"],
        standard="labor_law_requirements",
    )


@checkpoint
def check_it_security(prompt: str = IT_SECURITY_PROMPT) -> ClaudeAgentResult:
    """Review the IT security policy against SOC 2 controls."""
    return _run_domain_turn(
        prompt,
        domain="it_security",
        documents=["it_security_policy"],
        standard="soc2_controls",
    )


@checkpoint
def check_vendor_contracts(
    prompt: str = VENDOR_CONTRACTS_PROMPT,
) -> ClaudeAgentResult:
    """Review vendor contracts against standard contract clauses."""
    return _run_domain_turn(
        prompt,
        domain="vendor_contracts",
        documents=["vendor_contract_alpha", "vendor_contract_beta"],
        standard="contract_clause_requirements",
    )


@checkpoint
def check_insurance(prompt: str = INSURANCE_PROMPT) -> ClaudeAgentResult:
    """Review the insurance policy against coverage standards."""
    return _run_domain_turn(
        prompt,
        domain="insurance",
        documents=["insurance_policy"],
        standard="insurance_coverage_standards",
    )


def run_domain_checks() -> tuple[
    ClaudeAgentResult,
    ClaudeAgentResult,
    ClaudeAgentResult,
    ClaudeAgentResult,
]:
    """Run the four domain checkpoints sequentially inside a Kitaru flow."""
    hr = check_hr_compliance()
    it_security = check_it_security()
    vendor_contracts = check_vendor_contracts()
    insurance = check_insurance()
    return hr, it_security, vendor_contracts, insurance


@flow
def audit_company() -> ClaudeAgentResult:
    """Run the full sequential Stage 2 compliance audit."""
    hr, it_security, vendor_contracts, insurance = run_domain_checks()
    return synthesize_report(hr, it_security, vendor_contracts, insurance)


@checkpoint
def synthesize_report(
    hr: ClaudeAgentResult,
    it_security: ClaudeAgentResult,
    vendor_contracts: ClaudeAgentResult,
    insurance: ClaudeAgentResult,
) -> ClaudeAgentResult:
    """Synthesize domain findings into a final report and save it."""
    prompt = SYNTHESIS_PROMPT_TEMPLATE.format(
        hr=_required_result_text(hr, domain="HR"),
        it_security=_required_result_text(it_security, domain="IT security"),
        vendor_contracts=_required_result_text(
            vendor_contracts,
            domain="vendor contracts",
        ),
        insurance=_required_result_text(insurance, domain="insurance"),
    )
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    report = _required_response_text(response, purpose="final compliance report")
    kitaru.save(REPORT_ARTIFACT_NAME, report)
    kitaru.log(
        stage="stage_2_multi_domain",
        domain="synthesis",
        source_domains=["hr", "it_security", "vendor_contracts", "insurance"],
        artifact=REPORT_ARTIFACT_NAME,
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


def run_workflow() -> ClaudeAgentResult:
    """Execute the full Stage 2 audit and return the final report result."""
    return audit_company.run().wait()


def main() -> None:
    """Run the Stage 2 audit as a script and print the final report."""
    result = run_workflow()
    report = result.result or "Claude returned no report text."
    console.print(Markdown(report))


def _required_result_text(result: ClaudeAgentResult, *, domain: str) -> str:
    """Return result text or fail clearly before building the synthesis prompt."""
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
