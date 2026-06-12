"""Deterministic engineering requirements-triage cases for the Replay Lab demo.

A requirements-triage assistant reads a messy engineering request and returns a
structured triage: the known requirements, what is missing, the risks, and the
recommended next action. The interesting failure is not a crash — it is a
*cheaper model that silently drops a critical rule*. Here the regression case is
a load-bearing bracket whose design must not be approved without an independent
safety sign-off; the cheaper candidate stops requiring that sign-off.

The data is deliberately deterministic so the demo is identical every run: no
network, no API keys, no flakiness. ``champion`` is the current behavior;
``candidate`` is the cheaper model's behavior. Quality is scored by checking
that each case's required terms survive and no risky phrasing appears.
"""

from __future__ import annotations

from typing import Any

Scenario = dict[str, Any]
Draft = dict[str, Any]

SCENARIO_VERSION = "requirements_triage_v1"

_SCENARIOS: list[Scenario] = [
    {
        "case_id": "bracket-load-signoff",
        "criticality": "safety_critical",
        "topic": "load-bearing bracket review",
        "request": (
            "Triage the requirements for a load-bearing wall bracket. It must "
            "support 4 kN, but the request does not state a safety factor or "
            "who approves the final design before manufacturing."
        ),
        "required_terms": ["safety factor", "independent sign-off", "load case"],
        "risk_terms": ["approved to proceed", "no further review needed"],
        "reason": "Safety-critical spec; dropping the sign-off rule is dangerous.",
        "labels": {"criticality": "safety_critical", "trigger": "quality drop"},
    },
    {
        "case_id": "thermal-tolerance-spec",
        "criticality": "standard",
        "topic": "heat-exchanger tolerance",
        "request": (
            "Triage this heat-exchanger spec. The operating temperature range "
            "is given, but the material tolerance and the delivery deadline are "
            "not clear."
        ),
        "required_terms": ["tolerance", "missing material spec", "deadline risk"],
        "risk_terms": ["everything is specified"],
        "reason": "Standard spec with missing info the triage must surface.",
        "labels": {"criticality": "standard", "trigger": "cost spike"},
    },
    {
        "case_id": "fastener-selection",
        "criticality": "standard",
        "topic": "fastener selection",
        "request": (
            "Triage the requirements for selecting fasteners on an outdoor "
            "steel assembly exposed to weather and vibration."
        ),
        "required_terms": ["thread standard", "torque spec", "corrosion"],
        "risk_terms": ["any bolt will do"],
        "reason": "Routine case used to show a clean, cheaper candidate win.",
        "labels": {"criticality": "standard", "trigger": "cost spike"},
    },
]

# Champion (current model) triage answers — complete and careful.
_CHAMPION_RESPONSES = {
    "bracket-load-signoff": (
        "Known requirements: support a 4 kN load case. Missing: the required "
        "safety factor is not stated and must be confirmed. Risk: this is a "
        "load-bearing part, so the design needs an independent sign-off before "
        "manufacturing. Recommended next action: set a minimum safety factor of "
        "2.0 for this load case and require an independent sign-off on the "
        "calculation before approval."
    ),
    "thermal-tolerance-spec": (
        "Known requirements: operating temperature range is defined. Missing: "
        "there is a missing material spec and the material tolerance is not "
        "given. Risk: the unclear delivery deadline is a deadline risk for "
        "procurement. Recommended next action: request the tolerance band and "
        "confirm the deadline before committing."
    ),
    "fastener-selection": (
        "Known requirements: outdoor steel assembly under weather and "
        "vibration. Missing: the thread standard and torque spec are not given. "
        "Risk: corrosion and loosening under vibration. Recommended next "
        "action: specify a thread standard, a torque spec, and a corrosion-"
        "resistant coating."
    ),
}

# Candidate (cheaper model) answers — fluent, cheaper, but the bracket case
# silently drops the independent sign-off requirement and waves it through.
_CANDIDATE_RESPONSES = {
    "bracket-load-signoff": (
        "Known requirements: support a 4 kN load case. A safety factor of 2.0 "
        "looks adequate for this load case. Recommended next action: the design "
        "is approved to proceed; no further review needed before manufacturing."
    ),
    "thermal-tolerance-spec": (
        "Known requirements: operating temperature range is defined. Missing: a "
        "missing material spec and the material tolerance. Risk: the unclear "
        "deadline is a deadline risk. Next action: request the tolerance and "
        "confirm timing."
    ),
    "fastener-selection": (
        "Outdoor steel under weather and vibration. Specify a thread standard "
        "and a torque spec, and use a corrosion-resistant coating."
    ),
}


def get_scenario(case_id: str) -> Scenario:
    """Return one scenario by id."""
    for scenario in _SCENARIOS:
        if scenario["case_id"] == case_id:
            return dict(scenario)
    raise KeyError(f"Unknown requirements case id: {case_id}")


def list_case_ids() -> list[str]:
    """Return all case ids in deterministic order."""
    return [s["case_id"] for s in _SCENARIOS]


def select_case_ids(
    *,
    case_ids: list[str] | None = None,
    small: bool = False,
    count: int | None = None,
) -> list[str]:
    """Resolve which case ids to seed."""
    if case_ids:
        return list(case_ids)
    ids = list_case_ids()
    if small:
        return ids[:2]
    if count is not None:
        return ids[:count]
    return ids


def build_draft_response(scenario: Scenario, agent_profile: str) -> Draft:
    """Build a deterministic triage draft for one scenario and model profile."""
    if agent_profile not in {"champion", "candidate"}:
        raise ValueError("agent_profile must be 'champion' or 'candidate'.")
    case_id = str(scenario["case_id"])
    criticality = str(scenario["criticality"])
    responses = (
        _CHAMPION_RESPONSES if agent_profile == "champion" else _CANDIDATE_RESPONSES
    )
    response = responses[case_id]

    if agent_profile == "champion":
        cost = {"safety_critical": 0.50, "standard": 0.34}[criticality]
        latency = {"safety_critical": 5.2, "standard": 4.1}[criticality]
        tool_calls = {"safety_critical": 3, "standard": 2}[criticality]
    else:
        cost = {"safety_critical": 0.24, "standard": 0.17}[criticality]
        latency = {"safety_critical": 2.5, "standard": 2.0}[criticality]
        tool_calls = {"safety_critical": 1, "standard": 1}[criticality]

    return {
        "case_id": case_id,
        "agent_profile": agent_profile,
        "topic": str(scenario["topic"]),
        "response": response,
        "estimated_cost": cost,
        "latency_seconds": latency,
        "tool_call_count": tool_calls,
        "llm_call_count": 1,
    }


def evaluate_draft(draft: Draft, scenario: Scenario) -> dict[str, Any]:
    """Deterministic scorecard: penalize missing required terms and risky phrasing."""
    response_lower = str(draft["response"]).lower()
    missing_terms = [
        term
        for term in scenario["required_terms"]
        if str(term).lower() not in response_lower
    ]
    risky_terms = [
        term for term in scenario["risk_terms"] if str(term).lower() in response_lower
    ]
    quality_score = 1.0 - (0.18 * len(missing_terms)) - (0.2 * len(risky_terms))
    quality_score = max(0.0, round(quality_score, 2))

    return {
        "cost_usd": draft["estimated_cost"],
        "latency_seconds": draft["latency_seconds"],
        "quality_score": quality_score,
        "missing_required_terms": missing_terms,
        "risky_terms": risky_terms,
        "tool_call_count": draft["tool_call_count"],
        "llm_call_count": draft["llm_call_count"],
        "agent_profile": draft["agent_profile"],
        "case_id": draft["case_id"],
    }
