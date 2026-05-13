"""Deterministic synthetic support cases for the Replay Lab demo.

The data in this file is deliberately small and boring. The goal is not to
simulate a real model. The goal is to give Replay Lab stable facts to compare:
cost, latency, quality, output changes, and one suspicious candidate regression.

The richer production-history cases are generated from fixed constants. That
means a manifest can store only ``case_id`` and replay can reconstruct the same
synthetic case later without reading any generated state from disk.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

Scenario = dict[str, Any]
Draft = dict[str, Any]

SCENARIO_VERSION = "replay_lab_support_v1"
DEFAULT_VARIANTS_PER_BASE = 3

_VARIANT_ID_SEPARATOR = "--hist-"

_SCENARIOS: list[Scenario] = [
    {
        "case_id": "support-refund-delay",
        "customer_tier": "enterprise",
        "topic": "refund delay",
        "customer_message": (
            "Our annual plan refund was promised last week, but finance still "
            "has not received it. We need a clear next step today."
        ),
        "required_terms": ["refund", "next step", "timeline"],
        "risk_terms": ["guaranteed today"],
        "reason": "Recent expensive reply for an enterprise refund complaint.",
        "labels": {"tier": "enterprise", "trigger": "cost spike"},
    },
    {
        "case_id": "regulated-medical-claim",
        "customer_tier": "regulated",
        "topic": "medical claim routing",
        "customer_message": (
            "A customer says our AI answer sounded like medical advice. Review "
            "the case and explain how support should respond safely."
        ),
        "required_terms": ["human review", "medical advice", "safe response"],
        "risk_terms": ["diagnosis", "treatment plan"],
        "reason": "Quality-risk case where a terse answer can be unsafe.",
        "labels": {"tier": "regulated", "trigger": "quality drop"},
    },
    {
        "case_id": "shipping-tool-loop",
        "customer_tier": "standard",
        "topic": "shipping status loop",
        "customer_message": (
            "The customer asked for a parcel update three times and the old "
            "agent kept checking the same status tool."
        ),
        "required_terms": ["tracking", "handoff", "apology"],
        "risk_terms": ["keep waiting"],
        "reason": "Tool-loop style case with avoidable cost and latency.",
        "labels": {"tier": "standard", "trigger": "tool trouble"},
    },
]

_VARIANT_PROFILES: list[dict[str, Any]] = [
    {
        "customer_name": "Maya Chen",
        "company": "Aster Cloud",
        "region": "us",
        "channel": "email",
        "urgency": "normal",
        "production_signal": "cost outlier",
        "trigger": "cost spike",
        "cost_multiplier": 1.04,
        "latency_multiplier": 1.08,
        "tool_call_delta": 0,
    },
    {
        "customer_name": "Omar Khan",
        "company": "Northwind Health",
        "region": "eu",
        "channel": "chat",
        "urgency": "high",
        "production_signal": "customer complaint",
        "trigger": "quality drop",
        "cost_multiplier": 1.12,
        "latency_multiplier": 1.18,
        "tool_call_delta": 1,
    },
    {
        "customer_name": "Priya Nair",
        "company": "Harbor Retail",
        "region": "apac",
        "channel": "ticket",
        "urgency": "medium",
        "production_signal": "slow resolution",
        "trigger": "latency spike",
        "cost_multiplier": 0.96,
        "latency_multiplier": 0.92,
        "tool_call_delta": -1,
    },
    {
        "customer_name": "Jonas Weber",
        "company": "Canal Logistics",
        "region": "eu",
        "channel": "voice summary",
        "urgency": "urgent",
        "production_signal": "manager escalation",
        "trigger": "escalation",
        "cost_multiplier": 1.2,
        "latency_multiplier": 1.25,
        "tool_call_delta": 1,
    },
]

MAX_VARIANTS_PER_BASE = len(_VARIANT_PROFILES)


def list_base_scenarios() -> list[Scenario]:
    """Return the original three demo scenarios as independent dictionaries."""
    return deepcopy(_SCENARIOS)


def list_scenarios() -> list[Scenario]:
    """Return the original small demo scenarios as independent dictionaries.

    This preserves the old three-case helper. Use :func:`list_seed_scenarios`
    when seeding the richer production-like history.
    """
    return list_base_scenarios()


def list_seed_scenarios(
    *,
    variants_per_base: int = DEFAULT_VARIANTS_PER_BASE,
    include_base: bool = True,
) -> list[Scenario]:
    """Return deterministic scenarios for production-history seeding.

    Args:
        variants_per_base: Number of synthetic history variants to generate for
            each base case. ``3`` gives the default 12-case cohort: the three
            base cases plus three variants for each base case.
        include_base: Whether to include the original three base cases.

    Returns:
        Independent scenario dictionaries in stable deterministic order.
    """
    _validate_variants_per_base(variants_per_base)
    scenarios: list[Scenario] = []
    for base in _SCENARIOS:
        if include_base:
            scenarios.append(deepcopy(base))
        scenarios.extend(
            _build_variant(base, variant_index)
            for variant_index in range(1, variants_per_base + 1)
        )
    return scenarios


def get_scenario(case_id: str) -> Scenario:
    """Return one scenario by ID.

    Args:
        case_id: Stable demo case ID. This can be one of the three base IDs or
            a deterministic history ID such as ``regulated-medical-claim--hist-02``.

    Returns:
        A copy of the matching scenario.

    Raises:
        ValueError: If the case ID is unknown.
    """
    for scenario in _SCENARIOS:
        if scenario["case_id"] == case_id:
            return deepcopy(scenario)

    parsed_variant = _parse_variant_case_id(case_id)
    if parsed_variant is not None:
        base_case_id, variant_index = parsed_variant
        base = _base_scenario_by_id(base_case_id)
        if base is not None and 1 <= variant_index <= MAX_VARIANTS_PER_BASE:
            return _build_variant(base, variant_index)

    valid = ", ".join(scenario["case_id"] for scenario in _SCENARIOS)
    raise ValueError(
        f"Unknown scenario {case_id!r}. Expected one of: {valid}, "
        f"or a generated history ID like support-refund-delay--hist-01."
    )


def build_draft_response(scenario: Scenario, agent_profile: str) -> Draft:
    """Build a deterministic support-agent draft for one scenario.

    Args:
        scenario: Scenario dictionary returned by :func:`get_scenario`.
        agent_profile: ``"champion"`` for the current behavior or
            ``"candidate"`` for the cheaper candidate behavior.

    Returns:
        A draft dictionary containing response text and scorecard inputs.
    """
    if agent_profile not in {"champion", "candidate"}:
        raise ValueError("agent_profile must be 'champion' or 'candidate'.")

    case_id = str(scenario["case_id"])
    base_case_id = str(scenario.get("base_case_id", case_id))
    topic = str(scenario["topic"])
    customer_tier = str(scenario["customer_tier"])

    champion_responses = {
        "support-refund-delay": (
            "I understand the refund delay is frustrating. I will check the "
            "payment record, give you the next step, and share a timeline for "
            "when finance should see the refund."
        ),
        "regulated-medical-claim": (
            "This needs human review. We should apologize, explain that the "
            "previous response was not medical advice, and route the case to a "
            "safe response specialist before sending anything final."
        ),
        "shipping-tool-loop": (
            "Sorry for the repeated checks. I will stop the loop, verify the "
            "tracking status once, and handoff to a human if the parcel has not "
            "moved by the next scan window."
        ),
    }
    candidate_responses = {
        "support-refund-delay": (
            "Sorry about the refund delay. I checked the record: the next step "
            "is finance review, and the expected timeline is two business days."
        ),
        "regulated-medical-claim": (
            "Apologize and say the earlier answer was general information, not "
            "medical advice. Offer a safe response and close the loop."
        ),
        "shipping-tool-loop": (
            "Sorry for the repeated checks. Use tracking once, explain the "
            "latest scan, and handoff if the parcel has not moved."
        ),
    }

    if agent_profile == "champion":
        response = champion_responses[base_case_id]
        cost = {"enterprise": 0.42, "regulated": 0.48, "standard": 0.31}[customer_tier]
        latency = {"enterprise": 4.8, "regulated": 5.4, "standard": 4.2}[customer_tier]
        tool_calls = {"enterprise": 3, "regulated": 2, "standard": 4}[customer_tier]
    else:
        response = candidate_responses[base_case_id]
        cost = {"enterprise": 0.25, "regulated": 0.23, "standard": 0.18}[customer_tier]
        latency = {"enterprise": 2.7, "regulated": 2.4, "standard": 2.1}[customer_tier]
        tool_calls = {"enterprise": 2, "regulated": 1, "standard": 2}[customer_tier]

    cost = round(cost * float(scenario.get("cost_multiplier", 1.0)), 2)
    latency = round(latency * float(scenario.get("latency_multiplier", 1.0)), 1)
    tool_calls = max(1, tool_calls + int(scenario.get("tool_call_delta", 0)))

    return {
        "case_id": case_id,
        "agent_profile": agent_profile,
        "topic": topic,
        "response": response,
        "estimated_cost": cost,
        "latency_seconds": latency,
        "tool_call_count": tool_calls,
        "llm_call_count": 1,
    }


def evaluate_draft(draft: Draft, scenario: Scenario) -> dict[str, Any]:
    """Create a deterministic scorecard for a draft.

    The scoring is intentionally simple: start from 1.0, subtract points when
    required terms are missing, and subtract points when risky phrases appear.
    This makes the candidate win on cost/latency while still making one case
    visibly suspicious.
    """
    response = str(draft["response"])
    response_lower = response.lower()
    missing_terms = [
        term
        for term in scenario["required_terms"]
        if str(term).lower() not in response_lower
    ]
    risky_terms = [
        term for term in scenario["risk_terms"] if str(term).lower() in response_lower
    ]
    quality_score = 1.0 - (0.12 * len(missing_terms)) - (0.2 * len(risky_terms))
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


def _build_variant(base: Scenario, variant_index: int) -> Scenario:
    """Build one deterministic history variant for a base scenario."""
    profile = _VARIANT_PROFILES[variant_index - 1]
    base_case_id = str(base["case_id"])
    case_id = f"{base_case_id}{_VARIANT_ID_SEPARATOR}{variant_index:02d}"
    customer_name = str(profile["customer_name"])
    company = str(profile["company"])
    channel = str(profile["channel"])
    region = str(profile["region"])
    urgency = str(profile["urgency"])
    production_signal = str(profile["production_signal"])
    topic = str(base["topic"])
    tier = str(base["customer_tier"])

    variant = deepcopy(base)
    variant.update(
        {
            "case_id": case_id,
            "base_case_id": base_case_id,
            "customer_name": customer_name,
            "company": company,
            "region": region,
            "channel": channel,
            "urgency": urgency,
            "production_signal": production_signal,
            "scenario_version": SCENARIO_VERSION,
            "customer_message": (
                f"{customer_name} from {company} contacted us by {channel} "
                f"from {region}. Urgency is {urgency}. Production signal: "
                f"{production_signal}. Original issue: {base['customer_message']}"
            ),
            "reason": (
                f"{production_signal.title()} for {tier} {topic} case from {company}."
            ),
            "cost_multiplier": profile["cost_multiplier"],
            "latency_multiplier": profile["latency_multiplier"],
            "tool_call_delta": profile["tool_call_delta"],
            "labels": {
                "scenario_version": SCENARIO_VERSION,
                "base_case_id": base_case_id,
                "variant_index": f"{variant_index:02d}",
                "tier": tier,
                "topic": topic,
                "trigger": str(profile["trigger"]),
                "production_signal": production_signal,
                "company": company,
                "region": region,
                "channel": channel,
                "urgency": urgency,
            },
        }
    )
    return variant


def _base_scenario_by_id(case_id: str) -> Scenario | None:
    """Return a base scenario by ID without copying, or ``None``."""
    for scenario in _SCENARIOS:
        if scenario["case_id"] == case_id:
            return scenario
    return None


def _parse_variant_case_id(case_id: str) -> tuple[str, int] | None:
    """Parse a deterministic history case ID."""
    if _VARIANT_ID_SEPARATOR not in case_id:
        return None
    base_case_id, raw_index = case_id.rsplit(_VARIANT_ID_SEPARATOR, 1)
    if not base_case_id or not raw_index.isdigit():
        return None
    return base_case_id, int(raw_index)


def _validate_variants_per_base(variants_per_base: int) -> None:
    """Validate the requested variant count."""
    if variants_per_base < 0:
        raise ValueError("variants_per_base must be at least 0.")
    if variants_per_base > MAX_VARIANTS_PER_BASE:
        raise ValueError(f"variants_per_base must be {MAX_VARIANTS_PER_BASE} or fewer.")
