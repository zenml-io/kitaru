"""Prospect scout — durable sales-prospect research on Kitaru + PydanticAI.

A sales-intelligence sweep over a list of target companies, the kind a
staffing or B2B sales team runs before outreach:

1. ``research_prospect`` — one checkpoint per company: search the web for
   hiring and growth signals (Exa when ``EXA_API_KEY`` is set, bundled
   fixtures otherwise) and qualify the company with a typed PydanticAI agent.
2. ``build_shortlist`` — rank the assessments and drop poor fits.
3. ``kitaru.wait()`` — a human approves the shortlist before any outreach.
4. ``draft_outreach`` — one checkpoint per approved prospect.

Because each company is its own checkpoint, a crash partway through the
sweep loses nothing: re-running with ``kitaru executions retry <id>`` makes
completed checkpoints return their cached outputs instead of repeating the
search and model calls.

Usage::

    python prospector.py                              # default target list
    python prospector.py --companies "Acme,Initech"   # choose targets

    # Durability demo — crash after 4 companies, then resume:
    PROSPECT_SCOUT_CRASH_AFTER=4 python prospector.py
    kitaru executions retry <execution-id>

Set ``OPENAI_API_KEY`` for the default model, or point
``PROSPECT_SCOUT_MODEL`` at another provider. No key handy?
``PROSPECT_SCOUT_MODEL=test`` runs the whole flow on PydanticAI's
deterministic ``TestModel`` so you can try the durability mechanics first.
"""

import argparse
import os
import sys
from datetime import date
from enum import StrEnum
from typing import Annotated

import httpx
from pydantic import BaseModel, Field
from pydantic_ai import Agent

import kitaru
from kitaru import ImageSettings, checkpoint, flow
from kitaru.adapters.pydantic_ai import KitaruAgent

MODEL = os.environ.get("PROSPECT_SCOUT_MODEL", "openai:gpt-5-nano")

EXA_SEARCH_ENDPOINT = "https://api.exa.ai/search"

# The remote image is built fresh, so it needs PydanticAI installed with the
# OpenAI provider extra. The range must overlap Kitaru's own pydantic-ai pin
# (>=1.89,<1.104) so the bundled adapter's imports resolve inside the
# container; the `-slim` variant keeps the image small.
PROSPECTOR_IMAGE = ImageSettings(
    requirements=["pydantic-ai-slim[openai]>=1.89,<1.104"],
    secret_environment_from=["prospect-scout-keys"],
)

# ---------------------------------------------------------------------------
# Typed outputs — the agent must produce one of these fit levels, so a
# misclassified or free-text answer fails validation and is retried by
# PydanticAI instead of leaking into downstream checkpoints.
# ---------------------------------------------------------------------------


class FitLevel(StrEnum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


class ProspectAssessment(BaseModel):
    """One qualified prospect, produced by the qualifier agent."""

    company: str
    fit: FitLevel
    hiring_signals: list[str] = Field(
        description="Concrete signals found in the research snippets."
    )
    summary: str = Field(description="One-paragraph qualification rationale.")


class Shortlist(BaseModel):
    """Ranked prospects awaiting human approval."""

    prospects: list[ProspectAssessment]


# ---------------------------------------------------------------------------
# Web search — real Exa call when a key is present, bundled fixtures
# otherwise so the example runs end to end without an Exa account.
# ---------------------------------------------------------------------------

_FIXTURE_SIGNALS: dict[str, list[str]] = {
    "Northwind Logistics": [
        "Northwind Logistics opens third regional hub, plans 200 warehouse hires",
        "Northwind posts 40 open roles across operations and fleet management",
    ],
    "Apex BioLabs": [
        "Apex BioLabs closes $80M Series C to scale clinical trial platform",
        "Apex BioLabs hiring spree: 25 research and data science openings",
    ],
    "Cobalt Financial": [
        "Cobalt Financial announces hiring freeze amid restructuring",
        "Cobalt Financial cuts 10% of back-office staff",
    ],
    "Summit Retail Group": [
        "Summit Retail Group expands into two new states ahead of holiday season",
        "Summit Retail seeks seasonal staffing partners for 30 locations",
    ],
    "Helios Energy": [
        "Helios Energy wins state grid contract, expects engineering team growth",
    ],
    "Brightpath Health": [
        "Brightpath Health acquires two clinics, integration timeline unclear",
    ],
    "Quartz Manufacturing": [
        "Quartz Manufacturing automates assembly line, no new hiring announced",
    ],
    "Lakeside Software": [
        "Lakeside Software lays off 15% of engineering after missed targets",
    ],
}

DEFAULT_COMPANIES: list[str] = list(_FIXTURE_SIGNALS)


def search_company_signals(company: str) -> list[str]:
    """Return recent web snippets about a company's hiring and growth.

    Uses Exa's search API when ``EXA_API_KEY`` is set. Without a key it
    falls back to bundled fixture snippets, so the flow stays runnable
    while you evaluate the durability mechanics.
    """
    api_key = os.environ.get("EXA_API_KEY")
    if not api_key:
        return _FIXTURE_SIGNALS.get(company, [f"No recent news found for {company}."])

    response = httpx.post(
        EXA_SEARCH_ENDPOINT,
        headers={"x-api-key": api_key},
        json={
            "query": f'"{company}" hiring OR expansion OR funding OR layoffs',
            "numResults": 5,
            "contents": {"text": {"maxCharacters": 500}},
        },
        timeout=30,
    )
    response.raise_for_status()
    return [
        f"{result.get('title', 'untitled')}: {result.get('text', '')}"
        for result in response.json().get("results", [])
    ]


# ---------------------------------------------------------------------------
# Agents — built by factories, not at module scope.
#
# Constructing ``Agent("openai:...")`` eagerly builds the OpenAI client, which
# reads ``OPENAI_API_KEY`` at that moment. On a remote stack the runner pod
# imports this module before the run's secret is applied to the environment, so
# a module-scope agent would crash at import with a missing-key error. Building
# the agent inside the checkpoint defers that to run time, once the secret is
# present. (Same pattern as the openai_research_bot example.) Each agent is
# wrapped in KitaruAgent so model requests and validation retries are tracked as
# child events under the enclosing checkpoint.
# ---------------------------------------------------------------------------


def new_qualifier() -> KitaruAgent:
    """Build the typed qualifier agent that classifies a company's fit."""
    return KitaruAgent(
        Agent(
            MODEL,
            name="prospect_qualifier",
            output_type=ProspectAssessment,
            instructions=(
                "You are a sales-intelligence analyst for a staffing agency. "
                "Given web snippets about a company (each may be from a "
                "different date), assess its fit for staffing outreach. "
                "hot = actively hiring or expanding, warm = growth signals but "
                "no explicit hiring, cold = freezes, layoffs, or no signals. "
                "Quote concrete signals from the snippets; do not invent any. "
                "Weigh recent signals over older ones, and flag in your summary "
                "when a hiring signal predates more recent layoffs or closures."
            ),
        )
    )


def new_outreach_writer() -> KitaruAgent:
    """Build the agent that drafts a short outreach email."""
    return KitaruAgent(
        Agent(
            MODEL,
            name="outreach_writer",
            output_type=str,
            instructions=(
                "Write a short, specific outreach email (under 120 words) from a "
                "staffing agency to the given company. Reference the hiring "
                "signals provided. No subject line, no placeholders."
            ),
        )
    )


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(retries=2)
def research_prospect(company: str) -> ProspectAssessment:
    """Search the web for one company and qualify it.

    One checkpoint per company is the durability boundary of this flow: a
    completed company is never re-searched, whether the run crashes, is
    retried, or is replayed later with a different downstream candidate.
    """
    signals = search_company_signals(company)
    kitaru.log(company=company, signal_count=len(signals))
    prompt = (
        f"Today is {date.today().isoformat()}. Qualify {company} as a "
        f"staffing prospect based on these research snippets:\n- "
        + "\n- ".join(signals)
    )
    return new_qualifier().run_sync(prompt).output


@checkpoint
def build_shortlist(
    assessments: list[ProspectAssessment],
) -> Annotated[Shortlist, "shortlist"]:
    """Rank assessments hot-first and drop cold prospects."""
    order = {FitLevel.HOT: 0, FitLevel.WARM: 1, FitLevel.COLD: 2}
    ranked = sorted(assessments, key=lambda a: order[a.fit])
    keep = [a for a in ranked if a.fit is not FitLevel.COLD]
    kitaru.log(
        researched=len(assessments),
        shortlisted=len(keep),
        hot=sum(1 for a in keep if a.fit is FitLevel.HOT),
    )
    return Shortlist(prospects=keep)


@checkpoint(retries=2)
def draft_outreach(assessment: ProspectAssessment) -> str:
    """Draft one outreach email for an approved prospect."""
    prompt = (
        f"Company: {assessment.company}\n"
        f"Fit: {assessment.fit.value}\n"
        f"Hiring signals: {'; '.join(assessment.hiring_signals)}\n"
        f"Qualification summary: {assessment.summary}"
    )
    return new_outreach_writer().run_sync(prompt).output


@checkpoint
def publish_report(
    shortlist: Shortlist, drafts: list[str]
) -> Annotated[str, "outreach_report"]:
    """Assemble the final report from the per-prospect drafts.

    Besides producing a named artifact, this gives the fanned-out draft
    checkpoints a single sink, so ``.run().wait()`` has one unambiguous
    flow result to return.
    """
    report = "\n\n".join(
        f"--- {assessment.company} ({assessment.fit.value}) ---\n{draft}"
        for assessment, draft in zip(shortlist.prospects, drafts, strict=True)
    )
    return f"Drafted outreach for {len(drafts)} prospects:\n\n{report}"


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


def _crash_after_from_env() -> int | None:
    """Read the simulated-crash marker from the environment.

    This is an env var rather than a flow parameter on purpose: a retried
    execution reuses the original flow parameters, so a parameter would
    crash the retry too. The CLI process running ``kitaru executions retry``
    does not have the var set, so the retry sails past the crash point on
    cached checkpoints.
    """
    raw = os.environ.get("PROSPECT_SCOUT_CRASH_AFTER")
    return int(raw) if raw else None


@flow(image=PROSPECTOR_IMAGE)
def prospect_scout(companies: list[str]) -> str:
    """Research targets, get human approval, draft outreach for the rest."""
    crash_after = _crash_after_from_env()

    assessments: list[ProspectAssessment] = []
    for done, company in enumerate(companies, start=1):
        assessments.append(research_prospect(company))
        if crash_after is not None and done == crash_after:
            raise RuntimeError(
                f"Simulated crash after {done} of {len(companies)} companies. "
                "Run `kitaru executions retry <execution-id>` — the "
                f"{done} completed checkpoints will not re-run."
            )

    # Checkpoint calls return lazy output handles at flow scope. The handle
    # goes into publish_report as-is (keeping the checkpoint lineage in one
    # chain), while .load() materializes a copy here because the flow body
    # inspects it — to print the shortlist and branch on the human decision.
    shortlist_handle = build_shortlist(assessments)
    shortlist = shortlist_handle.load().prospects
    if not shortlist:
        return "No qualified prospects found; nothing to approve."

    lines = "\n".join(
        f"  [{a.fit.value.upper()}] {a.company} — {'; '.join(a.hiring_signals)}"
        for a in shortlist
    )
    exec_id = kitaru.current_execution_id()
    print(f"\nProposed shortlist:\n{lines}\n")
    print("To approve from another terminal, run:")
    print(f"  kitaru executions input {exec_id} --value true")
    print(f"  kitaru executions resume {exec_id}")
    print("(Use --value false to reject.)\n")

    approved = kitaru.wait(
        name="approve_shortlist",
        schema=bool,
        question=f"Approve outreach to these {len(shortlist)} prospects?",
        timeout=3600,  # Compute is released after 1 hour; resume via CLI later
        metadata={"shortlisted": len(shortlist)},
    )
    if approved is False:
        return f"Shortlist of {len(shortlist)} prospects rejected; no outreach sent."

    drafts = [draft_outreach(assessment) for assessment in shortlist]
    return publish_report(shortlist_handle, drafts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_companies(raw: str | None) -> list[str]:
    if raw is None:
        return DEFAULT_COMPANIES
    return [part.strip() for part in raw.split(",") if part.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Durable prospect-research sweep on Kitaru."
    )
    parser.add_argument(
        "--companies",
        type=str,
        default=None,
        help=(
            "Comma-separated target companies. Defaults to the bundled fixture list."
        ),
    )
    args = parser.parse_args(argv)

    result = prospect_scout.run(_parse_companies(args.companies)).wait()
    print(f"\n{result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
