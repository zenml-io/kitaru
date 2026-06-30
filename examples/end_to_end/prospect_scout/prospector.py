"""Prospect scout — a durable, agentic sales-prospecting sweep.

This models the kind of system a staffing or B2B sales team runs before
outreach: for each target company, an agent researches the web, classifies
the company against the team's lines of business, and judges whether it is
worth pursuing. A human approves the shortlist; only then is outreach drafted.

The agent is a real agent, not a workflow: it is given a company name and a
``search_web`` tool, and it *decides for itself* which searches to run
(hiring, funding, expansion, layoffs) and how many. Search uses Exa when
``EXA_API_KEY`` is set and bundled fixtures otherwise, so the whole thing
runs end to end without any accounts.

Why Kitaru shows up here:

* **Durability / cost** — each company is its own ``@checkpoint``. If the
  sweep crashes after researching 7 of 10 companies, ``kitaru executions
  retry`` makes those 7 return their cached results: no repeated Exa calls,
  no repeated model tokens. You pay once.
* **Agent observability** — the qualifier runs through ``KitaruAgent``, so
  every model request and ``search_web`` tool call the agent makes is tracked
  as a child event under that company's checkpoint. You can see what the
  agent searched for, not just what it concluded.
* **Type safety** — the agent must return a ``LineOfBusiness`` and a
  ``FitLevel`` enum. A misclassified or free-text answer fails validation and
  PydanticAI retries instead of letting bad data leak downstream.
* **Human-in-the-loop without idling** — ``kitaru.wait()`` releases compute
  while a human approves the shortlist, then resumes exactly where it paused.

Usage::

    python prospector.py                              # default target list
    python prospector.py --companies "Acme,Initech"   # choose targets

    # Durability demo — crash after 3 companies, then resume:
    PROSPECT_SCOUT_CRASH_AFTER=3 python prospector.py
    kitaru executions retry <execution-id>

Set ``OPENAI_API_KEY`` for the default model, or point
``PROSPECT_SCOUT_MODEL`` at another provider. No key handy?
``PROSPECT_SCOUT_MODEL=test`` runs the whole flow on PydanticAI's
deterministic ``TestModel`` so you can try the durability mechanics first.
"""

import argparse
import os
import sys
from enum import StrEnum
from typing import Annotated

import httpx
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

import kitaru
from kitaru import ImageSettings, checkpoint, flow
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

MODEL = os.environ.get("PROSPECT_SCOUT_MODEL", "openai:gpt-4o-mini")

EXA_SEARCH_ENDPOINT = "https://api.exa.ai/search"

# Match the Pydantic-AI range Kitaru's adapter supports — the adapter imports
# names (AgentNativeTool, DeferredToolResults) that only exist in >=1.89, so an
# older pin builds a remote image whose agent import fails. The `-slim` variant
# with an explicit provider extra keeps remote images small.
PROSPECTOR_IMAGE = ImageSettings(
    requirements=["pydantic-ai-slim[openai]>=1.102.0,<1.104"],
)

# ---------------------------------------------------------------------------
# Typed outputs — the agent must produce one of these enum values, so a
# misclassified or free-text answer fails validation and is retried by
# PydanticAI instead of leaking into downstream checkpoints. The
# line-of-business enum is the kind of classification a staffing team needs
# to route a prospect to the right desk.
# ---------------------------------------------------------------------------


class LineOfBusiness(StrEnum):
    FINANCE_ACCOUNTING = "finance_accounting"
    TECHNOLOGY = "technology"
    MARKETING_CREATIVE = "marketing_creative"
    LEGAL = "legal"
    ADMINISTRATIVE = "administrative"
    OTHER = "other"


class FitLevel(StrEnum):
    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


class ProspectAssessment(BaseModel):
    """One qualified prospect, produced by the qualifier agent."""

    company: str
    line_of_business: LineOfBusiness = Field(
        description="Primary staffing line of business this company fits."
    )
    fit: FitLevel
    hiring_signals: list[str] = Field(
        description="Concrete signals found while searching the web."
    )
    summary: str = Field(description="One-paragraph qualification rationale.")


class Shortlist(BaseModel):
    """Ranked prospects awaiting human approval."""

    prospects: list[ProspectAssessment]


class OutreachDraft(BaseModel):
    """A drafted outreach email tied to its company.

    Carrying the company (not just the email body) keeps every draft distinct
    as a persisted artifact, so the fan-out of drafts into ``publish_report``
    never collides even when two emails happen to be byte-identical.
    """

    company: str
    email: str


# ---------------------------------------------------------------------------
# Web search — exposed to the agent as a tool it chooses to call. Real Exa
# request when a key is present, bundled fixtures otherwise so the example
# runs end to end without an Exa account.
# ---------------------------------------------------------------------------

# Per-company, per-topic fixtures so the agent's different searches return
# genuinely different snippets, the way a real search engine would.
_FIXTURE_SIGNALS: dict[str, dict[str, list[str]]] = {
    "Northwind Logistics": {
        "hiring": [
            "Northwind Logistics posts 40 open roles across operations and finance",
            "Northwind hiring controllers and AP clerks for new regional hub",
        ],
        "expansion": [
            "Northwind Logistics opens third regional hub, plans 200 new hires",
        ],
    },
    "Apex BioLabs": {
        "funding": [
            "Apex BioLabs closes $80M Series C to scale its clinical trial platform",
        ],
        "hiring": [
            "Apex BioLabs hiring spree: 25 research, data, and accounting openings",
        ],
    },
    "Cobalt Financial": {
        "layoffs": [
            "Cobalt Financial announces hiring freeze amid restructuring",
            "Cobalt Financial cuts 10% of back-office staff",
        ],
    },
    "Summit Retail Group": {
        "expansion": [
            "Summit Retail Group expands into two new states ahead of the holidays",
        ],
        "hiring": [
            "Summit Retail seeks seasonal staffing partners for 30 locations",
        ],
    },
    "Helios Energy": {
        "hiring": [
            "Helios Energy wins state grid contract, expects engineering team growth",
        ],
    },
    "Brightpath Health": {
        "expansion": [
            "Brightpath Health acquires two clinics; integration timeline unclear",
        ],
    },
    "Quartz Manufacturing": {
        "general": [
            "Quartz Manufacturing automates an assembly line; no new hiring announced",
        ],
    },
    "Lakeside Software": {
        "layoffs": [
            "Lakeside Software lays off 15% of engineering after missed targets",
        ],
    },
}

DEFAULT_COMPANIES: list[str] = list(_FIXTURE_SIGNALS)


def _fixture_search(query: str) -> list[str]:
    """Return fixture snippets relevant to a free-text search query.

    Matches the company named in the query, then narrows to topics the query
    mentions (hiring, funding, expansion, layoffs) so distinct searches return
    distinct results — mimicking a real search engine closely enough to make
    the agent's tool-choice behavior meaningful offline.
    """
    lowered = query.lower()
    for company, topics in _FIXTURE_SIGNALS.items():
        if company.lower() not in lowered:
            continue
        matched = [
            snippet
            for topic, snippets in topics.items()
            if topic in lowered
            for snippet in snippets
        ]
        if matched:
            return matched
        # No topic keyword matched — return everything we have on the company.
        return [snippet for snippets in topics.values() for snippet in snippets]
    return [f"No recent news found for query: {query!r}."]


def _exa_search(query: str, api_key: str) -> list[str]:
    """Run a real Exa web search and return title/text snippets."""
    response = httpx.post(
        EXA_SEARCH_ENDPOINT,
        headers={"x-api-key": api_key},
        json={
            "query": query,
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


def search_web(ctx: RunContext[None], query: str) -> list[str]:
    """Search the web for recent news about a company.

    Call this with a focused query such as "Northwind Logistics hiring" or
    "Apex BioLabs funding". Call it more than once with different angles
    (hiring, funding, expansion, layoffs) to build a fuller picture before
    you classify the company.
    """
    # ctx is unused but marks this as a context-aware PydanticAI tool, which
    # keeps the signature stable if the example later threads run dependencies
    # (an Exa client, a per-run budget) through ctx.deps.
    del ctx
    api_key = os.environ.get("EXA_API_KEY")
    signals = _exa_search(query, api_key) if api_key else _fixture_search(query)
    kitaru.log(search_query=query, result_count=len(signals))
    return signals


# ---------------------------------------------------------------------------
# Agents — built by factories called *inside* the checkpoints, not at module
# scope. Constructing ``Agent("openai:...")`` reads ``OPENAI_API_KEY``
# immediately; on a remote stack the runner imports this module before the
# secret is applied, so a module-scope agent would crash at import. Building
# inside the checkpoint defers that until the secret (and search key) is
# present. The qualifier is wrapped in KitaruAgent so each model request and
# ``search_web`` tool call is tracked as a child event under the enclosing
# checkpoint; ``CapturePolicy(tool_capture="full")`` records the search
# arguments and results so you can see what the agent looked at.
# ---------------------------------------------------------------------------


def new_qualifier() -> KitaruAgent:
    """Build the research/qualification agent (it owns the search tool)."""
    return KitaruAgent(
        Agent(
            MODEL,
            name="prospect_qualifier",
            output_type=ProspectAssessment,
            tools=[search_web],
            instructions=(
                "You are a sales-intelligence analyst for a staffing agency. "
                "Given a company name, use the search_web tool to research it — "
                "run several targeted searches (hiring, funding, expansion, "
                "layoffs) before deciding. Then classify its primary staffing "
                "line of business and its fit for outreach: hot = actively "
                "hiring or expanding, warm = growth signals but no explicit "
                "hiring, cold = freezes, layoffs, or no signals. Quote concrete "
                "signals from the search results; do not invent any."
            ),
        ),
        capture=CapturePolicy(tool_capture="full"),
    )


def new_outreach_writer() -> KitaruAgent:
    """Build the outreach-email agent."""
    return KitaruAgent(
        Agent(
            MODEL,
            name="outreach_writer",
            output_type=str,
            instructions=(
                "Write a short, specific outreach email (under 120 words) from "
                "a staffing agency to the given company. Reference the hiring "
                "signals provided. No subject line, no placeholders."
            ),
        )
    )


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint(retries=2)
def research_prospect(company: str) -> ProspectAssessment:
    """Research one company with the agent and qualify it.

    One checkpoint per company is the durability boundary of this flow: a
    completed company is never re-researched, whether the run crashes, is
    retried, or is replayed later. Every search_web call the agent makes
    while researching this company is tracked under this checkpoint.
    """
    qualifier = new_qualifier()
    result = qualifier.run_sync(
        f"Research and qualify {company} as a staffing prospect."
    )
    assessment = result.output
    # The company we asked about is authoritative — the model shouldn't rename
    # it. This also keeps each assessment a distinct persisted artifact.
    assessment.company = company
    return assessment


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
def draft_outreach(assessment: ProspectAssessment) -> OutreachDraft:
    """Draft one outreach email for an approved prospect."""
    prompt = (
        f"Company: {assessment.company}\n"
        f"Line of business: {assessment.line_of_business.value}\n"
        f"Fit: {assessment.fit.value}\n"
        f"Hiring signals: {'; '.join(assessment.hiring_signals)}\n"
        f"Qualification summary: {assessment.summary}"
    )
    email = new_outreach_writer().run_sync(prompt).output
    return OutreachDraft(company=assessment.company, email=email)


@checkpoint
def publish_report(
    shortlist: Shortlist, drafts: list[OutreachDraft]
) -> Annotated[str, "outreach_report"]:
    """Assemble the final report from the per-prospect drafts.

    Besides producing a named artifact, this gives the fanned-out draft
    checkpoints a single sink, so ``.run().wait()`` has one unambiguous
    flow result to return.
    """
    report = "\n\n".join(
        f"--- {assessment.company} ({assessment.line_of_business.value}, "
        f"{assessment.fit.value}) ---\n{draft.email}"
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
                f"{done} researched companies will not be searched again."
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
        f"  [{a.fit.value.upper()}] {a.company} ({a.line_of_business.value}) — "
        f"{'; '.join(a.hiring_signals)}"
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
        description="Durable, agentic prospect-research sweep on Kitaru."
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
