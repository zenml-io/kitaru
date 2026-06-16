"""Deterministic tests for the prospect_scout example.

We don't run the durable flow end to end here — that needs a server and a
human-input resolution for the ``kitaru.wait()`` approval gate. These tests
cover the example-specific wiring: that the qualifier agent actually *calls*
its ``search_web`` tool (the thing that makes it an agent, not a workflow),
that outputs are enum-typed, that fixture-backed search narrows by topic,
that the shortlist ranking drops cold prospects, and that the CLI passes
parsed companies into the flow.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

pytest.importorskip("pydantic_ai")

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "end_to_end"
    / "prospect_scout"
)


def _load_prospector_from_path() -> ModuleType:
    """Load ``examples/end_to_end/prospect_scout/prospector.py`` by path."""
    spec = importlib.util.spec_from_file_location(
        "prospector", _EXAMPLE_DIR / "prospector.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["prospector"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def prospector_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Import the example with ``PROSPECT_SCOUT_MODEL=test``.

    ``test`` resolves to PydanticAI's deterministic ``TestModel``, so no
    provider key is needed. Agents are built lazily by factories, so import
    itself needs no key; the model choice is read when a factory runs. Any
    ambient ``EXA_API_KEY`` is cleared so search stays on fixtures.
    """
    monkeypatch.setenv("PROSPECT_SCOUT_MODEL", "test")
    monkeypatch.delenv("EXA_API_KEY", raising=False)
    monkeypatch.delenv("PROSPECT_SCOUT_CRASH_AFTER", raising=False)

    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    monkeypatch.delitem(sys.modules, "prospector", raising=False)

    return _load_prospector_from_path()


def test_factories_build_typed_agents(prospector_module: Any) -> None:
    """The factories return KitaruAgents and the flow is defined."""
    from kitaru.adapters.pydantic_ai import KitaruAgent

    qualifier = prospector_module.new_qualifier()
    writer = prospector_module.new_outreach_writer()
    assert isinstance(qualifier, KitaruAgent)
    assert isinstance(writer, KitaruAgent)
    assert qualifier.name == "prospect_qualifier"
    assert prospector_module.prospect_scout is not None


def test_qualifier_actually_calls_search_web(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard: the agent must invoke its search tool.

    The original version of this example pre-fetched search results and fed
    them into the prompt, so the model never chose anything — a workflow, not
    an agent. Run the agent's tool + output config under TestModel and assert
    it calls ``search_web``.
    """
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    # search_web logs metadata, which only works inside a flow; stub it.
    monkeypatch.setattr(prospector_module.kitaru, "log", lambda **_: None)

    agent = Agent(
        TestModel(),
        output_type=prospector_module.ProspectAssessment,
        tools=[prospector_module.search_web],
    )
    result = agent.run_sync("Research and qualify Apex BioLabs.")

    tool_calls = [
        part.tool_name
        for message in result.all_messages()
        for part in getattr(message, "parts", [])
        if type(part).__name__ == "ToolCallPart"
    ]
    assert "search_web" in tool_calls
    assert isinstance(result.output, prospector_module.ProspectAssessment)
    assert isinstance(result.output.line_of_business, prospector_module.LineOfBusiness)


def test_image_pins_adapter_compatible_pydantic_ai(prospector_module: Any) -> None:
    """PROSPECTOR_IMAGE pins a pydantic-ai the Kitaru adapter can import.

    The adapter imports names that only exist in pydantic-ai >=1.89, so a
    too-old pin (e.g. <1.80) builds a remote image whose agent import fails.
    """
    requirements = prospector_module.PROSPECTOR_IMAGE.requirements or []
    assert any("pydantic-ai-slim" in req for req in requirements)
    assert any(">=1.89" in req for req in requirements)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, "__default__"),
        ("Acme, Initech", ["Acme", "Initech"]),
        (" , ", []),
    ],
)
def test_parse_companies(
    prospector_module: Any, raw: str | None, expected: Any
) -> None:
    """A plain run uses the fixture list; CLI input is comma-split + trimmed."""
    parsed = prospector_module._parse_companies(raw)
    if expected == "__default__":
        assert parsed == prospector_module.DEFAULT_COMPANIES
    else:
        assert parsed == expected


def test_fixture_search_narrows_by_topic(prospector_module: Any) -> None:
    """Distinct queries return distinct snippets, like a real search engine."""
    funding = prospector_module._fixture_search("Apex BioLabs funding")
    hiring = prospector_module._fixture_search("Apex BioLabs hiring")
    assert funding != hiring
    assert any("Series C" in snippet for snippet in funding)

    # An unknown company still returns a usable, non-empty result.
    unknown = prospector_module._fixture_search("Nobody Inc hiring")
    assert len(unknown) == 1
    assert "Nobody Inc" in unknown[0]


def test_search_web_tool_falls_back_to_fixtures(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without EXA_API_KEY the tool returns fixture snippets."""
    monkeypatch.setattr(prospector_module.kitaru, "log", lambda **_: None)
    # ctx is unused by the tool body; None stands in for the run context.
    signals = prospector_module.search_web(None, "Apex BioLabs funding")
    assert any("Series C" in snippet for snippet in signals)


def test_build_shortlist_drops_cold_and_ranks_hot_first(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shortlist keeps warm/hot prospects, hot-first, and drops cold."""
    assessment = prospector_module.ProspectAssessment
    fit = prospector_module.FitLevel
    lob = prospector_module.LineOfBusiness
    assessments = [
        assessment(
            company="Warm Co",
            line_of_business=lob.TECHNOLOGY,
            fit=fit.WARM,
            hiring_signals=[],
            summary="w",
        ),
        assessment(
            company="Cold Co",
            line_of_business=lob.FINANCE_ACCOUNTING,
            fit=fit.COLD,
            hiring_signals=[],
            summary="c",
        ),
        assessment(
            company="Hot Co",
            line_of_business=lob.ADMINISTRATIVE,
            fit=fit.HOT,
            hiring_signals=[],
            summary="h",
        ),
    ]

    # The checkpoint body logs metadata, which only works inside a flow; stub
    # it so the pure ranking logic can be called directly.
    monkeypatch.setattr(prospector_module.kitaru, "log", lambda **_: None)
    shortlist = prospector_module.build_shortlist._func(assessments)

    companies = [p.company for p in shortlist.prospects]
    assert companies == ["Hot Co", "Warm Co"]
    assert "Cold Co" not in companies


def test_crash_after_reads_environment(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The simulated-crash marker comes from the environment, not a param."""
    assert prospector_module._crash_after_from_env() is None
    monkeypatch.setenv("PROSPECT_SCOUT_CRASH_AFTER", "3")
    assert prospector_module._crash_after_from_env() == 3


def test_main_passes_parsed_companies_into_the_flow(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The CLI runs the flow with the parsed company list and returns 0."""
    fake_flow = Mock()
    fake_flow.run.return_value.wait.return_value = "report"
    monkeypatch.setattr(prospector_module, "prospect_scout", fake_flow)

    assert prospector_module.main(["--companies", "Acme, Initech"]) == 0

    fake_flow.run.assert_called_once_with(["Acme", "Initech"])
    fake_flow.run.return_value.wait.assert_called_once_with()
