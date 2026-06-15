"""Deterministic tests for the prospect_scout example.

We don't run the durable flow end to end here — that needs a server and a
human-input resolution for the ``kitaru.wait()`` approval gate. These tests
cover the example-specific wiring instead: that the module imports cleanly,
that the qualifier agent is typed to the ``FitLevel`` enum, that web search
falls back to bundled fixtures without an Exa key, that the shortlist
ranking drops cold prospects and orders hot-first, and that the CLI passes
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

    ``test`` resolves to PydanticAI's deterministic ``TestModel``, so the agent
    factories build without a provider key. Any ambient ``EXA_API_KEY`` is
    cleared so search stays on fixtures.
    """
    monkeypatch.setenv("PROSPECT_SCOUT_MODEL", "test")
    monkeypatch.delenv("EXA_API_KEY", raising=False)
    monkeypatch.delenv("PROSPECT_SCOUT_CRASH_AFTER", raising=False)

    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    monkeypatch.delitem(sys.modules, "prospector", raising=False)

    return _load_prospector_from_path()


def test_module_imports_and_wires_typed_agents(prospector_module: Any) -> None:
    """The agent factories build typed KitaruAgents, and the flow is defined."""
    from kitaru.adapters.pydantic_ai import KitaruAgent

    qualifier = prospector_module.new_qualifier()
    outreach_writer = prospector_module.new_outreach_writer()
    assert isinstance(qualifier, KitaruAgent)
    assert isinstance(outreach_writer, KitaruAgent)
    assert qualifier.name == "prospect_qualifier"

    # The qualifier is constrained to the typed assessment so misclassified
    # or free-text answers fail validation instead of leaking downstream.
    assert qualifier.output_type is prospector_module.ProspectAssessment

    assert prospector_module.prospect_scout is not None


def test_module_imports_without_provider_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing the module must not need a provider key.

    The remote runner pod imports this module before the run's secret is
    applied to the environment. If an agent were built at module scope, the
    eager OpenAI client construction would crash the import with a missing-key
    error. Agents are built inside checkpoints instead, so import must succeed
    with the default model and no key present.
    """
    monkeypatch.delenv("PROSPECT_SCOUT_MODEL", raising=False)  # default gpt-5-nano
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)

    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    monkeypatch.delitem(sys.modules, "prospector", raising=False)

    module = _load_prospector_from_path()
    assert module.MODEL == "openai:gpt-5-nano"
    assert module.prospect_scout is not None


def test_image_requires_adapter_compatible_pydantic_ai(
    prospector_module: Any,
) -> None:
    """PROSPECTOR_IMAGE must install a pydantic-ai the adapter can import.

    The remote image installs bare ``kitaru``, whose pydantic-ai constraint
    sits behind an optional extra, so the example pins pydantic-ai itself.
    Kitaru's bundled PydanticAI adapter imports names added in pydantic-ai
    1.89; a floor below that crashes the pod at import time before any
    checkpoint runs.
    """
    requirements = prospector_module.PROSPECTOR_IMAGE.requirements or []
    assert any("pydantic-ai-slim[openai]" in req for req in requirements)
    assert any(">=1.89" in req for req in requirements)


@pytest.mark.parametrize(
    ("raw", "expected_is_default"),
    [
        (None, True),
        ("Acme, Initech", False),
        (" , ", False),
    ],
)
def test_parse_companies(
    prospector_module: Any, raw: str | None, expected_is_default: bool
) -> None:
    """A plain run uses the fixture list; CLI input is comma-split + trimmed."""
    parsed = prospector_module._parse_companies(raw)
    if expected_is_default:
        assert parsed == prospector_module.DEFAULT_COMPANIES
    elif raw is not None and raw.strip(" ,"):
        assert parsed == ["Acme", "Initech"]
    else:
        assert parsed == []


def test_search_falls_back_to_fixtures_without_exa_key(
    prospector_module: Any,
) -> None:
    """Without EXA_API_KEY, known companies return their bundled snippets."""
    known = next(iter(prospector_module._FIXTURE_SIGNALS))
    assert (
        prospector_module.search_company_signals(known)
        == prospector_module._FIXTURE_SIGNALS[known]
    )

    # An unknown company still returns a usable, non-empty signal list.
    unknown = prospector_module.search_company_signals("Nonexistent Corp")
    assert len(unknown) == 1
    assert "Nonexistent Corp" in unknown[0]


def test_build_shortlist_drops_cold_and_ranks_hot_first(
    prospector_module: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shortlist keeps warm/hot prospects, hot-first, and drops cold."""
    assessment = prospector_module.ProspectAssessment
    fit = prospector_module.FitLevel
    assessments = [
        assessment(company="Warm Co", fit=fit.WARM, hiring_signals=[], summary="w"),
        assessment(company="Cold Co", fit=fit.COLD, hiring_signals=[], summary="c"),
        assessment(company="Hot Co", fit=fit.HOT, hiring_signals=[], summary="h"),
    ]

    # The checkpoint body logs metadata, which only works inside a flow; stub
    # it so the pure ranking logic can be called directly.
    monkeypatch.setattr(prospector_module.kitaru, "log", lambda **_: None)

    # Call the undecorated checkpoint body — pure ranking logic, no flow.
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
