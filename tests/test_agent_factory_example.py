"""Hermetic smoke tests for the agent_factory example.

These tests exercise the *building blocks* of the example
(``examples/end_to_end/agent_factory``) without running Docker, hitting
OpenAI, or executing any kitaru flow end-to-end. They run as part of
the regular ``just test`` suite so a regression in the example shows up
immediately in CI.

What's deliberately NOT covered:

- Stage flows are never ``.run()``. Stages 2+ require Docker and the
  ``DockerSandbox`` / ``DockerProxy`` / ``DockerMockServices`` runners
  shell out to ``docker``; they're imported here but never entered as
  context managers.
- Stage 6's HITL flow (``ask_question`` + ``kitaru.wait()``) is only
  smoke-imported. The runtime wait choreography lives in the dedicated
  ``test_phase15_*`` and adapter tests.
- A "stage 1 with TestModel" end-to-end test is intentionally skipped:
  ``flow.run()`` requires a properly-initialized source root and the
  example's profile uses a hardcoded model string, so wiring deterministic
  responses through is heavier than the value justifies for a smoke test.

NOTE for future maintainers: when stages 7 / 8 ship, append the new
filenames to ``_STAGE_FILES`` and add their expected tool sets to
``_EXPECTED_TOOLS_PER_STAGE``. Both constants are intentionally
hardcoded for grep-ability.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest
from pydantic import ValidationError

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "end_to_end" / "agent_factory"
)

_STAGE_FILES = (
    "stage_1_basic_agent.py",
    "stage_2_sandboxed_exec.py",
    "stage_3_skills.py",
    "stage_4_credential_proxy.py",
    "stage_5_typed_services.py",
    "stage_6_hitl.py",
)

# Modules the autouse fixture should evict from `sys.modules` between
# tests so each test starts from a clean import. Using exact membership
# (rather than `startswith("stage_")` / `startswith("mocks")`) avoids
# accidentally evicting unrelated future modules — e.g. anything kitaru
# might one day ship under those generic names.
_STAGE_MODULE_NAMES = {f"_agent_factory_smoke.{f[:-3]}" for f in _STAGE_FILES}


@pytest.fixture(autouse=True)
def _example_on_syspath(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the example root on ``sys.path`` for ``agent_factory`` / ``mocks``.

    The example ships its own importable packages (``agent_factory``,
    ``mocks``) sitting at the example root; pytest doesn't know about
    them by default. Use ``syspath_prepend`` so the path is added for
    the duration of the test only.

    Defensive check: ``mocks`` is a generic name; if anything else has
    already imported a same-named module, refuse to silently shadow it.
    """
    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    # If a `mocks` module was loaded from somewhere other than this
    # example, bail loudly instead of letting our syspath_prepend silently
    # shadow it for the rest of the test run.
    existing_mocks = sys.modules.get("mocks")
    if existing_mocks is not None and existing_mocks.__file__ is not None:
        existing_path = Path(existing_mocks.__file__).resolve()
        if not str(existing_path).startswith(str(_EXAMPLE_DIR)):
            pytest.fail(
                f"`mocks` is already loaded from {existing_path}; the "
                "agent_factory smoke tests would shadow it. Rename the "
                "conflicting module or this example's `mocks` package."
            )
    # Evict any partially-loaded modules from a previous test, so each
    # test starts from a clean import. Exact membership only — see the
    # _STAGE_MODULE_NAMES comment.
    to_evict = {"agent_factory", "mocks"}
    for module_name in list(sys.modules):
        if (
            module_name in to_evict
            or module_name.startswith(("agent_factory.", "mocks."))
            or module_name in _STAGE_MODULE_NAMES
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)


def _load_stage(stage_filename: str) -> ModuleType:
    """Load a stage file by path under a namespaced sys.modules key.

    The bare filename stem (e.g. ``stage_1_basic_agent``) would collide
    across xdist workers if two parameter cases ran on the same worker
    back-to-back. Namespacing under ``_agent_factory_smoke.<stem>`` makes
    the key unique to this test module.
    """
    module_key = f"_agent_factory_smoke.{stage_filename[:-3]}"
    spec = importlib.util.spec_from_file_location(
        module_key, _EXAMPLE_DIR / stage_filename
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)
    return module


# === Stage file smoke imports ================================================


@pytest.mark.parametrize("stage_filename", _STAGE_FILES)
def test_stage_file_imports_cleanly(stage_filename: str) -> None:
    """Each stage exposes a real ``@kitaru.flow``-decorated ``agent_factory_flow``."""
    from agent_factory.profile import Profile

    module = _load_stage(stage_filename)

    assert hasattr(module, "agent_factory_flow"), (
        f"{stage_filename} must expose `agent_factory_flow`"
    )
    assert hasattr(module, "DEFAULT_PROFILE"), (
        f"{stage_filename} must expose `DEFAULT_PROFILE`"
    )
    assert isinstance(module.DEFAULT_PROFILE, Profile)
    # Catches a regression where `@kitaru.flow` silently stops decorating.
    assert callable(getattr(module.agent_factory_flow, "run", None)), (
        f"{stage_filename}'s flow object must expose `.run`"
    )
    assert callable(getattr(module.agent_factory_flow, "replay", None)), (
        f"{stage_filename}'s flow object must expose `.replay`"
    )


# === Profile shape per stage =================================================


_EXPECTED_TOOLS_PER_STAGE: dict[str, set[str]] = {
    # When stages 7 / 8 land, add their entries below — `_STAGE_FILES`
    # above also needs the new filename for the parametrize sweep.
    "stage_1_basic_agent.py": {"exec"},
    "stage_2_sandboxed_exec.py": {"exec"},
    "stage_3_skills.py": {"exec", "skill"},
    "stage_4_credential_proxy.py": {"exec", "skill"},
    "stage_5_typed_services.py": {"exec", "skill", "exec_service"},
    "stage_6_hitl.py": {"exec", "skill", "exec_service", "ask_question"},
}

_EXPECTED_SERVICES_PER_STAGE: dict[str, set[str]] = {
    # Stages 1-4 don't enable `exec_service`; they default to an empty set.
    "stage_1_basic_agent.py": set(),
    "stage_2_sandboxed_exec.py": set(),
    "stage_3_skills.py": set(),
    "stage_4_credential_proxy.py": set(),
    "stage_5_typed_services.py": {"lookup_wiki", "publish_summary"},
    "stage_6_hitl.py": {"lookup_wiki", "publish_summary"},
}


@pytest.mark.parametrize("stage_filename", _STAGE_FILES)
def test_stage_profile_allowed_tools_match_built_toolset(stage_filename: str) -> None:
    """``allowed_tools`` is a non-empty set; ``build_tools`` reflects it."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import build_tools

    module = _load_stage(stage_filename)
    profile = module.DEFAULT_PROFILE

    assert isinstance(profile.allowed_tools, set)
    assert profile.allowed_tools, "every stage must enable at least one tool"
    assert profile.allowed_tools == _EXPECTED_TOOLS_PER_STAGE[stage_filename]
    assert profile.allowed_services == _EXPECTED_SERVICES_PER_STAGE[stage_filename]

    skills_directory = (
        profile.skill_source.resolve() if profile.skill_source is not None else None
    )
    tools = build_tools(
        PermissionHandler(profile),
        sandbox=None,
        skills_directory=skills_directory,
        allowed_services=profile.allowed_services,
    )
    assert {t.name for t in tools} == profile.allowed_tools


@pytest.mark.parametrize("stage_filename", _STAGE_FILES)
def test_build_agent_constructs_pydantic_ai_agent(stage_filename: str) -> None:
    """``build_agent`` wires the profile into a vanilla pydantic-ai ``Agent``.

    Catches regressions in the Profile → tool-factory → Agent path that
    a pure import test would miss — e.g. a signature break on
    ``build_tools`` or a renamed Profile field that the agent factory
    relies on. We pass ``sandbox=None`` so no Docker is touched.

    Skipped when the `openai` package isn't installed: pydantic-ai's
    ``Agent('openai:...')`` constructor lazy-imports the openai sub-module
    and the example uses a hardcoded ``openai:`` provider string. The
    test is for the wiring shape, not the model client; substituting a
    non-openai model would mean we'd no longer be exercising the actual
    stage profile.
    """
    pytest.importorskip("pydantic_ai")
    # pydantic-ai's `openai:...` provider lazy-imports `pydantic_ai.models.openai`,
    # which requires `openai>=2`. The dev environment may pin `openai<2` for
    # other reasons; skip cleanly when the backend isn't loadable.
    try:
        import pydantic_ai.models.openai  # noqa: F401
    except ImportError:
        pytest.skip("pydantic_ai's OpenAI backend isn't loadable here (needs openai>=2)")

    from pydantic_ai import Agent

    from agent_factory.agent import build_agent

    module = _load_stage(stage_filename)
    agent = build_agent(module.DEFAULT_PROFILE, sandbox=None)
    assert isinstance(agent, Agent)


# === Tool building unit tests ================================================


def _profile(allowed: set[str], **extra: object) -> "object":
    """Build a minimal Profile for tool-factory unit tests.

    Returns ``object`` because importing ``Profile`` at module scope
    would force evaluation of the example tree before the autouse
    syspath fixture has run.
    """
    from agent_factory.profile import Profile

    return Profile(
        name="t",
        system_prompt="",
        model="openai:gpt-test",
        allowed_tools=allowed,
        **extra,
    )


@pytest.mark.skipif(
    sys.platform == "win32", reason="`printf` is not a Windows builtin"
)
def test_exec_tool_runs_subprocess_in_host_process() -> None:
    """``exec`` (no sandbox) routes through subprocess + returns ``ExecResult``."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import ExecResult, build_tools

    tools = build_tools(PermissionHandler(_profile({"exec"})))
    assert [t.name for t in tools] == ["exec"]
    result = tools[0].function("printf hello")
    assert isinstance(result, ExecResult)
    assert result.exit_code == 0
    assert result.stdout == "hello"
    assert result.stderr == ""


def test_skill_tool_requires_skill_source() -> None:
    """Profiles allowing ``skill`` without a ``skill_source`` raise at build time."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import build_tools

    profile = _profile({"skill"})
    with pytest.raises(ValueError, match="no skill_source was configured"):
        build_tools(PermissionHandler(profile))


def test_skill_tool_lists_files_and_rejects_path_traversal(tmp_path: Path) -> None:
    """``skill`` lists SKILL.md files and refuses paths that escape the root."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import build_tools

    skill_a = tmp_path / "agent_a"
    skill_b = tmp_path / "nested" / "agent_b"
    skill_a.mkdir(parents=True)
    skill_b.mkdir(parents=True)
    (skill_a / "SKILL.md").write_text("# agent a\n", encoding="utf-8")
    (skill_b / "SKILL.md").write_text("# agent b\n", encoding="utf-8")

    tools = build_tools(
        PermissionHandler(_profile({"skill"})),
        skills_directory=tmp_path,
    )
    skill_tool = next(t for t in tools if t.name == "skill")

    listing = skill_tool.function(action="list")
    assert listing["count"] == 2
    paths = sorted(item["path"] for item in listing["items"])
    assert paths == ["agent_a/SKILL.md", "nested/agent_b/SKILL.md"]

    with pytest.raises(ValueError, match="path must stay within"):
        skill_tool.function(action="read", path="../etc/passwd")


def test_exec_service_rejects_unknown_service_in_profile() -> None:
    """Profile-level unknown ``allowed_services`` must fail at ``build_tools`` time."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import build_tools

    profile = _profile({"exec_service"}, allowed_services={"nope"})
    with pytest.raises(ValueError, match="unknown service names"):
        build_tools(
            PermissionHandler(profile), allowed_services=profile.allowed_services
        )


def test_ask_question_tool_builds_without_a_flow() -> None:
    """``ask_question`` is wired to ``wait_for_input``; building doesn't suspend.

    The closure body calls ``wait_for_input`` only when invoked at
    runtime inside a flow. Smoke-importing each stage already covers
    the import-surface contract for ``wait_for_input`` (a rename
    upstream would break every stage import); this test verifies the
    Tool object is wired in when the profile asks for it.
    """
    from agent_factory.permissions import PermissionHandler
    from agent_factory.tools import build_tools

    tools = build_tools(PermissionHandler(_profile({"ask_question"})))
    assert [t.name for t in tools] == ["ask_question"]


# === Service registry + dynamic description ==================================


def test_build_service_description_renders_args_with_plain_type_names() -> None:
    """Description includes both services + arg names; type names render plainly."""
    from agent_factory.services import build_service_description

    description = build_service_description({"lookup_wiki", "publish_summary"})
    assert "lookup_wiki" in description
    assert "publish_summary" in description
    assert "topic" in description
    assert "webhook_id" in description
    assert "content" in description
    # Plain `str` (the type) renders via __name__, not `<class 'str'>`.
    assert "<class" not in description


def test_build_service_description_empty_set_returns_placeholder() -> None:
    from agent_factory.services import build_service_description

    description = build_service_description(set())
    assert description == "No services are currently enabled for this agent."


# === Schema validation =======================================================


def test_publish_summary_args_accepts_well_formed_input() -> None:
    from agent_factory.services import PublishSummaryArgs

    args = PublishSummaryArgs(webhook_id="ok-123", content="hi")
    assert args.webhook_id == "ok-123"
    assert args.content == "hi"


@pytest.mark.parametrize(
    "webhook_id",
    [
        "../snippets/durability",  # path traversal
        "x" * 65,  # over 64 chars
        "has spaces",  # disallowed character
    ],
)
def test_publish_summary_args_rejects_invalid_webhook_id(webhook_id: str) -> None:
    from agent_factory.services import PublishSummaryArgs

    with pytest.raises(ValidationError):
        PublishSummaryArgs(webhook_id=webhook_id, content="hi")


def test_lookup_wiki_args_accepts_topic_string() -> None:
    from agent_factory.services import LookupWikiArgs

    assert LookupWikiArgs(topic="durability").topic == "durability"


# === exec_service dispatch with mocked services ==============================


def test_exec_service_dispatch_with_mocked_lookup_wiki() -> None:
    """Build ``exec_service`` against a registry-patched ``lookup_wiki`` handler."""
    from agent_factory.permissions import PermissionHandler
    from agent_factory.services.registry import ALL_SERVICES, ServiceCall
    from agent_factory.services.schemas import (
        LookupWikiArgs,
        LookupWikiResult,
        WikiSnippet,
    )
    from agent_factory.tools import build_tools

    def fake_handler(args: LookupWikiArgs) -> LookupWikiResult:
        return LookupWikiResult(
            topic=f"canned-{args.topic}",
            snippets=[WikiSnippet(url="http://x", excerpt="hi")],
        )

    patched_call = ServiceCall(
        args_model=LookupWikiArgs,
        handler=fake_handler,
        summary="canned",
    )
    profile = _profile({"exec_service"}, allowed_services={"lookup_wiki"})

    with patch.dict(ALL_SERVICES, {"lookup_wiki": patched_call}):
        tools = build_tools(
            PermissionHandler(profile), allowed_services=profile.allowed_services
        )
        exec_service = next(t for t in tools if t.name == "exec_service")

        # Valid args → canned dict
        result = exec_service.function("lookup_wiki", {"topic": "durability"})
        assert result == {
            "topic": "canned-durability",
            "snippets": [{"url": "http://x", "excerpt": "hi"}],
        }

        # Invalid args → ValueError (translated from ValidationError)
        with pytest.raises(ValueError, match="Invalid args"):
            exec_service.function("lookup_wiki", {"wrong_field": 1})

        # Service not in this agent's allowed_services → ValueError
        with pytest.raises(ValueError, match="not in this agent's allowed_services"):
            exec_service.function(
                "publish_summary", {"webhook_id": "ok", "content": "x"}
            )
