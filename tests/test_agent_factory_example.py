"""Hermetic smoke tests for the agent_factory example.

Narrow safety net: catches import-surface drift on the kitaru side
(public symbol renames in `kitaru.adapters.pydantic_ai`, `Profile`
field renames, tool-name literal changes, etc.) by exercising the
import + Profile + tool-factory wiring path of every shipped stage.

Doesn't try to test what only the layer-B Docker + OpenAI integration
tests can: persistent shell, proxy credential injection, the actual
agent loop, HITL wait/resume. Deliberately ~80 lines, ~6 seconds in
CI — the cheap proxy for the layer-B suite at 1% of the runtime cost.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

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

# Tool sets each stage's profile is supposed to enable. When stages 7 / 8 land,
# add their entries here AND append the new filename to `_STAGE_FILES` above.
_EXPECTED_TOOLS_PER_STAGE: dict[str, set[str]] = {
    "stage_1_basic_agent.py": {"exec"},
    "stage_2_sandboxed_exec.py": {"exec"},
    "stage_3_skills.py": {"exec", "skill"},
    "stage_4_credential_proxy.py": {"exec", "skill"},
    "stage_5_typed_services.py": {"exec", "skill", "exec_service"},
    "stage_6_hitl.py": {"exec", "skill", "exec_service", "ask_question"},
}


@pytest.fixture(autouse=True)
def _example_on_syspath(monkeypatch: pytest.MonkeyPatch) -> None:
    """Put the example root on ``sys.path`` for ``agent_factory`` / ``mocks``.

    The example ships its own importable packages at the example root;
    pytest doesn't know about them by default. Use ``syspath_prepend`` so
    the path is added for the duration of the test only. Evict any
    partially-loaded modules between tests for a clean import each time.
    """
    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    evict_prefixes = ("agent_factory.", "mocks.", "_agent_factory_smoke.")
    for module_name in list(sys.modules):
        if module_name in {"agent_factory", "mocks"} or module_name.startswith(
            evict_prefixes
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)


def _load_stage(stage_filename: str) -> ModuleType:
    """Load a stage file by path under a namespaced ``sys.modules`` key."""
    module_key = f"_agent_factory_smoke.{stage_filename[:-3]}"
    spec = importlib.util.spec_from_file_location(
        module_key, _EXAMPLE_DIR / stage_filename
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("stage_filename", _STAGE_FILES)
def test_stage_imports_and_builds_tools(stage_filename: str) -> None:
    """Each stage imports, exposes a real flow, and builds the expected toolset.

    Catches public-symbol renames (anything imported at module scope from
    kitaru / agent_factory), Profile field drift, and any silent failure
    of `@kitaru.flow` to produce a flow object with `.run` / `.replay`.
    """
    from agent_factory.permissions import PermissionHandler
    from agent_factory.profile import Profile
    from agent_factory.tools import build_tools

    module = _load_stage(stage_filename)

    profile = module.DEFAULT_PROFILE
    assert isinstance(profile, Profile)
    assert profile.allowed_tools == _EXPECTED_TOOLS_PER_STAGE[stage_filename]

    flow = module.agent_factory_flow
    assert callable(getattr(flow, "run", None))
    assert callable(getattr(flow, "replay", None))

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


def test_publish_summary_args_validates_webhook_id() -> None:
    """`webhook_id` pattern blocks path-traversal and oversized values.

    Schema validation in general is Pydantic's job, not ours — this test
    exists specifically because the pattern was added in response to a
    security review finding (LLM-supplied path traversal into the URL).
    """
    from agent_factory.services import PublishSummaryArgs

    PublishSummaryArgs(webhook_id="ok-123", content="hi")  # accepts well-formed
    for bad in ("../snippets/durability", "x" * 65, "has spaces"):
        with pytest.raises(ValidationError):
            PublishSummaryArgs(webhook_id=bad, content="hi")
