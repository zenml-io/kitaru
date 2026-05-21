"""Hermetic smoke tests for the agent_harness_platform example.

Narrow safety net: catches import-surface drift on the kitaru side
(public symbol renames in `kitaru.adapters.pydantic_ai`, `Profile`
field renames, tool-name literal changes, etc.) by exercising the
import + Profile + tool-factory wiring path of every shipped stage.

Doesn't try to test what only the layer-B Docker + OpenAI integration
tests can: persistent shell, proxy credential injection, the actual
agent loop, HITL wait/resume. Deliberately ~80 lines, ~6 seconds in
CI — the cheap proxy for the layer-B suite at 1% of the runtime cost.
"""

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest
from pydantic import ValidationError

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "end_to_end"
    / "agent_harness_platform"
)

_STAGE_FILES = (
    "stage_1_basic_agent.py",
    "stage_2_sandboxed_exec.py",
    "stage_3_skills.py",
    "stage_4_credential_proxy.py",
    "stage_5_typed_services.py",
    "stage_6_hitl.py",
)

# Tool sets each stage's profile is supposed to enable.
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
    """Put the example root on ``sys.path`` for ``agent_harness_platform`` / ``mocks``.

    The example ships its own importable packages at the example root;
    pytest doesn't know about them by default. Use ``syspath_prepend`` so
    the path is added for the duration of the test only. Evict any
    partially-loaded modules between tests for a clean import each time.
    """
    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    evict_prefixes = (
        "agent_harness_platform.",
        "mocks.",
        "_agent_harness_platform_smoke.",
    )
    for module_name in list(sys.modules):
        if module_name in {"agent_harness_platform", "mocks"} or module_name.startswith(
            evict_prefixes
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)


def _load_stage(stage_filename: str) -> ModuleType:
    """Load a stage file by path under a namespaced ``sys.modules`` key."""
    module_key = f"_agent_harness_platform_smoke.{stage_filename[:-3]}"
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
    kitaru / agent_harness_platform), Profile field drift, and any silent failure
    of `@kitaru.flow` to produce a flow object with `.run` / `.replay`.
    """
    profile_module = importlib.import_module("agent_harness_platform.profile")
    tools_module = importlib.import_module("agent_harness_platform.tools")
    Profile = profile_module.Profile
    build_tools = tools_module.build_tools

    module = _load_stage(stage_filename)

    profile = module.DEFAULT_PROFILE
    assert isinstance(profile, Profile)
    assert profile.allowed_tools == _EXPECTED_TOOLS_PER_STAGE[stage_filename]

    flow = module.agent_harness_platform_flow
    assert callable(getattr(flow, "run", None))
    assert callable(getattr(flow, "replay", None))

    skills_directory = (
        profile.skill_source.resolve() if profile.skill_source is not None else None
    )
    tools = build_tools(
        profile.allowed_tools,
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
    services_module = importlib.import_module("agent_harness_platform.services")
    PublishSummaryArgs = services_module.PublishSummaryArgs

    PublishSummaryArgs(webhook_id="ok-123", content="hi")  # accepts well-formed
    for bad in ("../snippets/durability", "x" * 65, "has spaces"):
        with pytest.raises(ValidationError):
            PublishSummaryArgs(webhook_id=bad, content="hi")


def test_profile_round_trips_through_pydantic_json() -> None:
    """Profile serializes via `model_dump()` and reloads identically.

    Profiles cross flow / checkpoint boundaries as artifacts (durable
    state), so a Profile that can't round-trip through pydantic's
    serializer would silently drop fields on replay.
    """
    profile_module = importlib.import_module("agent_harness_platform.profile")
    LocalSkillSource = profile_module.LocalSkillSource
    Profile = profile_module.Profile
    SandboxProxyRule = profile_module.SandboxProxyRule

    original = Profile(
        name="researcher",
        system_prompt="You are a research agent.",
        model="openai:gpt-4o-mini",
        allowed_tools={"exec", "skill", "exec_service", "ask_question"},
        skill_source=LocalSkillSource(path="/tmp/skills"),
        sandbox_proxy_rules=[
            SandboxProxyRule(
                name="wiki-auth",
                hosts=["wiki.local"],
                headers={"Authorization": "Bearer {{ wiki-token.value }}"},
            ),
        ],
        allowed_services={"lookup_wiki", "publish_summary"},
    )

    reloaded = Profile.model_validate(original.model_dump())
    assert reloaded == original


def test_build_credential_map_resolves_secret_templates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`build_credential_map` resolves `{{ name.key }}` templates via kitaru.secrets.

    Template resolution is a security-critical seam: a typo or missing
    secret key must surface clearly rather than silently injecting an
    empty / wrong header into outbound requests.
    """
    profile_module = importlib.import_module("agent_harness_platform.profile")
    secrets_module = importlib.import_module("agent_harness_platform.secrets")
    Profile = profile_module.Profile
    SandboxProxyRule = profile_module.SandboxProxyRule
    build_credential_map = secrets_module.build_credential_map

    import kitaru
    from kitaru.secrets import Secret

    fake_secrets = {
        "wiki-token": Secret(
            name="wiki-token",
            id="sec_1",
            values={"value": "FAKE_TOKEN_123"},
        ),
    }

    def fake_get_secret(name: str) -> Secret:
        return fake_secrets[name]

    monkeypatch.setattr(kitaru, "get_secret", fake_get_secret)

    profile = Profile(
        name="researcher",
        system_prompt="x",
        model="openai:gpt-4o-mini",
        allowed_tools={"exec"},
        sandbox_proxy_rules=[
            SandboxProxyRule(
                name="wiki-auth",
                hosts=["wiki.local"],
                headers={"Authorization": "Bearer {{ wiki-token.value }}"},
            ),
        ],
    )

    credential_map = build_credential_map(profile)
    assert credential_map == {
        "wiki.local": {"Authorization": "Bearer FAKE_TOKEN_123"},
    }

    # Failure path: a template referencing a missing secret key surfaces
    # a clear KeyError so the operator can fix the rule rather than
    # silently injecting an empty header.
    bad_profile = Profile(
        name="researcher",
        system_prompt="x",
        model="openai:gpt-4o-mini",
        allowed_tools={"exec"},
        sandbox_proxy_rules=[
            SandboxProxyRule(
                name="wiki-auth",
                hosts=["wiki.local"],
                headers={"Authorization": "Bearer {{ wiki-token.missing }}"},
            ),
        ],
    )
    with pytest.raises(KeyError, match="missing"):
        build_credential_map(bad_profile)


@pytest.mark.parametrize(
    ("allowed_tools", "expected_tool_names"),
    [
        ({"exec"}, {"exec"}),
        (set(), set()),
    ],
)
def test_build_tools_omits_disallowed_tools(
    allowed_tools: set[str], expected_tool_names: set[str]
) -> None:
    """`build_tools` only emits tools in `allowed_tools`.

    The cheapest defense against accidental tool exposure: if the
    profile didn't ask for it, the tool isn't in the agent's toolset.
    """
    tools_module = importlib.import_module("agent_harness_platform.tools")
    build_tools = tools_module.build_tools

    tools = build_tools(allowed_tools, sandbox=None)
    assert {t.name for t in tools} == expected_tool_names


def test_exec_service_dispatch_handler_must_return_basemodel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`exec_service` rejects handlers that return a non-BaseModel.

    Regression test for the registry-handler runtime guard. Without it,
    a misregistered handler would AttributeError on `.model_dump()`
    mid-LLM-turn; with it, the LLM sees a typed dispatch error and can
    reason about the failure.
    """
    services_module = importlib.import_module("agent_harness_platform.services")
    registry_module = importlib.import_module(
        "agent_harness_platform.services.registry"
    )
    tools_module = importlib.import_module("agent_harness_platform.tools")
    ALL_SERVICES = services_module.ALL_SERVICES
    ServiceCall = registry_module.ServiceCall
    build_tools = tools_module.build_tools
    from pydantic import BaseModel

    class _FakeArgs(BaseModel):
        topic: str

    def _bad_handler(args: BaseModel) -> dict[str, str]:
        # Intentionally returns a dict — the handler signature is typed
        # but Python does not enforce the return type at registration.
        return {"topic": "x", "snippets": "[]"}  # type: ignore[return-value]

    fake_call = ServiceCall(
        args_model=_FakeArgs,
        handler=_bad_handler,  # type: ignore[arg-type]
        summary="test-only",
    )
    monkeypatch.setitem(ALL_SERVICES, "fake_dict_service", fake_call)

    tools = build_tools(
        {"exec_service"},
        sandbox=None,
        allowed_services={"fake_dict_service"},
    )
    exec_service_tool = next(t for t in tools if t.name == "exec_service")

    with pytest.raises(TypeError, match="fake_dict_service"):
        exec_service_tool.function(
            service_name="fake_dict_service",
            args={"topic": "anything"},
        )
