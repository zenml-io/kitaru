import pytest

from scripts import probe_plugin_artifacts as probe


@pytest.mark.parametrize("extra", ["", "[openai]"])
async def test_probe_checks_both_analyzers_for_one_distribution(
    monkeypatch: pytest.MonkeyPatch, extra: str
) -> None:
    definitions = tuple(
        definition
        for definition in probe.DEFAULT_PLUGIN_DEFINITIONS
        if definition.name
        in {
            "kitaru/post-import-insights",
            "kitaru/openai-post-import-insights",
        }
    )
    definitions = tuple(
        definition.model_copy(
            update={
                "requirement": (
                    "kitaru-post-import-insights[openai]==0.1.0"
                    if "openai" in definition.name
                    else "kitaru-post-import-insights==0.1.0"
                )
            },
        )
        for definition in definitions
    )
    loaded: list[str] = []
    distributions: list[str] = []

    def installed_version(name: str) -> str:
        distributions.append(name)
        return "0.1.0"

    monkeypatch.setattr(probe, "DEFAULT_PLUGIN_DEFINITIONS", definitions)
    monkeypatch.setattr(probe.importlib.metadata, "version", installed_version)
    monkeypatch.setattr(
        probe, "load_source_ref", lambda entrypoint, kind: loaded.append(entrypoint)
    )

    await probe._probe({f"kitaru-post-import-insights{extra}==0.1.0"}, set())

    assert distributions == ["kitaru-post-import-insights"]
    assert len(loaded) == 2
    assert set(loaded) == {definition.entrypoint for definition in definitions}
