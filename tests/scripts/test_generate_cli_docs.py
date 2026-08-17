"""Tests for the CLI documentation generator."""

import json
import re
from pathlib import Path
from typing import Any

import pytest
from scripts.generate_cli_docs import (
    CommandDoc,
    GroupDoc,
    ParameterDoc,
    SchemaError,
    _code_cell,
    _escape_mdx,
    _prose_cell,
    build_tree,
    collect_global_parameters,
    fetch_command_docs,
    parse_command,
    render_command_page,
    render_group_page,
    render_meta,
    render_root_page,
    run_schema_command,
    write_docs_tree,
)

FRONTMATTER = re.compile(r'\A---\ntitle: ".+"\ndescription: ".+"\n---\n')


def make_parameter(name: str, **overrides: Any) -> dict[str, Any]:
    """Build one schema parameter dict with defaults."""
    parameter = {
        "name": name,
        "type": "text",
        "kind": "option",
        "required": False,
        "description": f"Help for {name}.",
    }
    parameter.update(overrides)
    return parameter


GLOBAL_PARAMETER = make_parameter("--verbose", description="Global flag.")

SCHEMA_TOP_DESCRIPTIONS = {"box": "Manage boxes.", "ping": "Ping."}

SCHEMA_COMMANDS = [
    {
        "command": "box.get",
        "path": ["box", "get"],
        "description": "Get a box.",
        "parameters": [
            GLOBAL_PARAMETER,
            make_parameter("BOX", type="UUID", kind="argument", required=True),
        ],
    },
    {
        "command": "box.lid.open",
        "path": ["box", "lid", "open"],
        "description": "Open a lid with {braces} and <angles>.",
        "parameters": [
            GLOBAL_PARAMETER,
            make_parameter(
                "--mode",
                type="a|b",
                description="Pick <one> of {a, b}.",
            ),
        ],
    },
    {
        "command": "ping",
        "path": ["ping"],
        "description": "Ping.",
        "parameters": [GLOBAL_PARAMETER],
    },
]


@pytest.fixture
def tree() -> GroupDoc:
    """Build the fixture command tree once per test."""
    return build_tree(
        SCHEMA_TOP_DESCRIPTIONS, [parse_command(c) for c in SCHEMA_COMMANDS]
    )


class TestEscaping:
    def test_escapes_braces_and_angle_brackets(self) -> None:
        assert _escape_mdx("{x} <y>") == "\\{x\\} &lt;y&gt;"

    def test_prose_cell_escapes_pipes(self) -> None:
        assert _prose_cell("a|b {c}") == "a\\|b \\{c\\}"

    def test_code_cell_escapes_pipes_only(self) -> None:
        assert _code_cell("a|b {c}") == "`a\\|b {c}`"

    def test_code_cell_empty(self) -> None:
        assert _code_cell("") == ""


class TestParsing:
    def test_parse_command_builds_typed_model(self) -> None:
        command = parse_command(SCHEMA_COMMANDS[0])
        assert command.path == ("box", "get")
        assert command.description == "Get a box."
        assert command.parameters[1] == ParameterDoc(
            name="BOX",
            type_name="UUID",
            kind="argument",
            required=True,
            description="Help for BOX.",
        )

    def test_build_tree_nests_groups(self, tree: GroupDoc) -> None:
        assert [c.slug for c in tree.children] == ["box", "ping"]
        box = tree.groups[0]
        assert box.description == "Manage boxes."
        assert [c.slug for c in box.children] == ["get", "lid"]
        lid = box.groups[0]
        assert lid.path == ("box", "lid")
        assert [c.slug for c in lid.children] == ["open"]

    def test_leaf_that_prefixes_others_becomes_group_index(self) -> None:
        commands = [
            CommandDoc(path=("box",), description="Box itself."),
            CommandDoc(path=("box", "get"), description="Get a box."),
        ]
        root = build_tree({}, commands)
        assert root.commands == []
        box = root.groups[0]
        assert box.own_command is not None
        assert box.own_command.description == "Box itself."

    def test_collect_global_parameters(self) -> None:
        commands = [parse_command(c) for c in SCHEMA_COMMANDS]
        shared = collect_global_parameters(commands)
        assert [parameter.name for parameter in shared] == ["--verbose"]

    def test_collect_global_parameters_empty_input(self) -> None:
        assert collect_global_parameters([]) == []


class TestRendering:
    def test_command_page_has_frontmatter_and_usage(self) -> None:
        command = parse_command(SCHEMA_COMMANDS[0])
        page = render_command_page(command, [])
        assert FRONTMATTER.match(page)
        assert 'title: "kitaru box get"' in page
        assert "kitaru box get BOX [OPTIONS]" in page

    def test_command_page_escapes_mdx_hostile_text(self) -> None:
        command = parse_command(SCHEMA_COMMANDS[1])
        page = render_command_page(command, [])
        assert "Pick &lt;one&gt; of \\{a, b\\}." in page
        assert "`a\\|b`" in page
        assert "{a, b}" not in page

    def test_command_page_omits_global_parameters(self) -> None:
        commands = [parse_command(c) for c in SCHEMA_COMMANDS]
        shared = collect_global_parameters(commands)
        page = render_command_page(commands[0], shared)
        assert "--verbose" not in page
        assert "global options" in page

    def test_group_page_links_children(self, tree: GroupDoc) -> None:
        page = render_group_page(tree.groups[0], [])
        assert "[`get`](/cli/box/get/)" in page
        assert "[`lid`](/cli/box/lid/)" in page
        assert "kitaru box COMMAND" in page

    def test_root_page_lists_global_options_and_children(self, tree: GroupDoc) -> None:
        commands = [parse_command(c) for c in SCHEMA_COMMANDS]
        page = render_root_page(tree, collect_global_parameters(commands))
        assert 'title: "CLI Reference"' in page
        assert "## Global options" in page
        assert "[`box`](/cli/box/)" in page
        assert "[`ping`](/cli/ping/)" in page

    def test_meta_excludes_index(self, tree: GroupDoc) -> None:
        # FumaDocs uses the folder itself as the index link; listing "index"
        # would create a duplicate sidebar entry.
        meta = render_meta("CLI Reference", tree.children)
        assert meta == {"title": "CLI Reference", "pages": ["box", "ping"]}


class TestWriteDocsTree:
    def test_writes_expected_layout(self, tree: GroupDoc, tmp_path: Path) -> None:
        created = write_docs_tree(tree, [], tmp_path)
        assert set(created) == {
            "index.mdx",
            "meta.json",
            "ping.mdx",
            "box/index.mdx",
            "box/meta.json",
            "box/get.mdx",
            "box/lid/index.mdx",
            "box/lid/meta.json",
            "box/lid/open.mdx",
        }
        for name in created:
            if name.endswith(".mdx"):
                assert FRONTMATTER.match((tmp_path / name).read_text()), name


V1_COMMAND_NAMES = {"flow", "executions", "stack", "model", "secrets", "log-store"}


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate the full docs tree from the real CLI once for all e2e tests."""
    top_descriptions, commands = fetch_command_docs()
    tree = build_tree(top_descriptions, commands)
    output = tmp_path_factory.mktemp("cli-docs")
    write_docs_tree(tree, collect_global_parameters(commands), output)
    return output


class TestRealCliSchema:
    """End-to-end tests against the installed CLI's schema output."""

    def test_schema_error_on_unknown_path(self) -> None:
        with pytest.raises(SchemaError):
            run_schema_command(["definitely-not-a-command"])

    def test_index_and_meta_exist(self, generated: Path) -> None:
        assert (generated / "index.mdx").is_file()
        assert (generated / "meta.json").is_file()

    def test_no_v1_command_names(self, generated: Path) -> None:
        slugs = {
            path.relative_to(generated).parts[0].removesuffix(".mdx")
            for path in generated.rglob("*.mdx")
        }
        assert slugs.isdisjoint(V1_COMMAND_NAMES)

    def test_every_page_has_frontmatter(self, generated: Path) -> None:
        pages = list(generated.rglob("*.mdx"))
        assert len(pages) > 20
        for page in pages:
            assert FRONTMATTER.match(page.read_text()), str(page)

    def test_no_meta_lists_index(self, generated: Path) -> None:
        for meta_path in generated.rglob("meta.json"):
            meta = json.loads(meta_path.read_text())
            assert "index" not in meta["pages"], str(meta_path)
