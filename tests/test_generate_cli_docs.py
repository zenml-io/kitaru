"""Tests for the CLI documentation generator."""

import importlib
import json
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest

from generate_cli_docs import (
    CommandDoc,
    ParameterDoc,
    build_command_tree,
    render_command_page,
    render_meta,
    write_docs_tree,
)


def _find_command(root: CommandDoc, *names: str) -> CommandDoc:
    """Return a command by following its subcommand path."""
    current = root
    for name in names:
        current = next(sub for sub in current.subcommands if sub.name == name)
    return current


@pytest.fixture
def output_dir() -> Generator[Path]:
    """Temporary directory for generated docs."""
    d = Path(tempfile.mkdtemp(prefix="test-cli-docs-"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


class TestBuildCommandTree:
    """Tests for cyclopts command tree extraction."""

    def test_extracts_root_command(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        assert tree.name == "kitaru"
        assert tree.invocation == "kitaru"
        assert tree.description

    def test_root_has_current_subcommands(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        assert [sub.name for sub in tree.subcommands] == [
            "executions",
            "info",
            "init",
            "log-store",
            "login",
            "logout",
            "memory",
            "model",
            "secrets",
            "stack",
            "status",
        ]

    def test_executions_tree_includes_logs_and_replay(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        executions = _find_command(tree, "executions")
        assert [sub.name for sub in executions.subcommands] == [
            "cancel",
            "get",
            "input",
            "list",
            "logs",
            "replay",
            "resume",
            "retry",
        ]

    def test_stack_tree_includes_create_and_delete(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        stack = _find_command(tree, "stack")
        assert [sub.name for sub in stack.subcommands] == [
            "create",
            "current",
            "delete",
            "list",
            "show",
            "use",
        ]

    def test_building_tree_does_not_resolve_version_metadata(self) -> None:
        """CLI docs introspection should not trigger version metadata lookup."""
        with patch(
            "kitaru._version.resolve_installed_version",
            side_effect=AssertionError("should not resolve version"),
        ):
            import kitaru.cli as cli_module

            reloaded = importlib.reload(cli_module)
            tree = build_command_tree(reloaded.app)

        assert tree.name == "kitaru"

    def test_handles_subcommands(self) -> None:
        import cyclopts

        app = cyclopts.App(name="test", help="Test app.")

        @app.command
        def foo() -> None:
            """Do foo."""

        tree = build_command_tree(app)
        assert len(tree.subcommands) == 1
        assert tree.subcommands[0].name == "foo"
        assert tree.subcommands[0].invocation == "test foo"

    def test_extracts_parameters(self) -> None:
        from typing import Annotated

        import cyclopts
        from cyclopts import Parameter

        app = cyclopts.App(name="test", help="Test app.")

        @app.command
        def serve(
            host: Annotated[str, Parameter(help="Bind address.")] = "127.0.0.1",
        ) -> None:
            """Start server."""

        tree = build_command_tree(app)
        sub = tree.subcommands[0]
        assert len(sub.parameters) == 1
        assert sub.parameters[0].help == "Bind address."
        assert sub.parameters[0].required is False

    def test_builds_usage_from_positional_parameters(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)

        login = _find_command(tree, "login")
        get = _find_command(tree, "executions", "get")
        secrets_set = _find_command(tree, "secrets", "set")
        stack_create = _find_command(tree, "stack", "create")
        stack_use = _find_command(tree, "stack", "use")

        assert login.usage == "kitaru login [SERVER] [OPTIONS]"
        assert login.parameters[0].names == ["SERVER"]

        assert get.usage.startswith("kitaru executions get EXEC_ID")
        assert get.parameters[0].names == ["EXEC_ID"]

        assert secrets_set.usage.startswith("kitaru secrets set NAME ASSIGNMENTS...")
        assert [parameter.names for parameter in secrets_set.parameters[:2]] == [
            ["NAME"],
            ["ASSIGNMENTS..."],
        ]

        assert stack_create.parameters[0].names == ["NAME"]
        assert all(
            "--name" not in parameter.names for parameter in stack_create.parameters
        )

        assert stack_use.usage.startswith("kitaru stack use STACK")
        assert stack_use.parameters[0].names == ["STACK"]


class TestRenderCommandPage:
    """Tests for MDX page rendering."""

    def test_root_page_has_frontmatter(self) -> None:
        cmd = CommandDoc(
            slug="kitaru",
            name="kitaru",
            invocation="kitaru",
            description="Test description.",
            usage="kitaru",
        )
        page = render_command_page(cmd, is_root=True)
        assert page.startswith("---\n")
        assert 'title: "CLI Reference"' in page
        assert 'description: "Test description."' in page

    def test_root_page_has_global_flags(self) -> None:
        cmd = CommandDoc(
            slug="kitaru",
            name="kitaru",
            invocation="kitaru",
            description="Test.",
            usage="kitaru",
        )
        page = render_command_page(cmd, is_root=True)
        assert "## Global Flags" in page
        assert "`--help`" in page
        assert "`--version`" in page

    def test_root_page_has_output_formats_section(self) -> None:
        cmd = CommandDoc(
            slug="kitaru",
            name="kitaru",
            invocation="kitaru",
            description="Test.",
            usage="kitaru",
        )
        page = render_command_page(cmd, is_root=True)
        assert "## Output formats" in page
        assert "`--output json`" in page
        assert "{command, item}" in page

    def test_subcommand_page_has_no_global_flags(self) -> None:
        cmd = CommandDoc(
            slug="serve",
            name="serve",
            invocation="kitaru serve",
            description="Start server.",
            usage="kitaru serve",
        )
        page = render_command_page(cmd, is_root=False)
        assert "## Global Flags" not in page

    def test_renders_parameters_table(self) -> None:
        cmd = CommandDoc(
            slug="serve",
            name="serve",
            invocation="kitaru serve",
            description="Start.",
            usage="kitaru serve [OPTIONS]",
            parameters=[
                ParameterDoc(
                    names=["--port", "-p"],
                    help="Port number.",
                    type_name="int",
                    required=False,
                    default="`8000`",
                    is_flag=False,
                ),
            ],
        )
        page = render_command_page(cmd, is_root=False)
        assert "## Parameters" in page
        assert "`--port`, `-p`" in page
        assert "Port number." in page

    def test_renders_subcommands_table(self) -> None:
        child = CommandDoc(
            slug="run",
            name="run",
            invocation="kitaru agent run",
            description="Run an agent.",
            usage="kitaru agent run",
        )
        cmd = CommandDoc(
            slug="agent",
            name="agent",
            invocation="kitaru agent",
            description="Manage agents.",
            usage="kitaru agent COMMAND",
            subcommands=[child],
        )
        page = render_command_page(cmd, is_root=False)
        assert "## Commands" in page
        assert "[`run`](./run)" in page

    def test_escapes_mdx_special_chars(self) -> None:
        cmd = CommandDoc(
            slug="test",
            name="test",
            invocation="test",
            description="Uses <angle> and {braces}.",
            usage="test",
        )
        page = render_command_page(cmd, is_root=False)
        assert "&lt;angle&gt;" in page
        assert "\\{braces\\}" in page


class TestWriteDocsTree:
    """Tests for filesystem output."""

    def test_creates_index_and_meta(self, output_dir: Path) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        files = write_docs_tree(tree, output_dir)

        assert (output_dir / "index.mdx").exists()
        assert (output_dir / "meta.json").exists()
        assert "index.mdx" in files
        assert "meta.json" in files

    def test_meta_json_is_valid(self, output_dir: Path) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        meta = json.loads((output_dir / "meta.json").read_text())
        assert meta["title"] == "CLI Reference"
        assert meta["pages"] == [
            "executions",
            "info",
            "init",
            "log-store",
            "login",
            "logout",
            "memory",
            "model",
            "secrets",
            "stack",
            "status",
        ]

    def test_nested_meta_includes_all_execution_commands(
        self, output_dir: Path
    ) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        meta = json.loads((output_dir / "executions" / "meta.json").read_text())
        assert meta["pages"] == [
            "cancel",
            "get",
            "input",
            "list",
            "logs",
            "replay",
            "resume",
            "retry",
        ]

    def test_frontmatter_present_in_generated_page(self, output_dir: Path) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        content = (output_dir / "index.mdx").read_text()
        assert content.startswith("---\n")
        assert "title:" in content
        assert "description:" in content

    def test_leaf_subcommands_generate_flat_files(self, output_dir: Path) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        files = write_docs_tree(tree, output_dir)

        for command in ("info", "init", "login", "logout", "status"):
            assert (output_dir / f"{command}.mdx").exists()
            assert f"{command}.mdx" in files
            # No directory or meta.json for leaf commands
            assert not (output_dir / command / "index.mdx").exists()
            assert not (output_dir / command / "meta.json").exists()

    def test_generated_executions_docs_include_logs_and_replay(
        self, output_dir: Path
    ) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        executions_meta = json.loads(
            (output_dir / "executions" / "meta.json").read_text()
        )
        assert "logs" in executions_meta["pages"]
        assert "replay" in executions_meta["pages"]
        assert (output_dir / "executions" / "logs.mdx").exists()
        assert (output_dir / "executions" / "replay.mdx").exists()

    def test_generated_command_page_includes_output_option(
        self, output_dir: Path
    ) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        files = write_docs_tree(tree, output_dir)

        status_page = (output_dir / "status.mdx").read_text()
        assert "`--output`, `-o`" in status_page

        # executions, log-store, memory, model, secrets, and stack all
        # have nested subcommands.
        for command in (
            "executions",
            "log-store",
            "memory",
            "model",
            "secrets",
            "stack",
        ):
            assert (output_dir / command / "index.mdx").exists()
            assert (output_dir / command / "meta.json").exists()

        for command in (
            "cancel",
            "get",
            "input",
            "list",
            "logs",
            "replay",
            "resume",
            "retry",
        ):
            assert (output_dir / "executions" / f"{command}.mdx").exists()
            assert f"executions/{command}.mdx" in files

        for command in ("set", "show", "reset"):
            assert (output_dir / "log-store" / f"{command}.mdx").exists()
            assert f"log-store/{command}.mdx" in files

        for command in ("delete", "get", "history", "list", "set"):
            assert (output_dir / "memory" / f"{command}.mdx").exists()
            assert f"memory/{command}.mdx" in files

        for command in ("list", "register"):
            assert (output_dir / "model" / f"{command}.mdx").exists()
            assert f"model/{command}.mdx" in files

        for command in ("delete", "list", "set", "show"):
            assert (output_dir / "secrets" / f"{command}.mdx").exists()
            assert f"secrets/{command}.mdx" in files

        memory_get_content = (output_dir / "memory" / "get.mdx").read_text()
        assert "kitaru memory get KEY [OPTIONS]" in memory_get_content
        assert "| `KEY` | `str` | Yes |  | Memory key to read. |" in memory_get_content

        secrets_set_content = (output_dir / "secrets" / "set.mdx").read_text()
        assert "--KEY=value" in secrets_set_content

        for command in ("create", "current", "delete", "list", "use"):
            assert (output_dir / "stack" / f"{command}.mdx").exists()
            assert f"stack/{command}.mdx" in files

    def test_generated_pages_render_positional_usage_and_aliases(
        self, output_dir: Path
    ) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        get_content = (output_dir / "executions" / "get.mdx").read_text()
        assert "kitaru executions get EXEC_ID" in get_content
        assert "| `EXEC_ID` | `str` | Yes |  | Execution ID. |" in get_content

        login_content = (output_dir / "login.mdx").read_text()
        assert "kitaru login [SERVER] [OPTIONS]" in login_content
        assert "| `SERVER` | `str` | No | `None` |" in login_content
        assert "`--url`" not in login_content
        assert "`--pro-api-url`" not in login_content
        assert "`--cloud-api-url`" not in login_content
        assert "`--port`" in login_content
        assert "`--timeout`" in login_content

        secrets_set_content = (output_dir / "secrets" / "set.mdx").read_text()
        assert "--KEY=value" in secrets_set_content
        assert "| `ASSIGNMENTS...` | `list[str]` | Yes |  |" in secrets_set_content

        stack_use_content = (output_dir / "stack" / "use.mdx").read_text()
        assert "kitaru stack use STACK" in stack_use_content
        assert "| `STACK` | `str` | Yes |  |" in stack_use_content

        stack_create_content = (output_dir / "stack" / "create.mdx").read_text()
        assert "`--extra`" in stack_create_content
        assert "`--async`" in stack_create_content

    def test_nested_subcommands_create_directories(self, output_dir: Path) -> None:
        import cyclopts

        app = cyclopts.App(name="kitaru", help="Test.")
        sub = cyclopts.App(name="agent", help="Manage agents.")
        app.command(sub)

        @sub.command
        def run() -> None:
            """Run agent."""

        tree = build_command_tree(app)
        write_docs_tree(tree, output_dir)

        # Parent with children remains a directory
        assert (output_dir / "agent" / "index.mdx").exists()
        assert (output_dir / "agent" / "meta.json").exists()
        # Leaf child becomes a flat file, not a nested directory
        assert (output_dir / "agent" / "run.mdx").exists()
        assert not (output_dir / "agent" / "run" / "index.mdx").exists()


class TestRenderMeta:
    """Tests for meta.json rendering."""

    def test_empty_children_produces_empty_pages(self) -> None:
        meta = render_meta("CLI Reference", [])
        assert meta["pages"] == []

    def test_includes_child_slugs(self) -> None:
        children = [
            CommandDoc(
                slug="serve",
                name="serve",
                invocation="kitaru serve",
                description="",
                usage="",
            ),
            CommandDoc(
                slug="agent",
                name="agent",
                invocation="kitaru agent",
                description="",
                usage="",
            ),
        ]
        meta = render_meta("CLI Reference", children)
        assert meta["pages"] == ["serve", "agent"]
