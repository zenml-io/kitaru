"""Tests for the CLI documentation generator."""

import json
import shutil
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from generate_cli_docs import (
    CommandDoc,
    ParameterDoc,
    build_command_tree,
    render_command_page,
    render_meta,
    write_docs_tree,
)


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
            "info",
            "log-store",
            "login",
            "logout",
            "status",
        ]

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
            "index",
            "info",
            "log-store",
            "login",
            "logout",
            "status",
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

        for command in ("info", "login", "logout", "status"):
            assert (output_dir / f"{command}.mdx").exists()
            assert f"{command}.mdx" in files
            # No directory or meta.json for leaf commands
            assert not (output_dir / command / "index.mdx").exists()
            assert not (output_dir / command / "meta.json").exists()

        # log-store has nested subcommands, so it should be a directory
        assert (output_dir / "log-store" / "index.mdx").exists()
        assert (output_dir / "log-store" / "meta.json").exists()
        for command in ("set", "show", "reset"):
            assert (output_dir / "log-store" / f"{command}.mdx").exists()
            assert f"log-store/{command}.mdx" in files

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

    def test_includes_index_first(self) -> None:
        meta = render_meta("CLI Reference", [])
        assert meta["pages"][0] == "index"

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
        assert meta["pages"] == ["index", "serve", "agent"]
