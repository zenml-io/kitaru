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
    _get_description,
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


class TestGetDescription:
    """Tests for docstring summary/body extraction."""

    @staticmethod
    def _app(help_text: str | None = None, default_doc: str | None = None) -> object:
        """Build a minimal cyclopts-shaped app for `_get_description()`."""
        from types import SimpleNamespace

        default_command = None
        if default_doc is not None:

            def fn() -> None:
                """placeholder"""

            fn.__doc__ = default_doc
            default_command = fn
        return SimpleNamespace(help=help_text, default_command=default_command)

    def test_no_help_and_no_default_command(self) -> None:
        assert _get_description(self._app()) == ("", "")

    def test_summary_only(self) -> None:
        app = self._app("Start the local server.")
        assert _get_description(app) == ("Start the local server.", "")

    def test_summary_plus_body(self) -> None:
        app = self._app("Summary line.\n\nDetail paragraph explaining the nuance.")
        assert _get_description(app) == (
            "Summary line.",
            "Detail paragraph explaining the nuance.",
        )

    def test_multiple_body_paragraphs_preserved(self) -> None:
        app = self._app("Summary.\n\nFirst detail paragraph.\n\nSecond paragraph.")
        summary, body = _get_description(app)
        assert summary == "Summary."
        assert body == "First detail paragraph.\n\nSecond paragraph."

    def test_args_section_stripped_from_body(self) -> None:
        app = self._app(
            "Summary.\n\nDetail explaining flags.\n\n"
            "Args:\n    foo: the foo parameter.\n    bar: the bar parameter."
        )
        summary, body = _get_description(app)
        assert summary == "Summary."
        assert body == "Detail explaining flags."

    def test_docstring_is_only_args_section(self) -> None:
        app = self._app("Args:\n    foo: the foo parameter.")
        assert _get_description(app) == ("", "")

    def test_returns_section_also_stripped(self) -> None:
        app = self._app("Summary.\n\nReturns:\n    int: the result.")
        assert _get_description(app) == ("Summary.", "")

    def test_falls_back_to_default_command_docstring(self) -> None:
        app = self._app(default_doc="Fallback summary.\n\nFallback body.")
        assert _get_description(app) == (
            "Fallback summary.",
            "Fallback body.",
        )

    def test_collapses_hard_wrapped_summary_whitespace(self) -> None:
        app = self._app("Summary that is\nhard-wrapped across\nmultiple lines.")
        summary, body = _get_description(app)
        assert summary == "Summary that is hard-wrapped across multiple lines."
        assert body == ""


class TestBuildCommandTree:
    """Tests for cyclopts command tree extraction."""

    def test_extracts_root_command(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        assert tree.name == "kitaru"
        assert tree.invocation == "kitaru"
        assert tree.summary

    def test_root_has_current_subcommands(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        assert [sub.name for sub in tree.subcommands] == [
            "analytics",
            "auth",
            "build",
            "clean",
            "deploy",
            "executions",
            "flow",
            "import",
            "info",
            "init",
            "invoke",
            "log-store",
            "login",
            "logout",
            "model",
            "project",
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
            "cohort",
            "diff",
            "diff-matrix",
            "get",
            "input",
            "list",
            "logs",
            "replay",
            "resume",
            "retry",
            "statistics",
        ]

    def test_auth_tree_includes_service_accounts_and_api_keys(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        auth = _find_command(tree, "auth")
        assert [sub.name for sub in auth.subcommands] == [
            "api-keys",
            "service-accounts",
            "token",
        ]

        service_accounts = _find_command(tree, "auth", "service-accounts")
        assert [sub.name for sub in service_accounts.subcommands] == [
            "create",
            "delete",
            "list",
            "show",
            "update",
        ]

        api_keys = _find_command(tree, "auth", "api-keys")
        assert [sub.name for sub in api_keys.subcommands] == [
            "create",
            "delete",
            "list",
            "rotate",
            "show",
            "update",
        ]

    def test_flow_tree_includes_deployment_commands(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        flow = _find_command(tree, "flow")
        assert [sub.name for sub in flow.subcommands] == [
            "deployments",
            "list",
            "show",
            "tag",
            "untag",
        ]
        deployments = _find_command(tree, "flow", "deployments")
        assert [sub.name for sub in deployments.subcommands] == [
            "curl",
            "delete",
            "list",
            "logs",
            "show",
        ]

    def test_builds_canonical_docs_urls_for_nested_commands(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)

        executions_list = _find_command(tree, "executions", "list")
        assert executions_list.docs_path == ("executions", "list")
        assert executions_list.docs_url == "/cli/executions/list/"

        flow_deployment_logs = _find_command(tree, "flow", "deployments", "logs")
        assert flow_deployment_logs.docs_path == (
            "flow",
            "deployments",
            "logs",
        )
        assert flow_deployment_logs.docs_url == "/cli/flow/deployments/logs/"
        assert not flow_deployment_logs.docs_url.startswith("/docs/docs/")
        assert not flow_deployment_logs.docs_url.startswith("/docs/cli/")

    def test_project_tree_includes_management_commands(self) -> None:
        from kitaru.cli import app

        tree = build_command_tree(app)
        project = _find_command(tree, "project")
        assert [sub.name for sub in project.subcommands] == [
            "create",
            "current",
            "delete",
            "list",
            "show",
            "use",
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

        assert get.usage.startswith("kitaru executions get EXEC-ID")
        assert get.parameters[0].names == ["EXEC-ID"]

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
            summary="Test description.",
            body="",
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
            summary="Test.",
            body="",
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
            summary="Test.",
            body="",
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
            summary="Start server.",
            body="",
            usage="kitaru serve",
        )
        page = render_command_page(cmd, is_root=False)
        assert "## Global Flags" not in page

    def test_body_renders_below_frontmatter(self) -> None:
        cmd = CommandDoc(
            slug="retry",
            name="retry",
            invocation="kitaru executions retry",
            summary="Retry a failed execution.",
            body="Use --from-step to resume from a specific checkpoint.",
            usage="kitaru executions retry EXECUTION_ID [OPTIONS]",
        )
        page = render_command_page(cmd, is_root=False)
        assert 'description: "Retry a failed execution."' in page
        assert "Use --from-step to resume" in page
        # Guard against duplication in both directions:
        # - summary must stay in frontmatter only (not copied to body)
        # - body must stay in body only (not promoted to frontmatter)
        assert page.count("Retry a failed execution.") == 1
        assert page.count("Use --from-step to resume") == 1

    def test_renders_parameters_table(self) -> None:
        cmd = CommandDoc(
            slug="serve",
            name="serve",
            invocation="kitaru serve",
            summary="Start.",
            body="",
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
            summary="Run an agent.",
            body="",
            usage="kitaru agent run",
        )
        cmd = CommandDoc(
            slug="agent",
            name="agent",
            invocation="kitaru agent",
            summary="Manage agents.",
            body="",
            usage="kitaru agent COMMAND",
            subcommands=[child],
        )
        page = render_command_page(cmd, is_root=False)
        assert "## Commands" in page
        assert "[`run`](/cli/agent/run/)" in page

    def test_subcommand_table_omits_body(self) -> None:
        # Markdown tables can't contain block-level content, so the child's
        # body must NEVER appear in the parent's subcommand table — only
        # its summary. The body stays on the child's own page.
        child = CommandDoc(
            slug="retry",
            name="retry",
            invocation="kitaru executions retry",
            summary="Retry a failed execution.",
            body="Use --from-step to resume from a specific checkpoint.",
            usage="kitaru executions retry EXECUTION_ID [OPTIONS]",
        )
        parent = CommandDoc(
            slug="executions",
            name="executions",
            invocation="kitaru executions",
            summary="Manage executions.",
            body="",
            usage="kitaru executions COMMAND",
            subcommands=[child],
        )
        page = render_command_page(parent, is_root=False)
        assert "Retry a failed execution." in page
        assert "Use --from-step to resume" not in page

    def test_escapes_mdx_special_chars(self) -> None:
        cmd = CommandDoc(
            slug="test",
            name="test",
            invocation="test",
            summary="Summary.",
            body="Uses <angle> and {braces}.",
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
            "analytics",
            "auth",
            "build",
            "clean",
            "deploy",
            "executions",
            "flow",
            "import",
            "info",
            "init",
            "invoke",
            "log-store",
            "login",
            "logout",
            "model",
            "project",
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
            "cohort",
            "diff",
            "diff-matrix",
            "get",
            "input",
            "list",
            "logs",
            "replay",
            "resume",
            "retry",
            "statistics",
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

        # auth, executions, flow, log-store, model, project, secrets, and stack
        # all have nested subcommands.
        for command in (
            "auth",
            "executions",
            "flow",
            "log-store",
            "model",
            "project",
            "secrets",
            "stack",
        ):
            assert (output_dir / command / "index.mdx").exists()
            assert (output_dir / command / "meta.json").exists()

        assert (output_dir / "auth" / "token.mdx").exists()
        assert "auth/token.mdx" in files
        assert (output_dir / "auth" / "api-keys" / "meta.json").exists()
        assert (output_dir / "auth" / "service-accounts" / "meta.json").exists()

        for command in ("create", "delete", "list", "show", "update"):
            assert (
                output_dir / "auth" / "service-accounts" / f"{command}.mdx"
            ).exists()
            assert f"auth/service-accounts/{command}.mdx" in files

        for command in ("create", "delete", "list", "rotate", "show", "update"):
            assert (output_dir / "auth" / "api-keys" / f"{command}.mdx").exists()
            assert f"auth/api-keys/{command}.mdx" in files

        auth_token_content = (output_dir / "auth" / "token.mdx").read_text()
        assert "short-lived bearer token" in auth_token_content
        api_key_create_content = (
            output_dir / "auth" / "api-keys" / "create.mdx"
        ).read_text()
        assert "one-time value" in api_key_create_content
        assert "kitaru auth api-keys create SERVICE-ACCOUNT NAME" in (
            api_key_create_content
        )

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

        for command in ("list", "show", "tag", "untag"):
            assert (output_dir / "flow" / f"{command}.mdx").exists()
            assert f"flow/{command}.mdx" in files
        for command in ("curl", "delete", "list", "logs", "show"):
            assert (output_dir / "flow" / "deployments" / f"{command}.mdx").exists()
            assert f"flow/deployments/{command}.mdx" in files

        for command in ("set", "show", "reset"):
            assert (output_dir / "log-store" / f"{command}.mdx").exists()
            assert f"log-store/{command}.mdx" in files

        for command in ("list", "register"):
            assert (output_dir / "model" / f"{command}.mdx").exists()
            assert f"model/{command}.mdx" in files

        for command in ("create", "current", "delete", "list", "show", "use"):
            assert (output_dir / "project" / f"{command}.mdx").exists()
            assert f"project/{command}.mdx" in files

        for command in ("delete", "list", "set", "show"):
            assert (output_dir / "secrets" / f"{command}.mdx").exists()
            assert f"secrets/{command}.mdx" in files

        secrets_set_content = (output_dir / "secrets" / "set.mdx").read_text()
        assert "--KEY=value" in secrets_set_content
        assert "`--private`" in secrets_set_content

        for list_page in (
            output_dir / "executions" / "list.mdx",
            output_dir / "model" / "list.mdx",
            output_dir / "project" / "list.mdx",
            output_dir / "secrets" / "list.mdx",
            output_dir / "stack" / "list.mdx",
        ):
            list_content = list_page.read_text()
            assert "`--page`" in list_content
            assert "`--size`" in list_content

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
        assert "kitaru executions get EXEC-ID" in get_content
        assert "| `EXEC-ID` | `str` | Yes |  | Execution ID. |" in get_content

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
        assert "`--private`" in secrets_set_content
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
                summary="",
                body="",
                usage="",
            ),
            CommandDoc(
                slug="agent",
                name="agent",
                invocation="kitaru agent",
                summary="",
                body="",
                usage="",
            ),
        ]
        meta = render_meta("CLI Reference", children)
        assert meta["pages"] == ["serve", "agent"]
