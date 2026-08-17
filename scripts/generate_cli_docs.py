"""Generate MDX documentation pages from the Kitaru CLI schema contract.

Runs the CLI in-process (``kitaru schema`` / ``kitaru schema <command>``),
parses the structured JSON it emits, and writes MDX files with frontmatter
plus meta.json files for FumaDocs navigation. Everything on the pages comes
from the schema output, so the generator needs no knowledge of individual
commands.

Output directory: docs/content/docs/cli/
Generated files are gitignored and should be regenerated locally before docs
builds or after CLI changes.

Usage:
    uv run python scripts/generate_cli_docs.py
"""

import contextlib
import io
import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "content" / "docs" / "cli"


class SchemaError(Exception):
    """Raised when the CLI schema output cannot be used for docs generation."""


# ---------------------------------------------------------------------------
# Normalized data model — all rendering works from these, never raw JSON
# ---------------------------------------------------------------------------


@dataclass
class ParameterDoc:
    """A single CLI parameter (argument or option)."""

    name: str
    type_name: str
    kind: str
    required: bool
    description: str

    @property
    def identity(self) -> tuple[str, str, str, bool, str]:
        """Return a hashable value identifying an identical parameter."""
        return (self.name, self.type_name, self.kind, self.required, self.description)


@dataclass
class CommandDoc:
    """A single leaf CLI command."""

    path: tuple[str, ...]
    description: str
    parameters: list[ParameterDoc] = field(default_factory=list)

    @property
    def slug(self) -> str:
        return self.path[-1]

    @property
    def invocation(self) -> str:
        return " ".join(("kitaru", *self.path))

    @property
    def docs_url(self) -> str:
        return f"/cli/{'/'.join(self.path)}/"


@dataclass
class GroupDoc:
    """A CLI command group (a path prefix with child commands)."""

    path: tuple[str, ...]
    description: str
    commands: list[CommandDoc] = field(default_factory=list)
    groups: list["GroupDoc"] = field(default_factory=list)
    # A command registered at the group path itself; its reference content is
    # rendered on the group's index page.
    own_command: CommandDoc | None = None

    @property
    def slug(self) -> str:
        return self.path[-1]

    @property
    def invocation(self) -> str:
        return " ".join(("kitaru", *self.path))

    @property
    def docs_url(self) -> str:
        return f"/cli/{'/'.join(self.path)}/" if self.path else "/cli/"

    @property
    def children(self) -> list["CommandDoc | GroupDoc"]:
        """Return direct children ordered by name."""
        merged: list[CommandDoc | GroupDoc] = [*self.commands, *self.groups]
        return sorted(merged, key=lambda child: child.slug)


# ---------------------------------------------------------------------------
# Extraction — run the CLI schema command in-process and parse its JSON
# ---------------------------------------------------------------------------


def run_schema_command(path: list[str]) -> list[dict[str, Any]]:
    """Run ``kitaru schema <path>`` in-process and return the emitted items."""
    from kitaru.cli.app import main as cli_main

    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        exit_code = cli_main(["schema", *path, "--output", "json"])
    if exit_code != 0:
        raise SchemaError(f"'kitaru schema {' '.join(path)}' exited with {exit_code}")
    envelope = json.loads(stdout.getvalue())
    if not envelope.get("ok", False):
        raise SchemaError(f"'kitaru schema {' '.join(path)}' reported ok=false")
    if envelope.get("page", {}).get("truncated", False):
        raise SchemaError(f"'kitaru schema {' '.join(path)}' returned a truncated page")
    items = envelope.get("items")
    if not isinstance(items, list):
        raise SchemaError(f"'kitaru schema {' '.join(path)}' emitted no items list")
    return items


def parse_command(item: dict[str, Any]) -> CommandDoc:
    """Convert one per-command schema entry into a CommandDoc."""
    parameters = [
        ParameterDoc(
            name=str(parameter["name"]),
            type_name=str(parameter["type"]),
            kind=str(parameter["kind"]),
            required=bool(parameter["required"]),
            description=str(parameter["description"]),
        )
        for parameter in item.get("parameters", [])
    ]
    return CommandDoc(
        path=tuple(item["path"]),
        description=str(item.get("description", "")),
        parameters=parameters,
    )


def fetch_command_docs() -> tuple[list[dict[str, Any]], list[CommandDoc]]:
    """Fetch the top-level summaries and every leaf command from the CLI."""
    top_items = run_schema_command([])
    commands: list[CommandDoc] = []
    for top_item in top_items:
        commands.extend(
            parse_command(item) for item in run_schema_command([top_item["name"]])
        )
    return top_items, commands


def build_tree(top_items: list[dict[str, Any]], commands: list[CommandDoc]) -> GroupDoc:
    """Assemble the flat leaf-command list into a nested group tree.

    ``top_items`` supplies the human-written descriptions for top-level
    groups; nested groups have no description in the schema output and get a
    generated one.
    """
    top_descriptions = {
        str(item["name"]): str(item.get("description", "")) for item in top_items
    }
    root = GroupDoc(path=(), description="")
    groups: dict[tuple[str, ...], GroupDoc] = {(): root}

    def get_group(path: tuple[str, ...]) -> GroupDoc:
        existing = groups.get(path)
        if existing is not None:
            return existing
        description = (
            top_descriptions.get(path[0], "")
            if len(path) == 1
            else f"Subcommands of `kitaru {' '.join(path)}`."
        )
        group = GroupDoc(path=path, description=description)
        groups[path] = group
        get_group(path[:-1]).groups.append(group)
        return group

    # Any leaf whose path is a strict prefix of another leaf's path doubles as
    # a group; its reference content then lives on the group index page.
    prefix_paths = {
        command.path
        for command in commands
        for other in commands
        if other.path != command.path
        and other.path[: len(command.path)] == command.path
    }
    for command in commands:
        if command.path in prefix_paths:
            get_group(command.path).own_command = command
        else:
            get_group(command.path[:-1]).commands.append(command)
    return root


def collect_global_parameters(commands: list[CommandDoc]) -> list[ParameterDoc]:
    """Return the parameters shared verbatim by every leaf command.

    The schema repeats the CLI-wide options (output mode, server override,
    ...) on every command; documenting them once on the index page keeps the
    per-command tables down to what is specific to each command.
    """
    if not commands:
        return []
    shared = {parameter.identity for parameter in commands[0].parameters}
    for command in commands[1:]:
        shared &= {parameter.identity for parameter in command.parameters}
    return [
        parameter
        for parameter in commands[0].parameters
        if parameter.identity in shared
    ]


# ---------------------------------------------------------------------------
# Rendering — normalized model to MDX strings
# ---------------------------------------------------------------------------


def _escape_mdx(text: str) -> str:
    """Escape characters that MDX treats specially in prose."""
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


def _prose_cell(text: str) -> str:
    """Render prose safely inside a markdown table cell."""
    return _escape_mdx(text).replace("|", "\\|")


def _code_cell(text: str) -> str:
    """Render a code span safely inside a markdown table cell."""
    # Code spans keep MDX from interpreting braces/angle brackets, but a raw
    # pipe still terminates the table cell.
    return f"`{text}`".replace("|", "\\|") if text else ""


def _render_frontmatter(title: str, description: str) -> list[str]:
    """Render MDX frontmatter with escaped YAML strings."""
    safe_title = title.replace("\\", "\\\\").replace('"', '\\"')
    safe_description = description.replace("\\", "\\\\").replace('"', '\\"')
    return [
        "---",
        f'title: "{safe_title}"',
        f'description: "{safe_description}"',
        "---",
        "",
    ]


def _render_usage(command: CommandDoc) -> str:
    """Render a one-line usage string from the command's arguments."""
    tokens = [command.invocation]
    for parameter in command.parameters:
        if parameter.kind != "argument":
            continue
        token = parameter.name
        if parameter.type_name.endswith("[]"):
            token += "..."
        tokens.append(token if parameter.required else f"[{token}]")
    if any(parameter.kind == "option" for parameter in command.parameters):
        tokens.append("[OPTIONS]")
    return " ".join(tokens)


def _render_parameter_table(parameters: list[ParameterDoc]) -> list[str]:
    """Render one parameter table."""
    lines = [
        "| Name | Type | Required | Description |",
        "| --- | --- | --- | --- |",
    ]
    for parameter in parameters:
        lines.append(
            f"| {_code_cell(parameter.name)} | {_code_cell(parameter.type_name)} "
            f"| {'Yes' if parameter.required else 'No'} "
            f"| {_prose_cell(parameter.description)} |"
        )
    return lines


def render_command_sections(
    command: CommandDoc, global_parameters: list[ParameterDoc]
) -> list[str]:
    """Render the usage and parameter sections shared by leaf and group pages."""
    lines = ["## Usage", "", "```bash", _render_usage(command), "```", ""]

    global_identities = {parameter.identity for parameter in global_parameters}
    local_parameters = [
        parameter
        for parameter in command.parameters
        if parameter.identity not in global_identities
    ]
    arguments = [p for p in local_parameters if p.kind == "argument"]
    options = [p for p in local_parameters if p.kind == "option"]

    if arguments:
        lines.extend(["## Arguments", "", *_render_parameter_table(arguments), ""])
    if options:
        lines.extend(["## Options", "", *_render_parameter_table(options), ""])
    if global_identities:
        lines.extend(
            [
                "Every command also accepts the [global options](/cli/) listed "
                "on the CLI overview page.",
                "",
            ]
        )
    return lines


def render_command_page(
    command: CommandDoc, global_parameters: list[ParameterDoc]
) -> str:
    """Render a leaf command's MDX page."""
    lines = _render_frontmatter(command.invocation, command.description)
    lines.extend(render_command_sections(command, global_parameters))
    return "\n".join(lines)


def _render_children_table(group: GroupDoc) -> list[str]:
    """Render the table linking a group's direct children."""
    lines = ["## Commands", "", "| Command | Description |", "| --- | --- |"]
    for child in group.children:
        description = (
            child.own_command.description
            if isinstance(child, GroupDoc) and child.own_command is not None
            else child.description
        )
        lines.append(
            f"| [{_code_cell(child.slug)}]({child.docs_url}) "
            f"| {_prose_cell(description)} |"
        )
    lines.append("")
    return lines


def render_group_page(group: GroupDoc, global_parameters: list[ParameterDoc]) -> str:
    """Render a command group's index MDX page."""
    description = (
        group.own_command.description if group.own_command else group.description
    )
    lines = _render_frontmatter(group.invocation, description)
    if group.own_command is not None:
        lines.extend(render_command_sections(group.own_command, global_parameters))
    else:
        lines.extend(
            ["## Usage", "", "```bash", f"{group.invocation} COMMAND", "```", ""]
        )
    lines.extend(_render_children_table(group))
    return "\n".join(lines)


def render_root_page(root: GroupDoc, global_parameters: list[ParameterDoc]) -> str:
    """Render the CLI reference index MDX page."""
    lines = _render_frontmatter(
        "CLI Reference", "Reference for the Kitaru command-line interface."
    )
    lines.extend(
        [
            "Reference pages for every `kitaru` command, generated from the "
            "CLI's own schema output.",
            "",
        ]
    )
    if global_parameters:
        lines.extend(
            [
                "## Global options",
                "",
                "Every command accepts these options in addition to its own.",
                "",
                *_render_parameter_table(global_parameters),
                "",
            ]
        )
    lines.extend(_render_children_table(root))
    return "\n".join(lines)


def render_meta(title: str, children: list[CommandDoc | GroupDoc]) -> dict[str, Any]:
    """Build a meta.json dict for a directory.

    The pages array deliberately excludes "index": FumaDocs already uses the
    folder itself as the index link, and listing it would add a duplicate
    sidebar entry.
    """
    return {"title": title, "pages": [child.slug for child in children]}


# ---------------------------------------------------------------------------
# Filesystem — write the generated tree
# ---------------------------------------------------------------------------


def write_docs_tree(
    root: GroupDoc, global_parameters: list[ParameterDoc], output_dir: Path
) -> list[str]:
    """Write the full CLI docs tree to output_dir. Returns created file paths."""
    created: list[str] = []

    def _write_page(path: Path, content: str) -> None:
        path.write_text(content + "\n" if not content.endswith("\n") else content)
        created.append(str(path.relative_to(output_dir)))

    def _write_group(group: GroupDoc, directory: Path, *, is_root: bool) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        index_content = (
            render_root_page(group, global_parameters)
            if is_root
            else render_group_page(group, global_parameters)
        )
        _write_page(directory / "index.mdx", index_content)

        title = "CLI Reference" if is_root else group.invocation
        meta_path = directory / "meta.json"
        meta_path.write_text(
            json.dumps(render_meta(title, group.children), indent=2) + "\n"
        )
        created.append(str(meta_path.relative_to(output_dir)))

        for child in group.children:
            if isinstance(child, GroupDoc):
                _write_group(child, directory / child.slug, is_root=False)
            else:
                _write_page(
                    directory / f"{child.slug}.mdx",
                    render_command_page(child, global_parameters),
                )

    _write_group(root, output_dir, is_root=True)
    return created


def main() -> int:
    """Generate the CLI reference docs from the CLI schema contract."""
    print("Extracting CLI schema...")
    try:
        top_items, commands = fetch_command_docs()
    except (SchemaError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}")
        return 1

    tree = build_tree(top_items, commands)
    global_parameters = collect_global_parameters(commands)

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    files = write_docs_tree(tree, global_parameters, OUTPUT_DIR)

    print(
        f"Generated {len(files)} files for {len(commands)} commands "
        f"in {OUTPUT_DIR.relative_to(REPO_ROOT)}/"
    )
    for name in sorted(files):
        print(f"  {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
