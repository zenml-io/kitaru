"""Generate MDX documentation pages from the Kitaru CLI (cyclopts) command tree.

Introspects the cyclopts App object, extracts command metadata, and writes
structured MDX files with frontmatter + meta.json files for FumaDocs navigation.

Output directory: docs/content/docs/cli/
Generated files are gitignored — they only exist after running this script.

Usage:
    uv run python scripts/generate_cli_docs.py
"""

from __future__ import annotations

import inspect
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "docs" / "content" / "docs" / "cli"


# ---------------------------------------------------------------------------
# Normalized data model — all rendering works from these, never raw cyclopts
# ---------------------------------------------------------------------------


@dataclass
class ParameterDoc:
    """A single CLI parameter (argument or option)."""

    names: list[str]
    help: str
    type_name: str
    required: bool
    default: str | None
    is_flag: bool

    @property
    def names_display(self) -> str:
        return ", ".join(f"`{n}`" for n in self.names)


@dataclass
class CommandDoc:
    """A single CLI command or subcommand."""

    slug: str
    name: str
    invocation: str
    description: str
    usage: str
    parameters: list[ParameterDoc] = field(default_factory=list)
    subcommands: list[CommandDoc] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Extraction — cyclopts introspection
# ---------------------------------------------------------------------------


def _type_display(hint: Any) -> str:
    """Human-readable type name from a Python type hint."""
    if hint is inspect.Parameter.empty or hint is None:
        return ""
    origin = getattr(hint, "__origin__", None)
    if origin is not None:
        args = getattr(hint, "__args__", ())
        arg_strs = ", ".join(_type_display(a) for a in args)
        origin_name = getattr(origin, "__name__", str(origin))
        return f"{origin_name}[{arg_strs}]" if arg_strs else origin_name
    name = getattr(hint, "__name__", None)
    if name:
        return name
    return str(hint)


def _format_default(value: Any) -> str | None:
    """Format a default value for display, or None if no meaningful default."""
    if value is inspect.Parameter.empty:
        return None
    sentinel_names = {"UNSET", "MISSING", "empty"}
    type_name = type(value).__name__
    if type_name in sentinel_names:
        return None
    if isinstance(value, str):
        return f'`"{value}"`'
    return f"`{value!r}`"


def _extract_parameters(app: Any) -> list[ParameterDoc]:
    """Extract parameter docs from a cyclopts App's argument collection."""
    try:
        args = app.assemble_argument_collection(parse_docstring=True)
    except Exception:
        return []

    params: list[ParameterDoc] = []
    for arg in args:
        if not arg.show:
            continue

        names = list(arg.parameter.name) if arg.parameter.name else list(arg.names)
        help_text = arg.parameter.help or ""
        type_name = _type_display(arg.hint)
        required = arg.required
        default = _format_default(arg.field_info.default)
        is_flag = arg.is_flag()

        params.append(
            ParameterDoc(
                names=names,
                help=help_text,
                type_name=type_name,
                required=required,
                default=default,
                is_flag=is_flag,
            )
        )
    return params


def _get_description(app: Any) -> str:
    """Extract a description from a cyclopts App."""
    if app.help:
        # Take the first paragraph (before any Args: section)
        lines: list[str] = []
        for line in str(app.help).splitlines():
            stripped = line.strip()
            if stripped.lower().startswith(
                ("args:", "arguments:", "returns:", "raises:")
            ):
                break
            lines.append(stripped)
        desc = " ".join(lines).strip()
        if desc:
            return desc

    if app.default_command and callable(app.default_command):
        doc = inspect.getdoc(app.default_command)
        if doc:
            return doc.split("\n\n")[0].strip()

    return ""


def build_command_tree(
    app: Any,
    parent_invocation: str = "",
) -> CommandDoc:
    """Recursively build a normalized command tree from a cyclopts App."""
    name_parts: tuple[str, ...] = (
        app.name if isinstance(app.name, tuple) else (str(app.name),)
    )
    name = name_parts[-1]
    invocation = f"{parent_invocation} {name}".strip() if parent_invocation else name
    slug = name

    description = _get_description(app)

    # Build usage string
    params = _extract_parameters(app)
    has_options = any(n.startswith("--") for p in params for n in p.names)
    has_args = any(not n.startswith("-") for p in params for n in p.names)
    registered = getattr(app, "_registered_commands", {})
    has_subcommands = bool(registered)

    usage_parts = [invocation]
    if has_subcommands:
        usage_parts.append("COMMAND")
    if has_args:
        usage_parts.append("[ARGS]")
    if has_options:
        usage_parts.append("[OPTIONS]")
    usage = " ".join(usage_parts)

    # Recurse into subcommands
    subcommands: list[CommandDoc] = []
    for _cmd_name, sub_app in sorted(registered.items()):
        subcommands.append(build_command_tree(sub_app, parent_invocation=invocation))

    return CommandDoc(
        slug=slug,
        name=name,
        invocation=invocation,
        description=description,
        usage=usage,
        parameters=params,
        subcommands=subcommands,
    )


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


def render_command_page(cmd: CommandDoc, *, is_root: bool = False) -> str:
    """Render a single command's MDX page content."""
    lines: list[str] = []

    # Frontmatter
    title = "CLI Reference" if is_root else cmd.invocation
    desc = cmd.description or f"Reference for the `{cmd.invocation}` command."
    # Escape YAML special characters in description
    safe_desc = desc.replace('"', '\\"')
    lines.append("---")
    lines.append(f'title: "{title}"')
    lines.append(f'description: "{safe_desc}"')
    lines.append("---")
    lines.append("")

    # Description
    if cmd.description:
        lines.append(_escape_mdx(cmd.description))
        lines.append("")

    # Usage
    lines.append("## Usage")
    lines.append("")
    lines.append("```bash")
    lines.append(cmd.usage)
    lines.append("```")
    lines.append("")

    # Global flags (root only)
    if is_root:
        lines.append("## Global Flags")
        lines.append("")
        lines.append("| Flag | Description |")
        lines.append("| --- | --- |")
        lines.append("| `--help`, `-h` | Display help and exit |")
        lines.append("| `--version`, `-V` | Display the installed version and exit |")
        lines.append("")

    # Parameters table
    if cmd.parameters:
        lines.append("## Parameters")
        lines.append("")
        lines.append("| Name | Type | Required | Default | Description |")
        lines.append("| --- | --- | --- | --- | --- |")
        for p in cmd.parameters:
            names_str = ", ".join(f"`{n}`" for n in p.names)
            type_str = f"`{p.type_name}`" if p.type_name else ""
            req_str = "Yes" if p.required else "No"
            default_str = p.default or ""
            desc_str = _escape_mdx(p.help)
            lines.append(
                f"| {names_str} | {type_str} | {req_str} | {default_str} | {desc_str} |"
            )
        lines.append("")

    # Subcommands list
    if cmd.subcommands:
        lines.append("## Commands")
        lines.append("")
        lines.append("| Command | Description |")
        lines.append("| --- | --- |")
        for sub in cmd.subcommands:
            desc_text = _escape_mdx(sub.description) if sub.description else ""
            lines.append(f"| [`{sub.name}`](./{sub.slug}) | {desc_text} |")
        lines.append("")

    return "\n".join(lines)


def render_meta(
    title: str, children: list[CommandDoc], *, default_open: bool = False
) -> dict[str, Any]:
    """Build a meta.json dict for a directory."""
    pages: list[str] = ["index"]
    for child in children:
        pages.append(child.slug)
    meta: dict[str, Any] = {"title": title}
    if default_open:
        meta["defaultOpen"] = True
    meta["pages"] = pages
    return meta


# ---------------------------------------------------------------------------
# Filesystem — write the generated tree
# ---------------------------------------------------------------------------


def write_docs_tree(root: CommandDoc, output_dir: Path) -> list[str]:
    """Write the full CLI docs tree to output_dir. Returns list of created files."""
    created: list[str] = []

    def _write_command(
        cmd: CommandDoc, dir_path: Path, *, is_root: bool = False
    ) -> None:
        dir_path.mkdir(parents=True, exist_ok=True)

        # Write the command's own page
        page_path = dir_path / "index.mdx"
        page_path.write_text(render_command_page(cmd, is_root=is_root))
        created.append(str(page_path.relative_to(output_dir)))

        # Write meta.json
        title = "CLI Reference" if is_root else cmd.invocation
        meta = render_meta(title, cmd.subcommands, default_open=is_root)
        meta_path = dir_path / "meta.json"
        meta_path.write_text(json.dumps(meta, indent=2) + "\n")
        created.append(str(meta_path.relative_to(output_dir)))

        # Recurse into subcommands
        for sub in cmd.subcommands:
            _write_command(sub, dir_path / sub.slug)

    _write_command(root, output_dir, is_root=True)
    return created


def _clean_output(output_dir: Path, flat_file: Path) -> None:
    """Remove previous output (both directory and flat file forms)."""
    if output_dir.exists():
        shutil.rmtree(output_dir)
    if flat_file.exists():
        flat_file.unlink()


def main() -> int:
    """Generate CLI reference docs from the Kitaru cyclopts app."""
    from kitaru.cli import app

    print("Extracting CLI command tree...")
    tree = build_command_tree(app)

    flat_file = OUTPUT_DIR.with_suffix(".mdx")
    _clean_output(OUTPUT_DIR, flat_file)

    if not tree.subcommands:
        # No subcommands: generate a single flat file (avoids nested sidebar)
        flat_file.write_text(render_command_page(tree, is_root=True))
        print(f"Generated {flat_file.relative_to(REPO_ROOT)} (flat, no subcommands)")
    else:
        # Has subcommands: generate a directory tree
        tmp_dir = Path(tempfile.mkdtemp(prefix="kitaru-cli-docs-"))
        try:
            files = write_docs_tree(tree, tmp_dir)
            shutil.copytree(tmp_dir, OUTPUT_DIR)
            print(
                f"Generated {len(files)} files in {OUTPUT_DIR.relative_to(REPO_ROOT)}/"
            )
            for f in sorted(files):
                print(f"  {f}")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
