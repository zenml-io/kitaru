#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Offline command metadata shared by registration and schema output."""

from dataclasses import asdict, dataclass
from typing import Any, Literal

from kitaru.cli.output import ERROR_EXIT_CODES, CLIError

SideEffect = Literal[
    "reads_local_file",
    "writes_local_file",
    "writes_local_config",
    "uploads_data",
    "creates_remote_state",
    "mutates_remote_state",
    "deletes_remote_state",
    "executes_local_code",
]


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    """Machine-readable description of one CLI parameter."""

    name: str
    type: str
    kind: Literal["argument", "option"]
    required: bool
    description: str


@dataclass(frozen=True, slots=True)
class CommandSpec:
    """Stable behavior metadata for one leaf command."""

    path: tuple[str, ...]
    description: str
    parameters: tuple[ParameterSpec, ...] = ()
    read_only: bool = True
    side_effects: tuple[SideEffect, ...] = ()
    idempotency: str = "read_only"
    interaction: str = "none"
    streams: bool = False
    error_kinds: tuple[str, ...] = (
        "invalid_arguments",
        "internal_error",
    )
    output_modes: tuple[str, ...] = ("auto", "text", "json")
    offline: bool = False

    @property
    def command(self) -> str:
        """Return the dotted command identifier used in envelopes.

        Returns:
            Dotted command path.
        """
        return ".".join(self.path)


_COMMANDS: dict[tuple[str, ...], CommandSpec] = {}
GROUP_DESCRIPTIONS = {
    "agent": "Register and inspect agents.",
    "annotation": "Create and manage session annotations.",
    "cohort": "Manage cohort namespaces and immutable membership versions.",
    "config": "Manage allowlisted CLI preferences.",
    "evaluation": "Inspect stored evaluations.",
    "evaluator": "Develop, register, and inspect evaluators.",
    "experiment": "Configure experiments and manage asynchronous runs.",
    "import": "Inspect imports.",
    "importer": "Develop, register, and inspect importers.",
    "insight": "Create and inspect agent insights.",
    "investigation": "Create investigations and review their linked sessions.",
    "replay": "Create and inspect standalone replays.",
    "session": "Import and inspect sessions and their nodes.",
    "worker": "Run and inspect generic local workers.",
    "job": "Inspect, watch, and cancel jobs.",
    "local": "Inspect the CLI-owned local Kitaru deployment.",
}


def register_spec(spec: CommandSpec) -> None:
    """Register the metadata used to create a command.

    Args:
        spec: Leaf-command behavior contract.

    Raises:
        RuntimeError: The command path is registered twice.
    """
    if spec.path in _COMMANDS:
        raise RuntimeError(f"Command metadata already registered: {spec.command}")
    unknown = set(spec.error_kinds) - ERROR_EXIT_CODES.keys()
    if unknown:
        raise RuntimeError(f"Unknown error kinds for {spec.command}: {sorted(unknown)}")
    _COMMANDS[spec.path] = spec


def get_spec(path: tuple[str, ...]) -> CommandSpec | None:
    """Return metadata for an exact leaf command.

    Args:
        path: Tokenized command path.

    Returns:
        Registered metadata, when present.
    """
    return _COMMANDS.get(path)


def is_command_group(path: tuple[str, ...]) -> bool:
    """Return whether a path is a registered command-group prefix."""
    return any(
        len(command_path) > len(path) and command_path[: len(path)] == path
        for command_path in _COMMANDS
    )


def describe_schema(path: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    """Describe the bounded command tree without local or network access.

    Args:
        path: Optional command or group path.

    Raises:
        CLIError: The path does not identify a command or group.

    Returns:
        Top-level summaries or full descriptions for a subtree.
    """
    matching = [
        spec
        for command_path, spec in sorted(_COMMANDS.items())
        if command_path[: len(path)] == path
    ]
    if path and not matching:
        raise CLIError(
            "invalid_arguments",
            f"Unknown command path: {' '.join(path)}.",
        )
    if not path:
        roots: dict[str, dict[str, Any]] = {}
        for spec in matching:
            name = spec.path[0]
            entry = roots.setdefault(
                name,
                {
                    "name": name,
                    "path": [name],
                    "description": GROUP_DESCRIPTIONS.get(name, spec.description),
                    "has_children": len(spec.path) > 1,
                },
            )
            if len(spec.path) == 1:
                entry["description"] = spec.description
        return list(roots.values())
    return [_describe_command(spec) for spec in matching]


def is_offline(path: tuple[str, ...]) -> bool:
    """Report whether a command must skip config and credential bootstrap.

    Args:
        path: Exact leaf command path.

    Returns:
        Whether the command is offline-safe.
    """
    spec = get_spec(path)
    return bool(spec and spec.offline)


def _describe_command(spec: CommandSpec) -> dict[str, Any]:
    """Convert one command contract to its version-1 schema shape."""
    errors = [
        {"kind": kind, "exit_code": ERROR_EXIT_CODES[kind]} for kind in spec.error_kinds
    ]
    side_effects = {
        name: name in spec.side_effects
        for name in (
            "reads_local_file",
            "writes_local_file",
            "writes_local_config",
            "uploads_data",
            "creates_remote_state",
            "mutates_remote_state",
            "deletes_remote_state",
            "executes_local_code",
        )
    }
    return {
        "schema_version": "1",
        "command": spec.command,
        "path": list(spec.path),
        "description": spec.description,
        "parameters": [asdict(parameter) for parameter in spec.parameters],
        "output_modes": list(spec.output_modes),
        "errors": errors,
        "read_only": spec.read_only,
        "mutating": not spec.read_only,
        "side_effects": side_effects,
        "idempotency": spec.idempotency,
        "interaction": spec.interaction,
        "streams": spec.streams,
        "offline": spec.offline,
    }
