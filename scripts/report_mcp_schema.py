#!/usr/bin/env python3
#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Report and verify the public Kitaru MCP discovery schemas."""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings

SNAPSHOT_DIRECTORY = Path(__file__).parents[1] / "tests" / "mcp" / "snapshots"
METRICS_PATH = SNAPSHOT_DIRECTORY / "metrics.json"
EXPECTED_TOOL_COUNTS = {
    CapabilityMode.READ_ONLY: 3,
    CapabilityMode.STANDARD: 9,
    CapabilityMode.DESTRUCTIVE: 11,
}
MAX_TOOLS = 12
MAX_TOOL_SCHEMA_BYTES = 32 * 1024
MAX_DESTRUCTIVE_DISCOVERY_BYTES = 192 * 1024


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _maximum_container_depth(value: object, depth: int = 0) -> int:
    if isinstance(value, dict):
        if not value:
            return depth + 1
        return max(_maximum_container_depth(item, depth + 1) for item in value.values())
    if isinstance(value, list):
        if not value:
            return depth + 1
        return max(_maximum_container_depth(item, depth + 1) for item in value)
    return depth


def _count_definitions(value: object) -> int:
    if isinstance(value, dict):
        definitions = value.get("$defs")
        count = len(definitions) if isinstance(definitions, dict) else 0
        return count + sum(_count_definitions(item) for item in value.values())
    if isinstance(value, list):
        return sum(_count_definitions(item) for item in value)
    return 0


def _validate_input_schema(tool_name: str, schema: dict[str, Any]) -> None:
    if schema.get("type") == "object" and not schema.get("properties"):
        raise ValueError(
            f"{tool_name}: input schema is an unconstrained top-level object"
        )


async def _collect() -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    snapshots: dict[str, list[dict[str, Any]]] = {}
    mode_metrics: dict[str, Any] = {}
    for mode in CapabilityMode:
        tools = await create_server(MCPSettings(mode=mode)).list_tools()
        dumped = [tool.model_dump(by_alias=True, exclude_none=True) for tool in tools]
        snapshots[mode.value] = dumped
        tool_metrics: dict[str, Any] = {}
        for tool in dumped:
            input_schema = tool["inputSchema"]
            output_schema = tool.get("outputSchema", {})
            _validate_input_schema(tool["name"], input_schema)
            input_bytes = len(_canonical_bytes(input_schema))
            output_bytes = len(_canonical_bytes(output_schema))
            combined_bytes = input_bytes + output_bytes
            if combined_bytes > MAX_TOOL_SCHEMA_BYTES:
                raise ValueError(
                    f"{tool['name']}: {combined_bytes} schema bytes exceed "
                    f"{MAX_TOOL_SCHEMA_BYTES}"
                )
            tool_metrics[tool["name"]] = {
                "input_bytes": input_bytes,
                "output_bytes": output_bytes,
                "combined_bytes": combined_bytes,
                "$defs_count": _count_definitions(input_schema)
                + _count_definitions(output_schema),
                "maximum_nesting_depth": max(
                    _maximum_container_depth(input_schema),
                    _maximum_container_depth(output_schema),
                ),
            }
        discovery_bytes = len(_canonical_bytes({"tools": dumped}))
        expected_count = EXPECTED_TOOL_COUNTS[mode]
        if len(dumped) != expected_count:
            raise ValueError(
                f"{mode.value}: expected {expected_count} tools, found {len(dumped)}"
            )
        if len(dumped) > MAX_TOOLS:
            raise ValueError(f"{mode.value}: {len(dumped)} tools exceed {MAX_TOOLS}")
        if (
            mode is CapabilityMode.DESTRUCTIVE
            and discovery_bytes > MAX_DESTRUCTIVE_DISCOVERY_BYTES
        ):
            raise ValueError(
                f"destructive: {discovery_bytes} discovery bytes exceed "
                f"{MAX_DESTRUCTIVE_DISCOVERY_BYTES}"
            )
        mode_metrics[mode.value] = {
            "tool_count": len(dumped),
            "discovery_response_bytes": discovery_bytes,
            "tools": tool_metrics,
        }
    metrics = {
        "schema_version": "1",
        "budgets": {
            "maximum_tools": MAX_TOOLS,
            "maximum_tool_schema_bytes": MAX_TOOL_SCHEMA_BYTES,
            "maximum_destructive_discovery_bytes": MAX_DESTRUCTIVE_DISCOVERY_BYTES,
        },
        "modes": mode_metrics,
    }
    return snapshots, metrics


def _formatted_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _write(snapshots: dict[str, list[dict[str, Any]]], metrics: dict[str, Any]) -> None:
    SNAPSHOT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    for mode, snapshot in snapshots.items():
        (SNAPSHOT_DIRECTORY / f"{mode}.json").write_text(_formatted_json(snapshot))
    METRICS_PATH.write_text(_formatted_json(metrics))


def _check(snapshots: dict[str, list[dict[str, Any]]], metrics: dict[str, Any]) -> None:
    expected = {
        SNAPSHOT_DIRECTORY / f"{mode}.json": _formatted_json(snapshot)
        for mode, snapshot in snapshots.items()
    }
    expected[METRICS_PATH] = _formatted_json(metrics)
    stale = [
        str(path.relative_to(Path(__file__).parents[1]))
        for path, content in expected.items()
        if not path.exists() or path.read_text() != content
    ]
    if stale:
        raise ValueError(
            "MCP schema snapshots are stale: "
            + ", ".join(stale)
            + ". Run scripts/report_mcp_schema.py --write."
        )


def _print_report(metrics: dict[str, Any]) -> None:
    for mode, values in metrics["modes"].items():
        print(
            f"{mode}: {values['tool_count']} tools, "
            f"{values['discovery_response_bytes']} discovery bytes"
        )
        for name, tool in values["tools"].items():
            print(
                f"  {name}: input={tool['input_bytes']} output={tool['output_bytes']} "
                f"combined={tool['combined_bytes']} defs={tool['$defs_count']} "
                f"depth={tool['maximum_nesting_depth']}"
            )


async def _main() -> int:
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true")
    action.add_argument("--write", action="store_true")
    arguments = parser.parse_args()
    try:
        snapshots, metrics = await _collect()
        if arguments.write:
            _write(snapshots, metrics)
        elif arguments.check:
            _check(snapshots, metrics)
        _print_report(metrics)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
