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
"""Run a trusted, preinstalled JavaScript evaluator through Node."""

import asyncio
import hashlib
import json
import math
import re
from contextlib import suppress
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import EvaluationError, SessionView

_MAX_OUTPUT_BYTES = 1024 * 1024
_RESULT_FIELD_TYPES = {
    "name": (str,),
    "score": (int, float, bool, type(None)),
    "value": (str, type(None)),
    "explanation": (str, type(None)),
    "passed": (bool, type(None)),
    "min_score": (int, float, type(None)),
    "max_score": (int, float, type(None)),
    "target_score": (int, float, type(None)),
}


async def _write_input(stream: asyncio.StreamWriter, request: bytes) -> None:
    """Send a JSON request and close the subprocess input.

    Args:
        stream: Subprocess stdin.
        request: UTF-8 JSON request.
    """
    try:
        stream.write(request)
        await stream.drain()
    except (BrokenPipeError, ConnectionResetError):
        # A process that exits before reading is diagnosed by its exit/response.
        pass
    finally:
        stream.close()


async def _read_output(stream: asyncio.StreamReader) -> bytes:
    """Read stdout within the protocol response size limit.

    Args:
        stream: Subprocess stdout.

    Raises:
        EvaluationError: Output exceeds the fixed byte limit.

    Returns:
        Complete response bytes.
    """
    output = bytearray()
    while chunk := await stream.read(65536):
        output.extend(chunk)
        if len(output) > _MAX_OUTPUT_BYTES:
            raise EvaluationError("TypeScript evaluator exceeded response size limit")
    return bytes(output)


def _decode_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Decode a JSON object without ambiguous duplicate keys.

    Args:
        pairs: Object key/value pairs from the JSON parser.

    Raises:
        ValueError: An object contains duplicate keys.

    Returns:
        Decoded object.
    """
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("Duplicate JSON keys")
    return result


def _parse_results(output: bytes) -> list[EvaluationResult]:
    """Validate the versioned response and evaluation result models.

    Args:
        output: Complete subprocess stdout.

    Raises:
        EvaluationError: JSON, protocol, or result validation failed.

    Returns:
        Nonempty evaluation results with unique names.
    """
    try:
        response = json.loads(output, object_pairs_hook=_decode_object)
        if (
            not isinstance(response, dict)
            or set(response) != {"schema_version", "results"}
            or type(response["schema_version"]) is not int
            or response["schema_version"] != 1
            or not isinstance(response["results"], list)
            or not response["results"]
        ):
            raise ValueError("Invalid response envelope")
        results = []
        for item in response["results"]:
            # EvaluationResult's positional-value constructor can coerce wire
            # fields even under strict validation. Check JSON types first.
            if not isinstance(item, dict) or any(
                type(value) not in _RESULT_FIELD_TYPES.get(key, ())
                for key, value in item.items()
            ):
                raise ValueError("Invalid result field types")
            results.append(EvaluationResult.model_validate(item, strict=True))
        if len({item.name for item in results}) != len(results):
            raise ValueError("Duplicate evaluation names")
    except (ValueError, TypeError, ValidationError, RecursionError):
        # Validation errors embed original values, which may contain secrets.
        raise EvaluationError(
            "TypeScript evaluator returned an invalid response"
        ) from None
    return results


async def run_typescript_evaluator(
    session: SessionView,
    *,
    artifact: Path,
    sha256: str,
    params: dict[str, Any],
    node: str = "node",
    timeout_seconds: float = 60,
) -> list[EvaluationResult]:
    """Score a session with a hash-pinned JavaScript evaluator artifact.

    The Python wrapper fixes the artifact, digest, and Node executable. Task
    parameters contain scorer configuration only. Deploy the trusted artifact
    read-only and pin its Node runtime and dependencies separately: the digest
    covers only the artifact file, not its imported dependencies or runtime.
    This function does not provide a sandbox for untrusted code.

    Node receives one UTF-8 JSON object on stdin with ``schema_version: 1``,
    the complete JSON-mode ``SessionView`` as ``session``, and ``params``.
    Stdout must contain only ``{schema_version: 1, results: [...]}`` and fit
    within 1 MiB. Stderr is discarded, and errors never include child output.
    The subprocess remains in the worker's process group so the outer worker
    timeout can terminate the complete task process tree.

    Args:
        session: Session and all nodes, including their hydrated payloads.
        artifact: Absolute path to the preinstalled executable JavaScript file.
        sha256: Expected 64-character hexadecimal SHA-256 of that file.
        params: JSON object containing scorer configuration.
        node: Fixed Node executable name or path supplied by the wrapper.
        timeout_seconds: Positive finite timeout for the subprocess exchange.

    Raises:
        EvaluationError: Configuration, integrity, process, timeout, or protocol
            validation fails. Failures produce no evaluation results.
        asyncio.CancelledError: The caller cancels; Node is killed and reaped.

    Returns:
        Validated evaluation results with unique names.
    """
    if not artifact.is_absolute():
        raise EvaluationError("TypeScript evaluator artifact path must be absolute")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", sha256):
        raise EvaluationError("TypeScript evaluator requires a SHA-256 digest")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise EvaluationError(
            "TypeScript evaluator timeout must be positive and finite"
        )
    try:
        with artifact.open("rb") as source:
            actual_digest = hashlib.file_digest(source, "sha256").hexdigest()
    except OSError:
        raise EvaluationError("TypeScript evaluator artifact is unavailable") from None
    if actual_digest != sha256.lower():
        raise EvaluationError("TypeScript evaluator artifact hash mismatch")
    try:
        if not isinstance(params, dict):
            raise ValueError("Params must be an object")
        request = json.dumps(
            {
                "schema_version": 1,
                "session": session.model_dump(mode="json"),
                "params": params,
            },
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError, RecursionError):
        raise EvaluationError("TypeScript evaluator input is not valid JSON") from None

    try:
        process = await asyncio.create_subprocess_exec(
            node,
            str(artifact),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except (OSError, ValueError):
        raise EvaluationError("TypeScript evaluator runtime could not start") from None
    assert process.stdin is not None
    assert process.stdout is not None
    input_task = asyncio.create_task(_write_input(process.stdin, request))
    output_task = asyncio.create_task(_read_output(process.stdout))
    exit_task = asyncio.create_task(process.wait())
    tasks = [input_task, output_task, exit_task]
    try:
        async with asyncio.timeout(timeout_seconds):
            _, output, returncode = await asyncio.gather(
                input_task, output_task, exit_task
            )
        if returncode:
            raise EvaluationError("TypeScript evaluator exited unsuccessfully")
        return _parse_results(output)
    except TimeoutError:
        raise EvaluationError("TypeScript evaluator timed out") from None
    except OSError:
        raise EvaluationError(
            "TypeScript evaluator subprocess communication failed"
        ) from None
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        # Draining releases a paused pipe transport before waiting for exit.
        # Descendants that inherit stdout must not hold cleanup open forever.
        try:
            async with asyncio.timeout(1):
                while await process.stdout.read(65536):
                    pass
                await process.wait()
        except TimeoutError:
            pass
