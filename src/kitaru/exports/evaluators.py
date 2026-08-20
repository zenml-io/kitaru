"""Load materialized evaluators and map their exact results to target rewards."""

import hashlib
import math
import os
import selectors
import subprocess
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.task.evaluator import (
    EvaluationError,
    EvaluatorReturn,
    SessionView,
    call_evaluator,
)
from kitaru.task.plugins import (
    PluginLoadError,
    load_plugin_entrypoint,
    load_source_ref,
)

from ._sanitize import EphemeralSanitizer
from .models import (
    ExportError,
    MaterializedEvaluator,
    RewardSelector,
)

_LABEL = "Evaluator"
LoadedEvaluator = Callable[..., EvaluatorReturn]


@dataclass(frozen=True)
class EvaluationOutcome:
    """Record the selected reward and all numeric evaluator outputs."""

    reward: float
    metrics: dict[str, float]
    results: dict[str, tuple[EvaluationResult, ...]]


@dataclass(frozen=True)
class BoundedProcessResult:
    """Record bounded output from one directly managed evaluator process."""

    return_code: int
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool


def _append_bounded(target: bytearray, chunk: bytes, limit: int) -> bool:
    remaining = max(0, limit - len(target))
    target.extend(chunk[:remaining])
    return len(chunk) > remaining


def run_evaluator_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: float,
    max_output_bytes: int = 64 * 1024,
) -> BoundedProcessResult:
    """Run one evaluator worker with a hard timeout and bounded captured output.

    The caller supplies the complete environment and a private working directory.
    Timeout and cancellation kill the directly owned process. This function does not
    claim to contain or terminate descendants started by trusted evaluator code.

    Raises:
        ExportError: The process cannot start or exceeds its timeout.
    """
    if not argv or timeout_seconds <= 0 or max_output_bytes <= 0:
        raise ExportError(
            "invalid_evaluator_process",
            "Evaluator argv, timeout, and output limit must be positive.",
        )
    if not cwd.is_dir():
        raise ExportError(
            "invalid_evaluator_process",
            "Evaluator working directory must be an existing private directory.",
        )

    try:
        process = subprocess.Popen(
            list(argv),
            cwd=cwd,
            env=dict(env),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as error:
        raise ExportError(
            "evaluator_process_failed", "Evaluator process could not start."
        ) from error

    assert process.stdout is not None
    assert process.stderr is not None
    stdout_fd = process.stdout.fileno()
    stderr_fd = process.stderr.fileno()
    streams: dict[int, tuple[IO[Any], bytearray, bool]] = {
        stdout_fd: (process.stdout, bytearray(), False),
        stderr_fd: (process.stderr, bytearray(), False),
    }
    selector = selectors.DefaultSelector()
    for stream, _, _ in streams.values():
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ)

    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    try:
        while process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                process.kill()
                process.wait()
                break
            for key, _ in selector.select(min(remaining, 0.1)):
                stream, captured, truncated = streams[key.fd]
                try:
                    chunk = os.read(key.fd, 64 * 1024)
                except BlockingIOError:
                    continue
                if chunk:
                    truncated = (
                        _append_bounded(captured, chunk, max_output_bytes) or truncated
                    )
                    streams[key.fd] = (stream, captured, truncated)
                else:
                    selector.unregister(stream)

        # Drain bytes already present without waiting for evaluator descendants that
        # may have inherited the descriptors.
        while True:
            events = selector.select(0)
            if not events:
                break
            progressed = False
            for key, _ in events:
                stream, captured, truncated = streams[key.fd]
                try:
                    chunk = os.read(key.fd, 64 * 1024)
                except BlockingIOError:
                    continue
                if chunk:
                    progressed = True
                    truncated = (
                        _append_bounded(captured, chunk, max_output_bytes) or truncated
                    )
                    streams[key.fd] = (stream, captured, truncated)
                else:
                    selector.unregister(stream)
            if not progressed:
                break
    except BaseException:
        if process.poll() is None:
            process.kill()
            process.wait()
        raise
    finally:
        selector.close()
        process.stdout.close()
        process.stderr.close()

    if timed_out:
        raise ExportError(
            "evaluator_timeout",
            f"Evaluator exceeded its {timeout_seconds:g} second scoring timeout.",
        )
    return_code = process.wait()
    stdout_stream = streams[stdout_fd]
    stderr_stream = streams[stderr_fd]
    return BoundedProcessResult(
        return_code=return_code,
        stdout=stdout_stream[1].decode("utf-8", errors="replace"),
        stderr=stderr_stream[1].decode("utf-8", errors="replace"),
        stdout_truncated=stdout_stream[2],
        stderr_truncated=stderr_stream[2],
    )


def load_evaluator(
    evaluator: MaterializedEvaluator, *, script_path: Path | None = None
) -> LoadedEvaluator:
    """Load one exact materialized script or installed package evaluator.

    Args:
        evaluator: Pinned evaluator metadata resolved by the exporter.
        script_path: Path where the generated artifact materialized script bytes.

    Raises:
        ExportError: The source is inconsistent or the entrypoint cannot load.

    Returns:
        Evaluator callable accepted by Kitaru's existing evaluator helper.
    """
    source = evaluator.version.source
    try:
        if isinstance(source, ScriptPluginSource):
            if script_path is None or evaluator.script is None:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} requires its materialized script.",
                )
            try:
                actual = script_path.read_bytes()
            except OSError as error:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Could not read evaluator {evaluator.name!r}: {error}",
                ) from error
            if actual != evaluator.script:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} script does not match "
                    "the exported source.",
                )
            if hashlib.sha256(actual).hexdigest() != evaluator.source_sha256:
                raise ExportError(
                    "evaluator_load_failed",
                    f"Evaluator {evaluator.name!r} script digest is invalid.",
                )
            return load_plugin_entrypoint(script_path, source.entrypoint, _LABEL)
        if isinstance(source, PackagePluginSource):
            return load_source_ref(source.entrypoint, _LABEL)
    except PluginLoadError as error:
        raise ExportError(
            "evaluator_load_failed",
            f"Evaluator {evaluator.name!r} failed to load: {error}",
        ) from error
    raise ExportError(
        "evaluator_load_failed",
        f"Evaluator {evaluator.name!r} has an unsupported source.",
    )


def _get_numeric_score(result: EvaluationResult) -> float | None:
    score = result.score
    if score is None or isinstance(score, bool):
        return None
    value = float(score)
    return value if math.isfinite(value) else None


def _get_selected_reward(
    selector: RewardSelector, results: Sequence[EvaluationResult]
) -> float:
    selected = [result for result in results if result.name == selector.result]
    if len(selected) != 1:
        raise ExportError(
            "missing_reward_result",
            f"Evaluator {selector.evaluator!r} did not return exactly one "
            f"result named {selector.result!r}.",
        )
    result = selected[0]
    if selector.field == "passed":
        if result.passed is None:
            raise ExportError(
                "invalid_reward_value",
                f"Selected result {selector.result!r} has no boolean passed value.",
            )
        return 1.0 if result.passed else 0.0
    score = _get_numeric_score(result)
    if score is None:
        raise ExportError(
            "invalid_reward_value",
            f"Selected result {selector.result!r} has no numeric score.",
        )
    return score


def evaluate_session(
    evaluators: Sequence[tuple[MaterializedEvaluator, LoadedEvaluator]],
    selector: RewardSelector,
    session: SessionView,
    *,
    secret_values: Iterable[str] = (),
) -> EvaluationOutcome:
    """Run pinned evaluators and map one explicit result to a numeric reward.

    Raises:
        ExportError: An evaluator fails or the selected reward is absent or invalid.
    """
    if (
        sum(materialized.name == selector.evaluator for materialized, _ in evaluators)
        != 1
    ):
        raise ExportError(
            "invalid_reward_selector",
            f"Primary reward evaluator {selector.evaluator!r} is not loaded "
            "exactly once.",
        )

    all_results: dict[str, tuple[EvaluationResult, ...]] = {}
    metrics: dict[str, float] = {}
    reward: float | None = None
    sanitizer = EphemeralSanitizer(list(secret_values))
    for materialized, evaluator in evaluators:
        try:
            raw_results = call_evaluator(
                materialized.name, evaluator, session, materialized.params
            )
        except EvaluationError as error:
            message = sanitizer.sanitize(str(error))
            raise ExportError("evaluator_failed", str(message)) from error

        redacted_results = tuple(
            EvaluationResult.model_validate(
                sanitizer.sanitize(result.model_dump(mode="python"))
            )
            for result in raw_results
        )
        all_results[materialized.name] = redacted_results
        for result in redacted_results:
            prefix = f"{materialized.name}:{result.name}"
            if (score := _get_numeric_score(result)) is not None:
                metrics[f"{prefix}:score"] = score
            if result.passed is not None:
                metrics[f"{prefix}:passed"] = 1.0 if result.passed else 0.0
        if materialized.name == selector.evaluator:
            reward = _get_selected_reward(selector, redacted_results)

    if reward is None:
        raise ExportError("invalid_reward_value", "Primary reward was not produced.")
    return EvaluationOutcome(reward=reward, metrics=metrics, results=all_results)
