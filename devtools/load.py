"""Concurrent load generation against a running Kitaru server."""

import argparse
import asyncio
import os
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from agent import record_session
from simulation import (
    DEFAULT_MODEL,
    SimulationConfig,
    build_session_inputs,
    passthrough_tool,
)

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.session import SessionListParams, SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError

WRITE_INPUT_OFFSET = 20_000_000


def percentile(values: list[float], fraction: float) -> float:
    """Return the value at a fraction of the sorted sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(fraction * len(ordered)))
    return ordered[index]


@dataclass
class OpStats:
    """Latency and outcome accumulator for one operation."""

    latencies_ms: list[float] = field(default_factory=list)
    outcomes: Counter[str] = field(default_factory=Counter)

    def record(self, elapsed_ms: float, outcome: str) -> None:
        """Record one call."""
        self.latencies_ms.append(elapsed_ms)
        self.outcomes[outcome] += 1


async def blast(
    factory: Callable[[int], Awaitable[Any]], count: int, concurrency: int = 20
) -> Counter[str]:
    """Run a call factory count times with bounded concurrency, counting outcomes."""
    semaphore = asyncio.Semaphore(concurrency)
    outcomes: Counter[str] = Counter()

    async def one(index: int) -> None:
        """Run one call and record its outcome."""
        async with semaphore:
            try:
                await factory(index)
                outcomes["ok"] += 1
            except APIError as exc:
                outcomes[f"http_{exc.status_code}"] += 1
            except Exception as exc:
                outcomes[f"exc_{type(exc).__name__}"] += 1

    await asyncio.gather(*(one(index) for index in range(count)))
    return outcomes


class _LoadDriver:
    """Weighted operation runner over one client."""

    def __init__(
        self,
        client: KitaruAPIClient,
        agent_id: uuid.UUID | None,
        write_rate: float,
        config: SimulationConfig,
    ) -> None:
        """Initialize the driver."""
        self._client = client
        self._agent_id = agent_id
        self._write_rate = write_rate
        self._config = config
        self._session_pool: list[uuid.UUID] = []
        self._write_counter = 0
        self.stats: dict[str, OpStats] = {}

    async def prime(self) -> None:
        """Prefetch session ids for the point-read operations."""
        page = await self._client.sessions.list(SessionListParams(size=200))
        self._session_pool = [session.id for session in page.items]

    def _pick(self, rng: random.Random) -> str:
        """Pick the next operation."""
        if (
            self._write_rate > 0
            and self._agent_id is not None
            and rng.random() < self._write_rate
        ):
            return "session_create"
        ops = ["sessions_list", "agents_list", "evaluations_list"]
        if self._session_pool:
            ops += ["session_nodes", "session_nodes"]
        return rng.choice(ops)

    async def _execute(self, op: str, rng: random.Random) -> None:
        """Execute one operation."""
        if op == "sessions_list":
            await self._client.sessions.list(SessionListParams(size=50))
        elif op == "agents_list":
            await self._client.agents.list(AgentListParams())
        elif op == "evaluations_list":
            await self._client.evaluations.list(EvaluationListParams())
        elif op == "session_nodes":
            await self._client.sessions.get_with_nodes(rng.choice(self._session_pool))
        elif op == "session_create":
            assert self._agent_id is not None
            self._write_counter += 1
            inputs = build_session_inputs(
                self._config, WRITE_INPUT_OFFSET + self._write_counter
            )
            await record_session(
                self._client,
                agent_id=self._agent_id,
                inputs=inputs,
                origin=SessionOrigin.RECORDED,
                resolver=passthrough_tool,
                model=DEFAULT_MODEL,
                model_params=None,
                session_name=None,
                latency_scale=0.0,
            )

    async def run_until(self, deadline: float, seed: int) -> None:
        """Issue operations until the deadline."""
        rng = random.Random(seed)
        while time.monotonic() < deadline:
            op = self._pick(rng)
            started = time.monotonic()
            try:
                await self._execute(op, rng)
                outcome = "ok"
            except APIError as exc:
                outcome = f"http_{exc.status_code}"
            except Exception as exc:
                outcome = f"exc_{type(exc).__name__}"
            elapsed_ms = (time.monotonic() - started) * 1000
            self.stats.setdefault(op, OpStats()).record(elapsed_ms, outcome)


def _print_report(stats: dict[str, OpStats], duration: float) -> None:
    """Print the per-operation latency and outcome table."""
    header = (
        f"{'op':<18} {'count':>7} {'errors':>7} {'rps':>7} "
        f"{'p50ms':>8} {'p90ms':>8} {'p99ms':>8} {'maxms':>8}"
    )
    print(header)
    print("-" * len(header))
    for op, op_stats in sorted(stats.items()):
        latencies = op_stats.latencies_ms
        count = len(latencies)
        errors = count - op_stats.outcomes.get("ok", 0)
        print(
            f"{op:<18} {count:>7} {errors:>7} {count / duration:>7.1f} "
            f"{percentile(latencies, 0.5):>8.1f} {percentile(latencies, 0.9):>8.1f} "
            f"{percentile(latencies, 0.99):>8.1f} {max(latencies, default=0):>8.1f}"
        )
    error_outcomes: Counter[str] = Counter()
    for op_stats in stats.values():
        for outcome, count in op_stats.outcomes.items():
            if outcome != "ok":
                error_outcomes[outcome] += count
    if error_outcomes:
        print(f"\nError outcomes: {dict(error_outcomes)}")


async def _run_load(args: argparse.Namespace) -> None:
    """Run the load test from parsed CLI flags."""
    if args.base_url is not None:
        os.environ["KITARU_API_URL"] = args.base_url
    config = SimulationConfig(seed=args.seed, min_turns=1, max_turns=1)
    async with KitaruAPIClient() as client:
        agent_id: uuid.UUID | None = None
        if args.write_rate > 0:
            page = await client.agents.list(AgentListParams())
            for agent in page.items:
                if agent.name == args.agent_name:
                    agent_id = agent.id
                    break
            if agent_id is None:
                raise RuntimeError(f"Agent {args.agent_name!r} not found")
        driver = _LoadDriver(client, agent_id, args.write_rate, config)
        await driver.prime()
        print(
            f"Running {args.concurrency} loops for {args.duration:.0f}s against "
            f"{os.environ.get('KITARU_API_URL', 'the configured server')} "
            f"(write rate {args.write_rate}) ..."
        )
        deadline = time.monotonic() + args.duration
        await asyncio.gather(
            *(
                driver.run_until(deadline, seed=index)
                for index in range(args.concurrency)
            )
        )
    _print_report(driver.stats, args.duration)


def main() -> int:
    """Run the load CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=30.0, help="Seconds to run.")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument(
        "--base-url", default=None, help="Server URL, KITARU_API_URL when omitted."
    )
    parser.add_argument(
        "--write-rate",
        type=float,
        default=0.0,
        help="Fraction of operations that record a new session.",
    )
    parser.add_argument(
        "--agent-name",
        default="dummy-agent",
        help="Agent recorded sessions attach to when --write-rate is set.",
    )
    parser.add_argument("--seed", default="kitaru-load")
    asyncio.run(_run_load(parser.parse_args()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
