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
"""Seed realistic structured Insight reports covering H1, R1, R2, and all 17 artifacts.

Targets a running server via ``--base-url`` or boots a disposable local Postgres
stack (keep it alive for UI development with ``--keep``).
"""

import argparse
import asyncio
import os
import uuid
from datetime import timedelta

from insight_showcase_artifacts import build_all_17_artifacts_report
from insight_showcase_data import (
    BASE_TIME,
    SeededSession,
    build_grid_card_reports,
    build_meridian_report,
)
from stack import (
    RUN_DIR,
    bootstrap_api_key,
    create_database,
    ensure_postgres,
    get_free_port,
    start_server,
    wait_for_health,
)

from kitaru.api_models.v1.agent import AgentCreateRequest, AgentListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.insight import InsightUpdateRequest
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionIngestRequest,
    SessionNodeCreateRequest,
)
from kitaru.client.api_client import KitaruAPIClient


async def _get_or_create_agent(client: KitaruAPIClient, name: str) -> uuid.UUID:
    """Return an existing agent's id by exact name, or create it."""
    page = await client.agents.list(
        AgentListParams(
            filter=FilterCondition(field="name", op=FilterOp.EQ, value=name),
            size=1,
        )
    )
    if page.items:
        return page.items[0].id
    created = await client.agents.create(
        AgentCreateRequest(name=name, description=f"Insights showcase agent: {name}")
    )
    return created.id


async def _seed_sessions(
    client: KitaruAPIClient,
    agent_id: uuid.UUID,
    *,
    provider: str,
    specs: list[tuple[str, SessionStatus, list[tuple[str, NodeStatus]]]],
) -> list[SeededSession]:
    """Ingest realistic sessions with tool nodes so every evidence link resolves."""
    seeded: list[SeededSession] = []
    for index, (label, status, steps) in enumerate(specs):
        started = BASE_TIME - timedelta(hours=len(specs) - index)
        nodes = [
            SessionNodeCreateRequest(
                external_id=f"{label}-step-{step_idx + 1}",
                node_type=NodeType.TOOL_CALL,
                name=tool_name,
                tool_name=tool_name,
                status=node_status,
                started_at=started + timedelta(seconds=step_idx * 2),
                ended_at=started + timedelta(seconds=step_idx * 2 + 1),
                inputs={"order_id": f"ORD-{4200 + index}", "step": step_idx + 1},
                outputs={"ok": node_status == NodeStatus.COMPLETED},
                attributes={},
            )
            for step_idx, (tool_name, node_status) in enumerate(steps)
        ]
        response = await client.sessions.ingest(
            SessionIngestRequest(
                session=SessionCreateRequest(
                    agent_id=agent_id,
                    origin=SessionOrigin.IMPORTED,
                    imported_from=provider.lower(),
                    external_id=f"showcase-{label}-{uuid.uuid4().hex[:8]}",
                    status=status,
                    started_at=started,
                    ended_at=started + timedelta(seconds=len(steps) * 2 + 1),
                    inputs={"message": f"Showcase customer request ({label})"},
                    outputs={"status": status.value},
                    metadata={},
                ),
                nodes=nodes,
            )
        )
        full = await client.sessions.get_with_nodes(response.id)
        last_node_id = full.nodes[-1].id if full.nodes else None
        seeded.append(
            SeededSession(session_id=response.id, label=label, node_id=last_node_id)
        )
    return seeded


async def _run(args: argparse.Namespace) -> int:
    """Seed realistic agents, sessions, and structured insight reports."""
    server_proc = None
    db_name = args.db_name or f"kitaru_insights_{uuid.uuid4().hex[:8]}"

    if args.base_url is None:
        await ensure_postgres()
        await create_database(db_name, drop_existing=True)
        port = args.port or get_free_port()
        base_url = f"http://127.0.0.1:{port}"
        log_path = RUN_DIR / "seed-insights-server.log"
        print(f"Starting server at {base_url} (db {db_name}, log {log_path}) ...")
        server_proc = start_server(db_name, port, log_path)
        await wait_for_health(base_url, server_proc, log_path)
        os.environ["KITARU_API_URL"] = base_url
        api_key = await bootstrap_api_key(base_url)
        if api_key is not None:
            os.environ["KITARU_API_KEY"] = api_key
    else:
        base_url = args.base_url.rstrip("/")
        os.environ["KITARU_API_URL"] = base_url

    try:
        async with KitaruAPIClient() as client:
            agent_ids: dict[str, uuid.UUID] = {}
            sessions_by_agent: dict[str, list[SeededSession]] = {}

            meridian_id = await _get_or_create_agent(client, "meridian-support")
            agent_ids["meridian-support"] = meridian_id
            meridian_sessions = await _seed_sessions(
                client,
                meridian_id,
                provider="Langfuse",
                specs=[
                    (
                        "ses_4f2a91",
                        SessionStatus.FAILED,
                        [
                            ("lookup_order", NodeStatus.COMPLETED),
                            ("edit_order", NodeStatus.COMPLETED),
                            ("issue_refund", NodeStatus.FAILED),
                        ],
                    ),
                    (
                        "ses_7b83e2",
                        SessionStatus.FAILED,
                        [
                            ("lookup_order", NodeStatus.COMPLETED),
                            ("edit_order", NodeStatus.COMPLETED),
                            ("issue_refund", NodeStatus.FAILED),
                        ],
                    ),
                    (
                        "ses_c918d4",
                        SessionStatus.COMPLETED,
                        [
                            ("lookup_order", NodeStatus.COMPLETED),
                            ("edit_order", NodeStatus.COMPLETED),
                            ("issue_refund", NodeStatus.COMPLETED),
                        ],
                    ),
                    (
                        "ses_2a77a8",
                        SessionStatus.COMPLETED,
                        [
                            ("lookup_order", NodeStatus.COMPLETED),
                            ("edit_order", NodeStatus.COMPLETED),
                            ("check_refund_policy", NodeStatus.COMPLETED),
                        ],
                    ),
                ],
            )
            sessions_by_agent["meridian-support"] = meridian_sessions

            other_agents = [
                ("harbor-returns", "LangSmith", "lookup_order"),
                ("atlas-research", "Braintrust", "fetch_page"),
                ("relay-triage", "Langfuse", "route_ticket"),
                ("quill-writer", "LangSmith", "expand_draft"),
                ("cedar-sales", "Langfuse", "quote_pricing"),
                ("beacon-onboarding", "Braintrust", "finish_setup"),
                ("orbit-scheduler", "Langfuse", "convert_timezone"),
            ]
            for name, provider, tool_name in other_agents:
                aid = await _get_or_create_agent(client, name)
                agent_ids[name] = aid
                sessions_by_agent[name] = await _seed_sessions(
                    client,
                    aid,
                    provider=provider,
                    specs=[
                        (
                            f"{name.split('-')[0]}_01",
                            SessionStatus.COMPLETED
                            if name == "beacon-onboarding"
                            else SessionStatus.FAILED,
                            [(tool_name, NodeStatus.COMPLETED)],
                        )
                    ],
                )

            created_rows: list[tuple[str, uuid.UUID, str, str]] = []

            meridian_inputs = [build_meridian_report(meridian_sessions)]
            if not args.skip_kitchen_sink:
                meridian_inputs.append(build_all_17_artifacts_report(meridian_sessions))
            meridian_reports = await client.insights.create(
                meridian_id, meridian_inputs
            )
            for report in meridian_reports:
                created_rows.append(
                    ("meridian-support", report.id, report.status, report.title)
                )

            for agent_name, report_input, target_status in build_grid_card_reports(
                sessions_by_agent
            ):
                (created,) = await client.insights.create(
                    agent_ids[agent_name], [report_input]
                )
                if target_status != created.status:
                    created = await client.insights.update(
                        created.id, InsightUpdateRequest(status=target_status)
                    )
                created_rows.append(
                    (agent_name, created.id, created.status, created.title)
                )

            print(
                f"\nSeeded {len(created_rows)} structured insight reports across "
                f"{len(agent_ids)} agents at {base_url}:\n"
            )
            for agent_name, report_id, status, title in created_rows:
                print(f"  [{status:8}] {agent_name:18} {report_id}  {title}")
            return 0
    finally:
        if server_proc is not None:
            if args.keep:
                print(
                    f"\nKeeping server at {base_url} (pid {server_proc.pid}, "
                    f"db {db_name})."
                )
                print(f"export KITARU_API_URL={base_url}")
            else:
                server_proc.terminate()


def main() -> int:
    """Parse CLI arguments and run the insights showcase seeder."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=None,
        help="Seed an already-running Kitaru server instead of starting a local one.",
    )
    parser.add_argument(
        "--db-name",
        default=None,
        help="Local Postgres database name when starting a server.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Local server port when starting a server (defaults to a free port).",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Leave the started local server running for UI development.",
    )
    parser.add_argument(
        "--skip-kitchen-sink",
        action="store_true",
        help="Seed only the 8 realistic H1/R1/R2 reports without the all-17 showcase.",
    )
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
